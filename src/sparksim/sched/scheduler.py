"""Task scheduling as Spark 3.5's `TaskSchedulerImpl` + `TaskSetManager` do it on a static YARN cluster.

* **Slots.** One task per executor core (`spark.task.cpus = 1`).
* **Offers.** A finished task frees its core and the driver offers that one
  executor (`makeOffers(executorId)`); stage submission, executor registration
  and the 1 s revive timer offer every executor (`makeOffers()`). Offers are
  shuffled each round, and every pass gives each offer at most one task, so
  the first wave spreads over executors round robin.
* **FIFO.** Task sets are offered in (job id, stage id) order.
* **Delay scheduling.** Ported from `TaskSetManager`: pending lists per host,
  rack, "no preference" and all; valid locality levels computed from live
  executors; `getAllowedLocalityLevel` with `spark.locality.wait = 3s`; and the
  Spark 3.1+ timer rule (SPARK-18886): the wait timer resets only after an
  offer round that rejected nothing for locality, over all free resources or
  with no reject since the last reset. Tasks without preferred locations are
  launched as PROCESS_LOCAL, exactly like Spark (`dequeueTaskHelper`).

The scheduler knows nothing about costs: `on_launch` returns a task's
duration, and `on_finish` is called when it ends.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field

from ..cost import calibration as cal
from ..model.cluster import Executor
from .sim import Signal, Sim

PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY = range(5)
LOCALITY_NAMES = ["PROCESS_LOCAL", "NODE_LOCAL", "NO_PREF", "RACK_LOCAL", "ANY"]


@dataclass
class Launch:
    taskset: TaskSet
    index: int
    executor: Executor
    locality: int  # TaskLocality value reported in TaskInfo
    launch_time: int
    attempt: int = 0
    task_id: int = -1
    finish_time: int = -1
    result: object = None


@dataclass
class TaskSet:
    """One stage attempt's tasks (a TaskSetManager)."""

    stage_id: int
    attempt: int
    priority: int  # job id (FIFO)
    preferred: list[tuple[str, ...]]  # per task index: preferred hosts
    on_launch: Callable[[Launch], int]  # returns the task's duration in ms
    on_finish: Callable[[Launch], None]
    done: Signal | None = None
    # --- TaskSetManager state
    launched: list[bool] = field(default_factory=list)
    succeeded: list[bool] = field(default_factory=list)
    num_launched: int = 0
    num_done: int = 0
    for_host: dict[str, list[int]] = field(default_factory=dict)
    for_rack: dict[str, list[int]] = field(default_factory=dict)
    no_prefs: list[int] = field(default_factory=list)
    all_pending: list[int] = field(default_factory=list)
    levels: list[int] = field(default_factory=list)
    cur: int = 0
    last_reset: int = 0
    no_rejects_since_reset: bool = True
    launches: list[Launch] = field(default_factory=list)

    @property
    def num_tasks(self) -> int:
        return len(self.preferred)

    @property
    def has_pending(self) -> bool:
        return self.num_launched < len(self.preferred)


class TaskScheduler:
    def __init__(self, sim: Sim, rng: random.Random, rack_of: Callable[[str], str],
                 locality_wait: int = cal.LOCALITY_WAIT_MS):
        self.sim = sim
        self.rng = rng
        self.rack_of = rack_of
        self.locality_wait = locality_wait
        self.executors: dict[str, Executor] = {}
        self.free: dict[str, int] = {}
        self.order: list[str] = []  # registration order (offers are shuffled anyway)
        self.tasksets: list[TaskSet] = []
        self.next_task_id = 0
        self.started = False
        self._revive_at: int | None = None
        self.launch_log: list[Launch] = []

    # ------------------------------------------------------------------ executors

    def add_executor(self, ex: Executor) -> None:
        self.executors[ex.id] = ex
        self.free[ex.id] = ex.cores
        self.order.append(ex.id)
        for ts in self.tasksets:
            self._recompute_levels(ts)
        if self.started:
            self._offer(list(self.order), all_free=True)

    def start(self) -> None:
        """Scheduling begins (YarnClusterScheduler.postStartHook returned)."""
        self.started = True
        self._offer(list(self.order), all_free=True)

    def _hosts_alive(self) -> set[str]:
        return {e.host.name for e in self.executors.values()}

    def _racks_alive(self) -> set[str]:
        return {e.host.rack for e in self.executors.values()}

    # ------------------------------------------------------------------ task sets

    def submit(self, ts: TaskSet) -> None:
        n = ts.num_tasks
        ts.launched = [False] * n
        ts.succeeded = [False] * n
        ts.done = ts.done or self.sim.signal()
        for i in reversed(range(n)):  # addPendingTasks: reversed so dequeue() pops index 0 first
            prefs = ts.preferred[i]
            for h in prefs:
                ts.for_host.setdefault(h, []).append(i)
                ts.for_rack.setdefault(self.rack_of(h), []).append(i)
            if not prefs:
                ts.no_prefs.append(i)
            ts.all_pending.append(i)
        self._recompute_levels(ts)
        ts.cur = 0
        ts.last_reset = self.sim.now
        self.tasksets.append(ts)
        self.tasksets.sort(key=lambda t: (t.priority, t.stage_id))
        if self.started:  # submitTasks → reviveOffers: the round runs a few ms later (task binary broadcast)
            self.sim.after(self.rng.randint(*cal.STAGE_LAUNCH_DELAY_MS), self._offer, list(self.order), True)

    def _recompute_levels(self, ts: TaskSet) -> None:
        prev = ts.levels[ts.cur] if ts.levels else None
        hosts, racks = self._hosts_alive(), self._racks_alive()
        levels = []
        if ts.for_host and any(h in hosts for h in ts.for_host):
            levels.append(NODE_LOCAL)
        if ts.no_prefs:
            levels.append(NO_PREF)
        if ts.for_rack and any(r in racks for r in ts.for_rack):
            levels.append(RACK_LOCAL)
        levels.append(ANY)
        ts.levels = levels
        if prev is not None:
            ts.cur = self._level_index(ts, prev)

    @staticmethod
    def _level_index(ts: TaskSet, locality: int) -> int:
        i = 0
        while i < len(ts.levels) - 1 and locality > ts.levels[i]:
            i += 1
        return i

    # ------------------------------------------------------------------ TaskSetManager

    def _needs(self, ts: TaskSet, lst: list[int]) -> bool:
        while lst:
            i = lst[-1]
            if not ts.launched[i]:
                return True
            lst.pop()
        return False

    def _more_in(self, ts: TaskSet, lists: dict[str, list[int]]) -> bool:
        has = False
        for k in list(lists):
            if self._needs(ts, lists[k]):
                has = True
            else:
                del lists[k]
        return has

    def _allowed_level(self, ts: TaskSet, now: int) -> int:
        while ts.cur < len(ts.levels) - 1:
            lvl = ts.levels[ts.cur]
            if lvl == NODE_LOCAL:
                more = self._more_in(ts, ts.for_host)
            elif lvl == NO_PREF:
                more = self._needs(ts, ts.no_prefs)
            elif lvl == RACK_LOCAL:
                more = self._more_in(ts, ts.for_rack)
            else:
                more = True
            wait = 0 if lvl == NO_PREF else self.locality_wait
            if not more:
                ts.last_reset = now
                ts.cur += 1
            elif now - ts.last_reset >= wait:
                ts.last_reset += wait
                ts.cur += 1
            else:
                return lvl
        return ts.levels[ts.cur]

    @staticmethod
    def _dequeue(ts: TaskSet, lst: list[int] | None) -> int | None:
        while lst:
            i = lst.pop()
            if not ts.launched[i]:
                return i
        return None

    def _dequeue_task(self, ts: TaskSet, host: str, max_loc: int) -> tuple[int, int] | None:
        """`TaskSetManager.dequeueTaskHelper` (no PROCESS_LOCAL lists: nothing is cached)."""
        if max_loc >= NODE_LOCAL:
            i = self._dequeue(ts, ts.for_host.get(host))
            if i is not None:
                return i, NODE_LOCAL
        if max_loc >= NO_PREF:
            i = self._dequeue(ts, ts.no_prefs)
            if i is not None:
                return i, PROCESS_LOCAL  # TaskSetManager reports noPref tasks as PROCESS_LOCAL
        if max_loc >= RACK_LOCAL:
            i = self._dequeue(ts, ts.for_rack.get(self.rack_of(host)))
            if i is not None:
                return i, RACK_LOCAL
        if max_loc >= ANY:
            i = self._dequeue(ts, ts.all_pending)
            if i is not None:
                return i, ANY
        return None

    def _resource_offer(self, ts: TaskSet, eid: str, max_loc: int, now: int) -> tuple[tuple[int, int] | None, bool]:
        allowed = max_loc
        if max_loc != NO_PREF:
            allowed = min(self._allowed_level(ts, now), max_loc)
        host = self.executors[eid].host.name
        got = self._dequeue_task(ts, host, allowed)
        reject = got is None and max_loc == ANY and ts.has_pending
        return got, reject

    # ------------------------------------------------------------------ offer rounds

    def _offer(self, executor_ids: list[str], all_free: bool) -> None:
        now = self.sim.now
        offers = [e for e in executor_ids if self.free.get(e, 0) > 0]
        if not offers:
            return
        self.rng.shuffle(offers)
        avail = {e: self.free[e] for e in offers}
        launched_this_round = 0
        for ts in list(self.tasksets):
            if not ts.has_pending:
                continue
            launched_any, no_rejects, best = False, True, None
            for max_loc in ts.levels:
                while True:
                    at_level = False
                    for eid in offers:
                        if avail[eid] < 1:
                            continue
                        got, reject = self._resource_offer(ts, eid, max_loc, now)
                        no_rejects &= not reject
                        if got is None:
                            continue
                        idx, loc = got
                        avail[eid] -= 1
                        self._launch(ts, idx, eid, loc, now + launched_this_round // 4)
                        launched_this_round += 1
                        at_level = True
                        best = loc if best is None else min(best, loc)
                    launched_any |= at_level
                    if not at_level:
                        break
            if no_rejects:
                if launched_any and (all_free or ts.no_rejects_since_reset):
                    ts.last_reset = now
                    if best is not None:
                        ts.cur = self._level_index(ts, best)
                    ts.no_rejects_since_reset = True
            else:
                ts.no_rejects_since_reset = False
        self._ensure_revive()

    def _launch(self, ts: TaskSet, idx: int, eid: str, loc: int, t: int) -> None:
        ex = self.executors[eid]
        ts.launched[idx] = True
        ts.num_launched += 1
        self.free[eid] -= 1
        attempt = Launch(ts, idx, ex, loc, t, task_id=self.next_task_id)
        self.next_task_id += 1
        duration = ts.on_launch(attempt)
        attempt.finish_time = t + max(1, int(duration))
        ts.launches.append(attempt)
        self.launch_log.append(attempt)
        self.sim.at(attempt.finish_time, self._finish, attempt)

    def _finish(self, a: Launch) -> None:
        ts = a.taskset
        ts.succeeded[a.index] = True
        ts.num_done += 1
        ts.on_finish(a)
        eid = a.executor.id
        self.free[eid] += 1
        if ts.num_done == ts.num_tasks:
            self.tasksets.remove(ts)
            ts.done.fire()
        gap = self.rng.randint(*cal.RELAUNCH_GAP_MS)
        self.sim.after(gap, self._offer, [eid], False)

    def _ensure_revive(self) -> None:
        """The driver's revive timer fires every second; only schedule ticks while something waits."""
        if self._revive_at is not None:
            return
        if not any(ts.has_pending for ts in self.tasksets) or not any(self.free.values()):
            return
        t = (self.sim.now // cal.REVIVE_INTERVAL_MS + 1) * cal.REVIVE_INTERVAL_MS
        self._revive_at = t
        self.sim.at(t, self._revive)

    def _revive(self) -> None:
        self._revive_at = None
        self._offer(list(self.order), all_free=True)
        self._ensure_revive()
