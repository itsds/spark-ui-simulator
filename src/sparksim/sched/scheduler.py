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

* **Executor loss.** `disable_executor` stops offers to an executor (pending
  removal or pending loss reason, `isExecutorActive`); `remove_executor`
  fails its running tasks (`on_fail`), puts them back in the pending lists
  (`addPendingTask`) and counts the failure towards `spark.task.maxFailures`
  when the exit was caused by the app. The next attempt of a task gets the
  next attempt number, as `TaskSetManager.taskAttempts` does.

* **Speculation** (`spark.speculation`, off by default). Ported from Spark 3.5.9 `TaskSchedulerImpl` and
  `TaskSetManager`, checked against a real run on YARN (`reference/event-logs/ref09-speculation`): every
  `spark.speculation.interval` (100 ms) each task set with at least quantile (0.75) x tasks succeeded
  computes threshold = max(multiplier (1.5) x the median successful duration (`PercentileHeap`),
  minTaskRuntime (100 ms)) and marks running tasks with one copy that ran longer as speculatable (the 3.4+
  efficiency rule never vetoes it: heartbeats don't carry a running task's run time, so its process rate
  stays 0). Marked tasks go to their own pending lists; an offer takes a regular task first, then a
  speculatable one on a host where the task has never run, and the round revives offers. The first
  attempt to succeed wins; the others are killed ("another attempt succeeded"): the kill reaches the
  executor, the task thread notices it, and the core is free only when the executor reports the task
  KILLED. A ResultStage's job end kills what is still running with "Stage finished" (`kill_all`), which
  overrides an earlier reason the executor hasn't acted on yet (ref09). An attempt whose kill arrives
  after it finished ends as "Finish but did not commit due to another attempt succeeded".

The scheduler knows nothing about costs: `on_launch` returns a task's
duration, `on_finish` is called when it ends, `on_fail` when it is lost and `on_kill` when a killed
attempt ends; `kill_latency` says how long an executor takes to act on a kill.
"""

from __future__ import annotations

import bisect
import random
from collections.abc import Callable
from dataclasses import dataclass, field

from ..cost import calibration as cal
from ..eventlog.ordering import ScalaHashSet
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
    lost: bool = False  # its executor was lost before it finished
    speculative: bool = False  # TaskInfo.speculative: launched from the speculatable lists
    killed_by_other: bool = False  # TaskSetManager.killedByOtherAttempt
    kill_reqs: list[tuple[int, str]] = field(default_factory=list)  # (time the driver asked, reason)
    kill_at: int | None = None  # when the executor acts on the first kill (KILLED reaches the driver)
    kill_reason: str | None = None  # TaskKilled reason it ended with
    ended: bool = False


@dataclass(frozen=True)
class SpeculationConf:
    """`spark.speculation.*` (Spark 3.5.9 defaults)."""

    enabled: bool = False
    interval_ms: int = 100
    multiplier: float = 1.5
    quantile: float = 0.75
    min_task_runtime_ms: int = 100
    task_duration_threshold_ms: int | None = None
    efficiency: bool = True
    process_rate_multiplier: float = 0.75
    long_run_task_factor: float = 2.0

    @staticmethod
    def from_conf(conf: dict[str, str]) -> SpeculationConf:
        def ms(key: str, default: int) -> int:
            v = conf.get(key)
            if v is None:
                return default
            v = v.strip().lower()
            for suffix, f in (("ms", 1), ("s", 1000), ("min", 60_000), ("m", 60_000)):
                if v.endswith(suffix) and v[: -len(suffix)].replace(".", "", 1).isdigit():
                    return int(float(v[: -len(suffix)]) * f)
            return int(v)  # a bare number is milliseconds (timeConf(TimeUnit.MILLISECONDS))

        def flag(key: str, default: bool) -> bool:
            return conf.get(key, str(default)).strip().lower() == "true"

        thr = conf.get("spark.speculation.task.duration.threshold")
        return SpeculationConf(
            enabled=flag("spark.speculation", False),
            interval_ms=ms("spark.speculation.interval", 100),
            multiplier=float(conf.get("spark.speculation.multiplier", 1.5)),
            quantile=float(conf.get("spark.speculation.quantile", 0.75)),
            min_task_runtime_ms=ms("spark.speculation.minTaskRuntime", 100),
            task_duration_threshold_ms=ms("spark.speculation.task.duration.threshold", 0) if thr else None,
            efficiency=flag("spark.speculation.efficiency.enabled", True),
            process_rate_multiplier=float(conf.get("spark.speculation.efficiency.processRateMultiplier", 0.75)),
            long_run_task_factor=float(conf.get("spark.speculation.efficiency.longRunTaskFactor", 2.0)),
        )


@dataclass
class TaskSet:
    """One stage attempt's tasks (a TaskSetManager)."""

    stage_id: int
    attempt: int
    priority: int  # job id (FIFO)
    preferred: list[tuple[str, ...]]  # per task index: preferred hosts
    on_launch: Callable[[Launch], int]  # returns the task's duration in ms
    on_finish: Callable[[Launch], None]
    on_fail: Callable[[Launch], None] | None = None
    on_kill: Callable[[Launch], None] | None = None
    on_speculatable: Callable[[int, Launch, float, int], None] | None = None  # (index, running attempt, threshold, n)
    records_of: Callable[[Launch], tuple[int, int]] | None = None  # (records read, run ms) of a finished task
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
    attempts: list[int] = field(default_factory=list)  # attempts launched so far, per index
    failures: list[int] = field(default_factory=list)  # failures counted towards spark.task.maxFailures
    # --- speculation (TaskSetManager)
    copies_running: list[int] = field(default_factory=list)
    attempt_hosts: list[set[str]] = field(default_factory=list)  # hasAttemptOnHost
    running_attempts: list[list[Launch]] = field(default_factory=list)  # per index
    running_set: ScalaHashSet = field(default_factory=ScalaHashSet)  # runningTasksSet (task ids)
    running_by_tid: dict[int, Launch] = field(default_factory=dict)
    speculatable: set[int] = field(default_factory=set)  # speculatableTasks
    spec_for_host: dict[str, list[int]] = field(default_factory=dict)  # pendingSpeculatableTasks
    spec_for_rack: dict[str, list[int]] = field(default_factory=dict)
    spec_no_prefs: list[int] = field(default_factory=list)
    spec_all: list[int] = field(default_factory=list)
    durations: list[int] = field(default_factory=list)  # successfulTaskDurations, kept sorted
    rate_records: int = 0  # TaskProcessRateCalculator: records read / run time of successful tasks
    rate_runtime: int = 0
    zombie: bool = False
    submitted_seq: int = 0

    @property
    def num_tasks(self) -> int:
        return len(self.preferred)

    @property
    def has_pending(self) -> bool:
        return self.num_launched < len(self.preferred) or bool(self.speculatable)

    @property
    def num_pending(self) -> int:
        return len(self.preferred) - self.num_launched

    @property
    def num_running(self) -> int:
        return self.num_launched - self.num_done


class TaskSetAborted(RuntimeError):
    """A task failed spark.task.maxFailures times (job abort is not modelled yet)."""


class TaskScheduler:
    def __init__(self, sim: Sim, rng: random.Random, rack_of: Callable[[str], str],
                 locality_wait: int = cal.LOCALITY_WAIT_MS, speculation: SpeculationConf | None = None,
                 kill_latency: Callable[[Launch, bool], int] | None = None):
        self.sim = sim
        self.rng = rng
        self.rack_of = rack_of
        self.locality_wait = locality_wait
        self.spec = speculation or SpeculationConf()
        self.kill_latency = kill_latency or (lambda a, interrupt: 20)
        self._spec_tick: int | None = None
        self._spec_phase = 0
        self.speculation_log: list[tuple[int, int, int, int, str, float, int]] = []
        self.executors: dict[str, Executor] = {}
        self.free: dict[str, int] = {}
        self.order: list[str] = []  # registration order (offers are shuffled anyway)
        self.disabled: set[str] = set()  # pending removal / pending loss reason: no offers
        self.running: dict[str, dict[int, Launch]] = {}  # executor -> task id -> running attempt
        self.max_failures = 4  # spark.task.maxFailures
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
        self.running[ex.id] = {}
        for ts in self.tasksets:
            self._recompute_levels(ts)
        if self.started:
            self._offer(list(self.order), all_free=True)

    def disable_executor(self, eid: str) -> None:
        """No more offers for this executor (CoarseGrainedSchedulerBackend.isExecutorActive is false)."""
        self.disabled.add(eid)

    def remove_executor(self, eid: str, counts_as_failure: bool) -> list[Launch]:
        """`TaskSchedulerImpl.executorLost`: fail the executor's running tasks and re-queue them.

        Returns the lost attempts (their `on_fail` has been called). Shuffle outputs on the executor stay
        available: the engine only allows executor loss with the external shuffle service.
        """
        lost = sorted(self.running.pop(eid, {}).values(), key=lambda a: a.task_id)
        self.executors.pop(eid, None)
        self.free.pop(eid, None)
        self.disabled.discard(eid)
        if eid in self.order:
            self.order.remove(eid)
        for a in lost:
            ts = a.taskset
            a.lost = a.ended = True
            a.finish_time = self.sim.now
            self._stop_running(ts, a)
            if ts.on_fail is not None:
                ts.on_fail(a)
            if ts.succeeded[a.index]:  # a copy already succeeded (or it was being killed): nothing to re-run
                continue
            if counts_as_failure:
                ts.failures[a.index] += 1
                if ts.failures[a.index] >= self.max_failures:
                    raise TaskSetAborted(f"Task {a.index} in stage {ts.stage_id}.{ts.attempt} failed "
                                         f"{self.max_failures} times")
            if ts.copies_running[a.index] == 0:
                ts.launched[a.index] = False
                ts.num_launched -= 1
            self._add_pending(ts, a.index)
        for ts in self.tasksets:
            self._recompute_levels(ts)
        if lost and self.started:
            self._offer(list(self.order), all_free=True)
        return lost

    @property
    def num_pending_tasks(self) -> int:
        return sum(ts.num_pending for ts in self.tasksets)

    @property
    def num_running_tasks(self) -> int:
        return sum(len(r) for r in self.running.values())

    def start_speculation_thread(self) -> None:
        """`TaskSchedulerImpl.start`: the speculation check runs every interval from now on (fixed delay)."""
        if self.spec.enabled:
            self._spec_phase = self.sim.now % self.spec.interval_ms

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
        ts.attempts = [0] * n
        ts.failures = [0] * n
        ts.copies_running = [0] * n
        ts.attempt_hosts = [set() for _ in range(n)]
        ts.running_attempts = [[] for _ in range(n)]
        ts.done = ts.done or self.sim.signal()
        for i in reversed(range(n)):  # addPendingTasks: reversed so dequeue() pops index 0 first
            self._add_pending(ts, i)
        self._recompute_levels(ts)
        ts.cur = 0
        ts.last_reset = self.sim.now
        ts.submitted_seq = self._submitted = getattr(self, "_submitted", 0) + 1
        self.tasksets.append(ts)
        self.tasksets.sort(key=lambda t: (t.priority, t.stage_id))
        if self.started:  # submitTasks → reviveOffers: the round runs a few ms later (task binary broadcast)
            self.sim.after(self.rng.randint(*cal.STAGE_LAUNCH_DELAY_MS), self._offer, list(self.order), True)
        self._ensure_speculation_tick()

    def _add_pending(self, ts: TaskSet, i: int, speculatable: bool = False) -> None:
        """`TaskSetManager.addPendingTask` (to `pendingSpeculatableTasks` when `speculatable`)."""
        prefs = ts.preferred[i]
        for_host, for_rack = (ts.spec_for_host, ts.spec_for_rack) if speculatable else (ts.for_host, ts.for_rack)
        for h in prefs:
            for_host.setdefault(h, []).append(i)
            for_rack.setdefault(self.rack_of(h), []).append(i)
        if not prefs:
            (ts.spec_no_prefs if speculatable else ts.no_prefs).append(i)
        (ts.spec_all if speculatable else ts.all_pending).append(i)

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

    @staticmethod
    def _dequeue_speculative(ts: TaskSet, lst: list[int] | None, host: str) -> int | None:
        """`dequeueTaskFromList(speculative = true)`: never on a host the task already ran on (such entries
        stay in the list); at most one running copy."""
        if not lst:
            return None
        k = len(lst)
        while k > 0:
            k -= 1
            i = lst[k]
            if host in ts.attempt_hosts[i]:
                continue
            del lst[k]
            if not ts.succeeded[i] and ts.copies_running[i] <= 1:
                ts.speculatable.discard(i)
                return i
        return None

    def _dequeue_task(self, ts: TaskSet, host: str, max_loc: int) -> tuple[int, int, bool] | None:
        """`TaskSetManager.dequeueTask`: a regular task first, else a speculatable one (no PROCESS_LOCAL
        lists: nothing is cached)."""
        got = self._dequeue_helper(ts, host, max_loc, False)
        if got is None and ts.speculatable:
            got = self._dequeue_helper(ts, host, max_loc, True)
        return got

    def _dequeue_helper(self, ts: TaskSet, host: str, max_loc: int, spec: bool) -> tuple[int, int, bool] | None:
        """`TaskSetManager.dequeueTaskHelper`."""
        if spec:
            def take(lst):
                return self._dequeue_speculative(ts, lst, host)
            for_host, no_prefs, for_rack, everything = ts.spec_for_host, ts.spec_no_prefs, ts.spec_for_rack, ts.spec_all
        else:
            def take(lst):
                return self._dequeue(ts, lst)
            for_host, no_prefs, for_rack, everything = ts.for_host, ts.no_prefs, ts.for_rack, ts.all_pending
        if max_loc >= NODE_LOCAL:
            i = take(for_host.get(host))
            if i is not None:
                return i, NODE_LOCAL, spec
        if max_loc >= NO_PREF:
            i = take(no_prefs)
            if i is not None:
                return i, PROCESS_LOCAL, spec  # TaskSetManager reports noPref tasks as PROCESS_LOCAL
        if max_loc >= RACK_LOCAL:
            i = take(for_rack.get(self.rack_of(host)))
            if i is not None:
                return i, RACK_LOCAL, spec
        if max_loc >= ANY:
            i = take(everything)
            if i is not None:
                return i, ANY, spec
        return None

    def _resource_offer(self, ts: TaskSet, eid: str, max_loc: int,
                        now: int) -> tuple[tuple[int, int, bool] | None, bool]:
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
        offers = [e for e in executor_ids if self.free.get(e, 0) > 0 and e not in self.disabled]
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
                        idx, loc, spec = got
                        avail[eid] -= 1
                        self._launch(ts, idx, eid, loc, now + launched_this_round // 4, spec)
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

    def _launch(self, ts: TaskSet, idx: int, eid: str, loc: int, t: int, speculative: bool = False) -> None:
        ex = self.executors[eid]
        if not ts.launched[idx]:
            ts.launched[idx] = True
            ts.num_launched += 1
        self.free[eid] -= 1
        attempt = Launch(ts, idx, ex, loc, t, attempt=ts.attempts[idx], task_id=self.next_task_id,
                         speculative=speculative)
        ts.attempts[idx] += 1
        self.next_task_id += 1
        ts.copies_running[idx] += 1
        ts.attempt_hosts[idx].add(ex.host.name)
        ts.running_attempts[idx].append(attempt)
        ts.running_set.add(attempt.task_id)
        ts.running_by_tid[attempt.task_id] = attempt
        duration = ts.on_launch(attempt)
        attempt.finish_time = t + max(1, int(duration))
        ts.launches.append(attempt)
        self.launch_log.append(attempt)
        self.running[eid][attempt.task_id] = attempt
        self.sim.at(attempt.finish_time, self._finish, attempt)

    @staticmethod
    def _stop_running(ts: TaskSet, a: Launch) -> None:
        ts.copies_running[a.index] -= 1
        ts.running_attempts[a.index].remove(a)
        ts.running_set.remove(a.task_id)
        del ts.running_by_tid[a.task_id]

    def _finish(self, a: Launch) -> None:
        """The attempt ran to its end (`handleSuccessfulTask`)."""
        if a.ended:
            return
        ts = a.taskset
        if a.killed_by_other:  # its copy succeeded first; the kill came too late to stop it
            a.kill_reason = "Finish but did not commit due to another attempt succeeded"
            self._end_killed(a)
            return
        # Every attempt still running when its task succeeded was marked killed_by_other, and no copy starts
        # after that (dequeue skips successful tasks): a second success can't happen here.
        assert not ts.succeeded[a.index], f"task {a.index} of stage {ts.stage_id} succeeded twice"
        a.ended = True
        eid = a.executor.id
        del self.running[eid][a.task_id]
        self._stop_running(ts, a)
        ts.succeeded[a.index] = True
        ts.num_done += 1
        if self.spec.enabled:
            bisect.insort(ts.durations, a.finish_time - a.launch_time)
            if ts.records_of is not None:
                records, run_ms = ts.records_of(a)
                ts.rate_records += records
                ts.rate_runtime += run_ms
        for other in list(ts.running_attempts[a.index]):  # kill the other attempts of this task
            other.killed_by_other = True
            self.kill(other, "another attempt succeeded", interrupt=True)
        ts.on_finish(a)
        self.free[eid] += 1
        if ts.num_done == ts.num_tasks:
            ts.zombie = True
            self.tasksets.remove(ts)
            ts.done.fire()
        gap = self.rng.randint(*cal.RELAUNCH_GAP_MS)
        self.sim.after(gap, self._offer, [eid], False)

    # ------------------------------------------------------------------ killing attempts

    def kill(self, a: Launch, reason: str, interrupt: bool) -> None:
        """`SchedulerBackend.killTask`: the executor acts on it after `kill_latency`; a second kill before that
        only replaces the reason (`TaskRunner.kill` overwrites `reasonIfKilled`)."""
        if a.ended:
            return
        now = self.sim.now
        a.kill_reqs.append((now, reason))
        a.kill_reason = reason
        if a.kill_at is None:
            a.kill_at = now + max(1, int(self.kill_latency(a, interrupt)))
            if a.kill_at < a.finish_time:
                self.sim.at(a.kill_at, self._killed, a)

    def kill_all(self, ts: TaskSet, reason: str) -> None:
        """`killAllTaskAttempts(stageId, interruptThread = false, reason)`."""
        for a in sorted(ts.running_by_tid.values(), key=lambda a: a.task_id):
            self.kill(a, reason, interrupt=False)

    def _killed(self, a: Launch) -> None:
        if a.ended:
            return
        self._end_killed(a)

    def _end_killed(self, a: Launch) -> None:
        """The executor reported the attempt KILLED (`handleFailedTask` with TaskKilled): its core is free."""
        ts = a.taskset
        a.ended = True
        a.finish_time = self.sim.now
        eid = a.executor.id
        self.running.get(eid, {}).pop(a.task_id, None)
        self._stop_running(ts, a)
        if ts.on_kill is not None:
            ts.on_kill(a)
        if eid in self.free:
            self.free[eid] += 1
            gap = self.rng.randint(*cal.RELAUNCH_GAP_MS)
            self.sim.after(gap, self._offer, [eid], False)

    # ------------------------------------------------------------------ speculation

    def _ensure_speculation_tick(self) -> None:
        if not self.spec.enabled or self._spec_tick is not None or not self.tasksets:
            return
        iv = self.spec.interval_ms
        now = self.sim.now
        t = now + (self._spec_phase - now) % iv
        if t <= now:
            t += iv
        self._spec_tick = t
        self.sim.at(t, self._speculation_tick)

    def _speculation_tick(self) -> None:
        """`TaskSchedulerImpl.checkSpeculatableTasks`: every task set in submission order; revive offers if
        any task became speculatable."""
        self._spec_tick = None
        found = False
        for ts in sorted(self.tasksets, key=lambda t: t.submitted_seq):
            found |= self._check_speculatable(ts)
        if found and self.started:
            self._offer(list(self.order), all_free=True)
        self._ensure_speculation_tick()

    def _check_speculatable(self, ts: TaskSet) -> bool:
        """`TaskSetManager.checkSpeculatableTasks(minTimeToSpeculation)`."""
        c = self.spec
        n = ts.num_tasks
        threshold_set = c.task_duration_threshold_ms is not None and c.task_duration_threshold_ms > 0
        if ts.zombie or (n == 1 and not threshold_set):
            return False
        min_finished = max(int(c.quantile * n), 1)
        now = self.sim.now
        if len(ts.durations) >= min_finished:
            median = ts.durations[int(len(ts.durations) * 0.5)]  # PercentileHeap(0.5).percentile
            threshold = max(c.multiplier * median, c.min_task_runtime_ms)
            custom = False
        elif threshold_set and n <= self._max_slots_per_executor():
            threshold, custom = float(c.task_duration_threshold_ms), True
        else:
            return False
        found = False
        for tid in list(ts.running_set):
            a = ts.running_by_tid[tid]
            i = a.index
            if ts.succeeded[i] or ts.copies_running[i] != 1 or i in ts.speculatable:
                continue
            runtime = now - a.launch_time
            if runtime > threshold and (custom or not c.efficiency or self._inefficient(ts, runtime, threshold)):
                self._add_pending(ts, i, speculatable=True)
                ts.speculatable.add(i)
                self.speculation_log.append((now, ts.stage_id, ts.attempt, i, a.executor.host.name, threshold,
                                             len(ts.speculatable)))
                if ts.on_speculatable is not None:
                    ts.on_speculatable(i, a, threshold, len(ts.speculatable))
                found = True
        return found

    def _inefficient(self, ts: TaskSet, runtime: int, threshold: float) -> bool:
        """The 3.4+ efficiency rule. A running task's process rate comes from its heartbeats' records read and
        executor run time, but executors only set the run time when a task ends, so the driver never stores a
        rate for a running task and reads 0.0: below any average, the task counts as inefficient."""
        if runtime > self.spec.long_run_task_factor * threshold:
            return True
        avg = (ts.rate_records / (ts.rate_runtime / 1000.0)
               if ts.rate_records > 0 and ts.rate_runtime > 0 else float("inf"))
        running_rate = 0.0
        return running_rate < avg * self.spec.process_rate_multiplier

    def _max_slots_per_executor(self) -> int:
        return max((e.cores for e in self.executors.values()), default=1)

    def _ensure_revive(self) -> None:
        """The driver's revive timer fires every second; only schedule ticks while something waits."""
        if self._revive_at is not None:
            return
        if not any(ts.has_pending for ts in self.tasksets) or not any(
                v for e, v in self.free.items() if e not in self.disabled):
            return
        t = (self.sim.now // cal.REVIVE_INTERVAL_MS + 1) * cal.REVIVE_INTERVAL_MS
        self._revive_at = t
        self.sim.at(t, self._revive)

    def _revive(self) -> None:
        self._revive_at = None
        self._offer(list(self.order), all_free=True)
        self._ensure_revive()
