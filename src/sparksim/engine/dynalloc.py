"""Dynamic allocation: Spark 3.5.9's `ExecutorAllocationManager` + `ExecutorMonitor`, one resource profile.

* Every 100 ms (`schedule()`): executors idle longer than `spark.dynamicAllocation.executorIdleTimeout`
  (60 s) time out; the target is updated; timed-out executors are killed unless that would go below
  `minExecutors` or the target.
* Target: `maxNeeded = ceil((pending + running tasks) * executorAllocationRatio / tasksPerExecutor)`.
  Above it, the target drops at once (no kill, just fewer requests). Below it, once tasks have been
  backlogged for `schedulerBacklogTimeout` (1 s), the target grows by 1, 2, 4, 8... every
  `sustainedSchedulerBacklogTimeout` (1 s) while the backlog lasts, capped by `maxNeeded` and
  `maxExecutors`.
* While `initializing` (until the first stage is submitted or an executor times out) the target stays
  at the initial number of executors.
* With the external shuffle service an executor's shuffle files don't keep it alive
  (`ExecutorMonitor.hasActiveShuffle` is only tracked with `shuffleTracking` and no shuffle service).

The engine supports dynamic allocation with the external shuffle service only (the production YARN
setup); lost shuffle files are not modelled.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..model.cluster import Executor
    from .runtime import AppRun

TICK_MS = 100  # ExecutorAllocationManager's scheduler interval


def _seconds(v: str) -> float:
    v = v.strip().lower()
    for suffix, mult in (("ms", 1e-3), ("min", 60.0), ("s", 1.0), ("m", 60.0), ("h", 3600.0)):
        if v.endswith(suffix):
            return float(v[: -len(suffix)]) * mult
    return float(v)


@dataclass
class DynAllocConf:
    min_executors: int = 0
    max_executors: int = 2_147_483_647
    initial_executors: int = 0
    backlog_timeout_s: float = 1.0
    sustained_backlog_timeout_s: float = 1.0
    idle_timeout_s: float = 60.0
    allocation_ratio: float = 1.0

    @classmethod
    def from_conf(cls, conf: dict[str, str]) -> DynAllocConf:
        g = conf.get
        mn = int(g("spark.dynamicAllocation.minExecutors", "0"))
        initial = max(mn, int(g("spark.dynamicAllocation.initialExecutors", str(mn))),
                      int(g("spark.executor.instances", "0")))  # Utils.getDynamicAllocationInitialExecutors
        return cls(
            min_executors=mn,
            max_executors=int(g("spark.dynamicAllocation.maxExecutors", "2147483647")),
            initial_executors=initial,
            backlog_timeout_s=_seconds(g("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")),
            sustained_backlog_timeout_s=_seconds(g("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
                                                   g("spark.dynamicAllocation.schedulerBacklogTimeout", "1s"))),
            idle_timeout_s=_seconds(g("spark.dynamicAllocation.executorIdleTimeout", "60s")),
            allocation_ratio=float(g("spark.dynamicAllocation.executorAllocationRatio", "1")),
        )


@dataclass
class Tracker:
    """ExecutorMonitor.Tracker: idle since when, pending removal."""

    idle_since: int | None
    running: int = 0
    pending_removal: bool = False

    def timeout_at(self, idle_ms: int) -> int | None:
        return None if self.idle_since is None or self.running else self.idle_since + idle_ms


@dataclass
class ExecutorAllocationManager:
    run: AppRun
    conf: DynAllocConf
    tasks_per_executor: int
    target: int = 0
    to_add: int = 1
    add_time: int | None = None
    initializing: bool = True
    executors: dict[str, Tracker] = field(default_factory=dict)
    removed_stats: dict[str, int] = field(default_factory=lambda: {"driver killed": 0, "unexpectedly exited": 0})

    def __post_init__(self):
        self.target = self.conf.initial_executors

    def start(self) -> None:
        self.run.sim.after(TICK_MS, self._tick)

    # ------------------------------------------------------------------ listener callbacks

    def executor_added(self, ex: Executor) -> None:
        self.executors[ex.id] = Tracker(idle_since=self.run.sim.now)

    def executor_removed(self, ex: Executor) -> None:
        if self.executors.pop(ex.id, None) is not None:
            key = "driver killed" if ex.killed_by_driver else "unexpectedly exited"
            self.removed_stats[key] += 1

    def task_started(self, eid: str) -> None:
        tr = self.executors.get(eid)
        if tr is not None:
            tr.running += 1
            tr.idle_since = None
        if self.run.sched.num_pending_tasks == 0:  # onSchedulerQueueEmpty
            self.add_time = None
            self.to_add = 1

    def task_ended(self, eid: str) -> None:
        tr = self.executors.get(eid)
        if tr is not None:
            tr.running = max(0, tr.running - 1)
            if tr.running == 0:
                tr.idle_since = self.run.sim.now

    def stage_submitted(self) -> None:
        self.initializing = False
        if self.add_time is None and self.run.sched.num_pending_tasks > 0:  # onSchedulerBacklogged
            self.add_time = self.run.sim.now + int(self.conf.backlog_timeout_s * 1000)

    # ------------------------------------------------------------------ schedule()

    def _tick(self) -> None:
        if self.run.app_end is not None or self.run.stopping:
            return
        now = self.run.sim.now
        idle_ms = int(self.conf.idle_timeout_s * 1000)
        timed_out = sorted((eid for eid, tr in self.executors.items()
                            if not tr.pending_removal and (t := tr.timeout_at(idle_ms)) is not None and t <= now),
                           key=lambda e: e)  # ExecutorMonitor sorts by id (string order)
        if timed_out:
            self.initializing = False
        self._update_target(now)
        if timed_out:
            self._remove(timed_out)
        if self.add_time is None and self.run.sched.num_pending_tasks > 0 and not self.initializing:
            self.add_time = now + int(self.conf.backlog_timeout_s * 1000)
        self.run.sim.after(TICK_MS, self._tick)

    def max_needed(self) -> int:
        s = self.run.sched
        return math.ceil((s.num_pending_tasks + s.num_running_tasks) * self.conf.allocation_ratio
                         / self.tasks_per_executor)

    def _update_target(self, now: int) -> None:
        if self.initializing:
            return
        needed = self.max_needed()
        old = self.target
        if needed < self.target:
            self.target = max(needed, self.conf.min_executors)
            self.to_add = 1
            if self.target != old:
                self.run._dynalloc_target(self.target, old, backlogged=False)
        elif self.add_time is not None and now >= self.add_time:
            if old >= self.conf.max_executors:
                self.to_add = 1
                return
            t = max(self.target, len(self.executors)) + self.to_add  # executorCountWithResourceProfile
            t = min(t, needed)
            t = max(min(t, self.conf.max_executors), self.conf.min_executors)
            delta = t - old
            self.target = t
            if delta == 0:
                self.to_add = 1
            else:
                self.run._dynalloc_target(self.target, old, backlogged=True, delta=delta)
                self.to_add = self.to_add * 2 if delta == self.to_add else 1
            self.add_time = now + int(self.conf.sustained_backlog_timeout_s * 1000)

    def _remove(self, ids: list[str]) -> None:
        alive = len(self.executors) - sum(tr.pending_removal for tr in self.executors.values())
        kill = []
        for eid in ids:
            if alive - 1 < self.conf.min_executors or alive - 1 < self.target:
                continue
            kill.append(eid)
            alive -= 1
        if kill:
            for eid in kill:
                self.executors[eid].pending_removal = True
            self.run._dynalloc_kill(kill)
