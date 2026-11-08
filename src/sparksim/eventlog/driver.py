"""What the Spark driver derives by itself while it logs events.

Generators supply task-level facts (task metrics, SQL metric updates). Spark's
driver then adds fields that are pure bookkeeping, and the event log must
carry them exactly as the driver would:

* **Accumulator `Value`.** `DAGScheduler.updateAccumulators` merges each task's
  update into the driver-side accumulator and logs `Value` = the running total
  after that task. For task metrics that total is per stage attempt, because
  each attempt registers fresh accumulators. For SQL metrics it is per plan node
  and spans every task that updates it.
* **Which accumulables a task lists.** Zero-valued updates are dropped
  (`!updates.isZero`), except metrics the task's code path touched
  (`LongAccumulator.add(0)` counts as non-zero), such as every shuffle-read
  metric on a reduce task. Only Success, ExceptionFailure and TaskKilled
  merge updates. FetchFailed and ExecutorLostFailure tasks list none, and
  neither does any task that ends after its stage was removed from the
  DAGScheduler (its job was cleaned up): a speculative or killed attempt that
  reports after the stage completed (`ref09-speculation`).
* **Internal accumulator IDs.** 35 consecutive IDs per stage attempt, in
  `TaskMetrics.nameToAccums` order (`metrics.TASK_METRIC_ACCUMS`).
* **Stage accumulables.** `StageCompleted.Stage Info.Accumulables` holds the
  latest `Value` of every accumulator the stage's tasks updated, recorded on
  the stage's latest attempt (`stage.latestInfo`).

`tests/test_eventlog_golden.py` re-derives all of this for every reference log
and compares it field by field with what Spark wrote.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .common import INTERNAL_METRICS_PREFIX, AccumulableInfo, SparkPlanInfo, accum_value
from .metrics import INTERNAL_ACCUM_COUNT, TASK_METRIC_ACCUMS, TaskMetrics
from .reasons import TaskEndReason


class IdAllocator:
    """Monotonic ID source, like `AccumulatorContext.newId`, task IDs or RDD IDs."""

    def __init__(self, start: int = 0):
        self._next = start

    def next(self) -> int:
        n = self._next
        self._next += 1
        return n

    def take(self, n: int) -> range:
        r = range(self._next, self._next + n)
        self._next += n
        return r

    def skip(self, n: int) -> None:
        """Burn IDs that real Spark allocates for objects we don't log."""
        self._next += n

    @property
    def peek(self) -> int:
        return self._next


@dataclass(frozen=True)
class AccumulatorDef:
    """A named accumulator other than a task metric (SQL metrics carry metadata 'sql')."""

    id: int
    name: str
    metadata: str | None = "sql"
    internal: bool = True  # SQLMetric.toInfo reports internal=true, countFailedValues=true
    count_failed_values: bool = True


StageKey = tuple[int, int]  # (stage id, stage attempt id)


class DriverLedger:
    def __init__(self) -> None:
        self.defs: dict[int, AccumulatorDef] = {}
        self.values: dict[int, int] = {}
        self._internal_base: dict[StageKey, int] = {}
        self._latest_attempt: dict[int, int] = {}
        self._stage_accums: dict[StageKey, dict[int, AccumulableInfo]] = {}
        self._removed: set[int] = set()

    # ---------------------------------------------------------------- registration

    def define(self, acc: AccumulatorDef) -> None:
        self.defs[acc.id] = acc

    def define_plan_metrics(self, plan: SparkPlanInfo) -> None:
        for node in plan.walk():
            for m in node.metrics:
                if m.accumulator_id not in self.defs:
                    self.define(AccumulatorDef(m.accumulator_id, m.name))

    def register_stage_attempt(self, stage_id: int, attempt: int, internal_base: int) -> range:
        """A new stage attempt registers its task-metric accumulators: IDs base .. base+34."""
        key = (stage_id, attempt)
        self._internal_base[key] = internal_base
        self._latest_attempt[stage_id] = attempt
        self._stage_accums.setdefault(key, {})
        return range(internal_base, internal_base + INTERNAL_ACCUM_COUNT)

    def stage_removed(self, stage_id: int) -> None:
        """`cleanupStateForJobAndIndependentStages`: the stage left `stageIdToStage`. A task of it that ends
        later is posted without merging its updates (DAGScheduler.handleTaskCompletion's early return)."""
        self._removed.add(stage_id)

    def internal_accum_id(self, stage_id: int, attempt: int, name: str) -> int:
        from .metrics import INTERNAL_ACCUM_OFFSETS

        return self._internal_base[(stage_id, attempt)] + INTERNAL_ACCUM_OFFSETS[name]

    # ---------------------------------------------------------------- task completion

    def task_end(
        self,
        stage_id: int,
        attempt: int,
        reason: TaskEndReason,
        metrics: TaskMetrics | None,
        *,
        touched: Iterable[str] = (),
        external: Iterable[tuple[int, int]] = (),
        merge: bool | None = None,
    ) -> list[AccumulableInfo]:
        """Merge one task's updates; return its `Task Info.Accumulables` (sorted by ID, as JsonProtocol).

        `touched`: task-metric accumulator names reported even when 0 (see metrics.SHUFFLE_READ_ACCUMS).
        `external`: (accumulator id, update) for SQL/user accumulators, exactly the updates Spark reports.
        `merge`: override DAGScheduler's rule (e.g. a ResultTask finishing after its job ended merges nothing).
        """
        if merge is None:
            merge = reason.updates_accumulators and stage_id not in self._removed
        if not merge:
            return []
        touched = set(touched)
        updates: list[tuple[int, str, int, AccumulatorDef | None]] = []
        if metrics is not None:
            base = self._internal_base[(stage_id, attempt)]
            for off, (name, path) in enumerate(TASK_METRIC_ACCUMS):
                if path is None:
                    continue
                v = metrics.get(path)
                if v != 0 or name in touched:
                    updates.append((base + off, INTERNAL_METRICS_PREFIX + name, v, None))
        for acc_id, v in external:
            d = self.defs[acc_id]
            updates.append((acc_id, d.name, v, d))

        latest = (stage_id, self._latest_attempt.get(stage_id, attempt))
        stage_accums = self._stage_accums.setdefault(latest, {})
        out = []
        for acc_id, name, v, d in updates:
            total = self.values.get(acc_id, 0) + v
            self.values[acc_id] = total
            internal = True if d is None else d.internal
            cfv = True if d is None else d.count_failed_values
            md = None if d is None else d.metadata
            out.append(AccumulableInfo(acc_id, name, accum_value(name, v), accum_value(name, total), internal, cfv, md))
            stage_accums[acc_id] = AccumulableInfo(acc_id, name, None, accum_value(name, total), internal, cfv, md)
        out.sort(key=lambda a: a.id)
        return out

    def failure_updates(self, stage_id: int, attempt: int, metrics: TaskMetrics,
                        touched: Iterable[str] = ()) -> list[AccumulableInfo]:
        """The executor's `Accumulator Updates` inside a TaskKilled / ExceptionFailure reason.

        `Task.collectAccumulatorUpdates(taskFailed = true)`: the task's non-zero internal accumulators (an
        `add(0)` counts as non-zero), always including `resultSize` (it is 0 on the executor), in
        registration order; external accumulators (SQL metrics) don't count failed values and are left out.
        The executor sends them without names, so JsonProtocol writes `Internal: false` (ref09).
        """
        touched = set(touched) | {"resultSize"}
        base = self._internal_base[(stage_id, attempt)]
        out = []
        for off, (name, path) in enumerate(TASK_METRIC_ACCUMS):
            if path is None:
                continue
            v = metrics.get(path)
            if v != 0 or name in touched:
                out.append(AccumulableInfo(base + off, None, str(v), None, False, True, None))
        return out

    def stage_accumulables(self, stage_id: int, attempt: int) -> list[AccumulableInfo]:
        """`Stage Info.Accumulables` for StageCompleted."""
        return sorted(self._stage_accums.get((stage_id, attempt), {}).values(), key=lambda a: a.id)
