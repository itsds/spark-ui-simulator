"""Golden test: re-derive the driver's bookkeeping for every reference log and diff it with what Spark wrote.

For each real event log we keep only what a generator would supply (task
metrics, task outcomes, SQL metric updates, plan metric declarations, the
first task-metric accumulator ID of each stage attempt). Every `Task Info.Accumulables`
list and every `StageCompleted` `Stage Info.Accumulables` list is then rebuilt
by `DriverLedger` and compared field by field with the real one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from sparksim.eventlog import (
    INTERNAL_ACCUM_OFFSETS,
    INTERNAL_METRICS_PREFIX,
    AccumulatorDef,
    DriverLedger,
    SQLAdaptiveExecutionUpdate,
    SQLExecutionStart,
    StageCompleted,
    StageSubmitted,
    TaskEnd,
    read_event_log,
)

ROOT = Path(__file__).resolve().parents[1]
LOGS = sorted(p for p in (ROOT / "reference" / "event-logs").iterdir() if p.is_file())


def internal_bases(events) -> dict[tuple[int, int], int]:
    """Model input: first task-metric accumulator ID of each stage attempt (what IdAllocator hands out)."""
    bases = {}
    for ev in events:
        if isinstance(ev, TaskEnd):
            for a in ev.task_info.accumulables:
                if a.is_task_metric:
                    base = a.id - INTERNAL_ACCUM_OFFSETS[a.name[len(INTERNAL_METRICS_PREFIX):]]
                    key = (ev.stage_id, ev.stage_attempt_id)
                    assert bases.setdefault(key, base) == base, f"stage {key}: task-metric IDs not one block"
    return bases


def rederive(events):
    """Yield (where, derived, real) for every accumulable list Spark's driver computed."""
    ledger = DriverLedger()
    bases = internal_bases(events)
    undeclared: set[int] = set()
    for i, ev in enumerate(events):
        if isinstance(ev, (SQLExecutionStart, SQLAdaptiveExecutionUpdate)):
            ledger.define_plan_metrics(ev.spark_plan_info)
        elif isinstance(ev, StageSubmitted):
            si = ev.stage_info
            ledger.register_stage_attempt(si.stage_id, si.attempt_id, bases.get((si.stage_id, si.attempt_id), -1))
        elif isinstance(ev, TaskEnd):
            real = ev.task_info.accumulables
            touched = {a.name[len(INTERNAL_METRICS_PREFIX):] for a in real if a.is_task_metric and a.update == 0}
            external = []
            for a in real:
                if a.is_task_metric:
                    continue
                if a.id not in ledger.defs:  # not declared by any plan: take its identity from the log
                    undeclared.add(a.id)
                    ledger.define(AccumulatorDef(a.id, a.name, a.metadata, a.internal, a.count_failed_values))
                external.append((a.id, int(a.update)))
            derived = ledger.task_end(ev.stage_id, ev.stage_attempt_id, ev.reason, ev.task_metrics,
                                      touched=touched, external=external)
            yield f"line {i + 1} TaskEnd TID {ev.task_info.task_id}", derived, real
        elif isinstance(ev, StageCompleted):
            si = ev.stage_info
            yield (f"line {i + 1} StageCompleted {si.stage_id}.{si.attempt_id}",
                   ledger.stage_accumulables(si.stage_id, si.attempt_id), si.accumulables)
    rederive.undeclared = undeclared


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_driver_bookkeeping_matches_spark(path: Path):
    events = list(read_event_log(path))
    checked = 0
    for where, derived, real in rederive(events):
        d = [a.to_json() for a in derived]
        r = [a.to_json() for a in real]
        if d != r:
            diff = [(x, y) for x, y in zip(d, r, strict=False) if x != y] or [("len", len(d), len(r))]
            pytest.fail(f"{path.name} {where}: first difference {diff[0]}")
        checked += 1
    assert checked > 0
    # Every SQL metric a task reports should be declared in a plan (sparkPlanInfo) first.
    assert not rederive.undeclared, f"accumulators not declared by any plan: {sorted(rederive.undeclared)}"


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_task_metric_ids_grow_with_stage_submission(path: Path):
    events = list(read_event_log(path))
    bases = internal_bases(events)
    order = [(e.stage_info.stage_id, e.stage_info.attempt_id) for e in events if isinstance(e, StageSubmitted)]
    seq = [bases[k] for k in order if k in bases]
    assert seq == sorted(seq), "task-metric accumulators are registered at stage submission, so IDs grow"
    assert len(set(seq)) == len(seq)
