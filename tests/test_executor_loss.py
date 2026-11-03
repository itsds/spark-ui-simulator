"""Executor loss on YARN (physical-memory kills) and dynamic allocation, on scaled copies of Scenario 00.

The faults are causes, not symptoms: the pmem kills come from a memoryOverhead smaller than the JVM's
native footprint once its heap is committed (`model.memory`), the removals from idle executors.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import replace

import pytest

from sparksim.apps import daily_enrichment as d
from sparksim.engine.emit import build_events
from sparksim.engine.runtime import AppRun
from sparksim.eventlog import (
    BlockManagerRemoved,
    ExecutorAdded,
    ExecutorLostFailure,
    ExecutorRemoved,
    StageCompleted,
    TaskEnd,
    TaskStart,
    event_to_line,
    line_to_event,
)

PMEM = re.compile(r"^Container killed by YARN for exceeding physical memory limits\. [0-9.]+ [KMG]B of "
                  r"[0-9.]+ [KMG]B physical memory used\. Consider boosting spark\.executor\.memoryOverhead\.$")


def _spec(conf=(), scale=1.0, **changes):
    s = d.SPEC
    over = dict(conf)
    return replace(s, inputs=tuple(replace(i, files=max(1, int(i.files * scale))) for i in s.inputs),
                   conf=tuple(c for c in s.conf if c[0] not in over) + tuple(conf), **changes)


def of(events, cls):
    return [e for e in events if isinstance(e, cls)]


@pytest.fixture(scope="module")
def pmem_app():
    """memoryOverhead=384m on a cluster with a 512 MB allocation step: 16.5 GiB containers for 16 GiB heaps."""
    spec = _spec((("spark.executor.memoryOverhead", "384m"), ("spark.shuffle.service.enabled", "true")))
    spec = replace(spec, sizing=replace(spec.sizing, overhead_mb=384),
                   cluster=replace(spec.cluster, min_allocation_mb=512))
    run = AppRun(spec).run()
    return run, build_events(run)


@pytest.fixture(scope="module")
def da_app():
    """Dynamic allocation with the external shuffle service; a 95 s driver-side pause between queries."""
    qs = list(d.queries())
    qs[1] = replace(qs[1], think_ms=(95_000, 100_000))
    spec = _spec((("spark.dynamicAllocation.enabled", "true"), ("spark.shuffle.service.enabled", "true"),
                  ("spark.dynamicAllocation.maxExecutors", "40")), scale=0.25, queries=tuple(qs))
    spec = replace(spec, sizing=replace(spec.sizing, instances=0))
    run = AppRun(spec).run()
    return run, build_events(run)


# --------------------------------------------------------------------------- physical memory kills


def test_pmem_kills_follow_yarn_wording(pmem_app):
    run, ev = pmem_app
    removed = of(ev, ExecutorRemoved)
    assert removed, "a 384m overhead must not hold the native footprint of a busy 16 GiB executor"
    assert all(PMEM.match(r.removed_reason) for r in removed)
    # The AM sends RemoveExecutor and answers the driver's loss-reason query: the second one posts
    # ExecutorRemoved again for an executor that is already gone (SPARK-35011), 1 ms later (ref05).
    by_exec = defaultdict(list)
    for r in removed:
        by_exec[r.executor_id].append(r.timestamp)
    assert all(len(ts) == 2 and ts[1] == ts[0] + 1 for ts in by_exec.values())
    for ex in run.executors:
        if ex.exit_status is not None:
            assert ex.exit_status == -104
            assert "beyond the 'PHYSICAL' memory limit. Current usage:" in ex.diagnostics
            assert ex.diagnostics.endswith("Container exited with a non-zero exit code 143. \n")
            assert ex.extra["peak_rss"] > ex.container_mb * 1024 * 1024


def test_lost_tasks_rerun_elsewhere(pmem_app):
    run, ev = pmem_app
    bm_removed = {e.block_manager_id.executor_id: e.timestamp for e in of(ev, BlockManagerRemoved)}
    removed_at = {}
    for r in of(ev, ExecutorRemoved):
        removed_at.setdefault(r.executor_id, r.timestamp)
    ends = of(ev, TaskEnd)
    lost = [e for e in ends if isinstance(e.reason, ExecutorLostFailure)]
    assert lost
    ok = {(e.stage_id, e.task_info.index): e for e in ends if type(e.reason).__name__ == "Success"}
    for e in lost:
        ti = e.task_info
        assert e.reason.exit_caused_by_app and e.reason.executor_id == ti.executor_id
        assert ti.failed and ti.accumulables == [] and e.task_metrics is None
        assert e.task_executor_metrics.jvm_heap_memory == -1  # ExecutorMetrics(Array.empty)
        # the block manager goes when the driver sees the disconnect; tasks fail when the AM gives the reason
        assert bm_removed[ti.executor_id] <= ti.finish_time <= removed_at[ti.executor_id]
        retry = ok[(e.stage_id, ti.index)]
        assert retry.task_info.attempt > ti.attempt and retry.task_info.executor_id != ti.executor_id
    # nothing is launched on an executor after the driver noticed it was gone
    for s in of(ev, TaskStart):
        eid = s.task_info.executor_id
        assert eid not in bm_removed or s.task_info.launch_time < bm_removed[eid]
    # YARN starts replacements: more executors than the 40 requested, never more than 40 alive at once
    alive = 0
    for e in ev:
        if isinstance(e, ExecutorAdded):
            alive += 1
            assert alive <= 40
        elif isinstance(e, BlockManagerRemoved):
            alive -= 1
    assert len(of(ev, ExecutorAdded)) > 40


def test_stage_totals_ignore_lost_attempts(pmem_app):
    _run, ev = pmem_app
    runs = defaultdict(int)
    for e in of(ev, TaskEnd):
        if type(e.reason).__name__ == "Success":
            runs[(e.stage_id, e.stage_attempt_id)] += e.task_metrics.executor_run_time
    for sc in of(ev, StageCompleted):
        si = sc.stage_info
        acc = {a.name: a.value for a in si.accumulables}
        assert acc["internal.metrics.executorRunTime"] == runs[(si.stage_id, si.attempt_id)]


def test_loss_events_roundtrip(pmem_app):
    _run, ev = pmem_app
    for e in ev:
        if isinstance(e, (TaskEnd, ExecutorRemoved, BlockManagerRemoved)):
            line = event_to_line(e)
            assert event_to_line(line_to_event(line)) == line


# --------------------------------------------------------------------------- dynamic allocation


def test_dynamic_allocation_ramps_up_exponentially(da_app):
    run, _ev = da_app
    deltas = [p[3] for _t, kind, p in run.yarn_log if kind == "da_target" and p[2]]
    assert deltas[:4] == [1, 2, 4, 8]  # numExecutorsToAdd doubles while the backlog lasts
    targets = [p[0] for _t, kind, p in run.yarn_log if kind == "da_target"]
    assert max(targets) == 40  # spark.dynamicAllocation.maxExecutors


def test_idle_executors_are_released(da_app):
    run, ev = da_app
    removed = of(ev, ExecutorRemoved)
    assert removed and {r.removed_reason for r in removed} == {"Executor killed by driver."}
    last_end = defaultdict(int)
    for e in of(ev, TaskEnd):
        last_end[e.task_info.executor_id] = max(last_end[e.task_info.executor_id], e.task_info.finish_time)
    for r in removed:  # executorIdleTimeout = 60 s, then the AM releases the container
        assert r.timestamp - last_end[r.executor_id] >= 60_000
    # the next query ramps up again with new executors (new ids, new containers)
    added = of(ev, ExecutorAdded)
    assert max(a.timestamp for a in added) > min(r.timestamp for r in removed)
    assert len({a.executor_info.attributes["CONTAINER_ID"] for a in added}) == len(added)


# --------------------------------------------------------------------------- container logs of these runs


def test_killed_executor_logs(pmem_app):
    from sparksim.yarnlogs import am_stderr, executor_stderr

    run, ev = pmem_app
    echo: list = []
    killed = [ex for ex in run.executors if ex.exit_status == -104]
    ex = killed[0]
    text = executor_stderr(run, ex, echo)
    tail = text.rstrip().splitlines()[-3:]
    assert tail[0].endswith("ERROR CoarseGrainedExecutorBackend: RECEIVED SIGNAL TERM")
    assert tail[-1].endswith("INFO ShutdownHookManager: Shutdown hook called")
    lost = {e.task_info.task_id for e in of(ev, TaskEnd)
            if isinstance(e.reason, ExecutorLostFailure) and e.task_info.executor_id == ex.id}
    finished = {int(x) for x in re.findall(r"Finished task \S+ in stage \S+ \(TID (\d+)\)", text)}
    started = {int(x) for x in re.findall(r"Running task \S+ in stage \S+ \(TID (\d+)\)", text)}
    assert lost and lost <= started and not lost & finished
    driver = am_stderr(run, echo)
    reason = ex.removed_reason
    assert f"WARN YarnAllocator: {reason}" in driver
    assert (f"INFO YarnAllocator: Completed container {ex.container_id} on host: {ex.host.name} (state: COMPLETE, "
            "exit status: -104)") in driver
    assert f"ERROR YarnClusterScheduler: Lost executor {ex.id} on {ex.host.name}: {reason}" in driver
    assert re.search(rf"WARN TaskSetManager: Lost task \S+ in stage \S+ \(TID \d+\) \({re.escape(ex.host.name)} "
                     rf"executor {ex.id}\): ExecutorLostFailure \(executor {ex.id} exited caused by one of the "
                     rf"running tasks\) Reason: {re.escape(reason)}", driver)


def test_dynamic_allocation_logs(da_app):
    from sparksim.yarnlogs import am_stderr, executor_stderr

    run, _ev = da_app
    driver = am_stderr(run)
    assert "INFO ExecutorAllocationManager: Requesting 1 new executor because tasks are backlogged (new desired " \
           "total will be 1 for resource profile id: 0)" in driver
    assert re.search(r"INFO ExecutorAllocationManager: Executors [\d,]+ removed due to idle timeout\.", driver)
    released = [ex for ex in run.executors if ex.killed_by_driver]
    assert released
    ex = released[0]
    assert f"INFO YarnClusterScheduler: Executor {ex.id} on {ex.host.name} killed by driver." in driver
    assert "ERROR CoarseGrainedExecutorBackend: RECEIVED SIGNAL TERM" in executor_stderr(run, ex)


def test_container_logs_are_deterministic(da_app):
    from sparksim.yarnlogs import am_stderr

    run, _ev = da_app
    assert am_stderr(run) == am_stderr(run)


def test_explicit_overhead_is_part_of_the_default_resource_profile(pmem_app):
    from sparksim.eventlog import ResourceProfileAdded

    _run, ev = pmem_app
    rp = of(ev, ResourceProfileAdded)[0]
    # ConcurrentHashMap order of the request names, as JsonProtocol writes them
    assert list(rp.executor_resources) == ["memoryOverhead", "cores", "memory", "offHeap"]
    assert rp.executor_resources["memoryOverhead"].amount == 384
