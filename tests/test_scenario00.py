"""Scenario 00 (healthy baseline): simulation invariants on the full production-scale app."""

from __future__ import annotations

import hashlib
import statistics
from collections import defaultdict

import pytest

from sparksim.apps import daily_enrichment
from sparksim.eventlog import (
    ApplicationEnd,
    ApplicationStart,
    BlockManagerAdded,
    ExecutorAdded,
    JobEnd,
    JobStart,
    StageCompleted,
    StageSubmitted,
    TaskEnd,
    TaskStart,
    event_to_line,
    line_to_event,
)
from sparksim.eventlog.ordering import java_properties_order, scala_int_hashset_order


@pytest.fixture(scope="module")
def app():
    run, events = daily_enrichment.build()
    return run, events


def of(events, cls):
    return [e for e in events if isinstance(e, cls)]


def test_shape(app):
    run, ev = app
    execs = of(ev, ExecutorAdded)
    assert len(execs) == 40 and {e.executor_info.total_cores for e in execs} == {4}
    assert len({e.executor_info.host for e in execs}) >= 25, "executors spread over many NodeManagers"
    ends = of(ev, TaskEnd)
    assert 14_000 <= len(ends) <= 16_500
    assert len(of(ev, TaskStart)) == len(ends)
    jobs = of(ev, JobStart)
    assert len(jobs) >= 6 and len(of(ev, JobEnd)) == len(jobs)
    submitted = {e.stage_info.stage_id for e in of(ev, StageSubmitted)}
    listed = {s.stage_id for j in jobs for s in j.stage_infos}
    assert listed - submitted, "AQE's final jobs list re-created map stages that are skipped"
    assert {type(t.reason).__name__ for t in ends} == {"Success"}


def test_every_event_roundtrips(app):
    _, ev = app
    for e in ev:
        line = event_to_line(e)
        assert event_to_line(line_to_event(line)) == line


def test_time_is_consistent(app):
    _, ev = app
    start = of(ev, ApplicationStart)[0].timestamp
    end = of(ev, ApplicationEnd)[0].timestamp
    submitted = {e.stage_info.stage_id: e.stage_info.submission_time for e in of(ev, StageSubmitted)}
    completed = {e.stage_info.stage_id: e.stage_info.completion_time for e in of(ev, StageCompleted)}
    for t in of(ev, TaskEnd):
        ti = t.task_info
        assert submitted[t.stage_id] <= ti.launch_time < ti.finish_time <= completed[t.stage_id] <= end
        m = t.task_metrics
        assert m.executor_deserialize_time + m.executor_run_time + m.result_serialization_time \
            < ti.finish_time - ti.launch_time
        assert m.executor_cpu_time <= m.executor_run_time * 1_000_000
        assert m.jvm_gc_time < m.executor_run_time
    jobs = {j.job_id: j for j in of(ev, JobStart)}
    for je in of(ev, JobEnd):
        js = jobs[je.job_id]
        assert start <= js.submission_time < je.completion_time <= end
        for s in js.stage_ids:
            if s in completed:
                assert js.submission_time <= submitted[s] and completed[s] <= je.completion_time
    times = [_when(e) for e in ev[5:]]
    assert times == sorted(times), "after the fixed preamble the log is in time order"


def _when(e) -> int:
    if isinstance(e, TaskStart):
        return e.task_info.launch_time
    if isinstance(e, TaskEnd):
        return e.task_info.finish_time
    if isinstance(e, StageSubmitted):
        return e.stage_info.submission_time
    if isinstance(e, StageCompleted):
        return e.stage_info.completion_time
    if isinstance(e, JobStart):
        return e.submission_time
    if isinstance(e, JobEnd):
        return e.completion_time
    return e.timestamp


def test_no_core_runs_two_tasks(app):
    _, ev = app
    cores = {e.executor_id: e.executor_info.total_cores for e in of(ev, ExecutorAdded)}
    added = {e.executor_id: e.timestamp for e in of(ev, ExecutorAdded)}
    spans = defaultdict(list)
    for t in of(ev, TaskEnd):
        spans[t.task_info.executor_id].append((t.task_info.launch_time, t.task_info.finish_time))
    for eid, iv in spans.items():
        assert min(s for s, _ in iv) >= added[eid], "no task before its executor registered"
        points = sorted([(s, 1) for s, _ in iv] + [(f, -1) for _, f in iv], key=lambda p: (p[0], p[1]))
        running = 0
        for _, d in points:
            running += d
            assert running <= cores[eid], f"executor {eid} oversubscribed"


def test_no_task_before_parents_finish(app):
    run, ev = app
    completed = {e.stage_info.stage_id: e.stage_info.completion_time for e in of(ev, StageCompleted)}
    first_launch = defaultdict(lambda: 1 << 62)
    for t in of(ev, TaskStart):
        first_launch[t.stage_id] = min(first_launch[t.stage_id], t.task_info.launch_time)
    for st in run.stages.values():
        if st.skipped:
            continue
        for x in st.spec.shuffle_inputs:  # the stage that actually wrote each shuffle it reads
            producer = run.map_stage_of[x]
            assert completed[producer.stage_id] < first_launch[st.stage_id]


def test_shuffle_conservation(app):
    run, ev = app
    ends = of(ev, TaskEnd)
    by_stage = defaultdict(list)
    for t in ends:
        by_stage[t.stage_id].append(t.task_metrics)
    for name, ex in run.exchanges.items():
        producer = run.map_stage_of[name]
        w = by_stage[producer.stage_id]
        assert sum(m.shuffle_write.bytes_written for m in w) == int(ex.bytes.sum())
        assert sum(m.shuffle_write.records_written for m in w) == int(ex.records.sum())
        readers = [s for s in run.stages.values() if not s.skipped and name in s.spec.shuffle_inputs]
        assert len(readers) == 1
        r = by_stage[readers[0].stage_id]
        if len(readers[0].spec.shuffle_inputs) == 1:
            assert sum(m.shuffle_read.total_bytes_read for m in r) == int(ex.bytes.sum())
            assert sum(m.shuffle_read.records_read for m in r) == int(ex.records.sum())
    # the join reads both of its inputs completely
    join = next(s for s in run.stages.values() if not s.skipped and len(s.spec.shuffle_inputs) == 2)
    both = sum(int(run.exchanges[x].bytes.sum()) for x in join.spec.shuffle_inputs)
    assert sum(m.shuffle_read.total_bytes_read for m in by_stage[join.stage_id]) == both


def test_rows_flow_between_queries(app):
    """What the write stage wrote is what the next queries scan."""
    run, ev = app
    by_stage = defaultdict(list)
    for t in of(ev, TaskEnd):
        by_stage[t.stage_id].append(t.task_metrics)
    write = next(s for s in run.stages.values() if s.spec.name == "join_write")
    written = sum(m.output.records_written for m in by_stage[write.stage_id])
    for name in ("funnel_partial", "activity_partial"):
        scan = next(s for s in run.stages.values() if s.spec.name == name)
        assert sum(m.input.records_read for m in by_stage[scan.stage_id]) == written


def test_stage_accumulables_are_task_totals(app):
    _, ev = app
    totals = defaultdict(lambda: defaultdict(int))
    for t in of(ev, TaskEnd):
        for a in t.task_info.accumulables:
            totals[t.stage_id][a.id] += a.update
    for sc in of(ev, StageCompleted):
        sid = sc.stage_info.stage_id
        assert {a.id: a.value for a in sc.stage_info.accumulables} == dict(totals[sid])


def test_driver_orders(app):
    _, ev = app
    for js in of(ev, JobStart):
        assert js.stage_ids == scala_int_hashset_order(js.stage_ids)
        assert [s.stage_id for s in js.stage_infos] == js.stage_ids
        assert list(js.properties) == java_properties_order(list(js.properties))
    for ss in of(ev, StageSubmitted):
        assert list(ss.properties) == java_properties_order(list(ss.properties))


def test_healthy_quantiles(app):
    """What 'normal' looks like: task max within ~2x median, little GC, no spill, even executors."""
    run, ev = app
    by_stage = defaultdict(list)
    for t in of(ev, TaskEnd):
        by_stage[t.stage_id].append(t)
    for sid, tasks in by_stage.items():
        if len(tasks) < 100:
            continue
        d = [t.task_info.finish_time - t.task_info.launch_time for t in tasks]
        assert max(d) <= 2.5 * statistics.median(d), sid
        assert all(t.task_metrics.memory_bytes_spilled == 0 for t in tasks)
        # Above ~1.6x the slow tasks are the ones with more data (two small files packed into one split),
        # not tasks that are slower per byte: time tracks input.
        scans = [t for t in tasks if t.task_metrics.input.bytes_read > 16 * 1024**2]
        if scans:
            rate = [(t.task_info.finish_time - t.task_info.launch_time) / t.task_metrics.input.bytes_read
                    for t in scans]
            assert max(rate) <= 2.0 * statistics.median(rate), sid
    per_exec = defaultdict(lambda: [0, 0, 0])
    for t in of(ev, TaskEnd):
        e = per_exec[t.task_info.executor_id]
        e[0] += t.task_info.finish_time - t.task_info.launch_time
        e[1] += t.task_metrics.jvm_gc_time
        e[2] += 1
    for eid, (task_time, gc, _n) in per_exec.items():
        assert gc / task_time < 0.10, f"executor {eid}: GC would turn the Executors tab cell red"
    counts = [n for _, _, n in per_exec.values()]
    assert max(counts) <= 1.3 * statistics.median(counts)


def test_locality_mix(app):
    _, ev = app
    loc = defaultdict(lambda: defaultdict(int))
    for t in of(ev, TaskEnd):
        loc[t.stage_id][t.task_info.locality] += 1
    scan_levels = set()
    for counts in loc.values():
        scan_levels |= set(counts)
    assert {"NODE_LOCAL", "PROCESS_LOCAL"} <= scan_levels
    assert "RACK_LOCAL" in scan_levels


def test_executor_metrics_sampled_at_heartbeats(app):
    run, ev = app
    ends = of(ev, TaskEnd)
    long_tasks = [t for t in ends if t.task_info.finish_time - t.task_info.launch_time > 10_000]
    short = [t for t in ends if t.task_info.finish_time - t.task_info.launch_time < 300]
    nonzero = [t for t in ends if not t.task_executor_metrics.is_zero()]
    assert nonzero and len(nonzero) < len(ends)
    assert all(not t.task_executor_metrics.is_zero() for t in long_tasks)
    assert sum(not t.task_executor_metrics.is_zero() for t in short) < len(short) * 0.1 + 1
    heaps = {e.block_manager_id.executor_id: e.max_memory for e in of(ev, BlockManagerAdded)}
    for t in nonzero:
        tem = t.task_executor_metrics
        assert tem.jvm_heap_memory < 16 * 1024**3
        assert tem.on_heap_unified_memory <= heaps[t.task_info.executor_id]
        assert tem.total_gc_time == tem.minor_gc_time + tem.major_gc_time


def test_deterministic(app, tmp_path):
    a = daily_enrichment.generate(tmp_path / "a")
    digest = hashlib.sha256(a.read_bytes()).hexdigest()
    expected = hashlib.sha256("".join(event_to_line(e) + "\n" for e in app[1]).encode()).hexdigest()
    assert digest == expected, "same scenario + seed must give a byte-identical log"
    assert a.name == "application_1791902144377_48213_1"
