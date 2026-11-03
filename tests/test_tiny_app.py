"""Phase 2 smoke app: determinism, simulation invariants, and a real History Server round trip."""

from __future__ import annotations

import hashlib
import sys
from collections import defaultdict
from pathlib import Path

import pytest
import requests

from sparksim.apps import tiny
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
from sparksim.yarn import unified_memory_max

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import hs_api  # noqa: E402


@pytest.fixture(scope="module")
def app():
    yapp, events = tiny.build_events()
    return yapp, events


def of(events, cls):
    return [e for e in events if isinstance(e, cls)]


def test_deterministic(tmp_path):
    a = tiny.generate(tmp_path / "a")
    b = tiny.generate(tmp_path / "b")
    c = tiny.generate(tmp_path / "c", tiny.TinySpec(seed=8))
    digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()  # noqa: E731
    assert digest(a) == digest(b)
    assert digest(a) != digest(c)
    assert a.name == "application_1792541274210_4217_1"


def test_shape(app):
    _, ev = app
    assert ev[0].short_name == "SparkListenerLogStart"
    assert isinstance(ev[-1], ApplicationEnd)
    assert len(of(ev, JobStart)) == 1 and len(of(ev, JobEnd)) == 1
    assert len(of(ev, StageSubmitted)) == 2 and len(of(ev, StageCompleted)) == 2
    assert len(of(ev, TaskStart)) == len(of(ev, TaskEnd)) == 8
    assert {e.executor_id for e in of(ev, ExecutorAdded)} == {"1", "2"}
    assert len({e.executor_info.host for e in of(ev, ExecutorAdded)}) == 2, "executors on two NodeManagers"


def test_every_event_roundtrips(app):
    for e in app[1]:
        line = event_to_line(e)
        assert event_to_line(line_to_event(line)) == line


def test_time_is_consistent(app):
    _, ev = app
    start = of(ev, ApplicationStart)[0].timestamp
    submitted = {e.stage_info.stage_id: e.stage_info.submission_time for e in of(ev, StageSubmitted)}
    completed = {e.stage_info.stage_id: e.stage_info.completion_time for e in of(ev, StageCompleted)}
    job_end = of(ev, JobEnd)[0].completion_time
    for t in of(ev, TaskEnd):
        ti = t.task_info
        assert submitted[t.stage_id] <= ti.launch_time < ti.finish_time <= completed[t.stage_id] <= job_end
        m = t.task_metrics
        busy = m.executor_deserialize_time + m.executor_run_time + m.result_serialization_time
        assert busy <= ti.finish_time - ti.launch_time, "phases must fit in the task's wall time"
    assert completed[0] <= submitted[1], "reduce stage starts after its parent map stage"
    assert start <= min(submitted.values())
    # After the fixed preamble (5 events) the log is in time order.
    times = [when(e) for e in ev[5:]]
    assert times == sorted(times)


def when(e) -> int:
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
    spans = defaultdict(list)
    for t in of(ev, TaskEnd):
        spans[t.task_info.executor_id].append((t.task_info.launch_time, t.task_info.finish_time))
    for eid, iv in spans.items():
        points = sorted([(s, 1) for s, _ in iv] + [(f, -1) for _, f in iv], key=lambda p: (p[0], p[1]))
        running = 0
        for _, d in points:
            running += d
            assert running <= cores[eid], f"executor {eid} oversubscribed"


def test_more_tasks_than_cores_run_in_waves():
    """10 partitions on 4 cores: nothing dropped, no core oversubscribed, waves start as cores free up."""
    _, ev = tiny.build_events(tiny.TinySpec(partitions=10, records_per_partition=150_000))
    ends = of(ev, TaskEnd)
    assert len(ends) == 20 and sorted(t.task_info.index for t in ends if t.stage_id == 0) == list(range(10))
    test_no_core_runs_two_tasks((None, ev))
    test_shuffle_conservation_and_locality((None, ev))
    test_time_is_consistent((None, ev))


def test_shuffle_conservation_and_locality(app):
    _, ev = app
    ends = of(ev, TaskEnd)
    maps = [t for t in ends if t.stage_id == 0]
    reduces = [t for t in ends if t.stage_id == 1]
    w_bytes = sum(t.task_metrics.shuffle_write.bytes_written for t in maps)
    w_recs = sum(t.task_metrics.shuffle_write.records_written for t in maps)
    r = [t.task_metrics.shuffle_read for t in reduces]
    assert sum(x.total_bytes_read for x in r) == w_bytes
    assert sum(x.records_read for x in r) == w_recs
    # Each reducer fetches one block per map task; it's local iff that map ran on the same executor.
    for t in reduces:
        same = sum(1 for m in maps if m.task_info.executor_id == t.task_info.executor_id)
        assert t.task_metrics.shuffle_read.local_blocks_fetched == same
        assert t.task_metrics.shuffle_read.remote_blocks_fetched == len(maps) - same


def test_stage_accumulables_are_task_totals(app):
    _, ev = app
    for sc in of(ev, StageCompleted):
        sid = sc.stage_info.stage_id
        totals = defaultdict(int)
        for t in of(ev, TaskEnd):
            if t.stage_id == sid:
                for a in t.task_info.accumulables:
                    totals[a.id] += a.update
        assert {a.id: a.value for a in sc.stage_info.accumulables} == dict(totals)


def test_block_manager_memory(app):
    _, ev = app
    bms = {e.block_manager_id.executor_id: e.max_memory for e in of(ev, BlockManagerAdded)}
    assert bms["driver"] == unified_memory_max(2 * 1024**3) == 1099746508  # same as the real 2g drivers
    assert bms["1"] == bms["2"] == unified_memory_max(4 * 1024**3)


# ----------------------------------------------------------------------------- real History Server


@pytest.mark.integration
def test_history_server_round_trip(tmp_path, app):
    from conftest import run_history_server

    yapp, ev = app
    tiny.generate(tmp_path)
    ends = of(ev, TaskEnd)
    with run_history_server(tmp_path, 1, log_name="hs-tiny-test.log") as url:
        hs = hs_api.HistoryServer(url)
        assert hs_api.validate(hs, tmp_path) == 0

        (a,) = hs.apps()
        att = a["attempts"][0]
        assert a["id"] == yapp.app_id and a["name"] == "tiny-key-counts"
        assert att["attemptId"] == "1" and att["completed"] and att["sparkUser"] == "svc_etl"
        assert att["duration"] == of(ev, ApplicationEnd)[0].timestamp - of(ev, ApplicationStart)[0].timestamp

        (job,) = hs.app(a["id"], "jobs")
        assert job["status"] == "SUCCEEDED" and job["numTasks"] == 8 and job["numCompletedTasks"] == 8
        assert sorted(job["stageIds"]) == [0, 1] and job["description"] == "count events per key"

        stages = {s["stageId"]: s for s in hs.app(a["id"], "stages")}
        assert {s["status"] for s in stages.values()} == {"COMPLETE"}
        # Totals are the History Server's own aggregation of our task events.
        for sid in (0, 1):
            mine = [t.task_metrics for t in ends if t.stage_id == sid]
            assert stages[sid]["numCompleteTasks"] == 4
            assert stages[sid]["executorRunTime"] == sum(m.executor_run_time for m in mine)
            assert stages[sid]["jvmGcTime"] == sum(m.jvm_gc_time for m in mine)
        written = sum(t.task_metrics.shuffle_write.bytes_written for t in ends)
        assert stages[0]["shuffleWriteBytes"] == written == stages[1]["shuffleReadBytes"]
        assert stages[0]["shuffleWriteRecords"] == stages[1]["shuffleReadRecords"]
        assert stages[1]["shuffleRemoteBytesRead"] + stages[1]["shuffleLocalBytesRead"] == written

        tasks = {t["taskId"]: t for t in hs.all_tasks(a["id"], 1)}
        for t in ends:
            if t.stage_id != 1:
                continue
            got, ti = tasks[t.task_info.task_id], t.task_info
            assert got["executorId"] == ti.executor_id and got["host"] == ti.host
            assert got["taskLocality"] == "NODE_LOCAL" and got["status"] == "SUCCESS"
            assert got["duration"] == ti.finish_time - ti.launch_time

        summary = hs.app(a["id"], "stages/0/0/taskSummary", quantiles="0,1.0")
        durations = sorted(t.task_info.finish_time - t.task_info.launch_time for t in ends if t.stage_id == 0)
        assert summary["duration"] == [durations[0], durations[-1]]

        execs = {e["id"]: e for e in hs.app(a["id"], "allexecutors")}
        assert set(execs) == {"driver", "1", "2"}
        assert sum(execs[e]["completedTasks"] for e in ("1", "2")) == 8
        for eid in ("1", "2"):
            assert execs[eid]["totalCores"] == 2
            assert execs[eid]["maxMemory"] == unified_memory_max(4 * 1024**3)
            assert yapp.container_id(int(eid) + 1) in execs[eid]["executorLogs"]["stderr"]

        env = hs.app(a["id"], "environment")
        props = dict(env["sparkProperties"])
        assert props["spark.master"] == "yarn" and props["spark.submit.deployMode"] == "cluster"
        assert props["spark.app.id"] == yapp.app_id

        # The UI pages themselves render (HTML served without errors).
        base = f"{url}/history/{a['id']}/1"
        for page in ("jobs/", "jobs/job/?id=0", "stages/", "stages/stage/?id=0&attempt=0", "executors/",
                     "environment/", "storage/"):
            r = requests.get(f"{base}/{page}", timeout=60)
            assert r.status_code == 200, page
