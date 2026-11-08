"""Speculative execution (`spark.speculation`) and `spark.yarn.exclude.nodes`, on a scaled Scenario 00 whose
executor host is CPU-throttled (`slow_node`). The rules come from Spark 3.5.9's TaskSetManager and are checked
here against what the simulation does; the event shapes against the real run in
`reference/event-logs/ref09-speculation` (a throttled NodeManager on a two-node YARN cluster)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_faults import variant

from sparksim.eventlog import StageCompleted, TaskEnd, TaskKilled, TaskStart, read_event_log
from sparksim.sched.scheduler import SpeculationConf
from sparksim.yarnlogs.driver import am_stderr
from sparksim.yarnlogs.executor import executor_stderr

ROOT = Path(__file__).resolve().parents[1]
REF09 = ROOT / "reference" / "event-logs" / "ref09-speculation"
SPEC = {"spark.speculation": "true"}


def slow_host() -> str:
    good, _ = variant({"type": "config", "set": {}}).build("last_good")
    return good.executors[5].host.name


@pytest.fixture(scope="module")
def host():
    return slow_host()


@pytest.fixture(scope="module")
def spec_run(host):
    return variant({"type": "slow_node", "host": host, "cpu": 0.05}, conf=SPEC).build("incident")


@pytest.fixture(scope="module")
def plain_run(host):
    return variant({"type": "slow_node", "host": host, "cpu": 0.05}).build("incident")


def attempts(run):
    by: dict[tuple[int, int], list] = {}
    for t in run.task_records.values():
        by.setdefault((t.stage_id, t.index), []).append(t)
    return by


def test_conf_defaults_and_parsing():
    c = SpeculationConf.from_conf({})
    assert (c.enabled, c.interval_ms, c.multiplier, c.quantile, c.min_task_runtime_ms) == (False, 100, 1.5, 0.75, 100)
    assert c.efficiency and c.task_duration_threshold_ms is None
    c = SpeculationConf.from_conf({"spark.speculation": "true", "spark.speculation.interval": "1s",
                                   "spark.speculation.minTaskRuntime": "250ms", "spark.speculation.quantile": "0.9"})
    assert (c.enabled, c.interval_ms, c.min_task_runtime_ms, c.quantile) == (True, 1000, 250, 0.9)


def test_copies_win_and_originals_are_killed(spec_run, plain_run, host):
    run, _ = spec_run
    plain, _ = plain_run
    assert not any(t.speculative or t.killed for t in plain.task_records.values())  # off by default
    by = attempts(run)
    spec = [t for t in run.task_records.values() if t.speculative]
    assert spec and any(t.ok for t in spec)
    for (_sid, _i), ts in by.items():
        ok = [t for t in ts if t.ok]
        assert len(ok) == 1, ts  # exactly one successful attempt per task
        assert [t.task_attempt for t in sorted(ts, key=lambda t: t.task_id)] == list(range(len(ts)))
        if len(ts) > 1:
            assert len({t.executor.host.name for t in ts}) == len(ts)  # a copy never runs where the task ran
            for k in (t for t in ts if not t.ok):
                assert k.killed in ("another attempt succeeded", "Stage finished",
                                    "Finish but did not commit due to another attempt succeeded")
                assert k.finish >= ok[0].finish  # killed only after the winner succeeded
    assert all(t.executor.host.name == host for t in run.task_records.values() if t.killed and not t.speculative)
    # the stage tail no longer waits for the slow host
    assert run.app_end - run.t0 < 0.85 * (plain.app_end - plain.t0)


def test_speculation_waits_for_the_quantile_and_the_threshold(spec_run):
    run, _ = spec_run
    log = run.sched.speculation_log
    assert log
    for t, sid, _att, index, _host, threshold, _n in log:
        st = run.stages[sid]
        done = sorted(x.finish - x.launch for x in st.tasks if x.ok and x.finish <= t)
        assert len(done) >= max(int(0.75 * st.num_tasks), 1)
        assert threshold == max(1.5 * done[len(done) // 2], 100)
        orig = next(x for x in st.tasks if x.index == index and x.task_attempt == 0)
        assert t - orig.launch > threshold
    # a copy launches after its task was marked, on a free core
    marked = {(sid, index): t for t, sid, _a, index, _h, _thr, _n in log}
    for x in run.task_records.values():
        if x.speculative:
            assert x.launch >= marked[(x.stage_id, x.index)]


def test_killed_task_events_match_ref09(spec_run):
    run, events = spec_run
    killed = [e for e in events if isinstance(e, TaskEnd) and isinstance(e.reason, TaskKilled)]
    assert killed
    completed = {(e.stage_info.stage_id, e.stage_info.attempt_id): i for i, e in enumerate(events)
                 if isinstance(e, StageCompleted)}
    for e in killed:
        assert e.task_info.killed and not e.task_info.failed
        if e.task_metrics is not None:
            m = e.task_metrics
            assert m.executor_deserialize_time == 0 and m.executor_cpu_time == 0 and m.result_size == 0
            assert m.shuffle_write.bytes_written == 0 and m.output.records_written == 0
            assert e.reason.accum_updates and all(a.name is None and not a.internal for a in e.reason.accum_updates)
        i = events.index(e)
        if i > completed[(e.stage_id, e.stage_attempt_id)]:
            assert e.task_info.accumulables == []  # the stage left the DAGScheduler: nothing merged
    assert any(events.index(e) > completed[(e.stage_id, e.stage_attempt_id)] for e in killed)
    assert any(isinstance(e, TaskStart) and e.task_info.speculative for e in events)
    # The same JSON shape as a real killed attempt and a real speculative launch (ref09).
    real = [json.loads(line) for line in REF09.open()]
    rk = next(e for e in real
              if e["Event"] == "SparkListenerTaskEnd" and e["Task End Reason"]["Reason"] == "TaskKilled")
    rs = next(e for e in real if e["Event"] == "SparkListenerTaskStart" and e["Task Info"]["Speculative"])
    ours_k = next(e for e in killed if e.task_metrics is not None).to_json()
    ours_s = next(e for e in events if isinstance(e, TaskStart) and e.task_info.speculative).to_json()
    for ours, ref in ((ours_k, rk), (ours_s, rs)):
        assert list(ours) == list(ref)
        assert list(ours["Task Info"]) == list(ref["Task Info"])
    assert list(ours_k["Task End Reason"]) == list(rk["Task End Reason"])
    assert list(ours_k["Task Metrics"]) == list(rk["Task Metrics"])
    assert not any(e["Event"].endswith("SpeculativeTaskSubmitted") for e in real)  # never written to the log


def test_driver_and_executor_logs(spec_run, host):
    run, _ = spec_run
    drv = am_stderr(run)
    assert "INFO YarnClusterScheduler: Starting speculative execution thread" in drv
    assert "as speculatable because it ran more than" in drv
    killed = [t for t in run.task_records.values() if t.killed and t.killed_metrics is not None]
    k = next(t for t in killed if t.killed == "another attempt succeeded")
    name = f"task {k.index}.{k.task_attempt} in stage {k.stage_id}.{k.attempt} (TID {k.task_id})"
    assert f"Killing attempt {k.task_attempt} for {name} on {k.executor.host.name} as the attempt" in drv
    assert f"WARN TaskSetManager: Lost {name} ({k.executor.host.name} executor {k.executor.id}): TaskKilled " \
           "(another attempt succeeded)" in drv
    ex = k.executor
    log = executor_stderr(run, ex)
    assert f"INFO Executor: Executor is trying to kill {name}, reason: another attempt succeeded" in log
    assert f"killed {name}, reason: another attempt succeeded" in log
    assert f"Finished {name}." not in log
    lines = log.splitlines()
    end = max(i for i, ln in enumerate(lines) if f"killed {name}" in ln)
    assert not any(f"(TID {k.task_id})" in ln for ln in lines[end + 1:])  # nothing of it after the kill


def test_excluded_node_gets_no_executor(host):
    run, _ = variant({"type": "config", "set": {"spark.yarn.exclude.nodes": host}}).build("incident")
    assert run.excluded_nodes == [host]
    assert len(run.executors) == 40 and all(e.host.name != host for e in run.executors)
    assert ("YarnAllocatorNodeHealthTracker: adding nodes to YARN application master's excluded node list: "
            f"List({host})") in am_stderr(run)


def test_ref09_is_a_real_speculation_capture():
    """The reference: speculative copies on the healthy host, originals on the throttled one killed."""
    events = list(read_event_log(REF09))
    starts = [e for e in events if isinstance(e, TaskStart) and e.task_info.speculative]
    killed = [e for e in events if isinstance(e, TaskEnd) and isinstance(e.reason, TaskKilled)]
    assert {e.task_info.host for e in starts} == {"localhost"}
    assert {e.reason.kill_reason for e in killed} == {"another attempt succeeded", "Stage finished"}
    assert {e.task_info.host for e in killed if not e.task_info.speculative} == {"slownode"}
