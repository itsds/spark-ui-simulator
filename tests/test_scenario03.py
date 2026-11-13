"""Scenario 03 (memoryOverhead kills) without the History Server: the only cause is release 2.4's geo UDF,
whose every task memory-maps a 2.0 GiB IP database. The symptoms follow from it (NodeManager pmem kills of
executors whose heaps are far from full, ExecutorLostFailures counted towards spark.task.maxFailures, a task
index lost 4 times, the job, the query and the application failing, YARN's second attempt failing the same
way), the night before was fine, and the fixes the solution recommends are verified here. The REST-checked
evidence is in `test_scenario03_hs.py`."""

from __future__ import annotations

import copy
import json
import re

import pytest

from sparksim.engine.emit import build_events
from sparksim.engine.runtime import run_attempts
from sparksim.eventlog import event_to_line
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict
from sparksim.yarnlogs.driver import am_stderr, am_stdout

SCENARIO = "03-memory-overhead-kills"
GiB = 1024 ** 3
PMEM = re.compile(r"^Container killed by YARN for exceeding physical memory limits\. [0-9.]+ GB of 18 GB physical "
                  r"memory used\. Consider boosting spark\.executor\.memoryOverhead\.$")


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return run_attempts(sc.spec("incident"))


def variant(params=None, executors=None, conf=None):
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["params"].update(params or {})
    raw["job"]["executors"].update(executors or {})
    raw["job"]["conf"].update(conf or {})
    return run_attempts(scenario_from_dict(raw).spec("incident"))


def events(run) -> list[dict]:
    return [json.loads(event_to_line(e)) for e in build_events(run)]


def kills(run) -> int:
    return sum(1 for _, kind, _ in run.yarn_log if kind == "nm_kill_pmem")


def test_the_cause_is_the_release(sc):
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.sizing == bad.sizing and good.cluster == bad.cluster
    extra = [kv for kv in bad.conf if kv not in good.conf]  # --jars / --files of release 2.4, nothing else
    assert [k for k, _ in extra] == ["spark.yarn.dist.jars", "spark.yarn.dist.files"]
    assert "spark.executor.memoryOverhead" not in dict(bad.conf)
    assert bad.native_memory_per_task == 0 and not bad.executor_kills
    assert [(p.rsplit("/", 1)[1], n) for p, n in bad.dist_resources] == [
        ("dp-udfs-2.3.1.jar", 18_452_771), ("ip-intel-2026q4.mmdb", 2_143_289_344)]


def test_last_good_run_is_clean(sc):
    (run,) = run_attempts(sc.spec("last_good"))
    assert run.final_status == "SUCCEEDED" and kills(run) == 0 and len(run.executors) == 40 and len(run.jobs) == 8


def test_both_yarn_attempts_fail_on_pmem_kills(incident):
    assert [r.yapp.attempt for r in incident] == [1, 2]
    assert [r.final_status for r in incident] == ["FAILED", "FAILED"]
    assert [r.last_attempt for r in incident] == [False, True]
    a1, a2 = incident
    assert [kills(r) for r in incident] == [145, 173]  # the solution's side-by-side table
    assert [re.search(r"Task (\d+) in stage 2\.0", next(s.failure for s in r.stages.values() if s.failure)).group(1)
            for r in incident] == ["386", "2598"]
    assert a2.t0 > a1.app_end and a2.submit_time == a1.submit_time
    assert a2.yapp.am_container_id.endswith("_02_000001")
    for r in incident:
        assert kills(r) > 100  # containers die by the dozen; pmem kills never count towards maxNumFailures
        (stage,) = [s for s in r.stages.values() if s.failure]
        assert stage.native_funcs and stage.failure.startswith("Job aborted due to stage failure: Task ")
        assert " failed 4 times, most recent failure: Lost task " in stage.failure
        assert stage.failure.endswith("Consider boosting spark.executor.memoryOverhead.\nDriver stacktrace:")
        assert len(r.queries) == 1 and r.queries[0].error == stage.failure  # the later queries never ran


def test_failure_events(incident):
    for r in incident:
        ev = events(r)
        names = [e["Event"].rsplit(".", 1)[-1] for e in ev]
        assert names[-1] == "SparkListenerApplicationEnd"
        sc_i = next(i for i, e in enumerate(ev) if e["Event"] == "SparkListenerStageCompleted"
                    and "Failure Reason" in e["Stage Info"])
        assert names[sc_i + 1] == "SparkListenerJobEnd"
        job = ev[sc_i + 1]["Job Result"]
        assert job["Result"] == "JobFailed" and len(job["Exception"]["Stack Trace"]) == 15
        assert job["Exception"]["Message"] == ev[sc_i]["Stage Info"]["Failure Reason"]
        last_straw = ev[sc_i - 1] if names[sc_i - 1] == "SparkListenerTaskEnd" else next(
            e for e in reversed(ev[:sc_i]) if e["Event"] == "SparkListenerTaskEnd")
        assert last_straw["Task End Reason"]["Reason"] == "ExecutorLostFailure"
        after = [e for e in ev[sc_i + 2:] if e["Event"] == "SparkListenerTaskEnd"]
        assert after and all(not e["Task Info"]["Accumulables"] for e in after)  # stage gone: nothing merged
        killed = [e for e in after if e["Task End Reason"]["Reason"] == "TaskKilled"]
        assert killed and all(e["Task End Reason"]["Kill Reason"].startswith("Stage cancelled: Job aborted")
                              for e in killed)
        (end,) = [e for e in ev if e["Event"].endswith("SQLExecutionEnd")]
        assert end["errorMessage"] == job["Exception"]["Message"]
        removed = [e["Removed Reason"] for e in ev if e["Event"] == "SparkListenerExecutorRemoved"]
        assert removed and all(PMEM.match(x) for x in removed)
        # the heap is nowhere near full: it's the memory outside it
        peak = max(e["Task Executor Metrics"]["JVMHeapMemory"] for e in ev if e["Event"] == "SparkListenerTaskEnd")
        assert peak < 6 * GiB


def test_driver_and_executor_logs(incident):
    a1, a2 = incident
    for r in incident:
        err = am_stderr(r)
        assert "ERROR ApplicationMaster: User application exited with status 1" in err
        assert "Final app status: FAILED, exitCode: 1, (reason: User application exited with status 1)" in err
        assert re.search(r"ERROR TaskSetManager: Task \d+ in stage 2\.0 failed 4 times; aborting job", err)
        assert "Killing all running tasks in stage 2: Stage cancelled: Job aborted due to stage failure" in err
        assert "ip-intel-2026q4.mmdb -> resource" in err  # the AM localizes --files with every container
        out = am_stdout(r)
        assert out.startswith("Traceback (most recent call last):") and "daily_enrichment.py\", line 61" in out
        assert ": org.apache.spark.SparkException: Job aborted due to stage failure:" in out
    assert "Unregistering ApplicationMaster" not in am_stderr(a1)  # YARN starts attempt 2
    assert "Unregistering ApplicationMaster with FAILED (diag message: User application exited with status 1)" \
        in am_stderr(a2)


def minutes(run) -> float:
    return round((run.app_end - run.t0) / 60_000, 1)


def test_fixes(sc):
    """The numbers solution.md quotes for its fixes."""
    # tonight: room for four mappings (4 x 2.0 GiB) and the JVM's own native memory
    (r,) = variant(executors={"memory_overhead": "10g"})
    assert (r.final_status, kills(r), r.executors[0].container_mb, minutes(r)) == ("SUCCEEDED", 0, 26 * 1024, 7.3)
    # durable: one reader per executor JVM (a fixed UDF), and room for one mapping
    (r,) = variant(params={"geo_reader": "per_executor"}, executors={"memory_overhead": "4g"})
    assert (r.final_status, kills(r), r.executors[0].container_mb, minutes(r)) == ("SUCCEEDED", 0, 20 * 1024, 7.2)


def test_what_does_not_fix_it(sc):
    # a bigger heap brings 10 % more overhead, not 6+ GiB more: a coin flip
    a1, a2 = variant(executors={"memory": "24g"})
    assert a1.executors[0].container_mb == 27 * 1024
    assert (a1.final_status, a2.final_status, minutes(a2), kills(a2)) == ("FAILED", "SUCCEEDED", 8.2, 140)
    # the fixed UDF alone: one 2.0 GiB mapping per JVM still doesn't fit next to a fully grown 16 GiB heap
    (r,) = variant(params={"geo_reader": "per_executor"})
    assert r.final_status == "SUCCEEDED" and kills(r) > 50
    # 2 cores: two mappings still don't fit in 2 GiB
    (r,) = variant(executors={"cores": 2})
    assert r.final_status == "SUCCEEDED" and kills(r) > 100 and minutes(r) > 14
