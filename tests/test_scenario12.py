"""Scenario 12 (fetch failure / stage retry) without the History Server: the causes are a job submitted without
the external shuffle service and YARN preempting its youngest executors for another queue; the symptoms
(Resubmitted map tasks, FetchFailed, a failed stage attempt, map stages re-run, a skipped stage that runs)
follow from them, in the shape real Spark 3.5.9 gives them (`reference/event-logs/ref12-fetch-failure`), and
the fix the solution recommends (the shuffle service back on) takes the retries away. The REST-checked
evidence is in `test_scenario12_hs.py`."""

from __future__ import annotations

import collections
import dataclasses
import json
import re

import pytest

from sparksim.engine.emit import build_events
from sparksim.engine.runtime import AppRun
from sparksim.eventlog import (
    ExecutorLostFailure,
    ExecutorRemoved,
    StageCompleted,
    StageSubmitted,
    TaskEnd,
    TaskStart,
    event_to_line,
)
from sparksim.eventlog.reasons import FetchFailed, Resubmitted, Success
from sparksim.scenario import load_scenario
from sparksim.yarnlogs.driver import am_stderr
from sparksim.yarnlogs.executor import executor_stderr

SCENARIO = "12-fetch-failure-stage-retry"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    run = AppRun(sc.spec("incident")).run()
    return run, build_events(run)


@pytest.fixture(scope="module")
def last_good(sc):
    return AppRun(sc.spec("last_good")).run()


def minutes(run) -> float:
    return (run.app_end - run.t0) / 60_000


def of(events, cls):
    return [e for e in events if isinstance(e, cls)]


def test_the_causes(sc):
    good, bad = sc.spec("last_good"), sc.spec("incident")
    ess = "spark.shuffle.service.enabled"
    assert dict(good.conf)[ess] == "false" == dict(bad.conf)[ess]  # the job's own --conf, both nights
    assert good.conf == bad.conf and good.sizing == bad.sizing and good.cluster == bad.cluster
    assert not good.preemptions
    assert [(p.at_ms, p.containers) for p in bad.preemptions] == [(200_000, 6), (290_000, 4)]
    assert not bad.executor_kills and bad.native_memory_per_task == 0


def test_last_good_is_a_normal_night(last_good):
    assert 5.4 < minutes(last_good) < 5.6
    assert all(t.ok for t in last_good.task_records.values())
    assert not [k for k in last_good.stage_attempts if k[1] > 0]
    assert all(ex.removed_at is None for ex in last_good.executors)


def test_preemption_takes_the_youngest_containers(incident):
    run, ev = incident
    removed = {e.executor_id: e.removed_reason for e in of(ev, ExecutorRemoved)}
    assert len(removed) == 10
    lost = sorted((run.executor_by_id[e] for e in removed), key=lambda x: x.container_n)
    survivors = [x for x in run.executors if x.id not in removed]
    assert min(x.container_n for x in lost) > max(x.container_n for x in survivors)  # youngest first, never the AM
    for ex in lost:
        assert removed[ex.id] == f"Container {ex.container_id} on host: {ex.host.name} was preempted."
        assert ex.exit_status == -102 and not ex.exit_caused_by_app
    assert len(run.executors) == 40  # the queue has no room for replacements
    lost_tasks = [e for e in of(ev, TaskEnd) if isinstance(e.reason, ExecutorLostFailure)]
    assert lost_tasks and not any(e.reason.exit_caused_by_app for e in lost_tasks)  # not counted towards maxFailures


def test_lost_map_output_is_resubmitted(incident):
    """Executors lost while the events map stage runs: their finished map tasks are re-queued (Resubmitted), the
    stage still completes as one attempt (ref12 round 1)."""
    run, ev = incident
    events_map = next(s for s in run.stages.values() if not s.skipped and s.num_tasks == 5200)
    ends = [e for e in of(ev, TaskEnd) if (e.stage_id, e.stage_attempt_id) == (events_map.stage_id, 0)]
    kinds = collections.Counter(type(e.reason).__name__ for e in ends)
    assert kinds["Resubmitted"] > 500 and kinds["ExecutorLostFailure"] > 10
    assert kinds["Success"] == events_map.num_tasks + kinds["Resubmitted"]
    successes = {e.task_info.task_id: e for e in ends if isinstance(e.reason, Success)}
    for e in ends:
        if isinstance(e.reason, Resubmitted):  # the same TaskInfo, no metrics, an empty executor-metrics peak
            ok = successes[e.task_info.task_id]
            assert e.task_info == ok.task_info and e.task_metrics is None
            assert e.task_executor_metrics.jvm_heap_memory == -1
            assert run.executor_by_id[e.task_info.executor_id].removed_at is not None


def test_fetch_failure_fails_the_write_stage_once(incident):
    run, ev = incident
    write = next(s for s in run.stages.values() if s.task_type == "ResultTask" and s.num_tasks == 2400)
    sid = write.stage_id
    ends = [e for e in of(ev, TaskEnd) if e.stage_id == sid]
    ff = [e for e in ends if isinstance(e.reason, FetchFailed)]
    assert ff and all(e.stage_attempt_id == 0 for e in ff)
    done = {e.task_info.partition_id for e in ends if isinstance(e.reason, Success)}
    assert done == set(range(2400))  # every partition written once, by attempt 0 or 1
    completed = [e for e in of(ev, StageCompleted) if e.stage_info.stage_id == sid]
    failed, ok = completed
    assert failed.stage_info.attempt_id == 0 and failed.stage_info.failure_reason == ff[0].reason.message
    assert ok.stage_info.attempt_id == 1 and ok.stage_info.failure_reason is None
    first_ff = next(i for i, e in enumerate(ev) if e is ff[0])
    assert ev.index(failed) > first_ff and 0 < failed.stage_info.completion_time - ff[0].task_info.finish_time < 10
    retry = next(e for e in of(ev, StageSubmitted) if (e.stage_info.stage_id, e.stage_info.attempt_id) == (sid, 1))
    zero = {e.task_info.partition_id for e in ends if e.stage_attempt_id == 0 and isinstance(e.reason, Success)}
    assert retry.stage_info.num_tasks == 2400 - len(zero)  # only the partitions attempt 0 didn't finish
    starts = [e.task_info for e in of(ev, TaskStart) if (e.stage_id, e.stage_attempt_id) == (sid, 1)]
    assert sorted(t.partition_id for t in starts if t.attempt == 0) == sorted(set(range(2400)) - zero)
    assert any(t.index != t.partition_id for t in starts)
    for e in ff:  # the block manager of a dead executor, the map attempt that wrote the lost block
        bm = e.reason.block_manager_address
        dead = run.executor_by_id[bm.executor_id]
        assert dead.exit_at is not None and dead.exit_at <= e.task_info.finish_time
        assert (bm.host, bm.port) == (dead.host.name, dead.block_manager_port)
        writer = run.task_records[e.reason.map_id]
        assert writer.executor is dead and writer.partition == e.reason.map_index and writer.ok
        assert e.reason.reduce_id == e.task_info.partition_id
        assert not e.task_info.accumulables and e.task_metrics is None


def test_fetch_failed_message_is_sparks(incident):
    """FetchFailedException as the task thread throws it in a write after a sort-merge join (ref12), caused by
    ExecutorDeadException with the request's own frames and Java's '... N more'."""
    run, ev = incident
    msg = next(e.reason.message for e in of(ev, TaskEnd) if isinstance(e.reason, FetchFailed))
    lines = msg.split("\n")
    assert lines[0] == "org.apache.spark.shuffle.FetchFailedException"
    assert lines[1] == "\tat org.apache.spark.errors.SparkCoreErrors$.fetchFailedError(SparkCoreErrors.scala:437)"
    cause = next(i for i, x in enumerate(lines) if x.startswith("Caused by: "))
    assert re.fullmatch(r"Caused by: org\.apache\.spark\.ExecutorDeadException: \[INTERNAL_ERROR_NETWORK\] The "
                        r"relative remote executor\(Id: \d+\), which maintains the block data to fetch is dead\.",
                        lines[cause])
    task = lines[1:cause]
    assert any("sort_addToSorter_0$" in x for x in task) and any("smj_findNextJoinRows_0$" in x for x in task)
    assert task[-1] == "\tat java.base/java.lang.Thread.run(Thread.java:840)"
    assert lines[cause + 1].endswith("NettyBlockTransferService$$anon$2.createAndStart(NettyBlockTransferService."
                                     "scala:141)")
    assert msg.endswith(" more\n") or msg.endswith("(Thread.java:840)\n")


def test_skipped_stages_rerun_with_spark_attempt_numbers(incident):
    """The final job re-creates the finished map stages. One that is available is skipped (its next attempt
    becomes 1: increaseAttemptIdOnFirstSkip); one that lost outputs before the job started runs as attempt 0."""
    run, ev = incident
    final = run.jobs[3]
    recreated = [run.stages[s] for s in final.stage_ids if run.stages[s].orig is not None]
    attempts = {r.stage_id: [a.attempt for a in run.attempts_of(r)] for r in recreated}
    started_missing = set(final.missing_parents)
    assert started_missing
    for sid, atts in attempts.items():
        assert atts and atts == sorted(atts)
        assert atts[0] == (0 if sid in started_missing else 1)
    submitted = [(e.stage_info.stage_id, e.stage_info.attempt_id) for e in of(ev, StageSubmitted)]
    assert all((sid, a) in submitted for sid, atts in attempts.items() for a in atts)


def test_the_cost_and_the_fix(sc, incident, last_good):
    run, _ = incident
    assert 7.4 < minutes(run) < 7.8 and minutes(run) > 1.3 * minutes(last_good)
    spec = sc.spec("incident")
    fixed = AppRun(dataclasses.replace(spec, conf=tuple(c for c in spec.conf
                                                        if c[0] != "spark.shuffle.service.enabled"))).run()
    assert fixed.ess_enabled
    assert not any(t.resubmitted or isinstance(t.failure, FetchFailed) for t in fixed.task_records.values())
    assert not [k for k in fixed.stage_attempts if k[1] > 0]
    assert minutes(fixed) < 6.0  # the same preemptions cost ~20 s with the shuffle service, ~2 min without


def test_container_logs(incident):
    run, _ = incident
    am = am_stderr(run)
    assert re.search(r"INFO YarnAllocator: Completed container container_e23_\S+ on host: \S+ \(state: COMPLETE, "
                     r"exit status: -102\)\n\S+ \S+ INFO YarnAllocator: Container \S+ on host: \S+ was preempted\.", am)
    assert re.search(r"INFO DAGScheduler: Shuffle files lost for executor: \d+ \(epoch \d+\)", am)
    assert re.search(r"INFO DAGScheduler: Resubmitted ShuffleMapTask\(\d+, \d+\), so marking it as still running\.", am)
    assert re.search(r"WARN TaskSetManager: Lost task \d+\.\d+ in stage 5\.0 \(TID \d+\) \(\S+ executor \d+\): "
                     r"FetchFailed\(BlockManagerId\(\d+, \S+, \d+, None\), shuffleId=\d, mapIndex=\d+, mapId=\d+, "
                     r"reduceId=\d+, message=\norg\.apache\.spark\.shuffle\.FetchFailedException\n", am)
    assert re.search(r"INFO DAGScheduler: Marking ResultStage 5 \(parquet at NativeMethodAccessorImpl\.java:0\) as "
                     r"failed due to a fetch failure from ShuffleMapStage \d \(parquet at", am)
    assert "INFO DAGScheduler: Resubmitting failed stages" in am
    assert re.search(r"failed because while it was being computed, its executor exited for a reason unrelated to "
                     r"the task\. Not counting this failure towards the maximum number of failures for the task\.", am)
    tk = next(t for t in run.task_records.values() if isinstance(t.failure, FetchFailed))
    log = executor_stderr(run, tk.executor)
    dead = tk.extra_fetch[2]
    assert re.search(rf"ERROR RetryingBlockTransferor: Exception while beginning fetch of \d+ outstanding blocks.*\n"
                     rf"org\.apache\.spark\.ExecutorDeadException: \[INTERNAL_ERROR_NETWORK\] The relative remote "
                     rf"executor\(Id: {dead.id}\)", log)
    assert f"ERROR ShuffleBlockFetcherIterator: Failed to get block(s) from {dead.host.name}:" in log
    assert f"Finished task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}.{tk.attempt} (TID {tk.task_id})" \
        not in log
    gone = executor_stderr(run, dead)
    assert "ERROR CoarseGrainedExecutorBackend: RECEIVED SIGNAL TERM" in gone and "BlockManager stopped" in gone


def test_events_serialize(incident):
    _, ev = incident
    for e in ev:
        json.loads(event_to_line(e))
