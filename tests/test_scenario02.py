"""Scenario 02 (too few shuffle partitions) without the History Server: the cause is in the submission
only, the symptoms (every reduce task spills, a partial second wave) follow from it, and the fix the
solution recommends brings the job back. The REST-checked evidence is in `test_scenario02_hs.py`."""

from __future__ import annotations

import copy
import dataclasses
import re

import pytest

from sparksim.engine.runtime import AppRun, quantile
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict
from sparksim.sql.dataframe import SparkSession
from sparksim.sql.expr import ExprIds
from sparksim.yarnlogs.executor import executor_stderr

SCENARIO = "02-shuffle-partitions-spill"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return AppRun(sc.spec("incident")).run()


@pytest.fixture(scope="module")
def last_good(sc):
    return AppRun(sc.spec("last_good")).run()


def join_stage(run):
    return next(st for st in run.stages.values() if not st.skipped and len(st.spec.shuffle_inputs) == 2)


def spilled(st):
    return [t for t in st.tasks if t.result.metrics.memory_bytes_spilled]


def with_fix(conf: dict):
    """The incident run with `conf` set after the migration's change (what the fix would submit)."""
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["faults"].append({"type": "config", "set": conf})
    return AppRun(scenario_from_dict(raw).spec("incident")).run()


def test_the_cause_is_only_in_the_submission(sc):
    """Same job, data, cluster and executors; the incident's conf lost the shuffle partitions and gained two
    harmless Airflow settings."""
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.sizing == bad.sizing and good.cluster == bad.cluster
    assert [dataclasses.replace(i, path="") for i in good.inputs] == [dataclasses.replace(i, path="")
                                                                      for i in bad.inputs]
    g, b = dict(good.conf), dict(bad.conf)
    assert g["spark.sql.shuffle.partitions"] == "2400" and "spark.sql.shuffle.partitions" not in b
    assert {k: v for k, v in b.items() if g.get(k) != v} == {
        "spark.yarn.maxAppAttempts": "1", "spark.yarn.tags": "airflow,dag_id=clickstream_daily,task_id=enrich_events"}
    assert bad.native_memory_per_task == 0 and not bad.executor_kills and not bad.cluster.host_faults
    assert all(not ks.hot for i in bad.inputs for ks in i.keys.values())


def test_every_reduce_task_spills(incident, last_good):
    good, bad = join_stage(last_good), join_stage(incident)
    assert len(good.tasks) == 2400 and len(bad.tasks) == 200  # Spark's default; AQE can't raise it
    assert not spilled(good)
    assert spilled(bad) == bad.tasks
    durs = [t.duration for t in bad.tasks]
    assert max(durs) < 1.3 * quantile(durs, 0.5) < 1.3 * 1.2 * min(durs)  # uniform: too big, not skewed
    assert quantile(durs, 0.5) > 30 * quantile([t.duration for t in good.tasks], 0.5)
    # the map side wrote the same data: only the reduce side's partitioning changed
    w = [sum(t.result.metrics.shuffle_write.bytes_written for t in run.stages[2].tasks)
         for run in (last_good, incident)]
    assert w[1] == pytest.approx(w[0], rel=0.02)
    # each task is held at its share of the executor's unified memory: (heap - 300 MiB) x 0.6 / 4 cores
    peak = {t.result.metrics.peak_execution_memory for t in bad.tasks}
    assert len(peak) == 1 and 2.3 * 2**30 < peak.pop() < 2.4 * 2**30
    assert not any(t.failure for t in incident.task_records.values())
    assert not any(e.removed_at for e in incident.executors)


def test_200_tasks_on_160_cores_leave_a_partial_wave(incident):
    st = join_stage(incident)
    first = min(t.launch for t in st.tasks)
    wave1 = [t for t in st.tasks if t.launch - first < 1_000]
    wave2 = [t for t in st.tasks if t.launch - first >= 1_000]
    assert len(wave1) == 160 and len(wave2) == 40
    assert min(t.launch for t in wave2) >= min(t.finish for t in wave1)


def test_executor_log_spills_like_spark(incident):
    """Concurrent tasks log on their own threads; each sorter counts its spills from 0; the Parquet file is
    opened only after the write's sort has consumed its input (after its spills)."""
    st = join_stage(incident)
    ex = next(t for t in st.tasks if t.index == 0).executor
    log = executor_stderr(incident, ex).splitlines()
    spills = [(i, re.search(r"Thread (\d+) spilling sort data of (\S+ \S+) to disk \((\d+) +(times?) so far\)", ln))
              for i, ln in enumerate(log) if "spilling sort data" in ln]
    mine = [t for t in st.tasks if t.executor is ex]
    assert len(spills) == 4 * len(mine)  # two spills for each of the two sorts of every task
    by_thread: dict[str, list] = {}
    for i, m in spills:
        by_thread.setdefault(m.group(1), []).append((i, m))
        n = int(m.group(3))
        assert m.group(4) == ("times" if n > 1 else "time")
    first_wave = [m for _, m in spills[:8]]
    assert len({m.group(1) for m in first_wave}) == 4  # four tasks at once, four threads
    for thread, seq in by_thread.items():
        counts = [int(m.group(3)) for _, m in seq]
        assert counts == [0, 1, 0, 1] * (len(counts) // 4), thread  # join sort, then write sort, per task
    # The two sorts spill different sizes (the write sort holds the wider joined rows); no Parquet file of
    # stage 5 opens before the first write-sort spill, while the committer is set up at the start.
    start = next(i for i, ln in enumerate(log) if "Running task" in ln and "in stage 5.0" in ln)
    end = max(i for i, ln in enumerate(log) if "Finished task" in ln and "in stage 5.0" in ln)
    join_size = spills[0][1].group(2)
    write_spills = [i for i, m in spills if m.group(2) != join_size]
    opens = [i for i, ln in enumerate(log) if "ParquetRecordWriter [" in ln and start < i < end]
    setups = [i for i, ln in enumerate(log) if "Output Committer Algorithm" in ln and start < i < end]
    assert len(write_spills) == len(spills) // 2 and len(opens) == len(mine)
    assert setups[0] < spills[0][0] < write_spills[0] < opens[0]


def test_initial_partition_num_follows_spark():
    """SQLConf.numShufflePartitions: AQE's initial partition number wins only while coalescing is on."""
    conf = {"spark.sql.adaptive.coalescePartitions.initialPartitionNum": "8000"}
    assert SparkSession(ExprIds(), conf).shuffle_partitions == 8000
    assert SparkSession(ExprIds(), {**conf, "spark.sql.adaptive.coalescePartitions.enabled": "false"}
                        ).shuffle_partitions == 200
    assert SparkSession(ExprIds(), {"spark.sql.shuffle.partitions": "2400"}).shuffle_partitions == 2400


@pytest.mark.parametrize("fix, tasks", [
    ({"spark.sql.shuffle.partitions": 2400}, (2400, 2400)),  # tonight's fix: the last good run's conf
    ({"spark.sql.adaptive.coalescePartitions.initialPartitionNum": 8000,  # the durable one: AQE coalesces down
      "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m"}, (2000, 3000)),
])
def test_the_fix_brings_the_job_back(fix, tasks, last_good):
    run = with_fix(fix)
    st = join_stage(run)
    assert tasks[0] <= len(st.tasks) <= tasks[1]
    assert not spilled(st)
    good = join_stage(last_good)
    assert (st.completed - st.submitted) < 1.15 * (good.completed - good.submitted)
