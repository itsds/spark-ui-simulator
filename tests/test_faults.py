"""Fault mechanisms (`sparksim.scenario.faults`) on scaled-down variants of Scenario 00.

Each test injects a cause through `scenario.yaml`'s fault list and checks that the symptom a learner would
look for comes out of the simulation: a key skew gives one huge shuffle partition (in the partition Spark's
Murmur3 picks) that spills; a slow node slows every task on it; native memory gets containers killed by
YARN; a SIGKILL ends an executor with exit code 137.
"""

from __future__ import annotations

import copy
import re
import statistics

import numpy as np
import pytest

from sparksim.engine.runtime import quantile
from sparksim.eventlog import ExecutorLostFailure, ExecutorRemoved, TaskEnd
from sparksim.model.murmur3 import partition_id
from sparksim.model.shuffle import key_shares, row_shares, row_shares_split
from sparksim.model.workload import HotKey, KeySpace
from sparksim.scenario import load_raw, scenario_from_dict
from sparksim.yarnlogs.executor import executor_stderr

SCALE = 0.15


def variant(*faults, scale=SCALE, conf=None):
    """Scenario 00's job on `scale` of its data: a last good run and an incident run with `faults`."""
    raw = copy.deepcopy(load_raw("00-healthy-baseline"))
    base = raw["runs"].pop("baseline")
    if conf:
        raw["job"]["conf"].update(conf)
    data = {"events": {"files": int(4300 * scale)}, "customers": {"files": int(520 * scale)}}
    raw["runs"] = {"last_good": {**base, "data": data},
                   "incident": {**base, "app_seq": base["app_seq"] + 1, "started_at": "2026-11-04T02:10:07.318Z",
                                "data": data, "faults": list(faults)}}
    return scenario_from_dict(raw)


def of(events, cls):
    return [e for e in events if isinstance(e, cls)]


def join_stage(run):
    """The stage that sort-merge joins events with customers (reads two shuffles)."""
    return next(st for st in run.stages.values() if not st.skipped and len(st.spec.shuffle_inputs) == 2)


# --------------------------------------------------------------------------- key distributions (unit)


def test_hot_key_lands_in_its_spark_partition():
    ks = KeySpace("customer_id", distinct=1_000_000, freq_cv=1.0, hot=(HotKey((-1,), 0.2),))
    sh = row_shares(ks, 2400, seed=7)
    p = partition_id([-1], ["bigint"], 2400)
    assert p == 1993  # pmod(Murmur3Hash(-1L, 42), 2400), checked against PySpark
    assert sh.sum() == pytest.approx(1.0)
    assert sh[p] == pytest.approx(0.2 + 0.8 / 2400, rel=0.05)
    assert np.argmax(sh) == p
    others = np.delete(sh, p)
    assert others.max() < 3 / 2400
    # the tail (every other key) is what files written from a reducer range are clustered on
    _, tail = row_shares_split(ks, 2400, seed=7)
    assert tail[p] < 2 / 2400


def test_unskewed_keys_are_unchanged():
    ks = KeySpace("customer_id", distinct=208_000_000, freq_cv=2.5)
    a, b = row_shares_split(ks, 2400, seed=3)
    assert a is b
    assert np.array_equal(a, row_shares(ks, 2400, seed=3))


def test_zipf_head_and_tail():
    ks = KeySpace("sku", distinct=2_000_000, zipf=1.1)
    sh = row_shares(ks, 1000, seed=1)
    assert sh.sum() == pytest.approx(1.0)
    # rank 1 alone holds 1/H(N, 1.1) ≈ 9 % of the rows: the biggest partition is far above the median
    assert sh.max() > 0.08
    assert sh.max() / np.median(sh) > 50
    flat = row_shares(KeySpace("sku", distinct=2_000_000, freq_cv=1.0), 1000, seed=1)
    assert flat.max() / np.median(flat) < 1.2
    assert np.array_equal(key_shares(ks, 1000, 1), key_shares(KeySpace("sku", 2_000_000), 1000, 1))


# --------------------------------------------------------------------------- key_skew


@pytest.fixture(scope="module")
def skewed():
    sc = variant({"type": "key_skew", "dataset": "events", "column": "customer_id",
                  "hot_keys": [{"value": -1, "share": 0.12}]})
    return sc.build("last_good"), sc.build("incident")


def test_key_skew_makes_one_straggler_that_spills(skewed):
    (good, _), (bad, _) = skewed
    g, b = join_stage(good), join_stage(bad)
    hot = max(b.tasks, key=lambda t: t.result.metrics.shuffle_read.records_read)
    # the reducer of customer_id = -1, left alone by AQE's coalescing (it is far above the target size)
    assert b.plans[hot.index].reads[0][1:] == (1993, 1994)
    recs = [t.result.metrics.shuffle_read.records_read for t in b.tasks]
    assert hot.result.metrics.shuffle_read.records_read > 0.11 * sum(recs)
    durs = [t.duration for t in b.tasks]
    assert max(durs) > 50 * quantile(durs, 0.5)
    assert max(durs) == hot.duration
    # spill follows from the partition's size against the task's execution memory: only the hot task
    spilled = [t for t in b.tasks if t.result.metrics.memory_bytes_spilled]
    assert spilled == [hot]
    m = hot.result.metrics
    assert m.disk_bytes_spilled < m.memory_bytes_spilled
    # both sorts that buffer the hot key's rows spill: the join's input sort and the write's sort
    sorts = [op for op in hot.result.trace if op.kind == "sort" and op.spill > 0]
    assert len(sorts) == 2
    assert sum(op.spill for op in hot.result.trace) == pytest.approx(m.memory_bytes_spilled, abs=2)
    # the last good run of the same job: no straggler, no spill
    gd = [t.duration for t in g.tasks]
    assert max(gd) < 2.5 * quantile(gd, 0.5)
    assert not any(t.result.metrics.memory_bytes_spilled for t in g.tasks)


def test_key_skew_shows_in_sql_metrics_and_logs(skewed):
    _, (bad, ev) = skewed
    st = join_stage(bad)
    hot = max(st.tasks, key=lambda t: t.duration)
    # SQL tab: each Sort reports its own spill size
    names = {}
    for n in st.plan_root.walk():
        for key in getattr(n, "metric_ids", {}) or {}:
            names[n.metric_id(key)] = (n.node_name, key)
    spills = [v for aid, v in hot.sql if names.get(aid, ("", ""))[1] == "spillSize" and v > 0]
    assert len(spills) == 2 and sum(spills) == pytest.approx(hot.result.metrics.memory_bytes_spilled, abs=2)
    log = executor_stderr(bad, hot.executor)
    lines = [ln for ln in log.splitlines() if "UnsafeExternalSorter: Thread" in ln and "spilling sort data" in ln]
    assert len(lines) > 10
    assert "(1  time so far)" in lines[0] and "times so far)" in lines[-1]
    assert sum(1 for ln in lines if "(1  time so far)" in ln) == 2  # two sorters


def test_skew_join_on_is_refused_not_faked():
    """With AQE skew join on (Spark's default) Spark would split the hot partition: not modelled, so refuse."""
    sc = variant({"type": "key_skew", "dataset": "events", "column": "customer_id",
                  "hot_keys": [{"value": -1, "share": 0.12}]},
                 conf={"spark.sql.adaptive.skewJoin.enabled": True})
    with pytest.raises(NotImplementedError, match="OptimizeSkewedJoin"):
        sc.build("incident")


# --------------------------------------------------------------------------- config


def test_config_fault_changes_what_spark_reads():
    sc = variant({"type": "config", "set": {"spark.sql.shuffle.partitions": 600,
                                            "spark.executor.memoryOverhead": "3g",
                                            "spark.sql.adaptive.coalescePartitions.enabled": False},
                  "unset": ["spark.sql.files.maxPartitionBytes"]})
    spec = sc.spec("incident")
    conf = dict(spec.conf)
    assert conf["spark.sql.shuffle.partitions"] == "600"
    assert "spark.sql.files.maxPartitionBytes" not in conf
    assert spec.sizing.overhead_mb == 3072 and conf["spark.executor.memoryOverhead"] == "3g"
    keys = [k for k, _ in spec.conf]
    assert keys.index("spark.sql.shuffle.partitions") == 2  # set in place: --conf order kept
    run, _ = sc.build("incident")
    assert len(join_stage(run).tasks) == 600
    assert all(ex.container_mb == 19 * 1024 for ex in run.executors)  # 16g + 3g, on the 1 GiB grid
    # no AQE coalescing: the aggregations' final stages read all 600 partitions
    finals = [st for st in run.stages.values() if not st.skipped and st.spec.shuffle_inputs and st.query > 0]
    assert finals and all(len(st.tasks) == 600 for st in finals)


# --------------------------------------------------------------------------- slow_node


def test_slow_node_slows_every_task_on_it():
    good, _ = variant({"type": "config", "set": {}}).build("last_good")
    host = good.executors[0].host.name
    sc = variant({"type": "slow_node", "host": host, "cpu": 0.5, "disk": 0.25})
    run, _ = sc.build("incident")
    assert any(e.host.name == host for e in run.executors)
    assert run.cluster.host(host).disk_factor == 0.25
    scan = next(st for st in run.stages.values() if not st.skipped and st.num_tasks > 500)
    on = [t.duration for t in scan.tasks if t.executor.host.name == host]
    off = [t.duration for t in scan.tasks if t.executor.host.name != host]
    assert statistics.median(on) > 2.5 * statistics.median(off)


# --------------------------------------------------------------------------- memory_pressure


PMEM = re.compile(r"^Container killed by YARN for exceeding physical memory limits\. ")


def test_memory_pressure_gets_containers_killed():
    run, ev = variant({"type": "memory_pressure", "native_per_task": "1g"}, scale=0.3).build("incident")
    removed = of(ev, ExecutorRemoved)
    assert removed and all(PMEM.match(r.removed_reason) for r in removed)
    lost = [e for e in of(ev, TaskEnd) if isinstance(e.reason, ExecutorLostFailure)]
    assert lost
    good, gev = variant({"type": "config", "set": {}}, scale=0.3).build("last_good")
    assert not of(gev, ExecutorRemoved)


# --------------------------------------------------------------------------- executor_killed


def test_executor_sigkill_exit_137():
    sc = variant({"type": "executor_killed", "executor": "7", "at": "30s"})
    run, ev = sc.build("incident")
    ex = run.executor_by_id["7"]
    assert ex.exit_status == 137 and ex.exit_at - run.t0 == 30_000
    (r1, r2) = [r for r in of(ev, ExecutorRemoved) if r.executor_id == "7"]
    assert r1.removed_reason.startswith(f"Container from a bad node: {ex.container_id} on host: {ex.host.name}. "
                                        "Exit status: 137. Diagnostics: [")
    assert r1.removed_reason.endswith("Killed by external signal\n.")
    lost = [e for e in of(ev, TaskEnd) if isinstance(e.reason, ExecutorLostFailure)]
    assert lost and all(e.reason.executor_id == "7" and e.reason.exit_caused_by_app for e in lost)
    # every lost task ran again and succeeded; YARN gave the app a replacement container
    ok = {(e.stage_id, e.task_info.index) for e in of(ev, TaskEnd) if e.reason.successful}
    assert all((e.stage_id, e.task_info.index) in ok for e in lost)
    assert "41" in run.executor_by_id
    # the killed JVM's log just stops: no SIGTERM handler, no shutdown hook
    log = executor_stderr(run, ex)
    assert "Shutdown hook called" not in log and "RECEIVED SIGNAL" not in log
    last = log.rstrip().splitlines()[-1]
    assert last[:17] <= "26/11/04 02:10:37"  # log4j yy/MM/dd HH:mm:ss, nothing after the kill


def test_unknown_executor_is_an_error():
    sc = variant({"type": "executor_killed", "executor": "99", "at": "30s"})
    with pytest.raises(ValueError, match="executor 99 is not running"):
        sc.build("incident")
