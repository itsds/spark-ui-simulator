"""Scenario 07 (exploding join) without the History Server: the cause is upstream data, the customer table turned
into an SCD2 history (fault `duplicate_keys`: 2.6 rows per customer, 6.2 per event); the symptoms (the same plan,
a sort-merge join that outputs 6.1 rows per event, the write's sort spilling, 6x the output, the aggregates'
groups unchanged) follow from it. The planner reproduces real Spark 3.5.9's plans for the snapshot, the history
and the fix (`reference/local/exploding_join.txt`), the fix (`is_current`) is verified and more shuffle
partitions are not one. The REST-checked evidence is in `test_scenario07_hs.py`."""

from __future__ import annotations

import copy
import random
import re
from pathlib import Path

import numpy as np
import pytest

from sparksim.apps.daily_enrichment import CUSTOMERS, WORKLOAD
from sparksim.cost import calibration as cal
from sparksim.engine.runtime import AppRun
from sparksim.model.shuffle import dimension_row_shares, fanout_weights, key_shares
from sparksim.model.workload import KeySpace, ScanSource
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict
from sparksim.sql.aqe import AdaptiveExecution
from sparksim.sql.dataframe import PlanContext, PlanIds, SparkSession, plan_physical
from sparksim.sql.explain import explain_formatted
from sparksim.sql.expr import ExprIds

SCENARIO = "07-exploding-join"
REAL = Path(__file__).resolve().parents[1] / "reference" / "local" / "exploding_join.txt"
DT = "2026-11-06"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return AppRun(sc.spec("incident")).run()


@pytest.fixture(scope="module")
def last_good(sc):
    return AppRun(sc.spec("last_good")).run()


def variant(params: dict | None = None, conf: dict | None = None, faults=None) -> AppRun:
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["params"].update(params or {})
    raw["job"]["conf"].update(conf or {})
    if faults is not None:
        raw["runs"]["incident"]["faults"] = faults
    return AppRun(scenario_from_dict(raw).spec("incident")).run()


@pytest.fixture(scope="module")
def fixed():
    return variant({"customers_current": True})


def minutes(run) -> float:
    return (run.app_end - run.t0) / 60_000


def join_stage(run):
    """Stage 5: the sort-merge join of the events with the customers, and the write."""
    st = run.stages[5]
    assert [op.kind for op in st.spec.ops] == ["smj", "project", "sort"]
    return st


def trace(t, kind: str, after_join: bool = False):
    ops = t.result.trace
    j = next(i for i, o in enumerate(ops) if o.kind == "smj")
    pool = ops[j + 1:] if after_join else ops[:j + 1]
    return [o for o in pool if o.kind == kind]


def join_rows(run) -> tuple[int, int, int]:
    """(streamed events, buffered customer rows, output rows) of the join over stage 5's tasks."""
    ev = cu = out = 0
    for t in join_stage(run).tasks:
        sorts = trace(t, "sort")
        (smj,) = trace(t, "smj")
        ev, cu, out = ev + sorts[0].rows_in, cu + sorts[1].rows_in, out + smj.rows_out
    return ev, cu, out


def customers_scan_rows(run) -> int:
    st = run.stages[0]
    (src,) = [s for s in st.spec.sources if isinstance(s, ScanSource)]
    assert src.dataset.path == CUSTOMERS.root
    return sum(t.result.metrics.input.records_read for t in st.tasks)


# ------------------------------------------------------------------ the cause


def test_the_cause_is_upstream_data(sc):
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.conf == bad.conf and good.sizing == bad.sizing and good.cluster == bad.cluster
    assert sc.model.runs["incident"].params == {"dt": DT}  # no code change
    assert not bad.executor_kills and not bad.preemptions and bad.native_memory_per_task == 0
    g = {d.path: d for d in good.inputs}[CUSTOMERS.root]
    b = {d.path: d for d in bad.inputs}[CUSTOMERS.root]
    assert g.keys["customer_id"].unique and not g.added_columns
    dim = b.keys["customer_id"]
    assert (dim.rows_per_key, dim.rows_per_match, dim.freq_cv) == (2.6, 6.2, 0.8)
    assert [c[0] for c in b.added_columns] == ["valid_from", "valid_to", "is_current"]
    # the same writer: files of the same size, 2.6x the rows, each 9.1 bytes wider in Parquet
    assert b.mean_file_bytes == g.mean_file_bytes
    assert b.bytes_per_row == pytest.approx(g.bytes_per_row + 9.1)
    assert b.files == round(520 * 2.6 * b.bytes_per_row / g.bytes_per_row) == 1_395


@pytest.mark.parametrize("fault,match", [
    ({"dataset": "suppliers"}, "no dataset 'suppliers'"),
    ({"column": "email"}, "not a key column"),
    ({"rows_per_key": 1.0}, "greater than 1"),
])
def test_duplicate_keys_fault_is_checked(fault, match):
    f = {"type": "duplicate_keys", "dataset": "customers", "column": "customer_id", "rows_per_key": 2.0,
         "rows_per_match": 3.0} | fault
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["faults"] = [f]
    with pytest.raises(ValueError, match=match):
        scenario_from_dict(raw).spec("incident")


def test_duplicate_keys_twice_is_refused():
    f = {"type": "duplicate_keys", "dataset": "customers", "column": "customer_id", "rows_per_key": 2.0,
         "rows_per_match": 3.0}
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["faults"] = [f, f]
    with pytest.raises(ValueError, match="already has several rows per key"):
        scenario_from_dict(raw).spec("incident")


def test_the_fix_needs_the_history():
    """`customers_current` filters on `is_current`, which the snapshot doesn't have."""
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["last_good"]["params"]["customers_current"] = True
    with pytest.raises(ValueError, match="no is_current column"):
        scenario_from_dict(raw).spec("last_good")


# ------------------------------------------------------------------ the planner against real Spark 3.5.9


def _real_plan(mode: str) -> str:
    text = REAL.read_text().split("=== SQLExecutionStart physicalPlanDescription", 1)[1]
    seg = text.split(f": enrich {mode}\n", 1)[1].split("--- execution", 1)[0]
    return seg.replace(f"events_enriched_{mode}", "events_enriched").strip()


def _norm(s: str) -> str:
    s = re.sub(r"#\d+", "#N", s)
    s = re.sub(r"plan_id=\d+", "plan_id=N", s)
    s = re.sub(r"dt=2026-11-0\d", "dt=DT", s)  # the local check ran every mode for one date
    return re.sub(r"(file:/\S*?|hdfs://nn1\.hadoop\.prod\.internal:8020)/warehouse", "WH", s)


def _initial_plan(sc, role: str, params: dict | None = None) -> str:
    inputs = {d.path: d for d in sc.spec(role).inputs}
    p = WORKLOAD.resolve({**sc.model.runs[role].params, **(params or {})})
    data = {k: inputs[v.path] for k, v in WORKLOAD.datasets(p).items()}  # the run's data, faults applied
    spark = SparkSession(ExprIds(0), {"spark.sql.shuffle.partitions": "2400"})
    logical = WORKLOAD.queries(p, data)[0].build(spark)
    stats = {d.path: 1e12 for d in data.values()} | {data["pages"].path: 3e6}  # only the join strategy reads it
    ctx = PlanContext(spark, PlanIds(random.Random(0)), dataset_stats=stats)
    return explain_formatted(AdaptiveExecution(logical, ctx, lambda st: None, name_prefix="e0.").root)


@pytest.mark.parametrize("role,params,mode", [("last_good", None, "snapshot"), ("incident", None, "history"),
                                              ("incident", {"customers_current": True}, "fix_and")])
def test_plans_match_spark(sc, role, params, mode):
    """The write's physicalPlanDescription (SQLExecutionStart) as real Spark 3.5.9 printed it for the same query
    over the snapshot, over the history, and with the fix (expression ids, plan ids, paths and dates normalized)."""
    assert _norm(_initial_plan(sc, role, params)).strip() == _norm(_real_plan(mode))


def test_the_plan_does_not_show_the_grain(sc):
    """Over the history the plan text is the snapshot's: the new columns aren't read, the rows per key don't show."""
    assert _norm(_initial_plan(sc, "last_good")) == _norm(_initial_plan(sc, "incident"))
    fix = _initial_plan(sc, "incident", {"customers_current": True})
    assert ("PushedFilters: [IsNotNull(is_current), IsNotNull(is_deleted), EqualTo(is_current,true), "
            "Not(EqualTo(is_deleted,true)), IsNotNull(customer_id)]") in fix
    assert "ReadSchema: struct<customer_id:bigint,home_country:string,segment:string,loyalty_tier:string," \
           "is_deleted:boolean,is_current:boolean>" in fix


def test_a_broadcast_join_with_duplicate_keys_is_refused(sc):
    """A broadcast hash join's probe cost per extra match is unmeasured: refused, not guessed."""
    p = WORKLOAD.resolve(sc.model.runs["incident"].params)
    inputs = {d.path: d for d in sc.spec("incident").inputs}
    data = {k: inputs[v.path] for k, v in WORKLOAD.datasets(p).items()}
    spark = SparkSession(ExprIds(0), {"spark.sql.shuffle.partitions": "2400"})
    logical = WORKLOAD.queries(p, data)[0].build(spark)
    stats = {d.path: 1e6 for d in data.values()}  # everything looks small enough to broadcast
    with pytest.raises(NotImplementedError, match="several rows per key"):
        plan_physical(logical, PlanContext(spark, PlanIds(random.Random(0)), dataset_stats=stats))


def test_evidence_helpers_have_unique_names():
    """`scripts/evidence.py` is one module for every scenario: a helper defined twice silently replaces the first
    one for every scenario that calls it by name (Scenario 05's `_join_inputs` once did)."""
    src = (Path(__file__).resolve().parents[1] / "scripts" / "evidence.py").read_text()
    names = re.findall(r"^def (\w+)", src, re.M)
    assert sorted({n for n in names if names.count(n) > 1}) == []


# ------------------------------------------------------------------ the model


def test_dimension_partitions_and_fanout_weights():
    n, seed = 2400, 7
    dim = KeySpace("customer_id", distinct=211_000_000, freq_cv=0.8, rows_per_key=2.6, rows_per_match=6.2)
    shares = dimension_row_shares(dim, n, seed)
    assert shares.sum() == pytest.approx(1.0)
    base = key_shares(dim, n, seed)  # the same keys as the other side's partitions
    assert np.corrcoef(shares, base)[0, 1] > 0.5
    streamed = KeySpace("customer_id", distinct=208_000_000, freq_cv=2.5)
    w = fanout_weights(streamed, dim, n, seed)
    assert w.mean() == pytest.approx(1.0)
    # a partition holds ~87 k keys: its output per event is the mean's within ~1 %
    assert 0.003 < w.std() < 0.012
    assert cal.SMJ_EXTRA_OUTPUT_NS_PER_ROW == 20.0


# ------------------------------------------------------------------ the symptoms


def test_the_join_multiplies_the_rows(incident, last_good):
    ev, cu, out = join_rows(incident)
    assert out / ev == pytest.approx(0.985 * 6.2, rel=0.005)  # 6.1 rows per event
    gev, gcu, gout = join_rows(last_good)
    assert gout / gev == pytest.approx(0.985, rel=0.002)  # at most one row per event
    assert ev == pytest.approx(gev, rel=0.01)  # the same day's events
    assert cu / gcu == pytest.approx(2.6, rel=0.02)  # the customers that aren't deleted, 2.6 versions each
    # every task: its output follows its events (the weights only spread it by ~1 %)
    ratios = np.array([trace(t, "smj")[0].rows_out / trace(t, "sort")[0].rows_in for t in join_stage(incident).tasks])
    assert ratios.std() / ratios.mean() < 0.02


def test_the_customer_scan_reads_the_history(incident, last_good):
    assert customers_scan_rows(incident) / customers_scan_rows(last_good) == pytest.approx(2.6, rel=0.02)
    assert len(incident.hdfs.listdir(CUSTOMERS.root)) == 1_395
    assert len(last_good.hdfs.listdir(CUSTOMERS.root)) == 520


def test_the_writes_sort_spills_not_the_join(incident, last_good):
    st = join_stage(incident)
    for t in st.tasks:
        (smj,) = trace(t, "smj")
        (write_sort,) = trace(t, "sort", after_join=True)
        assert smj.spill == 0 and all(s.spill == 0 for s in trace(t, "sort"))
        assert write_sort.spill > 1.4 * 2**30  # 1.5-1.7 GiB per task
    assert not any(t.result.metrics.memory_bytes_spilled for t in join_stage(last_good).tasks)
    disk = sum(t.result.metrics.disk_bytes_spilled for t in st.tasks)
    assert 1.4 * 2**40 < disk < 1.8 * 2**40


def test_the_output_and_the_runtime(incident, last_good):
    def written(run):
        return sum(t.result.metrics.output.records_written for t in join_stage(run).tasks)

    def bpr(run):
        ms = [t.result.metrics.output for t in join_stage(run).tasks]
        return sum(m.bytes_written for m in ms) / sum(m.records_written for m in ms)

    assert written(incident) / written(last_good) == pytest.approx(6.17, rel=0.01)
    assert bpr(incident) == pytest.approx(bpr(last_good), rel=0.005)  # compression didn't change
    assert 5.4 < minutes(last_good) < 5.9
    assert 2.5 < minutes(incident) / minutes(last_good) < 3.0
    # the events scan is the same; the join stage takes 10x as long
    for sid, lo, hi in ((2, 0.9, 1.1), (5, 8, 14)):
        a, b = incident.stages[sid], last_good.stages[sid]
        assert lo < (a.completed - a.submitted) / (b.completed - b.submitted) < hi


def test_the_aggregates_count_the_copies(incident, last_good):
    """The two daily aggregates read 6x the rows but output the same customers and funnel cells."""
    for sid in (8, 11):  # the final aggregations of daily_funnel and customer_daily_activity
        a = sum(t.result.metrics.output.records_written for t in incident.stages[sid].tasks)
        b = sum(t.result.metrics.output.records_written for t in last_good.stages[sid].tasks)
        assert a == pytest.approx(b, rel=0.002)
    for sid in (6, 9):  # their scans
        a = sum(t.result.metrics.input.records_read for t in incident.stages[sid].tasks)
        b = sum(t.result.metrics.input.records_read for t in last_good.stages[sid].tasks)
        assert a / b == pytest.approx(6.17, rel=0.01)


# ------------------------------------------------------------------ the fix, and a non-fix


def test_the_fix_restores_one_row_per_event(fixed, incident, last_good):
    ev, cu, out = join_rows(fixed)
    assert out / ev == pytest.approx(0.985, rel=0.002)
    assert cu == pytest.approx(join_rows(last_good)[1], rel=0.01)  # one current version per customer
    # the scan still reads the whole history (Parquet can't skip a boolean present in every row group)
    assert customers_scan_rows(fixed) == customers_scan_rows(incident)
    assert minutes(fixed) < 1.02 * minutes(last_good)
    assert not any(t.result.metrics.memory_bytes_spilled for t in join_stage(fixed).tasks)


def test_more_shuffle_partitions_treat_only_the_spill(last_good, incident):
    run = variant(conf={"spark.sql.shuffle.partitions": 9600})
    assert join_rows(run)[2] == pytest.approx(join_rows(incident)[2], rel=1e-4)  # still 25.1 billion rows
    assert not any(t.result.metrics.memory_bytes_spilled for t in join_stage(run).tasks)
    assert 1.6 < minutes(run) / minutes(last_good) < 2.0
