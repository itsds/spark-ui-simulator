"""Scenario 06 (partition pruning failure) without the History Server: the cause is the script's change
(DATA-4398) that derives `dt` from `event_ts` and filters on it, so the filter no longer names the partition
column; the symptoms (every partition listed and read, a scan stage 7x the size, a filter that drops most of
what was read) follow from it. The planner reproduces real Spark 3.5.9's plans for the three ways the script
reads the events (`reference/local/partition_pruning.txt`), casting the partition column itself still prunes,
and the fix (a filter on the partition column first) is verified. The REST-checked evidence is in
`test_scenario06_hs.py`."""

from __future__ import annotations

import copy
import random
import re
from pathlib import Path

import pytest

from sparksim.apps.daily_enrichment import EVENTS, WORKLOAD, event_day_share
from sparksim.engine.runtime import AppRun, _refuse_row_group_skipping, partition_matches
from sparksim.model.workload import ScanSource
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict
from sparksim.sql.aqe import AdaptiveExecution
from sparksim.sql.dataframe import PlanContext, PlanIds, SparkSession, Table, plan_physical
from sparksim.sql.explain import explain_formatted
from sparksim.sql.expr import ExprIds, Lit
from sparksim.sql.nodes import FileSourceScan
from sparksim.yarnlogs.driver import am_stderr

SCENARIO = "06-partition-pruning-failure"
REAL = Path(__file__).resolve().parents[1] / "reference" / "local" / "partition_pruning.txt"
DT = "2026-10-21"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return AppRun(sc.spec("incident")).run()


@pytest.fixture(scope="module")
def last_good(sc):
    return AppRun(sc.spec("last_good")).run()


def variant(params: dict | None = None, conf: dict | None = None) -> AppRun:
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["params"].update(params or {})
    raw["job"]["conf"].update(conf or {})
    return AppRun(scenario_from_dict(raw).spec("incident")).run()


@pytest.fixture(scope="module")
def fixed():
    return variant({"events_read": "event_date_pruned"})


def minutes(run) -> float:
    return (run.app_end - run.t0) / 60_000


def events_scan(run) -> tuple[FileSourceScan, list]:
    """The events scan of the enrichment's map stage (stage 2) and its tasks."""
    st = run.stages[2]
    (src,) = [s for s in st.spec.sources if isinstance(s, ScanSource)]
    assert src.dataset.path.startswith(EVENTS.root)
    return src.node, st.tasks


def filter_rows(tasks) -> tuple[int, int]:
    """(rows the events scan returned, rows its Filter kept) over the stage's tasks."""
    scanned = sum(t.result.metrics.input.records_read for t in tasks)
    kept = sum(next(o.rows_out for o in t.result.trace if o.kind == "filter") for t in tasks)
    return scanned, kept


# ------------------------------------------------------------------ the cause


def test_the_cause_is_the_code(sc):
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.conf == bad.conf and good.sizing == bad.sizing and good.cluster == bad.cluster
    assert not bad.executor_kills and not bad.preemptions and bad.native_memory_per_task == 0
    assert sc.model.runs["incident"].params == {"dt": DT, "events_read": "event_date"}
    events = {d.path: d for d in bad.inputs}[EVENTS.root]
    # the events table as it is on HDFS at 02:10: 7 days kept, and the first two hours of the next day
    assert [p.value for p in events.partitions] == [DT, "2026-10-15", "2026-10-16", "2026-10-17", "2026-10-18",
                                                    "2026-10-19", "2026-10-20", "2026-10-22"]
    assert events.partitions[0].volume == 1.0 and events.partitions[-1].volume < 0.1


def test_event_day_share_follows_the_arrival_lag(sc):
    events = {d.path: d for d in sc.spec("incident").inputs}[EVENTS.root]
    share = event_day_share(events, DT)
    assert share[f"dt={DT}"] == pytest.approx(0.986, abs=0.002)  # landed the same day
    assert share["dt=2026-10-22"] == pytest.approx(0.012, abs=0.002)  # yesterday's late mobile events
    assert all(v == 0.0 for k, v in share.items() if k < f"dt={DT}")  # nothing lands before it happens


# ------------------------------------------------------------------ the planner against real Spark 3.5.9


def _real_plan(execution: int, mode: str) -> str:
    text = REAL.read_text()
    seg = text.split(f"--- execution {execution}: enrich {mode}\n", 1)[1].split("--- execution", 1)[0]
    return seg.replace(f"events_enriched_{mode}", "events_enriched").strip()


def _norm(s: str) -> str:
    s = re.sub(r"#\d+", "#N", s)
    s = re.sub(r"plan_id=\d+", "plan_id=N", s)
    return re.sub(r"(file:/\S*?|hdfs://nn1\.hadoop\.prod\.internal:8020)/warehouse", "WH", s)


def _initial_plan(mode: str) -> str:
    p = WORKLOAD.resolve({"dt": DT, "events_read": mode})
    data = WORKLOAD.datasets(p)
    spark = SparkSession(ExprIds(0), {"spark.sql.shuffle.partitions": "2400"})
    logical = WORKLOAD.queries(p, data)[0].build(spark)
    stats = {d.path: 1e12 for d in data.values()} | {data["pages"].path: 3e6}  # only the join strategy reads it
    ctx = PlanContext(spark, PlanIds(random.Random(0)), dataset_stats=stats)
    return explain_formatted(AdaptiveExecution(logical, ctx, lambda st: None, name_prefix="e0.").root)


@pytest.mark.parametrize("execution,mode", [(0, "partition_path"), (1, "event_date"), (2, "event_date_pruned")])
def test_plans_match_spark(execution, mode):
    """The write's physicalPlanDescription (SQLExecutionStart) of each way of reading the events, as real Spark
    3.5.9 printed it for the same query over the same schemas (expression ids, plan ids and paths normalized)."""
    real_mode = {"partition_path": "path"}.get(mode, mode)
    assert _norm(_initial_plan(mode)).strip() == _norm(_real_plan(execution, real_mode))


def test_the_incident_plan_has_no_partition_filter():
    plan = _initial_plan("event_date")
    scan = plan.split("(1) Scan parquet", 1)[1].split("\n\n", 1)[0]
    assert "PartitionFilters" not in scan  # empty: not printed
    assert "Location: InMemoryFileIndex [hdfs://nn1.hadoop.prod.internal:8020/warehouse/web.db/events]" in scan
    assert f"GreaterThanOrEqual(event_ts,{DT} 00:00:00.0), LessThan(event_ts,2026-10-22 00:00:00.0)" in scan
    assert re.search(r"cast\(event_ts#\d+ as date\) AS dt#\d+\]", plan)  # the dt the job writes


def test_casting_the_partition_column_still_prunes():
    """The myth: a function or cast on the partition column does not stop pruning, any deterministic predicate
    over partition columns only is a partition filter (real 3.5.9: `myth cast(dt as string)`)."""
    spark = SparkSession(ExprIds(0))
    ev = spark.read(EVENTS)
    q = ev.where(ev["dt"].cast("string").eq(DT))
    scan = next(n for n in plan_physical(q.plan, PlanContext(spark, PlanIds(random.Random(0)))).walk()
                if isinstance(n, FileSourceScan))
    real = re.search(r"=== myth cast\(dt as string\).*?PartitionFilters: (\[[^\n]*\])", REAL.read_text(), re.S)
    assert _norm(str(scan.metadata()["PartitionFilters"])) == _norm(real.group(1))
    schema = scan.relation.partition_schema
    assert partition_matches(scan.partition_filters, schema, f"dt={DT}")
    assert not partition_matches(scan.partition_filters, schema, "dt=2026-10-22")
    assert not partition_matches(scan.partition_filters, schema, "dt=__HIVE_DEFAULT_PARTITION__")


def test_row_group_statistics_are_refused():
    """With INT64 timestamps Parquet would skip row groups by their statistics (real 3.5.9: `micros
    event_ts::date` read 19,203 of 30,000 rows); the simulator doesn't model column statistics, so it refuses."""
    micros = Table(EVENTS.root, EVENTS.columns, EVENTS.partition_columns, timestamp_type="TIMESTAMP_MICROS")
    for table, refused in ((EVENTS, False), (micros, True)):
        spark = SparkSession(ExprIds(0))
        ev = spark.read(table)
        q = ev.where(ev["event_ts"].cast("date").eq(Lit(DT, "date")))
        scan = next(n for n in plan_physical(q.plan, PlanContext(spark, PlanIds(random.Random(0)))).walk()
                    if isinstance(n, FileSourceScan))
        if refused:
            with pytest.raises(NotImplementedError, match="row groups"):
                _refuse_row_group_skipping(scan)
        else:
            _refuse_row_group_skipping(scan)


# ------------------------------------------------------------------ the symptoms


def test_last_good_reads_one_partition(last_good):
    assert 5.4 < minutes(last_good) < 5.7
    scan, tasks = events_scan(last_good)
    assert not scan.partition_filters
    assert last_good.selected_partitions(scan) == [f"{EVENTS.root}/dt=2026-10-19"]
    assert 5_100 < len(tasks) < 5_300


def test_the_incident_reads_every_partition(incident, last_good):
    scan, tasks = events_scan(incident)
    dirs = incident.selected_partitions(scan)
    assert not scan.partition_filters and len(dirs) == 8 == len(incident.hdfs.partition_dirs(EVENTS.root))
    files = [f for d in dirs for f in incident.hdfs.listdir(d)]
    assert 31_000 < len(files) < 32_500
    assert sum(f.size for f in files) > 8.5 * 2**40  # 8.6 TiB of files
    _, good_tasks = events_scan(last_good)
    assert 7.0 < len(tasks) / len(good_tasks) < 7.6
    assert minutes(incident) > 2.5 * minutes(last_good)
    # INT96 timestamps: no row group skipped, the scan returns every row of every partition it lists
    scanned, kept = filter_rows(tasks)
    assert scanned == sum(f.rows for f in files)
    assert 0.12 < kept / scanned < 0.13  # vs 0.93 (bots only) the night before
    good_scanned, good_kept = filter_rows(good_tasks)
    assert 0.925 < good_kept / good_scanned < 0.935


def test_tasks_on_other_days_keep_nothing(incident):
    _, tasks = events_scan(incident)
    empty = [t for t in tasks if t.result.metrics.shuffle_write.records_written == 0]
    assert len(empty) / len(tasks) > 0.8  # their rows all happened on other days
    st = incident.stages[2]
    for t in empty[:50]:
        plan = st.plans[t.partition]
        assert all(f"dt={DT}" not in pf.file.path and "dt=2026-10-22" not in pf.file.path for pf in plan.scan.files)


def test_downstream_is_unchanged(incident, last_good):
    """Only the scan grew: the join + write and the two aggregates process one day of events as before."""
    for sid in (5, 6, 9):
        a, b = incident.stages[sid], last_good.stages[sid]
        assert a.num_tasks == b.num_tasks
        da, db = a.completed - a.submitted, b.completed - b.submitted
        assert 0.85 < da / db < 1.15


def test_driver_log_shows_no_pruning(incident, fixed):
    bad = am_stderr(incident)
    assert re.search(r"INFO InMemoryFileIndex: It took (1[5-9]\d\d|2\d\d\d) ms to list leaf files for 1 paths\.", bad)
    assert "INFO DataSourceStrategy: Pruning directories with: \n" in bad
    assert "Selected " not in bad
    good = am_stderr(fixed)
    assert re.search(r"Pruning directories with: isnotnull\(dt#\d+\),\(dt#\d+ >= 2026-10-21\),\(dt#\d+ <= 2026-10-22\)",
                     good)
    assert "INFO InMemoryFileIndex: Selected 2 partitions out of 8, pruned 75.0% partitions." in good


# ------------------------------------------------------------------ the fix


def test_the_fix_prunes_and_keeps_the_same_rows(incident, fixed, last_good):
    scan, tasks = events_scan(fixed)
    dt = scan.partition_filters[0].child.expr_id
    assert [str(f) for f in scan.partition_filters] == [f"isnotnull(dt#{dt})", f"(dt#{dt} >= {DT})",
                                                        f"(dt#{dt} <= 2026-10-22)"]
    assert fixed.selected_partitions(scan) == [f"{EVENTS.root}/dt={DT}", f"{EVENTS.root}/dt=2026-10-22"]
    assert minutes(fixed) < 1.03 * minutes(last_good)
    # the same events of the day as the incident (only the other days' rows, which the filter dropped, are gone)
    _, kept_fixed = filter_rows(tasks)
    _, kept_bad = filter_rows(events_scan(incident)[1])
    assert kept_fixed == pytest.approx(kept_bad, rel=1e-5)


def test_bigger_splits_do_not_help(last_good):
    run = variant(conf={"spark.sql.files.maxPartitionBytes": "1g"})
    _, tasks = events_scan(run)
    assert len(tasks) < 11_000  # fewer, bigger tasks
    assert minutes(run) > 2.5 * minutes(last_good)  # the same 8.6 TiB to read
