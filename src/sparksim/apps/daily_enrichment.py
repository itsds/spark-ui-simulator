"""Scenario 00 (healthy baseline): the nightly clickstream enrichment of a large e-commerce site.

    daily_enrichment.py, PySpark 3.5.9 on YARN (cluster mode), 40 executors x 4 cores, AQE on (default)

    events    = spark.read.parquet(f"{WH}/web.db/events/dt={dt}")          # ~1.1 TB, ~3.9 B rows
    customers = spark.read.parquet(f"{WH}/crm.db/customers_snapshot")       # ~60 GB, 210 M rows
    pages     = spark.read.parquet(f"{WH}/web.db/page_catalog")             # 3 MB

    enriched = (events.where(~F.col("is_bot"))
                      .select(..., parse_url(...), regexp_extract(...))
                      .join(F.broadcast(pages), "page_id")
                      .join(customers, "customer_id"))                       # sort-merge join
    enriched.write.mode("overwrite").partitionBy("dt").parquet(f"{WH}/analytics.db/events_enriched")

    e = spark.read.parquet(f"{WH}/analytics.db/events_enriched/dt={dt}")
    e.groupBy("page_type", "funnel_step", "device", "country").agg(...).write...parquet(daily_funnel)
    e.groupBy("customer_id").agg(...).write...parquet(customer_daily_activity)

Nothing is wrong with this run: it is what "normal" looks like. Every number
comes from the workload below through the cost model and the scheduler.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ..engine.emit import build_events
from ..engine.runtime import AppRun, AppSpec, InputDataset
from ..eventlog import write_event_log
from ..model.cluster import ClusterSpec, ExecutorSizing
from ..model.workload import (
    BroadcastHashJoin,
    BroadcastSink,
    Dataset,
    Filter,
    FinalAggregate,
    KeySpace,
    PartialAggregate,
    Project,
    Query,
    ScanSource,
    ShuffleSink,
    ShuffleSource,
    Sort,
    SortMergeJoin,
    StageSpec,
    WriteSink,
)

MiB = 1024 * 1024
NN = "hdfs://nn1.hadoop.prod.internal:8020"
WH = f"{NN}/warehouse"
DT = "2026-11-02"

CUSTOMER = KeySpace("customer_id", distinct=208_000_000, freq_cv=2.5)
ACTIVE_CUSTOMER = KeySpace("customer_id", distinct=61_000_000, freq_cv=2.5)
FUNNEL = KeySpace("page_type,funnel_step,device,country", distinct=57_600)
SHUFFLE_PARTITIONS = 2400
# from_json(payload) + parse_url + regexp_extract per event: ~2 µs/row (Jackson on a ~400-byte payload).
PARSE_NS = 2_100.0

EVENTS = Dataset(f"{WH}/web.db/events/dt={DT}", read_fraction=0.42, row_bytes=168)
CUSTOMERS = Dataset(f"{WH}/crm.db/customers_snapshot", read_fraction=0.35, row_bytes=72)
PAGES = Dataset(f"{WH}/web.db/page_catalog", read_fraction=0.6, row_bytes=48)
ENRICHED_DIR = f"{WH}/analytics.db/events_enriched/dt={DT}"
ENRICHED_FUNNEL = Dataset(ENRICHED_DIR, read_fraction=0.17, row_bytes=56)
ENRICHED_CUSTOMER = Dataset(ENRICHED_DIR, read_fraction=0.21, row_bytes=64)


def queries() -> tuple[Query, ...]:
    enrich = Query(
        description=f"events_enriched dt={DT}: web events + page catalog (broadcast) + customers (sort-merge join)",
        stages=(
            StageSpec("pages_broadcast", (ScanSource(PAGES),), (Project(40),), BroadcastSink("pages")),
            StageSpec("events_exchange", (ScanSource(EVENTS),),
                      (Filter(0.93), Project(152, ns=PARSE_NS), BroadcastHashJoin(176)),
                      ShuffleSink("events_by_customer", CUSTOMER, SHUFFLE_PARTITIONS),
                      depends_on=("pages_broadcast",)),
            StageSpec("customers_exchange", (ScanSource(CUSTOMERS),), (Filter(0.97), Project(64)),
                      ShuffleSink("customers_by_id", CUSTOMER, SHUFFLE_PARTITIONS, unique_keys=True)),
            StageSpec("join_write",
                      (ShuffleSource("events_by_customer", 176), ShuffleSource("customers_by_id", 64)),
                      (SortMergeJoin(216, match=0.985), Project(208), Sort()),
                      WriteSink(f"{WH}/analytics.db/events_enriched/dt={DT}", bytes_per_row=82),
                      side_ops=((Sort(),), (Sort(),))),
        ),
        think_ms=(2_400, 4_200),
    )
    funnel = Query(
        description=f"daily_funnel dt={DT}: events by page type, funnel step, device, country",
        stages=(
            StageSpec("funnel_partial", (ScanSource(ENRICHED_FUNNEL),),
                      (PartialAggregate(FUNNEL, out_row_bytes=64, buffer_bytes=40),),
                      ShuffleSink("funnel_groups", FUNNEL, SHUFFLE_PARTITIONS)),
            StageSpec("funnel_final", (ShuffleSource("funnel_groups", 64),),
                      (FinalAggregate(FUNNEL, out_row_bytes=72, buffer_bytes=40), Project(72), Sort()),
                      WriteSink(f"{WH}/analytics.db/daily_funnel/dt={DT}", bytes_per_row=26)),
        ),
    )
    activity = Query(
        description=f"customer_daily_activity dt={DT}: sessions, events and spend per customer",
        stages=(
            StageSpec("activity_partial", (ScanSource(ENRICHED_CUSTOMER),),
                      (PartialAggregate(ACTIVE_CUSTOMER, out_row_bytes=72, buffer_bytes=56),),
                      ShuffleSink("activity_by_customer", ACTIVE_CUSTOMER, SHUFFLE_PARTITIONS)),
            StageSpec("activity_final", (ShuffleSource("activity_by_customer", 72),),
                      (FinalAggregate(ACTIVE_CUSTOMER, out_row_bytes=88, buffer_bytes=56), Project(80), Sort()),
                      WriteSink(f"{WH}/analytics.db/customer_daily_activity/dt={DT}", bytes_per_row=31)),
        ),
    )
    return (enrich, funnel, activity)


SPEC = AppSpec(
    seed=20261102,
    app_name="clickstream-daily-enrichment",
    user="svc_analytics",
    script="daily_enrichment.py",
    started_at="2026-11-03T02:10:07.318+00:00",
    app_seq=48213,
    cluster=ClusterSpec(cluster_timestamp=1791902144377),  # RM restarted 2026-10-13
    sizing=ExecutorSizing(instances=40, cores=4, memory_mb=16 * 1024, driver_memory_mb=8 * 1024, driver_cores=2),
    inputs=(
        InputDataset(EVENTS.path, files=4_300, mean_file_bytes=285 * MiB, file_size_sigma=0.22, bytes_per_row=290),
        InputDataset(CUSTOMERS.path, files=520, mean_file_bytes=116 * MiB, file_size_sigma=0.12, bytes_per_row=290),
        InputDataset(PAGES.path, files=1, mean_file_bytes=3 * MiB, file_size_sigma=0.0, bytes_per_row=68),
    ),
    queries=queries(),
    conf=(
        ("spark.yarn.queue", "etl"),
        ("spark.dynamicAllocation.enabled", "false"),
        ("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)),
        ("spark.sql.files.maxPartitionBytes", "256m"),
        ("spark.sql.sources.partitionOverwriteMode", "dynamic"),
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
    ),
    queue="etl",
)


def build(seed: int | None = None):
    spec = SPEC if seed is None else replace(SPEC, seed=seed)
    run = AppRun(spec).run()
    return run, build_events(run)


def generate(out_dir: str | Path, seed: int | None = None) -> Path:
    run, events = build(seed)
    return write_event_log(Path(out_dir) / run.yapp.event_log_name, events)
