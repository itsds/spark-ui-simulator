"""Scenario 00 (healthy baseline): the nightly clickstream enrichment of a large e-commerce site.

    daily_enrichment.py, PySpark 3.5.9 on YARN (cluster mode), 40 executors x 4 cores, AQE on (default)

    DT = "2026-11-02"
    events = spark.read.option("basePath", f"{WH}/web.db/events").parquet(f"{WH}/web.db/events/dt={DT}")
    customers = spark.read.parquet(f"{WH}/crm.db/customers_snapshot")          # ~60 GB, 210 M rows
    pages = spark.read.parquet(f"{WH}/web.db/page_catalog")                     # 3 MB

    sc.setJobDescription(f"events_enriched dt={DT}: ...")
    enriched = (events.where(~F.col("is_bot"))
                .select("event_id", "event_ts", "session_id", "customer_id", "page_id", "event_type",
                        "device", "country", "revenue",
                        F.lower(F.regexp_extract("page_url", "utm_campaign=([^&]+)", 1)).alias("campaign"), "dt")
                .join(F.broadcast(pages.select("page_id", "page_type", "funnel_step")), "page_id")
                .join(customers.where(~F.col("is_deleted"))
                               .select("customer_id", "segment", "loyalty_tier", "home_country"), "customer_id"))
    enriched.write.mode("overwrite").partitionBy("dt").parquet(f"{WH}/analytics.db/events_enriched")

    e = spark.read.option("basePath", ENRICHED).parquet(f"{ENRICHED}/dt={DT}")
    (e.groupBy("dt", "page_type", "funnel_step", "device", "country")
      .agg(F.count("*").alias("events"), F.sum("revenue").alias("revenue"))
      .write.mode("overwrite").partitionBy("dt").parquet(f"{WH}/analytics.db/daily_funnel"))
    e = spark.read.option("basePath", ENRICHED).parquet(f"{ENRICHED}/dt={DT}")
    (e.groupBy("dt", "customer_id")
      .agg(F.count("*").alias("events"), F.sum("revenue").alias("spend"),
           F.min("event_ts").alias("first_seen"), F.max("event_ts").alias("last_seen"))
      .write.mode("overwrite").partitionBy("dt").parquet(f"{WH}/analytics.db/customer_daily_activity"))

Nothing is wrong with this run: it is what "normal" looks like. The physical plans, the SQL tab and
every number come from the queries and tables below through the planner, the cost model and the
scheduler.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ..engine.emit import build_events
from ..engine.runtime import AppRun, AppSpec, InputDataset
from ..eventlog import write_event_log
from ..model.cluster import ClusterSpec, ExecutorSizing
from ..model.workload import KeySpace, WriteSink
from ..sql.dataframe import Column, SparkSession, SqlQuery, Table, agg
from ..sql.expr import Func, Not

MiB = 1024 * 1024
NN = "hdfs://nn1.hadoop.prod.internal:8020"
WH = f"{NN}/warehouse"
DT = "2026-11-02"

CUSTOMER = KeySpace("customer_id", distinct=208_000_000, freq_cv=2.5)
ACTIVE_CUSTOMER = KeySpace("dt,customer_id", distinct=61_000_000, freq_cv=2.5)
FUNNEL = KeySpace("dt,page_type,funnel_step,device,country", distinct=57_600)
SHUFFLE_PARTITIONS = 2400
# lower(regexp_extract(page_url, ...)) on a ~150-character URL (java.util.regex): ~2 µs per row.
PARSE_NS = 2_100.0


def C(name: str, dtype: str, avg: float = 0.0, pq: float = 8.0) -> Column:  # noqa: N802
    return Column(name, dtype, avg, pq)


# Parquet tables. pq = encoded + compressed bytes per row of the column chunk (sums to the files' bytes per
# row); avg = average string length (UnsafeRow sizes, so shuffle "data size" and memory follow the schema).
EVENTS = Table(f"{WH}/web.db/events", (
    C("event_id", "bigint", pq=6), C("event_ts", "timestamp", pq=6), C("session_id", "bigint", pq=5),
    C("customer_id", "bigint", pq=5), C("anonymous_id", "string", 36, 22), C("page_id", "bigint", pq=2),
    C("event_type", "string", 8, 1), C("page_url", "string", 150, 98), C("referrer_url", "string", 70, 38),
    C("user_agent", "string", 130, 14), C("ip_address", "string", 13, 9), C("device", "string", 7, 0.6),
    C("os", "string", 8, 0.6), C("browser", "string", 8, 0.6), C("country", "string", 2, 0.6),
    C("region", "string", 10, 1.5), C("is_bot", "boolean", pq=0.1), C("consent_flags", "string", 6, 0.8),
    C("experiment_ids", "string", 30, 7), C("payload", "string", 420, 62), C("revenue", "double", pq=1.2),
    C("currency", "string", 3, 0.3), C("app_version", "string", 8, 0.7), C("ingest_ts", "timestamp", pq=8)),
    (C("dt", "date"),))
CUSTOMERS = Table(f"{WH}/crm.db/customers_snapshot", (
    C("customer_id", "bigint", pq=7), C("email", "string", 26, 24), C("first_name", "string", 7, 6),
    C("last_name", "string", 8, 7), C("phone", "string", 13, 11), C("street", "string", 24, 22),
    C("city", "string", 10, 6), C("postcode", "string", 6, 4), C("home_country", "string", 2, 0.3),
    C("birth_date", "date", pq=2), C("signup_ts", "timestamp", pq=6), C("segment", "string", 8, 0.4),
    C("loyalty_tier", "string", 6, 0.3), C("marketing_opt_in", "boolean", pq=0.1),
    C("lifetime_value", "double", pq=5), C("preferences", "string", 180, 166), C("is_deleted", "boolean", pq=0.1),
    C("updated_at", "timestamp", pq=6), C("source_system", "string", 6, 0.2), C("crm_id", "string", 18, 14)))
PAGES = Table(f"{WH}/web.db/page_catalog", (
    C("page_id", "bigint", pq=4), C("path", "string", 40, 22), C("title", "string", 50, 26),
    C("page_type", "string", 9, 1), C("funnel_step", "int", pq=1), C("category", "string", 12, 4),
    C("is_active", "boolean", pq=0.1), C("updated_at", "timestamp", pq=9.9)))
ENRICHED_ROOT = f"{WH}/analytics.db/events_enriched"
ENRICHED = Table(ENRICHED_ROOT, (
    C("customer_id", "bigint", pq=5), C("page_id", "bigint", pq=2), C("event_id", "bigint", pq=8),
    C("event_ts", "timestamp", pq=8), C("session_id", "bigint", pq=6), C("event_type", "string", 8, 0.5),
    C("device", "string", 7, 0.4), C("country", "string", 2, 0.4), C("revenue", "double", pq=1.6),
    C("campaign", "string", 6, 0.9), C("page_type", "string", 9, 0.5), C("funnel_step", "int", pq=0.3),
    C("segment", "string", 8, 0.4), C("loyalty_tier", "string", 6, 0.3), C("home_country", "string", 2, 0.3)),
    (C("dt", "date"),))


def enrich(spark: SparkSession):
    events = spark.read(EVENTS, f"{EVENTS.root}/dt={DT}")
    customers = spark.read(CUSTOMERS)
    pages = spark.read(PAGES)
    e = events
    cols = [e[c] for c in ("event_id", "event_ts", "session_id", "customer_id", "page_id", "event_type",
                           "device", "country", "revenue")]
    campaign = Func("lower", (Func("regexp_extract", (e["page_url"], "utm_campaign=([^&]+)", 1), "string", 6),),
                    "string", 6)
    enriched = (events.where(Not(e["is_bot"]), selectivity=0.93)
                .select(*cols, campaign.alias("campaign"), e["dt"], ns=PARSE_NS)
                .join(pages.select(pages["page_id"], pages["page_type"], pages["funnel_step"]), "page_id",
                      broadcast=True)
                .join(customers.where(Not(customers["is_deleted"]), selectivity=0.97)
                      .select(*(customers[c] for c in ("customer_id", "segment", "loyalty_tier", "home_country"))),
                      "customer_id", match=0.985, key_space=CUSTOMER))
    return enriched.write_parquet(ENRICHED_ROOT, ["dt"], WriteSink(f"{ENRICHED_ROOT}/dt={DT}",
                                                                   ENRICHED.file_bytes_per_row))


def funnel(spark: SparkSession):
    e = spark.read(ENRICHED, f"{ENRICHED_ROOT}/dt={DT}")
    out = f"{WH}/analytics.db/daily_funnel"
    daily = (e.group_by("dt", "page_type", "funnel_step", "device", "country")
             .agg(agg("count", None, "events"), agg("sum", e["revenue"], "revenue"), groups=FUNNEL))
    return daily.write_parquet(out, ["dt"], WriteSink(f"{out}/dt={DT}", bytes_per_row=26))


def activity(spark: SparkSession):
    e = spark.read(ENRICHED, f"{ENRICHED_ROOT}/dt={DT}")
    out = f"{WH}/analytics.db/customer_daily_activity"
    per_customer = (e.group_by("dt", "customer_id")
                    .agg(agg("count", None, "events"), agg("sum", e["revenue"], "spend"),
                         agg("min", e["event_ts"], "first_seen"), agg("max", e["event_ts"], "last_seen"),
                         groups=ACTIVE_CUSTOMER))
    return per_customer.write_parquet(out, ["dt"], WriteSink(f"{out}/dt={DT}", bytes_per_row=31))


def queries() -> tuple[SqlQuery, ...]:
    return (
        SqlQuery(f"events_enriched dt={DT}: web events + page catalog (broadcast) + customers (sort-merge join)",
                 enrich, think_ms=(2_400, 4_200)),
        SqlQuery(f"daily_funnel dt={DT}: events by page type, funnel step, device, country", funnel),
        SqlQuery(f"customer_daily_activity dt={DT}: events, spend, first and last event per customer", activity),
    )


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
        InputDataset(f"{EVENTS.root}/dt={DT}", files=4_300, mean_file_bytes=285 * MiB, file_size_sigma=0.22,
                     bytes_per_row=EVENTS.file_bytes_per_row),
        InputDataset(CUSTOMERS.root, files=520, mean_file_bytes=116 * MiB, file_size_sigma=0.12,
                     bytes_per_row=CUSTOMERS.file_bytes_per_row),
        InputDataset(PAGES.root, files=1, mean_file_bytes=3 * MiB, file_size_sigma=0.0,
                     bytes_per_row=PAGES.file_bytes_per_row),
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
