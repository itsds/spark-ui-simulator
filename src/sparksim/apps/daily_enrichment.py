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

The workload `clickstream_enrichment`: the queries above, the tables' schemas and how big the day's input
usually is. Scenarios run it (`scenarios/00-healthy-baseline` is a normal night; `01-join-skew` a night
whose events carry a hot `customer_id`). `broadcast_hint=False` is the script before 2026-09-24, which joined
the page catalog without `F.broadcast()` and relied on Spark broadcasting it by size
(`05-missed-broadcast-join`). The physical plans, the SQL tab and every number come from the queries and the
data through the planner, the cost model and the scheduler.
"""

from __future__ import annotations

from ..engine.runtime import InputDataset
from ..model.workload import KeySpace, WriteSink
from ..sql.dataframe import Column, SparkSession, SqlQuery, Table, agg
from ..sql.expr import Func, Not
from .base import Params, Workload

MiB = 1024 * 1024
NN = "hdfs://nn1.hadoop.prod.internal:8020"
WH = f"{NN}/warehouse"

# The customer ids of a day of events: the snapshot's 208 M customers, rows per customer spread with CV 2.5.
CUSTOMER = KeySpace("customer_id", distinct=208_000_000, freq_cv=2.5)
ACTIVE_CUSTOMER = KeySpace("dt,customer_id", distinct=61_000_000, freq_cv=2.5)
FUNNEL = KeySpace("dt,page_type,funnel_step,device,country", distinct=57_600)
# The page ids of a day of events: ~44 k of the catalog's ~46 k pages are visited, and page popularity
# follows a Zipf law (home, search and cart pages first). It only matters where events are shuffled on it.
PAGE = KeySpace("page_id", distinct=44_000, zipf=0.85)
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


def queries(p: Params, data) -> tuple[SqlQuery, ...]:
    dt = p["dt"]
    customer = data["events"].keys["customer_id"]
    page = data["events"].keys["page_id"]

    def enrich(spark: SparkSession):
        events = spark.read(EVENTS, f"{EVENTS.root}/dt={dt}")
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
                          broadcast=bool(p["broadcast_hint"]), key_space=page)
                    .join(customers.where(Not(customers["is_deleted"]), selectivity=0.97)
                          .select(*(customers[c] for c in ("customer_id", "segment", "loyalty_tier", "home_country"))),
                          "customer_id", match=0.985, key_space=customer))
        return enriched.write_parquet(ENRICHED_ROOT, ["dt"], WriteSink(f"{ENRICHED_ROOT}/dt={dt}",
                                                                       ENRICHED.file_bytes_per_row))

    def funnel(spark: SparkSession):
        e = spark.read(ENRICHED, f"{ENRICHED_ROOT}/dt={dt}")
        out = f"{WH}/analytics.db/daily_funnel"
        daily = (e.group_by("dt", "page_type", "funnel_step", "device", "country")
                 .agg(agg("count", None, "events"), agg("sum", e["revenue"], "revenue"), groups=FUNNEL))
        return daily.write_parquet(out, ["dt"], WriteSink(f"{out}/dt={dt}", bytes_per_row=26))

    def activity(spark: SparkSession):
        e = spark.read(ENRICHED, f"{ENRICHED_ROOT}/dt={dt}")
        out = f"{WH}/analytics.db/customer_daily_activity"
        per_customer = (e.group_by("dt", "customer_id")
                        .agg(agg("count", None, "events"), agg("sum", e["revenue"], "spend"),
                             agg("min", e["event_ts"], "first_seen"), agg("max", e["event_ts"], "last_seen"),
                             groups=ACTIVE_CUSTOMER))
        return per_customer.write_parquet(out, ["dt"], WriteSink(f"{out}/dt={dt}", bytes_per_row=31))

    return (
        SqlQuery(f"events_enriched dt={dt}: web events + page catalog (broadcast) + customers (sort-merge join)",
                 enrich, think_ms=(2_400, 4_200)),
        SqlQuery(f"daily_funnel dt={dt}: events by page type, funnel step, device, country", funnel),
        SqlQuery(f"customer_daily_activity dt={dt}: events, spend, first and last event per customer", activity),
    )


def datasets(p: Params) -> dict[str, InputDataset]:
    """One day of events (~1.2 TiB), the customer snapshot (~60 GB, 210 M rows), the page catalog (3 MB)."""
    return {
        "events": InputDataset(f"{EVENTS.root}/dt={p['dt']}", files=4_300, mean_file_bytes=285 * MiB,
                               file_size_sigma=0.22, bytes_per_row=EVENTS.file_bytes_per_row,
                               keys={"customer_id": CUSTOMER, "page_id": PAGE}),
        "customers": InputDataset(CUSTOMERS.root, files=520, mean_file_bytes=116 * MiB, file_size_sigma=0.12,
                                  bytes_per_row=CUSTOMERS.file_bytes_per_row),
        "pages": InputDataset(PAGES.root, files=1, mean_file_bytes=3 * MiB, file_size_sigma=0.0,
                              bytes_per_row=PAGES.file_bytes_per_row),
    }


WORKLOAD = Workload(
    name="clickstream_enrichment",
    script="daily_enrichment.py",
    description="Nightly clickstream enrichment: one day of web events joined with the page catalog (broadcast) "
                "and the customer snapshot (sort-merge join), written to events_enriched, then two daily "
                "aggregates of it (daily_funnel, customer_daily_activity).",
    datasets=datasets,
    queries=queries,
    # broadcast_hint: F.broadcast() around the page catalog (in the script since 2026-09-24)
    params={"dt": "2026-11-02", "broadcast_hint": True},
)
