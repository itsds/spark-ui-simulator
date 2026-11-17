"""Partition pruning on real Spark 3.5.9 (local mode): the plans Scenario 06's planner must reproduce.

    services: none. Run: source scripts/env.sh; unset JAVA_TOOL_OPTIONS
              .venv/bin/python reference/local/partition_pruning.py build/pruning \
                  > reference/local/partition_pruning.txt

The enrichment query of `apps/daily_enrichment.py` over tiny tables with the production schemas, in three
forms of the events read:

* `path`: the partition directory with basePath (Scenario 00's script): one partition, no filter on dt;
* `event_date`: the table root, `withColumn("dt", F.to_date("event_ts"))`, `where(F.col("dt") == DT)` (the
  incident: the filter is on the event date that shadows the partition column, so nothing is pruned);
* `event_date_pruned`: the same, after `where(F.col("dt").between(DT, NEXT))` on the partition column (the fix).

Then the myth checks: predicates that *cast the partition column* still prune (PartitionFilters), and the
event-time range Spark pushes to Parquet (UnwrapCastInBinaryComparison) skips no row group when the files store
timestamps as INT96 (Spark's default writer) and does with TIMESTAMP_MICROS. Prints each plan
(`explain("formatted")` of the initial plan) and each scan's SQL metrics from the event log.
"""

import glob
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

base = os.path.abspath(sys.argv[1])
WH = f"file://{base}/warehouse"
logs = f"{base}/eventlog"
os.makedirs(logs, exist_ok=True)
for f in glob.glob(f"{logs}/*"):
    os.remove(f)
spark = (SparkSession.builder.master("local[2]").appName("partition-pruning")
         .config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", f"file://{logs}")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2400").config("spark.sql.files.maxPartitionBytes", "256m")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # customers: sort-merge join, as at scale
         .getOrCreate())
sc = spark.sparkContext
DAYS = ["2026-10-20", "2026-10-21", "2026-10-22"]
DT, NEXT = "2026-10-21", "2026-10-22"

if not os.path.exists(f"{base}/warehouse"):
    n = 30_000
    ev = spark.range(0, n).select(
        F.col("id").alias("event_id"),
        (F.lit(1792454400) + F.col("id") * 8).cast("timestamp").alias("event_ts"),  # 2026-10-20 00:00 UTC + 8 s
        (F.col("id") % 977).alias("session_id"), (F.col("id") % 501).alias("customer_id"),
        F.concat(F.lit("a-"), F.col("id").cast("string")).alias("anonymous_id"), (F.col("id") % 50).alias("page_id"),
        F.lit("view").alias("event_type"),
        F.concat(F.lit("https://shop/p?utm_campaign=c"), (F.col("id") % 7).cast("string")).alias("page_url"),
        F.lit("https://ref").alias("referrer_url"), F.lit("Mozilla/5.0").alias("user_agent"),
        F.lit("10.1.2.3").alias("ip_address"), F.lit("mobile").alias("device"), F.lit("ios").alias("os"),
        F.lit("safari").alias("browser"), F.lit("DE").alias("country"), F.lit("BY").alias("region"),
        (F.col("id") % 13 == 0).alias("is_bot"), F.lit("a1").alias("consent_flags"),
        F.lit("e1").alias("experiment_ids"), F.lit("{}").alias("payload"),
        (F.col("id") % 5).cast("double").alias("revenue"), F.lit("EUR").alias("currency"),
        F.lit("7.1").alias("app_version"),
        (F.lit(1792454400) + F.col("id") * 8 + 30).cast("timestamp").alias("ingest_ts"))
    ev = ev.withColumn("dt", F.to_date("ingest_ts"))  # partitioned by the day the event landed
    ev.repartition(2, "dt").write.partitionBy("dt").parquet(f"{WH}/web.db/events")
    spark.range(0, 501).select(
        F.col("id").alias("customer_id"), F.lit("x@y").alias("email"), F.lit("A").alias("first_name"),
        F.lit("B").alias("last_name"), F.lit("1").alias("phone"), F.lit("s").alias("street"), F.lit("c").alias("city"),
        F.lit("1").alias("postcode"), F.lit("DE").alias("home_country"), F.current_date().alias("birth_date"),
        F.current_timestamp().alias("signup_ts"), F.lit("gold").alias("segment"), F.lit("t1").alias("loyalty_tier"),
        F.lit(True).alias("marketing_opt_in"), F.lit(1.0).alias("lifetime_value"), F.lit("{}").alias("preferences"),
        (F.col("id") % 31 == 0).alias("is_deleted"), F.current_timestamp().alias("updated_at"),
        F.lit("crm").alias("source_system"), F.lit("c1").alias("crm_id"),
    ).write.parquet(f"{WH}/crm.db/customers_snapshot")
    spark.range(0, 50).select(
        F.col("id").alias("page_id"), F.lit("/p").alias("path"), F.lit("t").alias("title"),
        F.lit("product").alias("page_type"), (F.col("id") % 5).cast("int").alias("funnel_step"),
        F.lit("c").alias("category"), F.lit(True).alias("is_active"), F.current_timestamp().alias("updated_at"),
    ).write.parquet(f"{WH}/web.db/page_catalog")


def enrich(mode: str):
    if mode == "path":
        events = spark.read.option("basePath", f"{WH}/web.db/events").parquet(f"{WH}/web.db/events/dt={DT}")
    else:
        events = spark.read.parquet(f"{WH}/web.db/events")
        if mode == "event_date_pruned":
            events = events.where(F.col("dt").between(DT, NEXT))
        events = events.withColumn("dt", F.to_date("event_ts")).where(F.col("dt") == DT)
    customers = spark.read.parquet(f"{WH}/crm.db/customers_snapshot")
    pages = spark.read.parquet(f"{WH}/web.db/page_catalog")
    return (events.where(~F.col("is_bot"))
            .select("event_id", "event_ts", "session_id", "customer_id", "page_id", "event_type", "device",
                    "country", "revenue",
                    F.lower(F.regexp_extract("page_url", "utm_campaign=([^&]+)", 1)).alias("campaign"), "dt")
            .join(F.broadcast(pages.select("page_id", "page_type", "funnel_step")), "page_id")
            .join(customers.where(~F.col("is_deleted"))
                  .select("customer_id", "segment", "loyalty_tier", "home_country"), "customer_id"))


for mode in ("path", "event_date", "event_date_pruned"):
    sc.setJobDescription(f"enrich {mode}")
    df = enrich(mode)
    print(f"=== enrich {mode}")
    df.explain("formatted")
    df.write.mode("overwrite").partitionBy("dt").parquet(f"{WH}/analytics.db/events_enriched_{mode}")

ev = spark.read.parquet(f"{WH}/web.db/events")
checks = {
    "myth cast(dt as string)": ev.where(F.col("dt").cast("string") == DT),
    "myth to_date(cast(dt as string), fmt)": ev.where(F.to_date(F.col("dt").cast("string"), "yyyy-MM-dd") == DT),
    "int96 event_ts::date": ev.where(F.col("event_ts").cast("date") == DT),
}
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
ev.write.mode("overwrite").partitionBy("dt").parquet(f"{WH}/web.db/events_micros")
micros = spark.read.parquet(f"{WH}/web.db/events_micros")
checks["micros event_ts::date"] = micros.where(F.col("event_ts").cast("date") == DT)
for name, df in checks.items():
    sc.setJobDescription(name)
    print(f"=== {name}")
    df.explain("formatted")
    df.write.mode("overwrite").parquet(f"{base}/out/{name.split()[0]}_{abs(hash(name)) % 1000}")
spark.stop()

# ---------------------------------------------------------------- scan metrics from the event log
names, values, desc = {}, {}, {}


def walk(info, eid):
    for m in info.get("metrics", []):
        names[m["accumulatorId"]] = (eid, info["nodeName"], m["name"])
    for c in info.get("children", []):
        walk(c, eid)


for line in open(sorted(glob.glob(f"{logs}/*"))[-1]):
    d = json.loads(line)
    e = d["Event"]
    if e.endswith(("SQLExecutionStart", "SQLAdaptiveExecutionUpdate")):
        walk(d["sparkPlanInfo"], d["executionId"])
        if "description" in d:
            desc[d["executionId"]] = d["description"]
    elif e.endswith("DriverAccumUpdates"):
        for aid, v in d["accumUpdates"]:
            values[aid] = values.get(aid, 0) + v
    elif e == "SparkListenerTaskEnd":
        for a in d["Task Info"]["Accumulables"]:
            if a["ID"] in names:
                values[a["ID"]] = values.get(a["ID"], 0) + int(a["Update"])
print("=== scan metrics (execution: description)")
per = {}
for aid, (eid, node, name) in names.items():
    if node.startswith("Scan parquet") and "time" not in name:
        per.setdefault(eid, {})[name] = per.get(eid, {}).get(name, 0) + (values.get(aid) or 0)
for eid in sorted(per):
    print(f"{eid}: {desc[eid]}: " + ", ".join(f"{k} = {v}" for k, v in sorted(per[eid].items())))

# ---------------------------------------------------------------- the writes' plans as the event log has them
print("=== SQLExecutionStart physicalPlanDescription of the three enrich writes")
for line in open(sorted(glob.glob(f"{logs}/*"))[-1]):
    d = json.loads(line)
    if d["Event"].endswith("SQLExecutionStart") and d["description"].startswith("enrich ") and d["executionId"] < 3:
        print(f"--- execution {d['executionId']}: {d['description']}")
        print(d["physicalPlanDescription"])
