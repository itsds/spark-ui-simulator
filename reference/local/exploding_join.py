"""Exploding joins on real Spark 3.5.9 (local mode): what Scenario 07's simulator must reproduce.

    services: none. Run: source scripts/env.sh; unset JAVA_TOOL_OPTIONS
              .venv/bin/python reference/local/exploding_join.py "$PWD/build/exploding" \
                  > reference/local/exploding_join.txt

The enrichment query of `apps/daily_enrichment.py` over tiny tables with the production schemas, first with
the customer snapshot as it was (one row per customer), then with the same path rewritten as an SCD type 2
history (one row per customer *version*: the snapshot's columns + `valid_from`, `valid_to`, `is_current`):

* `snapshot`: the job against one row per customer (the last good night);
* `history`: the unchanged job against the history (the incident): the plan text is the same, the sort-merge
  join's `number of output rows` is the events' rows times the versions of their customers;
* `fix_*`: three ways of writing the fix (`is_current` before, after or with `~is_deleted`): the Filter and
  PushedFilters each one plans.

Then: the SortMergeJoin buffer's thresholds and what a key with 200,000 versions does to it (`spill size`), and
a small benchmark of the cost of an extra output row (one key's buffered matches iterated per streamed row).
Prints each plan (`explain("formatted")`), the SQL metrics of the scans, joins and sorts from the event log, and
the writes' `physicalPlanDescription` as the event log has them.
"""

import glob
import json
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

base = os.path.abspath(sys.argv[1])
WH = f"file://{base}/warehouse"
logs = f"{base}/eventlog"
os.makedirs(logs, exist_ok=True)
for f in glob.glob(f"{logs}/*"):
    os.remove(f)
spark = (SparkSession.builder.master("local[2]").appName("exploding-join")
         .config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", f"file://{logs}")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2400").config("spark.sql.files.maxPartitionBytes", "256m")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # customers: sort-merge join, as at scale
         .getOrCreate())
sc = spark.sparkContext
DT = "2026-11-06"
N_CUSTOMERS = 501
CUSTOMERS = f"{WH}/crm.db/customers_snapshot"


def customers_frame(versions: bool):
    """The snapshot's 20 columns; as a history, customer k has 1 + k % 4 versions (the last one current)."""
    c = spark.range(0, N_CUSTOMERS).withColumnRenamed("id", "customer_id")
    if versions:
        c = c.withColumn("version", F.explode(F.sequence(F.lit(1), (F.col("customer_id") % 4 + 1).cast("int"))))
    else:
        c = c.withColumn("version", F.lit(1))
    out = c.select(
        F.col("customer_id"), F.lit("x@y").alias("email"), F.lit("A").alias("first_name"),
        F.lit("B").alias("last_name"), F.lit("1").alias("phone"), F.lit("s").alias("street"), F.lit("c").alias("city"),
        F.lit("1").alias("postcode"), F.lit("DE").alias("home_country"), F.current_date().alias("birth_date"),
        F.current_timestamp().alias("signup_ts"),
        F.concat(F.lit("seg"), F.col("version").cast("string")).alias("segment"),
        F.lit("t1").alias("loyalty_tier"), F.lit(True).alias("marketing_opt_in"), F.lit(1.0).alias("lifetime_value"),
        F.lit("{}").alias("preferences"), (F.col("customer_id") % 31 == 0).alias("is_deleted"),
        F.current_timestamp().alias("updated_at"), F.lit("crm").alias("source_system"), F.lit("c1").alias("crm_id"),
        F.col("version"))
    if versions:
        last = F.col("version") == (F.col("customer_id") % 4 + 1)
        out = out.select(
            "*", (F.lit(1767225600) + F.col("version") * 86400).cast("timestamp").alias("valid_from"),
            F.when(last, F.lit("9999-12-31 00:00:00").cast("timestamp"))
             .otherwise((F.lit(1767225600) + (F.col("version") + 1) * 86400).cast("timestamp")).alias("valid_to"),
            last.alias("is_current"))
    return out.drop("version")


if not os.path.exists(f"{base}/warehouse"):
    n = 30_000
    ev = spark.range(0, n).select(
        F.col("id").alias("event_id"),
        (F.lit(1793923200) + F.col("id") * 2).cast("timestamp").alias("event_ts"),  # 2026-11-06 00:00 UTC + 2 s
        (F.col("id") % 977).alias("session_id"), (F.col("id") % N_CUSTOMERS).alias("customer_id"),
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
        (F.lit(1793923200) + F.col("id") * 2 + 30).cast("timestamp").alias("ingest_ts"))
    ev = ev.withColumn("dt", F.to_date("ingest_ts"))
    ev.repartition(2, "dt").write.partitionBy("dt").parquet(f"{WH}/web.db/events")
    spark.range(0, 50).select(
        F.col("id").alias("page_id"), F.lit("/p").alias("path"), F.lit("t").alias("title"),
        F.lit("product").alias("page_type"), (F.col("id") % 5).cast("int").alias("funnel_step"),
        F.lit("c").alias("category"), F.lit(True).alias("is_active"), F.current_timestamp().alias("updated_at"),
    ).write.parquet(f"{WH}/web.db/page_catalog")


def enrich(mode: str):
    events = spark.read.option("basePath", f"{WH}/web.db/events").parquet(f"{WH}/web.db/events/dt={DT}")
    customers = spark.read.parquet(CUSTOMERS)
    pages = spark.read.parquet(f"{WH}/web.db/page_catalog")
    if mode == "fix_before":  # the fix as one more where() before the existing one
        dim = customers.where(F.col("is_current")).where(~F.col("is_deleted"))
    elif mode == "fix_after":
        dim = customers.where(~F.col("is_deleted")).where(F.col("is_current"))
    elif mode == "fix_and":
        dim = customers.where(F.col("is_current") & ~F.col("is_deleted"))
    else:
        dim = customers.where(~F.col("is_deleted"))
    return (events.where(~F.col("is_bot"))
            .select("event_id", "event_ts", "session_id", "customer_id", "page_id", "event_type", "device",
                    "country", "revenue",
                    F.lower(F.regexp_extract("page_url", "utm_campaign=([^&]+)", 1)).alias("campaign"), "dt")
            .join(F.broadcast(pages.select("page_id", "page_type", "funnel_step")), "page_id")
            .join(dim.select("customer_id", "segment", "loyalty_tier", "home_country"), "customer_id"))


print("=== confs")
for k in ("spark.sql.sortMergeJoinExec.buffer.in.memory.threshold",
          "spark.sql.sortMergeJoinExec.buffer.spill.threshold"):
    print(f"{k} = {spark.conf.get(k)}")

expected = {}
for table, modes in (("snapshot", ("snapshot",)), ("history", ("history", "fix_before", "fix_after", "fix_and"))):
    customers_frame(table == "history").write.mode("overwrite").parquet(CUSTOMERS)
    for mode in modes:
        sc.setJobDescription(f"enrich {mode}")
        df = enrich(mode)
        print(f"=== enrich {mode}")
        df.explain("formatted")
        out = f"{WH}/analytics.db/events_enriched_{mode}"
        df.write.mode("overwrite").partitionBy("dt").parquet(out)
        expected[mode] = spark.read.parquet(out).count()

# events per customer x versions of that customer (inner join, not deleted): the rows the join must output
ev = (spark.read.option("basePath", f"{WH}/web.db/events").parquet(f"{WH}/web.db/events/dt={DT}")
      .where(~F.col("is_bot")).groupBy("customer_id").count())
hist = spark.read.parquet(CUSTOMERS).where(~F.col("is_deleted")).groupBy("customer_id").agg(F.count("*").alias("v"))
print("=== expected join rows")
print("events (not bots):", ev.agg(F.sum("count")).first()[0])
print("sum(events x versions):", ev.join(hist, "customer_id").agg(F.sum(F.col("count") * F.col("v"))).first()[0])
print("written rows per mode:", expected)

# ---------------------------------------------------------------- one key with 200,000 versions
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.sql.adaptive.enabled", "false")
sc.setJobDescription("hot key: 200000 buffered rows")
# (id % 1 + 7, not a literal: a literal key on both sides is folded and the join becomes a nested loop join)
left = spark.range(0, 2_000).select((F.col("id") % 1 + 7).alias("k"), F.col("id").alias("l"))
right = spark.range(0, 200_000).select((F.col("id") % 1 + 7).alias("k"), F.col("id").alias("r"))
left.join(right, "k").write.format("noop").mode("overwrite").save()

# ---------------------------------------------------------------- benchmark: cost of an extra output row
spark.conf.set("spark.sql.shuffle.partitions", "8")
bench = {}
for m in (1, 4, 16):
    keys = 100_000
    lt = spark.range(0, 8_000_000).select((F.col("id") % keys).alias("k"), F.col("id").alias("a"),
                                          (F.col("id") * 7).alias("b"))
    rt = spark.range(0, keys * m).select((F.col("id") % keys).alias("k"), F.col("id").alias("c"),
                                         F.lit("segment").alias("d"))
    for rep in range(2):  # the first run warms the JVM up
        sc.setJobDescription(f"bench m={m} rep={rep}")
        t = time.time()
        lt.join(rt, "k").write.format("noop").mode("overwrite").save()
        bench[(m, rep)] = time.time() - t
print("=== benchmark wall seconds (left 8 M rows, 100 k keys, m right rows per key)")
for (m, rep), s in sorted(bench.items()):
    print(f"m={m} rep={rep}: {s:.2f} s")
spark.stop()

# ---------------------------------------------------------------- SQL metrics and task times from the event log
names, values, desc, stage_run = {}, {}, {}, {}


def walk(info, eid):
    for m in info.get("metrics", []):
        names[m["accumulatorId"]] = (eid, info["nodeName"], m["name"])
    for c in info.get("children", []):
        walk(c, eid)


log = sorted(glob.glob(f"{logs}/*"))[-1]
stage_desc = {}
for line in open(log):
    d = json.loads(line)
    e = d["Event"]
    if e.endswith(("SQLExecutionStart", "SQLAdaptiveExecutionUpdate")):
        walk(d["sparkPlanInfo"], d["executionId"])
        if "description" in d:
            desc[d["executionId"]] = d["description"]
    elif e.endswith("DriverAccumUpdates"):
        for aid, v in d["accumUpdates"]:
            values[aid] = values.get(aid, 0) + v
    elif e == "SparkListenerJobStart":
        for s in d["Stage IDs"]:
            stage_desc[s] = d["Properties"].get("spark.job.description", "")
    elif e == "SparkListenerTaskEnd":
        for a in d["Task Info"]["Accumulables"]:
            if a["ID"] in names:
                values[a["ID"]] = values.get(a["ID"], 0) + int(a["Update"])
        sid = d["Stage ID"]
        tm = d["Task Metrics"]
        run, gc, peak = stage_run.get(sid, (0, 0, 0))
        stage_run[sid] = (run + tm["Executor Run Time"], gc + tm["JVM GC Time"], max(peak, tm["Peak Execution Memory"]))
print("=== SQL metrics (execution: description): scans, joins, sorts")
per = {}
for aid, (eid, node, name) in names.items():
    if node.startswith(("Scan parquet", "SortMergeJoin", "Sort", "Filter")) and "time" not in name:
        key = (eid, node)
        per.setdefault(key, {})[name] = per.get(key, {}).get(name, 0) + (values.get(aid) or 0)
for (eid, node) in sorted(per):
    print(f"{eid}: {desc[eid]}: {node}: " + ", ".join(f"{k} = {v}" for k, v in sorted(per[(eid, node)].items())))
print("=== executor run time / JVM GC time (ms) / max peak execution memory per stage, benchmark and hot key")
for sid in sorted(stage_run):
    if stage_desc.get(sid, "").startswith(("bench", "hot key")):
        print(f"stage {sid}: {stage_desc[sid]}: run {stage_run[sid][0]}, gc {stage_run[sid][1]}, "
              f"peak {stage_run[sid][2]}")

print("=== SQLExecutionStart physicalPlanDescription of the enrich writes")
for line in open(log):
    d = json.loads(line)
    if d["Event"].endswith("SQLExecutionStart") and d["description"].startswith("enrich "):
        print(f"--- execution {d['executionId']}: {d['description']}")
        print(d["physicalPlanDescription"])
