"""ref07 — driver-bound patterns: big collect()/toPandas(), driver-side loop of
small jobs (gaps in the Jobs event timeline), small files + parallel listing."""

import time

from common import session, table
from pyspark.sql import functions as F

spark = session("ref07-driver-bound", {"spark.sql.execution.arrow.pyspark.enabled": "true"})
sc = spark.sparkContext

orders = spark.read.parquet(table("orders"))

sc.setJobDescription("collect 1M rows to driver")
rows = orders.where(F.col("dt") <= "2026-10-02").select("order_id", "customer_id", "amount").limit(1_000_000).collect()

sc.setJobDescription("toPandas")
pdf = orders.where(F.col("dt") == "2026-10-03").select("order_id", "amount").toPandas()

# Driver-side loop: one tiny job per day, with driver work in between.
for day in ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04", "2026-10-05"]:
    sc.setJobDescription(f"per-day count {day}")
    orders.where(F.col("dt") == day).count()
    time.sleep(3)  # stands in for driver-side processing between actions

sc.setJobDescription("small files scan")
spark.read.parquet(table("events_small_files")).groupBy("event_type").count().collect()

spark.stop()
