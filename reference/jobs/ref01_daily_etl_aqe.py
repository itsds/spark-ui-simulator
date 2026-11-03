"""ref01 — a typical production daily ETL with Spark 3.5 defaults (AQE on).

Partition-pruned scan, filter, broadcast join (products), sort-merge join
(customers), aggregation, partitioned write. AQE coalesces shuffle partitions.
"""

from common import session, table
from pyspark.sql import functions as F

spark = session("ref01-daily-etl-aqe")
sc = spark.sparkContext

orders = spark.read.parquet(table("orders"))
customers = spark.read.parquet(table("customers"))
products = spark.read.parquet(table("products"))

sc.setJobDescription("daily_revenue: enrich + aggregate + write")
daily = (
    orders.where(F.col("dt").between("2026-10-03", "2026-10-06"))
    .where(F.col("status") != "RETURNED")
    .join(products, "product_id")
    .join(customers, "customer_id")
    .groupBy("dt", "country", "segment", "category")
    .agg(F.sum(F.col("amount") * F.col("quantity")).alias("revenue"),
         F.countDistinct("customer_id").alias("buyers"),
         F.count("*").alias("orders"))
)
daily.write.mode("overwrite").partitionBy("dt").parquet(table("out_daily_revenue"))

sc.setJobDescription("top customers report")
top = (
    orders.where(F.col("dt") == "2026-10-06")
    .groupBy("customer_id").agg(F.sum("amount").alias("spend"))
    .orderBy(F.desc("spend")).limit(100)
)
top.collect()

spark.stop()
