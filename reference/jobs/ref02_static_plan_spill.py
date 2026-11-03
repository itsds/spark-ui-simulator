"""ref02 — same kind of ETL with AQE off, broadcast disabled and tight memory.

Shows the static (non-adaptive) plan: SortMergeJoin everywhere, fixed shuffle
partition count, and real memory/disk spill from undersized execution memory.
"""

from common import session, table
from pyspark.sql import functions as F

spark = session(
    "ref02-static-plan-spill",
    {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": "8",
        "spark.memory.fraction": "0.1",
    },
)
sc = spark.sparkContext

orders = spark.read.parquet(table("orders"))
customers = spark.read.parquet(table("customers"))
products = spark.read.parquet(table("products"))

sc.setJobDescription("enrich all orders (sort-merge joins, spill)")
enriched = (
    orders.join(products, "product_id")
    .join(customers, "customer_id")
    .withColumn("line_total", F.col("amount") * F.col("quantity"))
)
(
    enriched.repartition(8, "country")
    .sortWithinPartitions("country", "customer_id", "order_ts")
    .write.mode("overwrite").parquet(table("out_enriched"))
)

sc.setJobDescription("customer lifetime value (window)")
from pyspark.sql.window import Window  # noqa: E402

w = Window.partitionBy("customer_id").orderBy("order_ts")
(
    orders.withColumn("running_spend", F.sum("amount").over(w))
    .groupBy("customer_id").agg(F.max("running_spend").alias("ltv"))
    .agg(F.avg("ltv")).collect()
)

spark.stop()
