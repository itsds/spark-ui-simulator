"""ref00 — generate the reference tables (and capture a realistic partitioned-write app).

Tables (parquet, under build/ref-data/):
  orders            4M rows, partitioned by dt (7 days), ~6 files per day
  orders_skewed     4M rows, ~30% of rows on customer_id = 0 (guest checkout)
  customers         1M rows, wide (too big to broadcast -> sort-merge join)
  products          5k rows (broadcast)
  events_small_files  600 tiny files (small-files / parallel listing behaviour)
"""

from common import session, table
from pyspark.sql import functions as F

spark = session("ref00-datagen")

N_ORDERS = 4_000_000
N_CUSTOMERS = 1_000_000
N_PRODUCTS = 5_000


def orders(skewed: bool):
    df = spark.range(0, N_ORDERS, numPartitions=12).withColumnRenamed("id", "order_id")
    cust = (F.xxhash64("order_id") % N_CUSTOMERS).cast("long")
    cust = F.abs(cust) + 1
    if skewed:
        cust = F.when(F.abs(F.xxhash64("order_id", F.lit(7))) % 10 < 3, F.lit(0)).otherwise(cust)
    return (
        df.withColumn("customer_id", cust)
        .withColumn("product_id", F.abs(F.xxhash64("order_id", F.lit(1)) % N_PRODUCTS) + 1)
        .withColumn("quantity", (F.abs(F.xxhash64("order_id", F.lit(2))) % 5 + 1).cast("int"))
        .withColumn("amount", F.round(F.abs(F.xxhash64("order_id", F.lit(3))) % 50000 / 100, 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in ["PLACED", "SHIPPED", "DELIVERED",
                                                                          "RETURNED"]]),
                                           (F.abs(F.xxhash64("order_id", F.lit(4))) % 4 + 1).cast("int")))
        .withColumn("dt", F.date_add(F.lit("2026-10-01").cast("date"), (F.col("order_id") % 7).cast("int")))
        .withColumn("order_ts", F.col("dt").cast("timestamp") + F.make_interval(
            secs=(F.abs(F.xxhash64("order_id", F.lit(5))) % 86400).cast("int")))
    )


sc = spark.sparkContext
sc.setJobGroup("datagen", "Generate reference tables")

sc.setJobDescription("write orders")
orders(False).repartition(42, "dt").write.mode("overwrite").partitionBy("dt").parquet(table("orders"))

sc.setJobDescription("write orders_skewed")
orders(True).repartition(42, "dt").write.mode("overwrite").partitionBy("dt").parquet(table("orders_skewed"))

sc.setJobDescription("write customers")
(
    spark.range(1, N_CUSTOMERS + 1, numPartitions=6).withColumnRenamed("id", "customer_id")
    .withColumn("name", F.concat(F.lit("customer_"), F.col("customer_id").cast("string")))
    .withColumn("segment", F.element_at(F.array(F.lit("CONSUMER"), F.lit("SMB"), F.lit("ENTERPRISE")),
                                        (F.col("customer_id") % 3 + 1).cast("int")))
    .withColumn("country", F.element_at(F.array(*[F.lit(c) for c in ["US", "IN", "GB", "DE", "BR", "JP"]]),
                                        (F.col("customer_id") % 6 + 1).cast("int")))
    .withColumn("signup_dt", F.date_sub(F.lit("2026-10-01").cast("date"), (F.col("customer_id") % 2000).cast("int")))
    # Wide, high-entropy columns keep the table well above the 10 MB broadcast threshold.
    .withColumn("email", F.concat(F.sha1(F.col("customer_id").cast("string")).substr(1, 16), F.lit("@example.com")))
    .withColumn("address", F.concat_ws(" ", (F.col("customer_id") % 9999).cast("string"),
                                       F.sha1(F.col("name")).substr(1, 12), F.lit("Street")))
    .withColumn("phone", F.concat(F.lit("+1-"), (F.abs(F.xxhash64("customer_id")) % 10_000_000_000).cast("string")))
    .write.mode("overwrite").parquet(table("customers"))
)

sc.setJobDescription("write products")
(
    spark.range(1, N_PRODUCTS + 1, numPartitions=1).withColumnRenamed("id", "product_id")
    .withColumn("category", F.element_at(F.array(*[F.lit(c) for c in ["BOOKS", "TOYS", "HOME", "TECH"]]),
                                         (F.col("product_id") % 4 + 1).cast("int")))
    .withColumn("list_price", F.round(F.col("product_id") % 997 + 0.99, 2))
    .write.mode("overwrite").parquet(table("products"))
)

sc.setJobDescription("write events_small_files")
(
    spark.range(0, 60_000, numPartitions=600)
    .withColumn("event_type", F.when(F.col("id") % 3 == 0, "click").otherwise("view"))
    .write.mode("overwrite").parquet(table("events_small_files"))
)

spark.stop()
