"""ref04 — caching: reuse with cache vs recomputation without it.

Block updates are logged (spark.eventLog.logBlockUpdates.enabled) so the
History Server can populate the Storage tab and executor storage memory.
NOTE: that flag is OFF by default in production; with it off the History
Server's Storage tab is empty even if the app cached data.
"""

from common import session, table
from pyspark import StorageLevel
from pyspark.sql import functions as F

spark = session("ref04-cache-reuse", {"spark.eventLog.logBlockUpdates.enabled": "true"})
sc = spark.sparkContext

orders = spark.read.parquet(table("orders"))
base = orders.where(F.col("status") == "DELIVERED").withColumn("line_total", F.col("amount") * F.col("quantity"))

# Without cache: each action re-reads and re-filters the input.
sc.setJobDescription("no-cache: count")
base.count()
sc.setJobDescription("no-cache: revenue by day")
base.groupBy("dt").agg(F.sum("line_total")).collect()

# With cache (MEMORY_AND_DISK, the DataFrame default).
cached = base.persist(StorageLevel.MEMORY_AND_DISK)
sc.setJobDescription("cache: materialize")
cached.count()
sc.setJobDescription("cache: revenue by day")
cached.groupBy("dt").agg(F.sum("line_total")).collect()
sc.setJobDescription("cache: top products")
cached.groupBy("product_id").agg(F.sum("line_total").alias("rev")).orderBy(F.desc("rev")).limit(10).collect()

# An RDD cache that stays persisted at app end (visible in Storage tab).
# (PySpark stores RDD data serialized anyway; there is no MEMORY_ONLY_SER in Python.)
rdd = sc.parallelize(range(500_000), 12).map(lambda x: (x % 997, x)).persist(StorageLevel.MEMORY_ONLY)
sc.setJobDescription("rdd cache: materialize")
rdd.count()

cached.unpersist()
spark.stop()
