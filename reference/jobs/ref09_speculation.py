"""ref09 — speculative execution against a slow node (format truth for speculation).

Captured on the two-NodeManager cluster (`REF_SECOND_NM=1 reference/yarn/cluster.sh start`) while
`reference/yarn/slow_node.sh start` throttles every executor on host `slownode` to a CPU quota: the same
tasks run several times slower there. `spark.speculation=true` with Spark's defaults otherwise (check
every 100 ms; once 75 % of a stage's tasks succeeded, a task running longer than 1.5x their median, and
inefficient by the 3.4+ efficiency rule, gets a copy on another host; the first attempt to succeed wins,
the other one is killed with "another attempt succeeded").

Job 0 is a CPU-bound map stage (nested SHA-256 per row) with a shuffle write, job 1 sorts and writes
Parquet (task commit through the OutputCommitCoordinator), job 2 aggregates and collects.
"""

from common import session, table
from pyspark.sql import functions as F

spark = session(
    "ref09-speculation",
    {
        "spark.speculation": "true",
        # keep 24 reduce tasks (AQE would coalesce this small shuffle into one task: nothing to speculate)
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
    },
)
sc = spark.sparkContext

N = 24_000_000
digest = F.concat_ws(":", F.col("id").cast("string"), F.lit("clickstream"))
for _ in range(8):  # CPU work per row, so that a throttled host shows
    digest = F.sha2(digest, 256)
events = (
    spark.range(0, N, numPartitions=48)
    .withColumn("customer_id", F.abs(F.xxhash64("id")) % 100_000)
    .withColumn("digest", F.substring(digest, 1, 16))
    .drop("id")
)

sc.setJobDescription("hash events, shuffle by customer and write (speculation on a slow node)")
(
    events.repartition(24, "customer_id")
    .sortWithinPartitions("customer_id", "digest")
    .write.mode("overwrite").parquet(table("ref09_events"))
)

sc.setJobDescription("digests per customer (speculation on a slow node)")
written = spark.read.parquet(table("ref09_events"))
written.groupBy("customer_id").agg(F.countDistinct("digest").alias("n")).agg(F.max("n")).collect()

spark.stop()
