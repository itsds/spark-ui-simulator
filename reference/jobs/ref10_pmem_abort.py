"""ref10 — pmem kills until the job aborts and the application fails, in both YARN attempts.

Format truth for Scenario 03 (YARN memoryOverhead kills). The containers are capture.sh's 1536m heap +
384m overhead = 2 GB (rounded to the 512 MB minimum allocation); the reference cluster's NodeManager has
the physical-memory check on (reference/yarn/conf/yarn-site.xml, incl. resource.memory.enforced=false).

Job 0 is healthy: hash events, shuffle them by customer and write Parquet.

Query 1 joins two shuffles, so AQE runs their map stages as two concurrent jobs:
* job 2 (left): 3 range partitions enriched in a Python worker (mapInPandas), ~90 s each. The task of
  partition 0 also maps a 1.6 GB "lookup table" into its Python worker and touches every page, so its
  container's process tree (JVM + pyspark daemon + workers) goes over 2 GB and the NodeManager's monitor
  (every 3 s) kills it: exit code -104, "Container killed by YARN for exceeding physical memory limits",
  ExecutorLostFailure for every task on that executor, counted towards spark.task.maxFailures (exit caused
  by the app). Each retry of partition 0 kills the executor it lands on; the 4th loss aborts the stage and
  job 2 while its other tasks still run (Python: they don't notice a kill without interrupt for
  spark.python.task.killTimeout, longer than the driver lives).
* job 3 (right): 6 CPU-bound JVM partitions (8 nested SHA-256 per row, partial aggregation), ~60-90 s each,
  still running when job 2 fails: AQE cancels it (cleanUpAndThrowException → the shuffle job is
  cancelled), its tasks notice the kill within milliseconds (RangeExec checks between batches).

No map task finishes before the abort, so the reference cluster's missing shuffle service (lost map output
→ Resubmitted) plays no part. The write fails, the uncaught Py4JJavaError ends the Python driver with status
1: the AM reports "Final app status: FAILED", does not unregister (not the last attempt) and YARN starts
attempt 2 (yarn.resourcemanager.am.max-attempts = 2), which fails the same way and unregisters the
application as FAILED. Four executor losses per attempt stay below the AM's own limit
(spark.executor.maxNumFailures, default max(2 x 3 executors, 3) = 6), as in a 40-executor production job
(limit 80).
"""

from common import session, table
from pyspark.sql import functions as F

spark = session("ref10-pmem-abort", driver_log_level="INFO")  # the AM / DAGScheduler lines of a failure
sc = spark.sparkContext

HOT_PARTITION = 0
HOLD_BYTES = 1600 * 1024 * 1024
WORK_S = 90

sc.setJobDescription("stage events by customer (healthy)")
events = (
    spark.range(0, 4_000_000, numPartitions=12)
    .withColumn("customer_id", F.abs(F.xxhash64("id")) % 50_000)
    .withColumn("digest", F.substring(F.sha2(F.col("id").cast("string"), 256), 1, 16))
)
events.repartition(12, "customer_id").write.mode("overwrite").parquet(table("ref10_events"))


def enrich(batches):
    import time

    from pyspark import TaskContext

    lookup = None
    if TaskContext.get().partitionId() == HOT_PARTITION:
        # native memory of the Python worker: outside the JVM heap, inside the container's process tree
        lookup = bytearray(HOLD_BYTES)
        lookup[:: 4096] = b"\x01" * len(range(0, HOLD_BYTES, 4096))  # touch every page: resident
    deadline = time.time() + WORK_S
    for b in batches:
        yield b.assign(geo=(b["customer_id"] % 97).astype("string"))
    while time.time() < deadline:
        time.sleep(0.5)
    del lookup


geo = (
    spark.range(0, 300_000, numPartitions=3)
    .withColumn("customer_id", F.col("id") % 50_000)
    .mapInPandas(enrich, "id long, customer_id long, geo string")
)
digest = F.col("id").cast("string")
for _ in range(8):  # CPU work per row in the JVM
    digest = F.sha2(digest, 256)
scores = (
    spark.range(0, 108_000_000, numPartitions=6)
    .withColumn("customer_id", F.abs(F.xxhash64("id")) % 50_000)
    .groupBy("customer_id")
    .agg(F.max(F.xxhash64(digest)).alias("score"))  # a long: HashAggregate (a string max would sort)
)

sc.setJobDescription("enrich events with the geo lookup (pmem kills)")
geo.join(scores, "customer_id").write.mode("overwrite").parquet(table("ref10_enriched"))

spark.stop()
