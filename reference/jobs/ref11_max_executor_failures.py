"""ref11 — many pmem kills, and the ApplicationMaster's executor-failure limit that they don't reach.

The other candidate ending of a pmem-killed application (ref10 is the first: a task index failing 4 times).
Every task of job 1's stage maps 1.2 GB into its Python worker, so any executor running two of them goes over
its 2 GB container (1536m heap + 384m overhead) and the NodeManager kills it (-104). Each kill fails the two
tasks on it once (ExecutorLostFailure, counted towards spark.task.maxFailures). The capture was meant to
reach the AM's own limit, spark.executor.maxNumFailures (default max(2 x 3 executors, 3) = 6, "Max number of
executor failures (6) reached", exit code 11). It does not: 9 containers die in attempt 1 and the app still
ends through the task path ("Task 1 in stage 3.0 failed 4 times"), in both attempts. Spark 3.5.9's
YarnAllocator counts only "Container from a bad node" exits towards that limit
(YarnAllocatorNodeHealthTracker.handleResourceAllocationFailure); KILLED_EXCEEDED_PMEM / VMEM have their own
branch and never count (checked with javap). The capture name keeps the original intent.

Job 0 is healthy, as in ref10.
"""

from common import session, table
from pyspark.sql import functions as F

spark = session("ref11-max-executor-failures", driver_log_level="INFO")
sc = spark.sparkContext

HOLD_BYTES = 1200 * 1024 * 1024
WORK_S = 60

sc.setJobDescription("stage events by customer (healthy)")
events = (
    spark.range(0, 4_000_000, numPartitions=12)
    .withColumn("customer_id", F.abs(F.xxhash64("id")) % 50_000)
    .withColumn("digest", F.substring(F.sha2(F.col("id").cast("string"), 256), 1, 16))
)
events.repartition(12, "customer_id").write.mode("overwrite").parquet(table("ref11_events"))


def enrich(batches):
    import time

    # every task: native memory of its Python worker, outside the JVM heap, inside the container
    lookup = bytearray(HOLD_BYTES)
    lookup[:: 4096] = b"\x01" * len(range(0, HOLD_BYTES, 4096))  # touch every page: resident
    deadline = time.time() + WORK_S
    for b in batches:
        yield b.assign(geo=(b["id"] % 97).astype("string"))
    while time.time() < deadline:
        time.sleep(0.5)
    del lookup


sc.setJobDescription("enrich events with the geo lookup (every task over the overhead)")
(
    spark.range(0, 1_200_000, numPartitions=12)
    .mapInPandas(enrich, "id long, geo string")
    .write.mode("overwrite").parquet(table("ref11_enriched"))
)

spark.stop()
