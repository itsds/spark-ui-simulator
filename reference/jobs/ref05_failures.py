"""ref05 — failures: task retry, failed job, executor loss + fetch failure + stage retry.

1. A task fails on its first attempt and succeeds on retry (ExceptionFailure).
2. A job fails: one task throws on every attempt (spark.task.maxFailures=4).
3. An executor JVM is killed mid-shuffle (no external shuffle service):
   ExecutorRemoved, ExecutorLostFailure, FetchFailed, map-stage resubmission
   (stage attempt 1), replacement executor added.
"""

from operator import add

from common import kill_own_executor_jvm, session
from pyspark import TaskContext
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

spark = session("ref05-failures")
sc = spark.sparkContext


# 1. transient failure, recovered by retry
@F.udf(LongType())
def flaky(x):
    ctx = TaskContext.get()
    if ctx.partitionId() == 2 and ctx.attemptNumber() == 0 and x % 1000 == 999:
        raise RuntimeError("Transient failure talking to lookup service (simulated)")
    return x * 2


sc.setJobDescription("transient task failure, recovered")
spark.range(0, 200_000, numPartitions=8).select(flaky("id").alias("v")).agg(F.sum("v")).collect()


# 2. deterministic failure -> job aborted after 4 attempts
@F.udf(LongType())
def parse_amount(x):
    if x == 123_456:
        raise ValueError(f"Malformed amount in record id={x}: '12,34.5'")
    return x


sc.setJobDescription("bad record, job fails")
try:
    spark.range(0, 200_000, numPartitions=8).select(parse_amount("id").alias("v")).agg(F.sum("v")).collect()
except Exception as e:  # noqa: BLE001 — expected
    print("expected job failure:", type(e).__name__)


# 3. executor lost during shuffle read -> FetchFailed -> map stage re-run
# PySpark's TaskContext has no stageAttemptNumber(); an O_EXCL marker file (single-node
# cluster) guarantees exactly one kill per application run.
KILL_MARKER = f"/tmp/ref05-kill-{sc.applicationId}"


def maybe_kill(idx, it):
    import os

    if idx == 3:
        try:
            os.close(os.open(KILL_MARKER, os.O_CREAT | os.O_EXCL))
        except FileExistsError:
            return it
        kill_own_executor_jvm()
    return it


sc.setJobDescription("executor lost mid-shuffle, stage retry")
(
    sc.parallelize(range(3_000_000), 12)
    .map(lambda x: (x % 5000, 1))
    .reduceByKey(add, 24)
    .mapPartitionsWithIndex(maybe_kill)
    .count()
)

spark.stop()
