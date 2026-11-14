"""ref12 — YARN preemption without the external shuffle service: lost map output, FetchFailed, stage retries.

Format truth for Scenario 12 (fetch failure / stage retry). The reference cluster runs no shuffle service
(executors serve their own shuffle files) and the CapacityScheduler has two queues with preemption on
(`reference/yarn/conf`: `default` and `ds`, 50 % each, both elastic up to 100 %; the RM's
ProportionalCapacityPreemptionPolicy with its default intervals: 3 s monitor, 15 s before the kill). This
app runs in `default` and takes ~70 % of the NodeManager; twice it starts a small distributed-shell app in
`ds` (`preempt()`), which is under its guarantee, so the RM preempts this app's youngest executor
container: exit status -102, "Container ... was preempted.", not the app's fault.

One query under AQE (coalescing off, no broadcast): events (24 range partitions, ~10 s of chained SHA-256
per map task) sort-merge joined with customers (6 partitions), ~25 s per reduce task, written as Parquet:

* round 1, while the events map stage runs: the preempted executor's finished map tasks are re-queued
  (`Resubmitted`), its running ones fail with ExecutorLostFailure (exit not caused by the app); the
  replacement executor arrives when the `ds` app is done.
* round 2, while the write stage (the result stage of the final job) runs: the preempted executor (the
  replacement, the youngest container again) held map output of both shuffles. Reduce tasks fetching from
  it fail with FetchFailed; the DAGScheduler resubmits the lost map partitions (the final job's re-created,
  until then skipped, map stages) and then the write stage's missing partitions as its attempt 1.
"""

import os
import subprocess
import threading
import time

from common import session, table
from pyspark.sql import functions as F

spark = session("ref12-fetch-failure", driver_log_level="INFO",  # DAGScheduler / YarnAllocator lines
                conf={"spark.sql.autoBroadcastJoinThreshold": "-1",
                      "spark.sql.adaptive.coalescePartitions.enabled": "false"})
sc = spark.sparkContext

HADOOP_HOME = os.environ.get("REF_HADOOP_HOME", "/opt/hadoop")
DS_JAR = f"{HADOOP_HOME}/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.3.6.jar"


def preempt(tag: str, containers: int, seconds: int) -> None:
    """A distributed-shell app in queue `ds` whose 2 GB containers don't fit: YARN preempts some of ours."""
    env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}
    env["HADOOP_CONF_DIR"] = os.environ.get("REF_HADOOP_CONF_DIR", env.get("HADOOP_CONF_DIR", ""))
    subprocess.Popen(
        [f"{HADOOP_HOME}/bin/yarn", "jar", DS_JAR, "-jar", DS_JAR, "-appname", f"ref12-ds-{tag}", "-queue", "ds",
         "-num_containers", str(containers), "-container_memory", "2048", "-master_memory", "512",
         "-shell_command", f"sleep {seconds}"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def watcher() -> None:
    """Round 1 once the events map stage (the first 24-task stage) runs: one container preempted, the `ds`
    app done in time for the replacement executor to run map tasks too. Round 2 once the write stage (the
    next 24-task stage) runs: two containers, the replacement (the youngest) and one original executor.
    (AQE's map stages of a write are named after the write's call site too: `parquet at ...`.)"""
    tracker = sc.statusTracker()
    rounds = [("map", 2, 30), ("write", 3, 60)]  # (tag, containers, seconds)
    seen = set()
    while rounds:
        for sid in tracker.getActiveStageIds():
            info = tracker.getStageInfo(sid)
            if info and sid not in seen and info.numTasks == 24 and info.numActiveTasks > 0:
                seen.add(sid)
                preempt(*rounds.pop(0))
                break
        time.sleep(0.2)


threading.Thread(target=watcher, daemon=True).start()

# ~10 s per map task, ~25 s per reduce task: chained SHA-256 in a higher-order function (interpreted)
def chain(col, n):
    return F.aggregate(F.sequence(F.lit(1), F.lit(n)), col, lambda acc, _: F.sha2(acc, 256))


sc.setJobDescription("enrich events with customers (preempted executors)")
events = (
    spark.range(0, 2_400_000, numPartitions=24)
    .withColumn("customer_id", F.abs(F.xxhash64("id")) % 50_000)
    .withColumn("digest", chain(F.sha2(F.col("id").cast("string"), 256), 200))
)
customers = (
    spark.range(0, 50_000, numPartitions=6)
    .withColumnRenamed("id", "customer_id")
    .withColumn("segment", (F.col("customer_id") % 7).cast("string"))
)
enriched = events.join(customers, "customer_id").withColumn("score", chain(F.col("digest"), 500))
enriched.write.mode("overwrite").parquet(table("ref12_enriched"))

spark.stop()
