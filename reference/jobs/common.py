"""Shared helpers for the reference jobs.

Reference jobs are *real* PySpark 3.5.9 programs. Their event logs are the
ground truth for (a) the exact JSON the event-log writer must produce and
(b) calibrating the cost model. They are small on purpose — format truth, not scale.

Two modes (REF_MODE, set by reference/capture.sh):
  yarn           (default in capture.sh) spark-submit --master yarn --deploy-mode cluster
                 on the single-node Hadoop cluster from reference/yarn/cluster.sh.
                 Tables and event logs live on HDFS. This is the source of truth.
  local-cluster  fallback without Hadoop: `local-cluster[3,2,2048]` (3 executor JVMs x 2 cores).
                 NOTE: in 3.5.9 its teardown can log dozens of bogus ExecutorRemoved
                 events ("Shutdown hooks cannot be modified during shutdown").
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

MODE = os.environ.get("REF_MODE", "local-cluster")
ROOT = Path(os.environ.get("REF_ROOT", Path(__file__).resolve().parents[2]))
if MODE == "yarn":
    DATA = os.environ.get("REF_DATA", "hdfs:///data/ref")
    EVENTLOG_BASE = os.environ.get("REF_EVENTLOG_BASE", "hdfs:///ref-logs")
else:
    DATA = (ROOT / "build" / "ref-data").as_uri()
    EVENTLOG_BASE = (ROOT / "build" / "ref-logs-raw").as_uri()
    # Executors must run the same interpreter (pyarrow/pandas for pandas UDFs).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def session(name: str, conf: dict | None = None, driver_log_level: str = "WARN") -> SparkSession:
    """SparkSession whose event log lands in <EVENTLOG_BASE>/<name>/ (exactly one file per YARN attempt).

    In yarn mode, master/memory/cores come from spark-submit (see capture.sh) and
    the log directory is created on HDFS by capture.sh beforehand. The driver logs at WARN unless a job
    needs the driver's INFO lines as format truth (ref10: the AM and DAGScheduler lines of a failed app).
    """
    log_dir = f"{EVENTLOG_BASE}/{name}"
    b = SparkSession.builder.appName(name)
    if MODE != "yarn":
        local = Path(log_dir.removeprefix("file://"))
        shutil.rmtree(local, ignore_errors=True)
        local.mkdir(parents=True)
        b = (
            b.master("local-cluster[3,2,2048]")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "1536m")
        )
    b = (
        b.config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", log_dir)
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.ui.showConsoleProgress", "false")
        # Single-node harness: without this, executors read each other's shuffle files
        # straight from the shared local disk, so every fetch is "local" and a dead
        # executor's map output stays readable (no FetchFailed). Disabling it makes
        # cross-executor fetches go over the network, as on a multi-node cluster.
        .config("spark.shuffle.readHostLocalDisk", "false")
    )
    for k, v in (conf or {}).items():
        b = b.config(k, str(v))
    spark = b.getOrCreate()
    if MODE != "yarn":
        spark.sparkContext.addPyFile(__file__)  # executors unpickle helpers from this module
    spark.sparkContext.setLogLevel(driver_log_level)
    return spark


def table(name: str) -> str:
    return f"{DATA}/{name}"


def kill_own_executor_jvm() -> None:
    """SIGKILL the executor JVM hosting this Python worker (simulates a lost container/node).

    Matches CoarseGrainedExecutorBackend and YarnCoarseGrainedExecutorBackend.
    """
    import signal

    pid = os.getpid()
    while pid > 1:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            cmd = fh.read()
        if b"CoarseGrainedExecutorBackend" in cmd:
            os.kill(pid, signal.SIGKILL)
            return
        with open(f"/proc/{pid}/stat") as fh:
            pid = int(fh.read().rsplit(")", 1)[1].split()[1])
