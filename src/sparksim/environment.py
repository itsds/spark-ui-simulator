"""Environment tab content (`SparkListenerEnvironmentUpdate`) for a PySpark app in YARN cluster mode.

The keys mirror what real Spark 3.5.9 drivers logged on our YARN cluster (reference/event-logs,
EnvironmentUpdate); values are rebuilt for the simulated production cluster:

* **Spark Properties**: what the driver's SparkConf holds: the cluster's `spark-defaults.conf`
  (`CLUSTER_SPARK_DEFAULTS`), what spark-submit and the YARN Client set, and the job's `--conf`s.
* **Hadoop Properties**: the full Hadoop `Configuration` the driver runs with, ~1,000 keys: Hadoop
  3.3.6's core/hdfs/yarn/mapred defaults as a real driver logged them (`data/hadoop-3.3.6-defaults.json`,
  built by `scripts/extract_hadoop_defaults.py`) plus the cluster's site files (`hadoop_site`):
  HA ResourceManagers, NodeManager resources and dirs, the Spark shuffle service, log aggregation.
* **System Properties** of the AM's JVM (JDK 17 on Ubuntu, as in the reference logs) and the
  **Classpath** of the real Spark 3.5.9 YARN archive.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import cache
from importlib import resources

from .eventlog import EnvironmentUpdate
from .eventlog.ordering import scala_map
from .model.cluster import ClusterSpec
from .yarn import NodeManager, YarnApp, container_work_dir

# Spark 3.5 adds these to both driver and executor JVM options on Java 17 (JavaModuleOptions).
JAVA17_MODULE_OPTIONS = (
    "-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions "
    + " ".join(
        f"--add-opens={m}=ALL-UNNAMED"
        for m in (
            "java.base/java.lang", "java.base/java.lang.invoke", "java.base/java.lang.reflect", "java.base/java.io",
            "java.base/java.net", "java.base/java.nio", "java.base/java.util", "java.base/java.util.concurrent",
            "java.base/java.util.concurrent.atomic", "java.base/jdk.internal.ref", "java.base/sun.nio.ch",
            "java.base/sun.nio.cs", "java.base/sun.security.action", "java.base/sun.util.calendar",
            "java.security.jgss/sun.security.krb5",
        )
    )
    + " -Djdk.reflect.useDirectMethodHandle=false"
)

METRICS_PROPERTIES = {
    "*.sink.servlet.class": "org.apache.spark.metrics.sink.MetricsServlet",
    "*.sink.servlet.path": "/metrics/json",
    "applications.sink.servlet.path": "/metrics/applications/json",
    "master.sink.servlet.path": "/metrics/master/json",
}


# The cluster's spark-defaults.conf (/etc/spark/conf): what every job on it inherits.
NATIVE_LIB = "/opt/hadoop/lib/native"
CLUSTER_SPARK_DEFAULTS = {
    "spark.shuffle.service.enabled": "true",  # YarnShuffleService runs in every NodeManager (aux service)
    "spark.yarn.historyServer.address": "shs1.hadoop.prod.internal:18080",
    "spark.driver.extraLibraryPath": NATIVE_LIB,
    "spark.executor.extraLibraryPath": NATIVE_LIB,
    "spark.yarn.am.extraLibraryPath": NATIVE_LIB,
}


@cache
def hadoop_defaults() -> dict[str, str]:
    text = resources.files("sparksim.data").joinpath("hadoop-3.3.6-defaults.json").read_text()
    return json.loads(text)


def spark_archive_jars() -> list[str]:
    text = resources.files("sparksim.data").joinpath("spark-3.5.9-yarn-archive-jars.txt").read_text()
    return [ln for ln in text.splitlines() if ln and not ln.startswith("#")]


@dataclass
class PySparkYarnApp:
    """Everything about how the app was submitted that the Environment tab shows."""

    app: YarnApp
    app_name: str
    user: str
    script: str  # primary .py file
    am_node: NodeManager
    rm_host: str
    submit_time: int
    start_time: int
    driver_port: int
    driver_memory: str = "2g"
    executor_memory: str = "4g"
    executor_cores: int = 2
    executor_instances: int | None = 2  # --num-executors (None with dynamic allocation)
    nm_local_dir: str = "/data/1/yarn/nm"
    nm_local_dirs: str = "/data/1/yarn/nm,/data/2/yarn/nm,/data/3/yarn/nm"
    nm_log_dir: str = "/data/1/yarn/container-logs"
    event_log_dir: str = "hdfs:///spark-history"
    yarn_archive: str = "hdfs:///apps/spark/spark-3.5.9-libs.zip"
    python: str = "/usr/bin/python3"
    extra_conf: dict[str, str] = field(default_factory=dict)  # spark-submit --conf
    rm_standby_hosts: tuple[str, ...] = ()  # ResourceManager HA: the AM filter accepts every RM's proxy

    @property
    def am_work_dir(self) -> str:
        return container_work_dir(self.nm_local_dir, self.user, self.app, self.app.am_container_id)


def spark_properties(s: PySparkYarnApp) -> dict[str, str]:
    a = s.app
    props = dict(CLUSTER_SPARK_DEFAULTS)
    props |= {
        "spark.app.name": s.app_name,
        "spark.app.id": a.app_id,
        "spark.app.attempt.id": str(a.attempt),
        "spark.app.submitTime": str(s.submit_time),
        "spark.app.startTime": str(s.start_time),
        "spark.master": "yarn",
        "spark.submit.deployMode": "cluster",
        "spark.yarn.isPython": "true",
        "spark.yarn.app.id": a.app_id,
        "spark.yarn.app.container.log.dir": f"{s.nm_log_dir}/{a.app_id}/{a.am_container_id}",
        "spark.yarn.archive": s.yarn_archive,
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": s.python,
        "spark.executorEnv.PYSPARK_PYTHON": s.python,
        "spark.executorEnv.PYTHONPATH": "{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.9.7-src.zip",
        "spark.driver.host": s.am_node.host,  # Utils.localCanonicalHostName(): the FQDN on a DNS-backed cluster
        "spark.driver.port": str(s.driver_port),
        "spark.driver.memory": s.driver_memory,
        "spark.driver.extraJavaOptions": JAVA17_MODULE_OPTIONS,
        "spark.executor.id": "driver",
        "spark.executor.memory": s.executor_memory,
        "spark.executor.cores": str(s.executor_cores),
        "spark.executor.extraJavaOptions": JAVA17_MODULE_OPTIONS,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": s.event_log_dir,
        "spark.scheduler.mode": "FIFO",
        "spark.serializer.objectStreamReset": "100",
        "spark.rdd.compress": "True",
        "spark.ui.port": "0",
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.filters": "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter",
        "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS":
            ",".join((s.rm_host, *s.rm_standby_hosts)),
        "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES":
            ",".join(f"http://{h}:8088/proxy/{a.app_id}" for h in (s.rm_host, *s.rm_standby_hosts)),
    }
    if s.executor_instances is not None:
        props["spark.executor.instances"] = str(s.executor_instances)
    props.update(s.extra_conf)
    return props


def hadoop_site(c: ClusterSpec, nm_local_dirs: str, nm_log_dirs: str) -> dict[str, str]:
    """The production cluster's core-, hdfs-, yarn- and mapred-site.xml (what a client sees of them)."""
    nn = c.namenode
    rms = (c.rm_host, *c.rm_standby_hosts)
    zk = ",".join(f"zk{i}.{c.domain}:2181" for i in (1, 2, 3))
    site = {
        # core-site
        "fs.defaultFS": f"hdfs://{nn}:8020",
        "fs.trash.interval": "1440",
        "io.file.buffer.size": "131072",
        "io.compression.codecs": "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,"
                                 "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec,"
                                 "org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.ZStandardCodec",
        "net.topology.script.file.name": "/etc/hadoop/conf/topology.py",  # rack awareness
        "hadoop.security.authentication": "simple",
        "ha.zookeeper.quorum": zk,
        # hdfs-site
        "dfs.replication": "3",
        "dfs.blocksize": "134217728",
        "dfs.namenode.http-address": f"{nn}:9870",
        "dfs.namenode.name.dir": "file:///data/1/dfs/nn,file:///data/2/dfs/nn",
        "dfs.datanode.data.dir": ",".join(f"file:///data/{i}/dfs/dn" for i in range(1, 13)),
        "dfs.client.read.shortcircuit": "true",
        "dfs.domain.socket.path": "/var/run/hdfs-sockets/dn",
        "dfs.permissions.superusergroup": "hadoop",
        # yarn-site
        "yarn.resourcemanager.scheduler.class":
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler",
        "yarn.resourcemanager.recovery.enabled": "true",
        "yarn.resourcemanager.store.class": "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore",
        "yarn.resourcemanager.zk-address": zk,
        "yarn.scheduler.minimum-allocation-mb": str(c.min_allocation_mb),
        "yarn.scheduler.maximum-allocation-mb": str(c.max_allocation_mb),
        "yarn.scheduler.maximum-allocation-vcores": "16",
        "yarn.nodemanager.resource.memory-mb": str(c.nm_memory_mb),
        "yarn.nodemanager.resource.cpu-vcores": str(c.nm_vcores),
        "yarn.nodemanager.address": "${yarn.nodemanager.hostname}:45454",
        "yarn.nodemanager.local-dirs": nm_local_dirs,
        "yarn.nodemanager.log-dirs": nm_log_dirs,
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "true",
        "yarn.nodemanager.recovery.enabled": "true",
        "yarn.nodemanager.recovery.dir": "/var/lib/hadoop-yarn/yarn-nm-recovery",
        "yarn.nodemanager.aux-services": "mapreduce_shuffle,spark_shuffle",
        "yarn.nodemanager.aux-services.mapreduce_shuffle.class": "org.apache.hadoop.mapred.ShuffleHandler",
        "yarn.nodemanager.aux-services.spark_shuffle.class": "org.apache.spark.network.yarn.YarnShuffleService",
        "yarn.log-aggregation-enable": "true",
        "yarn.log-aggregation.retain-seconds": "604800",
        "yarn.nodemanager.remote-app-log-dir": "/app-logs",
        "yarn.log.server.url": f"http://jhs1.{c.domain}:19888/jobhistory/logs",
        "yarn.timeline-service.enabled": "false",
        # mapred-site
        "mapreduce.framework.name": "yarn",
        "mapreduce.jobhistory.address": f"jhs1.{c.domain}:10020",
        "mapreduce.jobhistory.webapp.address": f"jhs1.{c.domain}:19888",
        "mapreduce.fileoutputcommitter.algorithm.version": "1",
    }
    if c.rm_standby_hosts:
        ids = [f"rm{i}" for i in range(1, len(rms) + 1)]
        site |= {
            "yarn.resourcemanager.ha.enabled": "true",
            "yarn.resourcemanager.ha.automatic-failover.enabled": "true",
            "yarn.resourcemanager.ha.rm-ids": ",".join(ids),
            "yarn.resourcemanager.cluster-id": c.rm_cluster_id,
        }
        for rid, host in zip(ids, rms, strict=True):
            site[f"yarn.resourcemanager.hostname.{rid}"] = host
            site[f"yarn.resourcemanager.webapp.address.{rid}"] = f"{host}:8088"
    else:
        site["yarn.resourcemanager.hostname"] = c.rm_host
    return site


def hadoop_properties(c: ClusterSpec, nm_local_dirs: str = "/data/1/yarn/nm,/data/2/yarn/nm,/data/3/yarn/nm",
                      nm_log_dirs: str = "/data/1/yarn/container-logs") -> dict[str, str]:
    return {**hadoop_defaults(), **hadoop_site(c, nm_local_dirs, nm_log_dirs)}


def system_properties(s: PySparkYarnApp, java_home: str, java_version: str) -> dict[str, str]:
    work = s.am_work_dir
    script = s.script.rsplit("/", 1)[-1]
    return {
        "java.io.tmpdir": f"{work}/tmp",
        "line.separator": "\n",
        "path.separator": ":",
        "sun.management.compiler": "HotSpot 64-Bit Tiered Compilers",
        "sun.cpu.endian": "little",
        "java.specification.version": "17",
        "java.vm.specification.name": "Java Virtual Machine Specification",
        "java.vendor": "Ubuntu",
        "user.home": f"/home/{s.user}",
        "sun.arch.data.model": "64",
        "sun.boot.library.path": f"{java_home}/lib",
        "user.dir": work,
        "os.arch": "amd64",
        "java.vm.version": f"{java_version}+1-1-22.04-Ubuntu",
        "java.runtime.version": f"{java_version}+1-1-22.04-Ubuntu",
        "java.vm.info": "mixed mode, sharing",
        "java.runtime.name": "OpenJDK Runtime Environment",
        "file.separator": "/",
        "java.net.preferIPv6Addresses": "false",
        "java.class.version": "61.0",
        "file.encoding": "UTF-8",
        "jdk.reflect.useDirectMethodHandle": "false",
        "user.timezone": "Etc/UTC",
        "java.specification.vendor": "Oracle Corporation",
        "sun.java.launcher": "SUN_STANDARD",
        "os.version": "5.15.0-1068-generic",
        "native.encoding": "UTF-8",
        "user.country": "US",
        "user.language": "en",
        "os.name": "Linux",
        "java.vm.vendor": "Ubuntu",
        "user.name": s.user,
        "java.vm.name": "OpenJDK 64-Bit Server VM",
        "sun.java.command": (
            "org.apache.spark.deploy.yarn.ApplicationMaster --class org.apache.spark.deploy.PythonRunner "
            f"--primary-py-file {script} --properties-file {work}/__spark_conf__/__spark_conf__.properties "
            f"--dist-cache-conf {work}/__spark_conf__/__spark_dist_cache__.properties"
        ),
        "java.home": java_home,
        "java.version": java_version,
        # JDK 17.0.20.1 (Ubuntu) constants, as the reference AMs logged them
        "java.library.path": f"{NATIVE_LIB}::/usr/java/packages/lib:/usr/lib/x86_64-linux-gnu/jni:"
                             "/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu:/usr/lib/jni:/lib:/usr/lib",
        "java.specification.maintenance.version": "1",
        "java.specification.name": "Java Platform API Specification",
        "java.vendor.url": "https://ubuntu.com/",
        "java.vendor.url.bug": "https://bugs.launchpad.net/ubuntu/+source/openjdk-17",
        "java.version.date": "2026-08-18",
        "java.vm.compressedOopsMode": _oops_mode(s.driver_memory),
        "java.vm.specification.vendor": "Oracle Corporation",
        "java.vm.specification.version": "17",
        "jdk.debug": "release",
        "jetty.git.hash": "8f1440587e9e4ae7db3d74cf205643f3d707148d",
        "sun.io.unicode.encoding": "UnicodeLittle",
        "sun.jnu.encoding": "UTF-8",
    }


def _oops_mode(heap: str) -> str:
    """HotSpot's compressed-oops mode for -Xmx<heap>: a heap that ends below 4 GiB is "32-bit" (the
    1.5-2 GiB reference drivers), below 32 GiB "Zero based"."""
    gib = float(heap[:-1]) / (1024 if heap[-1].lower() == "m" else 1) if heap[-1].lower() in "gm" else 1.0
    return "32-bit" if gib <= 3 else "Zero based" if gib < 31 else "Non-zero based"


def classpath_entries(s: PySparkYarnApp) -> dict[str, str]:
    work = s.am_work_dir
    entries = {f"{work}/__spark_libs__/{jar}": "System Classpath" for jar in spark_archive_jars()}
    entries[work] = "System Classpath"
    entries[f"{work}/__spark_conf__/__hadoop_conf__"] = "System Classpath"
    entries[f"{work}/__spark_conf__"] = "System Classpath"
    return entries


def environment_update(s: PySparkYarnApp, namenode: str | None = None, cluster: ClusterSpec | None = None,
                       java_home: str = "/usr/lib/jvm/java-17-openjdk-amd64",
                       java_version: str = "17.0.20.1") -> EnvironmentUpdate:
    if cluster is None:
        cluster = ClusterSpec(cluster_timestamp=s.app.cluster_timestamp, rm_host=s.rm_host,
                              **({"namenode": namenode} if namenode else {}))
    # JsonProtocol writes each section from `Seq[(String, String)].toMap`: Scala Map iteration order.
    return EnvironmentUpdate(
        jvm_information=scala_map({"Java Home": java_home, "Java Version": f"{java_version} (Ubuntu)",
                                   "Scala Version": "version 2.12.18"}),
        spark_properties=scala_map(spark_properties(s)),
        hadoop_properties=scala_map(hadoop_properties(cluster, s.nm_local_dirs, s.nm_log_dir)),
        system_properties=scala_map(system_properties(s, java_home, java_version)),
        metrics_properties=scala_map(dict(METRICS_PROPERTIES)),
        classpath_entries=scala_map(classpath_entries(s)),
    )
