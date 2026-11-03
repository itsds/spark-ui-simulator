"""Environment tab content (`SparkListenerEnvironmentUpdate`) for a PySpark app in YARN cluster mode.

The keys mirror what real Spark 3.5.9 drivers logged on our YARN cluster
(reference/event-logs, EnvironmentUpdate). Values are rebuilt for the
simulated cluster. This is the Phase 2 version: a realistic Spark property set,
a short curated Hadoop property set and the real Spark 3.5.9 jar list. Phase 5
replaces the Hadoop part with a full cluster configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources

from .eventlog import EnvironmentUpdate
from .eventlog.ordering import scala_map
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
    executor_instances: int = 2
    nm_local_dir: str = "/data/1/yarn/nm"
    nm_log_dir: str = "/data/1/yarn/container-logs"
    event_log_dir: str = "hdfs:///spark-history"
    yarn_archive: str = "hdfs:///apps/spark/spark-3.5.9-libs.zip"
    python: str = "/usr/bin/python3"
    extra_conf: dict[str, str] = field(default_factory=dict)  # spark-submit --conf

    @property
    def am_work_dir(self) -> str:
        return container_work_dir(self.nm_local_dir, self.user, self.app, self.app.am_container_id)


def spark_properties(s: PySparkYarnApp) -> dict[str, str]:
    a = s.app
    props = {
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
        "spark.driver.host": s.am_node.ip,
        "spark.driver.port": str(s.driver_port),
        "spark.driver.memory": s.driver_memory,
        "spark.driver.extraJavaOptions": JAVA17_MODULE_OPTIONS,
        "spark.executor.id": "driver",
        "spark.executor.memory": s.executor_memory,
        "spark.executor.cores": str(s.executor_cores),
        "spark.executor.instances": str(s.executor_instances),
        "spark.executor.extraJavaOptions": JAVA17_MODULE_OPTIONS,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": s.event_log_dir,
        "spark.scheduler.mode": "FIFO",
        "spark.serializer.objectStreamReset": "100",
        "spark.rdd.compress": "True",
        "spark.ui.port": "0",
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.filters": "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter",
        "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS": s.rm_host,
        "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES":
            f"http://{s.rm_host}:8088/proxy/{a.app_id}",
    }
    props.update(s.extra_conf)
    return props


# A short, realistic slice of a Hadoop 3.3 YARN cluster's configuration (Phase 5 brings the full set).
def hadoop_properties(rm_host: str, namenode: str) -> dict[str, str]:
    return {
        "fs.defaultFS": f"hdfs://{namenode}:8020",
        "dfs.replication": "3",
        "dfs.blocksize": "134217728",
        "dfs.client.read.shortcircuit": "true",
        "hadoop.security.authentication": "simple",
        "hadoop.tmp.dir": "/tmp/hadoop-${user.name}",
        "io.file.buffer.size": "131072",
        "io.compression.codecs": "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,"
                                 "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec,"
                                 "org.apache.hadoop.io.compress.Lz4Codec",
        "mapreduce.framework.name": "yarn",
        "mapreduce.fileoutputcommitter.algorithm.version": "1",
        "yarn.resourcemanager.hostname": rm_host,
        "yarn.resourcemanager.address": f"{rm_host}:8032",
        "yarn.resourcemanager.scheduler.address": f"{rm_host}:8030",
        "yarn.resourcemanager.webapp.address": f"{rm_host}:8088",
        "yarn.resourcemanager.scheduler.class":
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler",
        "yarn.scheduler.minimum-allocation-mb": "1024",
        "yarn.scheduler.maximum-allocation-mb": "65536",
        "yarn.scheduler.maximum-allocation-vcores": "16",
        "yarn.nodemanager.resource.memory-mb": "114688",
        "yarn.nodemanager.resource.cpu-vcores": "30",
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "true",
        "yarn.nodemanager.local-dirs": "/data/1/yarn/nm,/data/2/yarn/nm,/data/3/yarn/nm",
        "yarn.nodemanager.log-dirs": "/data/1/yarn/container-logs",
        "yarn.nodemanager.aux-services": "mapreduce_shuffle,spark_shuffle",
        "yarn.nodemanager.aux-services.spark_shuffle.class": "org.apache.spark.network.yarn.YarnShuffleService",
        "yarn.log-aggregation-enable": "true",
        "yarn.log.server.url": f"http://{rm_host}:19888/jobhistory/logs",
        "yarn.timeline-service.enabled": "false",
        "hadoop.service.shutdown.timeout": "30s",
    }


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
    }


def classpath_entries(s: PySparkYarnApp) -> dict[str, str]:
    work = s.am_work_dir
    entries = {f"{work}/__spark_libs__/{jar}": "System Classpath" for jar in spark_archive_jars()}
    entries[work] = "System Classpath"
    entries[f"{work}/__spark_conf__/__hadoop_conf__"] = "System Classpath"
    entries[f"{work}/__spark_conf__"] = "System Classpath"
    return entries


def environment_update(s: PySparkYarnApp, namenode: str,
                       java_home: str = "/usr/lib/jvm/java-17-openjdk-amd64",
                       java_version: str = "17.0.20.1") -> EnvironmentUpdate:
    # JsonProtocol writes each section from `Seq[(String, String)].toMap`: Scala Map iteration order.
    return EnvironmentUpdate(
        jvm_information=scala_map({"Java Home": java_home, "Java Version": f"{java_version} (Ubuntu)",
                                   "Scala Version": "version 2.12.18"}),
        spark_properties=scala_map(spark_properties(s)),
        hadoop_properties=scala_map(hadoop_properties(s.rm_host, namenode)),
        system_properties=scala_map(system_properties(s, java_home, java_version)),
        metrics_properties=scala_map(dict(METRICS_PROPERTIES)),
        classpath_entries=scala_map(classpath_entries(s)),
    )
