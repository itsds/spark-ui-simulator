"""Environment tab: complete like a real YARN driver's, consistent with the cluster the run used."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sparksim.engine.runtime import AppRun
from sparksim.environment import environment_update, hadoop_defaults
from sparksim.scenario import load_scenario

ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "reference" / "event-logs" / "ref01-daily-etl-aqe"
# Set only by our reference harness (capture.sh / common.py), never by a production job.
HARNESS_SPARK = {"spark.submit.pyFiles", "spark.yarn.dist.pyFiles", "spark.shuffle.readHostLocalDisk",
                 "spark.sql.shuffle.partitions"}


@pytest.fixture(scope="module")
def ref_env():
    return next(e for e in map(json.loads, REF.read_text().splitlines())
                if e["Event"] == "SparkListenerEnvironmentUpdate")


@pytest.fixture(scope="module")
def env():
    spec = load_scenario("00-healthy-baseline").spec("baseline")
    run = AppRun(spec)
    return run, environment_update(run.env_app, cluster=spec.cluster)


def test_sections_are_as_complete_as_a_real_driver(ref_env, env):
    _run, e = env
    hp, sp, sysp = e.hadoop_properties, e.spark_properties, e.system_properties
    assert set(ref_env["Hadoop Properties"]) <= set(hp) and len(hp) >= len(ref_env["Hadoop Properties"])
    assert set(ref_env["System Properties"]) <= set(sysp)
    ref_spark = {k for k in ref_env["Spark Properties"] if not k.startswith("spark.yarn.appMasterEnv.REF_")}
    assert ref_spark - HARNESS_SPARK <= set(sp)
    assert len(e.classpath_entries) == len(ref_env["Classpath Entries"])
    assert e.jvm_information == ref_env["JVM Information"]
    for section in (hp, sp, sysp, e.classpath_entries):
        blob = json.dumps(section)
        assert "/home/user" not in blob and "spark-ui-simulator" not in blob, "reference-cluster paths leaked"


def test_hadoop_defaults_are_the_reference_minus_its_site_files():
    defaults = hadoop_defaults()
    assert len(defaults) == 974
    assert defaults["fs.defaultFS"] == "file:///" and defaults["dfs.replication"] == "3"
    assert defaults["mapreduce.framework.name"] == "local"  # mapred-default; the cluster's mapred-site sets yarn


def test_configuration_matches_the_simulated_cluster(env):
    run, e = env
    hp, sp = e.hadoop_properties, e.spark_properties
    c = load_scenario("00-healthy-baseline").spec("baseline").cluster
    assert hp["fs.defaultFS"] == f"hdfs://{c.namenode}:8020"
    assert hp["yarn.nodemanager.resource.memory-mb"] == str(c.nm_memory_mb)
    assert hp["yarn.nodemanager.resource.cpu-vcores"] == str(c.nm_vcores)
    assert hp["yarn.scheduler.minimum-allocation-mb"] == str(c.min_allocation_mb)
    # RM HA: the cluster id executors report as CLUSTER_ID, both RMs listed, recovery (epochs) on
    assert hp["yarn.resourcemanager.cluster-id"] == c.rm_cluster_id
    assert hp["yarn.resourcemanager.ha.rm-ids"] == "rm1,rm2"
    assert hp["yarn.resourcemanager.hostname.rm1"] == c.rm_host
    assert hp["yarn.resourcemanager.recovery.enabled"] == "true"
    # the external shuffle service is configured on the NodeManagers and enabled for Spark
    assert "spark_shuffle" in hp["yarn.nodemanager.aux-services"]
    assert sp["spark.shuffle.service.enabled"] == "true" and run.ess_enabled
    # container dirs in the logs and in the configuration agree
    assert sp["spark.yarn.app.container.log.dir"].startswith(hp["yarn.nodemanager.log-dirs"].split(",")[0])
    assert hp["yarn.nodemanager.local-dirs"] == run.nm_local_dirs
    assert sp["spark.driver.host"] == run.am_host.name
    assert sp["spark.yarn.app.id"] == run.yapp.app_id
    # the job's own --conf wins over the cluster defaults
    assert sp["spark.dynamicAllocation.enabled"] == "false"
