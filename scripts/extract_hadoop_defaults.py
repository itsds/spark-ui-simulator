#!/usr/bin/env python3
"""Build `src/sparksim/data/hadoop-3.3.6-defaults.json` from a real reference event log.

A Spark driver on YARN logs the whole Hadoop `Configuration` it runs with (core-, hdfs-, yarn- and
mapred-default.xml of Hadoop 3.3.6, plus the cluster's *-site.xml) as the Environment tab's "Hadoop
Properties". We take that list from `reference/event-logs/ref01-daily-etl-aqe` and put back Hadoop's
default for every key our single-node harness set in `reference/yarn/conf/*.xml`. The result is
Hadoop 3.3.6's defaults as a real driver sees them; `sparksim.environment` layers a production
cluster's site configuration on top.

    uv run scripts/extract_hadoop_defaults.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "reference" / "event-logs" / "ref01-daily-etl-aqe"
OUT = ROOT / "src" / "sparksim" / "data" / "hadoop-3.3.6-defaults.json"

# Defaults from Hadoop 3.3.6 core-default.xml / hdfs-default.xml / yarn-default.xml for the keys the
# reference harness overrides.
DEFAULTS = {
    "dfs.datanode.data.dir": "file://${hadoop.tmp.dir}/dfs/data",
    "dfs.namenode.name.dir": "file://${hadoop.tmp.dir}/dfs/name",
    "dfs.permissions.enabled": "true",
    "dfs.replication": "3",
    "fs.defaultFS": "file:///",
    "hadoop.tmp.dir": "/tmp/hadoop-${user.name}",
    "yarn.log-aggregation-enable": "false",
    "yarn.nodemanager.delete.debug-delay-sec": "0",
    "yarn.nodemanager.env-whitelist": "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,"
                                      "CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ",
    "yarn.nodemanager.local-dirs": "${hadoop.tmp.dir}/nm-local-dir",
    "yarn.nodemanager.log-dirs": "${yarn.log.dir}/userlogs",
    "yarn.nodemanager.log.retain-seconds": "10800",
    "yarn.nodemanager.pmem-check-enabled": "true",
    "yarn.nodemanager.resource.cpu-vcores": "-1",
    "yarn.nodemanager.resource.memory-mb": "-1",
    "yarn.nodemanager.vmem-check-enabled": "true",
    "yarn.resourcemanager.hostname": "0.0.0.0",
    "yarn.scheduler.maximum-allocation-mb": "8192",
    "yarn.scheduler.minimum-allocation-mb": "1024",
}


def main() -> int:
    env = next(e for e in map(json.loads, REF.read_text().splitlines())
               if e["Event"] == "SparkListenerEnvironmentUpdate")
    props = dict(env["Hadoop Properties"])
    site = set()
    for f in (ROOT / "reference" / "yarn" / "conf").glob("*.xml"):
        site |= set(re.findall(r"<name>([^<]+)</name>", f.read_text()))
    harness = sorted(k for k in site if k in props)
    missing = [k for k in harness if k not in DEFAULTS]
    if missing:
        raise SystemExit(f"no Hadoop default recorded for harness keys: {missing}")
    for k in harness:
        props[k] = DEFAULTS[k]
    leftovers = [k for k, v in props.items() if "/home/user" in v or "spark-ui-simulator" in v]
    if leftovers:
        raise SystemExit(f"reference-cluster paths left in: {leftovers}")
    OUT.write_text(json.dumps(dict(sorted(props.items())), indent=0, ensure_ascii=False) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(props)} properties ({len(harness)} reset to Hadoop defaults)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
