#!/usr/bin/env bash
# Single-node HDFS + YARN (Hadoop 3.3.x) for capturing *real* YARN event logs.
#
#   reference/yarn/cluster.sh start    # first run formats HDFS and uploads Spark jars
#   reference/yarn/cluster.sh status
#   reference/yarn/cluster.sh stop
#
# Needs HADOOP_HOME (default /opt/hadoop; download hadoop-3.3.6.tar.gz from
# downloads.apache.org) and JDK 17. State lives in build/hadoop/.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/env.sh"
# Sandboxes sometimes set JAVA_TOOL_OPTIONS (proxy settings); keep it out of the
# daemons, otherwise every container's stderr starts with "Picked up JAVA_TOOL_OPTIONS".
unset JAVA_TOOL_OPTIONS
H="$HADOOP_HOME/bin"

prepare_conf() {
  mkdir -p "$HADOOP_CONF_DIR"
  cp -r "$HADOOP_HOME/etc/hadoop/." "$HADOOP_CONF_DIR/"
  for f in "$ROOT"/reference/yarn/conf/*; do
    sed "s#@BUILD@#$HADOOP_BUILD#g" "$f" > "$HADOOP_CONF_DIR/$(basename "$f")"
  done
}

wait_for() { for _ in $(seq 1 60); do curl -sf "$1" >/dev/null && return 0; sleep 1; done; echo "timeout: $1" >&2; return 1; }

case "${1:-}" in
  start)
    prepare_conf
    if [[ ! -d "$HADOOP_BUILD/dfs/name/current" ]]; then
      "$H/hdfs" namenode -format -nonInteractive -force >/dev/null 2>&1
      FRESH=1
    fi
    "$H/hdfs" --daemon start namenode
    "$H/hdfs" --daemon start datanode
    "$H/yarn" --daemon start resourcemanager
    "$H/yarn" --daemon start nodemanager
    wait_for http://localhost:9870 && wait_for http://localhost:8088/ws/v1/cluster/info
    "$H/hdfs" dfsadmin -safemode wait >/dev/null
    if [[ -n "${FRESH:-}" ]] || ! "$H/hdfs" dfs -test -e /spark/spark-libs.zip; then
      echo "uploading Spark 3.5.9 jars to hdfs:///spark/spark-libs.zip (spark.yarn.archive)"
      tmp="$(mktemp -d)"; (cd "$SPARK_HOME/jars" && zip -q -r "$tmp/spark-libs.zip" .)
      "$H/hdfs" dfs -mkdir -p /spark /spark-history /ref-logs /data
      "$H/hdfs" dfs -put -f "$tmp/spark-libs.zip" /spark/; rm -rf "$tmp"
    fi
    echo "HDFS NameNode UI     http://localhost:9870"
    echo "YARN ResourceManager http://localhost:8088"
    ;;
  stop)
    for d in nodemanager resourcemanager; do "$H/yarn" --daemon stop "$d" || true; done
    for d in datanode namenode; do "$H/hdfs" --daemon stop "$d" || true; done
    ;;
  status)
    curl -sf http://localhost:8088/ws/v1/cluster/metrics | python3 -m json.tool | grep -E 'activeNodes|totalMB|totalVirtualCores|appsRunning' || echo "YARN not running"
    ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
