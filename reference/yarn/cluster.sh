#!/usr/bin/env bash
# Single-node HDFS + YARN (Hadoop 3.3.x) for capturing *real* YARN event logs.
#
#   reference/yarn/cluster.sh start    # first run formats HDFS and uploads Spark jars
#   reference/yarn/cluster.sh status
#   reference/yarn/cluster.sh stop
#
# Needs HADOOP_HOME (default /opt/hadoop; download hadoop-3.3.6.tar.gz from
# downloads.apache.org) and JDK 17. State lives in build/hadoop/.
#
# REF_SECOND_NM=1 adds a second NodeManager on host `slownode` (an alias of 127.0.0.2 in /etc/hosts),
# so that a job's executors run on two hosts (ref09: speculation only launches a copy on another host).
# Both NodeManagers then offer 4.5 GB, so capture.sh's 3 executors split 1 + 2 around the AM.
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

NM2_HOST="${REF_NM2_HOST:-slownode}"
NM2_CONF="$HADOOP_BUILD/conf-nm2"

# Extra yarn-site properties: <name>=<value> lines inserted before </configuration>.
add_props() {
  local file="$1"; shift
  local xml=""
  for kv in "$@"; do xml+="  <property><name>${kv%%=*}</name><value>${kv#*=}</value></property>\n"; done
  sed -i "s#</configuration>#${xml}</configuration>#" "$file"
}

prepare_nm2_conf() {
  getent hosts "$NM2_HOST" >/dev/null || { echo "$NM2_HOST does not resolve: add '127.0.0.2 $NM2_HOST' to /etc/hosts" >&2; exit 1; }
  for c in "$HADOOP_CONF_DIR" "$NM2_CONF"; do
    [[ "$c" == "$NM2_CONF" ]] && { rm -rf "$NM2_CONF"; cp -r "$HADOOP_CONF_DIR" "$NM2_CONF"; }
    sed -i -e 's#<name>yarn.nodemanager.resource.memory-mb</name><value>[0-9]*<#<name>yarn.nodemanager.resource.memory-mb</name><value>4608<#' \
           -e 's#<name>yarn.nodemanager.resource.cpu-vcores</name><value>[0-9]*<#<name>yarn.nodemanager.resource.cpu-vcores</name><value>4<#' \
           "$c/yarn-site.xml"
  done
  sed -i -e "s#$HADOOP_BUILD/nm-local#$HADOOP_BUILD/nm2-local#" -e "s#$HADOOP_BUILD/nm-logs#$HADOOP_BUILD/nm2-logs#" \
    "$NM2_CONF/yarn-site.xml"
  add_props "$NM2_CONF/yarn-site.xml" "yarn.nodemanager.hostname=$NM2_HOST" \
    "yarn.nodemanager.localizer.address=$NM2_HOST:8140" "yarn.nodemanager.webapp.address=$NM2_HOST:8142"
}

nm2() {  # yarn --daemon <start|stop> nodemanager for the second NodeManager (own conf, pid and log dirs)
  HADOOP_CONF_DIR="$NM2_CONF" YARN_CONF_DIR="$NM2_CONF" HADOOP_PID_DIR="$HADOOP_BUILD/pid-nm2" \
    HADOOP_LOG_DIR="$HADOOP_BUILD/logs-nm2" "$H/yarn" --daemon "$1" nodemanager
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
    [[ -n "${REF_SECOND_NM:-}" ]] && prepare_nm2_conf
    "$H/yarn" --daemon start nodemanager
    [[ -n "${REF_SECOND_NM:-}" ]] && nm2 start
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
    [[ -d "$NM2_CONF" ]] && { nm2 stop || true; }
    for d in nodemanager resourcemanager; do "$H/yarn" --daemon stop "$d" || true; done
    for d in datanode namenode; do "$H/hdfs" --daemon stop "$d" || true; done
    ;;
  status)
    curl -sf http://localhost:8088/ws/v1/cluster/metrics | python3 -m json.tool | grep -E 'activeNodes|totalMB|totalVirtualCores|appsRunning' || echo "YARN not running"
    ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
