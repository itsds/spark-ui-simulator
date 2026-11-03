#!/usr/bin/env bash
# Start the real Apache Spark 3.5.9 History Server without Docker, using the
# Spark distribution bundled in the project's PySpark install (uv venv).
#
# Usage: services/history-server/run.sh [LOG_DIR] [PORT]
#   LOG_DIR defaults to ./event-logs (generated scenarios); use
#   reference/event-logs to browse the real reference captures.
#   Runs in the foreground; Ctrl+C to stop. Uses JDK 17 (see scripts/env.sh, ADR-007).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="$(realpath -m "${1:-$ROOT/event-logs}")"
PORT="${2:-18080}"
mkdir -p "$LOG_DIR"

source "$ROOT/scripts/env.sh"
unset HADOOP_CONF_DIR YARN_CONF_DIR   # event logs are read from the local filesystem
if ! command -v java >/dev/null 2>&1; then
  echo "Java not found. Install JDK 17 and retry." >&2
  exit 1
fi
export SPARK_CONF_DIR="$ROOT/services/history-server"
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$LOG_DIR -Dspark.history.ui.port=$PORT ${SPARK_HISTORY_OPTS:-}"

echo "Spark History Server 3.5.9  ->  http://localhost:$PORT   (logs: $LOG_DIR)"
exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
