#!/usr/bin/env bash
# Start the mock YARN NodeManager log server on the generated container logs.
#
# Usage: services/nm-logs/run.sh [LOG_DIR] [PORT]
#   LOG_DIR defaults to ./yarn-logs (written by `uv run sparksim generate ...`), PORT to 8042 (the
#   NodeManager web UI port). The History Server's executor stdout/stderr links point here
#   (spark.history.custom.executor.log.url in services/history-server/spark-defaults.conf).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="${1:-$ROOT/yarn-logs}"
PORT="${2:-8042}"
mkdir -p "$LOG_DIR"
exec python3 "$ROOT/services/nm-logs/server.py" --dir "$LOG_DIR" --port "$PORT"
