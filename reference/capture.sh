#!/usr/bin/env bash
# Run the reference jobs against real Spark 3.5.9 and collect their artifacts:
#   reference/event-logs/<app-name>                 the event log (uncompressed JSON lines)
#   reference/yarn-logs/<app-name>/<container>/     real container stdout/stderr (yarn mode)
#
# Usage: reference/capture.sh [job ...]        e.g. reference/capture.sh ref03_skew_join
#        REF_MODE=local-cluster reference/capture.sh   # fallback without Hadoop
# Default: all jobs in order (ref00_datagen first — later jobs read its tables).
# yarn mode needs the cluster from reference/yarn/cluster.sh start. ref09 (speculation) needs the
# second NodeManager and its throttled executors:
#   REF_SECOND_NM=1 reference/yarn/cluster.sh start && reference/yarn/slow_node.sh start
#   reference/capture.sh ref09_speculation && reference/yarn/slow_node.sh stop
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"
MODE="${REF_MODE:-yarn}"
# Sandboxes may set JAVA_TOOL_OPTIONS (proxy settings); everything here is localhost-only,
# and it would otherwise leak into HDFS client output and container stderr.
unset JAVA_TOOL_OPTIONS
OUT="$ROOT/reference/event-logs"
mkdir -p "$OUT"
cd "$ROOT/reference/jobs"

if [[ $# -gt 0 ]]; then JOBS=("$@"); else mapfile -t JOBS < <(ls ref*.py | sed 's/\.py$//' | sort); fi

filter_noise() { grep -vE "NativeCodeLoader|JAVA_TOOL_OPTIONS|^\s*$" | tail -n "${1:-15}" || true; }

for job in "${JOBS[@]}"; do
  name="$(echo "$job" | sed -E 's/^(ref[0-9]+)_/\1-/; s/_/-/g')"
  echo "==> $job  (app: $name, mode: $MODE)"
  start=$(date +%s)

  if [[ "$MODE" == "yarn" ]]; then
    "$HADOOP_HOME/bin/hdfs" dfs -rm -r -f -skipTrash "/ref-logs/$name" >/dev/null 2>&1 || true
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/ref-logs/$name"
    "$SPARK_HOME/bin/spark-submit" \
      --master yarn --deploy-mode cluster --name "$name" \
      --driver-memory 2g --executor-memory 1536m --executor-cores 2 --num-executors 3 \
      --py-files common.py \
      --conf spark.yarn.archive=hdfs:///spark/spark-libs.zip \
      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="$PYTHON_BIN" \
      --conf spark.executorEnv.PYSPARK_PYTHON="$PYTHON_BIN" \
      --conf spark.yarn.appMasterEnv.REF_MODE=yarn \
      --conf spark.yarn.appMasterEnv.REF_DATA=hdfs:///data/ref \
      --conf spark.yarn.appMasterEnv.REF_EVENTLOG_BASE=hdfs:///ref-logs \
      "$job.py" > "$ROOT/build/submit-$name.log" 2>&1 || {
        echo "!! $name failed on YARN. Driver (AM) output:" >&2
        app="$(grep -oE 'application_[0-9]+_[0-9]+' "$ROOT/build/submit-$name.log" | head -1)"
        tail -n 25 "$HADOOP_BUILD/nm-logs/$app"/container_*_01_000001/stdout >&2 || true
        exit 1
      }
    grep -E "final status: |tracking URL" "$ROOT/build/submit-$name.log" | tail -n 2
    tmp="$(mktemp -d)"
    "$HADOOP_HOME/bin/hdfs" dfs -get "/ref-logs/$name/*" "$tmp/"
    files=("$tmp"/*)
    [[ ${#files[@]} -eq 1 && -f "${files[0]}" ]] || { echo "!! expected one event log for $name" >&2; exit 1; }
    app_id="$(basename "${files[0]}" | sed -E 's/_[0-9]+$//')"
    cp "${files[0]}" "$OUT/$name"; rm -rf "$tmp"
    # Real container logs (driver = first container, the ApplicationMaster).
    dest="$ROOT/reference/yarn-logs/$name"; rm -rf "$dest"; mkdir -p "$dest"
    for c in "$HADOOP_BUILD"/nm-logs/"$app_id"/container_* "$HADOOP_BUILD"/nm2-logs/"$app_id"/container_*; do
      [[ -d "$c" ]] || continue
      mkdir -p "$dest/$(basename "$c")"
      cp "$c"/stdout "$c"/stderr "$dest/$(basename "$c")/" 2>/dev/null || true
    done
  else
    REF_MODE=local-cluster uv run --project "$ROOT" python "$job.py" 2>&1 | filter_noise 15
    files=("$ROOT/build/ref-logs-raw/$name"/*)
    [[ ${#files[@]} -eq 1 && -f "${files[0]}" ]] || { echo "!! expected one event log for $name" >&2; exit 1; }
    cp "${files[0]}" "$OUT/$name"
  fi
  echo "    -> reference/event-logs/$name  ($(du -h "$OUT/$name" | cut -f1), $(( $(date +%s) - start ))s)"
done
