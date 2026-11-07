#!/usr/bin/env bash
# Turn the second NodeManager's host into a slow node for a reference capture (ref09): every executor
# JVM YARN starts on that host, and every Python worker it forks, runs in one cgroup (v1 `cpu`) with a
# CPU quota for the whole host, as if the node's CPUs were throttled. The driver (AM) is left alone.
#
#   reference/yarn/slow_node.sh start [QUOTA_PERCENT]   # default 50: half a CPU for all of the host's executors
#   reference/yarn/slow_node.sh stop
#
# Needs root and a writable /sys/fs/cgroup/cpu (the cloud dev container has both).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOST="${REF_NM2_HOST:-slownode}"
CG=/sys/fs/cgroup/cpu/sparksim-slow-node
PIDFILE="$ROOT/build/hadoop/slow-node-watcher.pid"

case "${1:-}" in
  start)
    quota="${2:-50}"
    mkdir -p "$CG" "$(dirname "$PIDFILE")"
    echo 100000 > "$CG/cpu.cfs_period_us"
    echo $(( quota * 1000 )) > "$CG/cpu.cfs_quota_us"
    # Executors are found by their command line (YarnCoarseGrainedExecutorBackend --hostname <host>) and
    # moved within 100 ms of starting; threads and later children follow the process into the cgroup.
    (
      while true; do
        for pid in $(pgrep -f "YarnCoarseGrainedExecutorBackend .*--hostname $HOST " || true); do
          grep -qx "$pid" "$CG/cgroup.procs" 2>/dev/null || echo "$pid" > "$CG/cgroup.procs" 2>/dev/null || true
        done
        sleep 0.1
      done
    ) &
    echo $! > "$PIDFILE"
    echo "executors on $HOST now share ${quota} % of one CPU (cgroup $CG)"
    ;;
  stop)
    [[ -f "$PIDFILE" ]] && { kill "$(cat "$PIDFILE")" 2>/dev/null || true; rm -f "$PIDFILE"; }
    echo "slow-node watcher stopped"
    ;;
  *) echo "usage: $0 start [QUOTA_PERCENT] | stop" >&2; exit 2 ;;
esac
