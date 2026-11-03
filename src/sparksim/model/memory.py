"""Physical memory of an executor container, as the NodeManager's ContainersMonitor measures it.

YARN does not look at the JVM heap: every `yarn.nodemanager.container-monitor.interval-ms` the
NodeManager sums the resident set (RSS) of the container's process tree and kills the container when it
is above the container's size (`ContainerExitStatus.KILLED_EXCEEDED_PMEM`, -104), which Spark reports as
"Container killed by YARN for exceeding physical memory limits ... Consider boosting
spark.executor.memoryOverhead". The container size is `spark.executor.memory` + `memoryOverhead`, rounded
up to `yarn.scheduler.minimum-allocation-mb` (`ExecutorSizing.container_mb`).

The resident set of an executor JVM is

    committed heap                          G1 commits heap as allocation pressure grows, up to -Xmx
  + G1 native structures                    remembered sets, card table, mark bitmaps (% of committed)
  + metaspace + code cache                  the JVMOffHeapMemory metric (grows with generated classes)
  + netty direct buffers                    the DirectPoolMemory metric (shuffle fetches in flight)
  + thread stacks + malloc arenas           per JVM / per task thread
  + Python workers                          PySpark UDF workers (0 for pure DataFrame plans)

so a container whose overhead is smaller than the JVM's own native footprint fits while the heap is
small, and gets killed once the heap is fully committed: the classic "it worked for the first ten
minutes" pmem kill. Inputs come from the tasks running on the executor at the time of the check.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..cost import calibration as cal

MiB = 1024 * 1024


def heap_used(heap_bytes: int, running: int, exec_memory: float, stages_seen: int) -> float:
    """Expected live heap of an executor (the mean of the heartbeat snapshot model in `engine.emit`)."""
    storage = 1_100_000 + min(stages_seen, 40) * 38_000 + 30_000
    return (cal.JVM_BASE_HEAP + running * cal.JVM_HEAP_PER_RUNNING_TASK + exec_memory + storage
            + 0.08 * heap_bytes)


@dataclass
class ContainerMemory:
    """One executor container's memory state between NodeManager monitor checks."""

    heap_max: int  # -Xmx (spark.executor.memory)
    host_ram: int  # the NodeManager host's physical memory (JVM default initial heap)
    cores: int
    limit: int  # container size in bytes (what YARN enforces)
    python_rss: int = 0  # resident Python workers (PySpark UDFs)
    growth_core_s: float = cal.G1_HEAP_GROWTH_CORE_S  # this JVM's heap growth constant (per-container spread)
    native_factor: float = 1.0  # spread of the native footprint between JVMs
    committed: float = 0.0
    busy_core_ms: float = 0.0
    last_check: int | None = None

    def __post_init__(self):
        self.committed = min(self.heap_max, self.host_ram * cal.JVM_INITIAL_HEAP_FRACTION_OF_RAM)

    @property
    def heap_initial(self) -> float:
        return min(self.heap_max, self.host_ram * cal.JVM_INITIAL_HEAP_FRACTION_OF_RAM)

    def check(self, t: int, running: int, exec_memory: float, fetching: int, stages_seen: int) -> int:
        """Advance to time `t` with `running` tasks; return the process-tree RSS in bytes."""
        if self.last_check is not None:
            self.busy_core_ms += running * (t - self.last_check)
        self.last_check = t
        used = heap_used(self.heap_max, running, exec_memory, stages_seen)
        grown = self.heap_initial + (self.heap_max - self.heap_initial) * (
            1.0 - math.exp(-self.busy_core_ms / 1e3 / self.growth_core_s))
        self.committed = min(self.heap_max, max(self.committed, grown, used * cal.G1_COMMIT_HEADROOM))
        native = self.native_factor * (
            self.committed * cal.G1_NATIVE_FRACTION
            + cal.JVM_OFF_HEAP_BASE + stages_seen * cal.JVM_OFF_HEAP_PER_STAGE
            + cal.DIRECT_POOL_BASE + fetching * cal.DIRECT_POOL_PER_FETCH
            + cal.THREAD_STACKS_RSS + self.cores * cal.MALLOC_ARENA_RSS_PER_CORE)
        return int(self.committed + native + self.python_rss)


def long2string(n: int) -> str:
    """Hadoop's `TraditionalBinaryPrefix.long2String(n, "", 1)` (used in NodeManager diagnostics)."""
    if n < 1024:
        return str(n)
    prefixes = "KMGTPE"
    i = 0  # number of prefixes n reaches
    while i < len(prefixes) and n >= 1 << (10 * (i + 1)):
        i += 1
    shift = 10 * i
    if n & ((1 << shift) - 1) == 0:
        return f"{n >> shift} {prefixes[i - 1]}"
    s = f"{n / (1 << shift):.1f}"
    if s.startswith("1024") and i < len(prefixes):
        i += 1
        s = f"{n / (1 << (10 * i)):.1f}"
    return f"{s} {prefixes[i - 1]}"
