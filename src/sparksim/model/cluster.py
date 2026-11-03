"""The YARN cluster an app runs on: racks, NodeManagers, other tenants' load, container sizing.

Where our containers land is decided during the simulation (`engine.yarn_am`): the ApplicationMaster
(which hosts the driver in cluster mode) asks the ResourceManager for executor containers with no
locality preference. The CapacityScheduler hands out at most one such OFF_SWITCH container per
NodeManager heartbeat (`per-node-heartbeat.maximum-offswitch-assignments = 1`), and only on nodes with
enough free memory left by other tenants. So the executors spread over many hosts, and how many share a
host follows from how busy the cluster is.

Container sizes follow Spark's YARN rules: `spark.executor.memory` +
`memoryOverhead` (default max(384 MiB, 10 %)), rounded up to a multiple of
`yarn.scheduler.minimum-allocation-mb` (DefaultResourceCalculator: memory only).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from ..cost import calibration as cal
from ..rng import substream
from ..yarn import MiB, NodeManager


@dataclass(frozen=True)
class Host:
    name: str
    ip: str
    rack: str
    speed: float  # relative throughput of CPU and disks (1.0 = nominal)
    vcores: int
    memory_mb: int

    @property
    def nm(self) -> NodeManager:
        return NodeManager(self.name, self.ip)


@dataclass(frozen=True)
class ClusterSpec:
    cluster_timestamp: int  # ResourceManager start (ms): part of every application/container id
    racks: int = 6
    hosts_per_rack: int = 20
    nm_vcores: int = 48  # yarn.nodemanager.resource.cpu-vcores
    nm_memory_mb: int = 240 * 1024  # yarn.nodemanager.resource.memory-mb
    min_allocation_mb: int = 1024
    max_allocation_mb: int = 64 * 1024
    domain: str = "hadoop.prod.internal"
    host_prefix: str = "wn"
    subnet: str = "10.31"
    rm_host: str = "rm1.hadoop.prod.internal"  # the active ResourceManager
    namenode: str = "nn1.hadoop.prod.internal"
    # ResourceManager HA: standby RMs (yarn.resourcemanager.ha.rm-ids), the cluster id executors report as
    # CLUSTER_ID, and the RM epoch (work-preserving restarts/failovers) that prefixes container ids.
    rm_standby_hosts: tuple[str, ...] = ()
    rm_cluster_id: str = ""
    rm_epoch: int = 0
    # Share of each NodeManager's memory used by other tenants' containers when our app starts.
    occupancy: tuple[float, float] = (0.55, 0.97)
    host_speed_sigma: float = cal.HOST_SPEED_SIGMA


@dataclass
class Cluster:
    spec: ClusterSpec
    hosts: tuple[Host, ...]
    free_mb: dict[str, int]  # memory not used by other tenants at app start
    by_name: dict[str, Host] = field(init=False, repr=False)
    by_rack: dict[str, list[Host]] = field(init=False, repr=False)

    def __post_init__(self):
        self.by_name = {h.name: h for h in self.hosts}
        self.by_rack = {}
        for h in self.hosts:
            self.by_rack.setdefault(h.rack, []).append(h)

    def host(self, name: str) -> Host:
        return self.by_name[name]

    def rack_of(self, host: str) -> str:
        return self.by_name[host].rack

    @property
    def racks(self) -> list[str]:
        return sorted(self.by_rack)


def build_cluster(spec: ClusterSpec, seed: int) -> Cluster:
    rng = substream(seed, "cluster")
    hosts = []
    n = 0
    for r in range(spec.racks):
        for i in range(spec.hosts_per_rack):
            n += 1
            hosts.append(Host(
                name=f"{spec.host_prefix}{n:03d}.{spec.domain}",
                ip=f"{spec.subnet}.{r + 1}.{i + 11}",
                rack=f"/dc1/rack{r + 1:02d}",
                speed=math.exp(rng.gauss(0.0, spec.host_speed_sigma)),
                vcores=spec.nm_vcores,
                memory_mb=spec.nm_memory_mb,
            ))
    lo, hi = spec.occupancy
    free = {h.name: int(h.memory_mb * (1.0 - rng.uniform(lo, hi))) for h in hosts}
    return Cluster(spec, tuple(hosts), free)


# --------------------------------------------------------------------------- executor sizing


@dataclass(frozen=True)
class ExecutorSizing:
    instances: int = 40
    cores: int = 4
    memory_mb: int = 16 * 1024
    overhead_mb: int | None = None  # spark.executor.memoryOverhead (None: Spark's default)
    driver_memory_mb: int = 8 * 1024
    driver_cores: int = 2
    driver_overhead_mb: int | None = None

    @staticmethod
    def default_overhead(memory_mb: int) -> int:
        """`spark.executor.memoryOverhead` default: max(384 MiB, 0.10 x memory)."""
        return max(384, int(memory_mb * 0.10))

    @property
    def executor_overhead_mb(self) -> int:
        return self.overhead_mb if self.overhead_mb is not None else self.default_overhead(self.memory_mb)

    @property
    def am_overhead_mb(self) -> int:
        if self.driver_overhead_mb is not None:
            return self.driver_overhead_mb
        return self.default_overhead(self.driver_memory_mb)

    def container_mb(self, min_allocation_mb: int) -> int:
        return _round_up(self.memory_mb + self.executor_overhead_mb, min_allocation_mb)

    def am_container_mb(self, min_allocation_mb: int) -> int:
        return _round_up(self.driver_memory_mb + self.am_overhead_mb, min_allocation_mb)

    @property
    def heap_bytes(self) -> int:
        return self.memory_mb * MiB

    @property
    def driver_heap_bytes(self) -> int:
        return self.driver_memory_mb * MiB


def _round_up(x: int, step: int) -> int:
    return -(-x // step) * step


@dataclass
class Executor:
    id: str
    host: Host
    container_n: int  # container_..._<n:06d>
    container_id: str
    cores: int
    heap_bytes: int
    allocated_at: int  # RM allocation (NM heartbeat)
    registered_at: int  # ExecutorAdded
    block_manager_at: int
    block_manager_port: int
    max_memory: int  # BlockManager "Maximum Memory" (unified memory)
    removed_at: int | None = None
    extra: dict = field(default_factory=dict)
    launched_at: int = 0  # YarnAllocator launched the container (NM startContainer)
    container_mb: int = 0  # what YARN enforces
    pid: int = 0  # the executor JVM's process id (container log, NM diagnostics)
    # --- end of life (None while it runs until the app ends)
    nm_kill_at: int | None = None  # NodeManager stopped the container (SIGTERM)
    exit_at: int | None = None  # JVM exited
    exit_status: int | None = None  # ContainerStatus exit status the AM sees (-104 pmem, 143, ...)
    diagnostics: str = ""  # NodeManager container diagnostics
    removed_reason: str = ""  # SparkListenerExecutorRemoved reason
    exit_caused_by_app: bool = False
    killed_by_driver: bool = False
