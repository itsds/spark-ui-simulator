"""The YARN cluster an app runs on: racks, NodeManagers, and where our containers land.

Static allocation (Phase 3). The ApplicationMaster (which hosts the driver in
cluster mode) asks the ResourceManager for `spark.executor.instances`
containers with no locality preference. The CapacityScheduler hands out at
most one such OFF_SWITCH container per NodeManager heartbeat
(`per-node-heartbeat.maximum-offswitch-assignments = 1`), and only on nodes
with enough free memory left by other tenants. So the executors spread over
many hosts, and how many share a host follows from how busy the cluster is.

Container sizes follow Spark's YARN rules: `spark.executor.memory` +
`memoryOverhead` (default max(384 MiB, 10 %)), rounded up to a multiple of
`yarn.scheduler.minimum-allocation-mb` (DefaultResourceCalculator: memory only).
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field

from ..cost import calibration as cal
from ..rng import substream
from ..yarn import MiB, NodeManager, YarnApp, unified_memory_max


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
    rm_host: str = "rm1.hadoop.prod.internal"
    namenode: str = "nn1.hadoop.prod.internal"
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


@dataclass
class Placement:
    am_host: Host
    executors: list[Executor]
    request_time: int


def place_application(cluster: Cluster, sizing: ExecutorSizing, app: YarnApp, driver_start: int,
                      seed: int, am_host: str | None = None) -> Placement:
    """Place the AM and the executor containers; return executors in registration order."""
    rng = substream(seed, "yarn-allocation")
    spec = cluster.spec
    free = dict(cluster.free_mb)
    am_mb = sizing.am_container_mb(spec.min_allocation_mb)
    exec_mb = sizing.container_mb(spec.min_allocation_mb)
    if exec_mb > spec.max_allocation_mb:
        raise ValueError(f"executor container {exec_mb} MB exceeds yarn.scheduler.maximum-allocation-mb")

    if am_host is None:
        candidates = [h for h in cluster.hosts if free[h.name] >= am_mb]
        am = rng.choice(candidates)
    else:
        am = cluster.host(am_host)
    free[am.name] -= am_mb

    request = driver_start + rng.randint(*cal.AM_REQUEST_DELAY_MS)
    phase = {h.name: rng.randrange(cal.NM_HEARTBEAT_MS) for h in cluster.hosts}

    # NodeManager heartbeats after the request, in time order; one off-switch container per heartbeat.
    heap = []
    for h in cluster.hosts:
        first = request + (phase[h.name] - request) % cal.NM_HEARTBEAT_MS
        heap.append((first, h.name))
    heapq.heapify(heap)
    allocations: list[tuple[int, Host]] = []
    deadline = request + 600_000
    while len(allocations) < sizing.instances:
        t, name = heapq.heappop(heap)
        if t > deadline:
            raise RuntimeError("cluster has no room for the requested executors")
        if free[name] >= exec_mb:
            free[name] -= exec_mb
            allocations.append((t, cluster.host(name)))
        heapq.heappush(heap, (t + cal.NM_HEARTBEAT_MS, name))

    # The AM's allocate() loop picks containers up every 200 ms and launches them in that order;
    # executor ids follow container order (YarnAllocator.executorIdCounter).
    executors = []
    for k, (t_alloc, host) in enumerate(allocations):
        pickup = request + -(-(t_alloc - request) // cal.AM_ALLOCATE_INTERVAL_MS) * cal.AM_ALLOCATE_INTERVAL_MS
        launch = pickup + rng.randint(5, 60)
        registered = launch + int(rng.randint(*cal.EXECUTOR_START_MS) / host.speed)
        n = k + 2  # 000001 is the AM
        executors.append(Executor(
            id=str(k + 1), host=host, container_n=n, container_id=app.container_id(n), cores=sizing.cores,
            heap_bytes=sizing.heap_bytes, allocated_at=t_alloc, registered_at=registered,
            block_manager_at=registered + rng.randint(*cal.EXECUTOR_BLOCK_MANAGER_MS),
            block_manager_port=rng.randint(30000, 60999), max_memory=unified_memory_max(sizing.heap_bytes),
        ))
    executors.sort(key=lambda e: (e.registered_at, int(e.id)))
    return Placement(am, executors, request)


def scheduling_start(placement: Placement, instances: int, driver_ready: int) -> int:
    """When the driver may start scheduling: YarnClusterScheduler waits for 80 % of executors (or 30 s)."""
    need = math.ceil(instances * cal.MIN_REGISTERED_RATIO)
    times = sorted(e.registered_at for e in placement.executors)
    ready = times[need - 1] if need <= len(times) else times[-1]
    return max(driver_ready, min(ready, driver_ready + cal.MAX_REGISTERED_WAIT_MS))
