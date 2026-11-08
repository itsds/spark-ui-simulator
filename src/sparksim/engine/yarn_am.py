"""YARN inside the simulation: the ResourceManager's grants, Spark's `YarnAllocator`, NodeManager monitors.

Ported from Spark 3.5.9 (`ApplicationMaster.allocationThreadImpl`, `YarnAllocator`) and Hadoop 3.3.6
(`ContainersMonitorImpl`, `ContainerImpl`, `ContainerLaunch`):

* **AM allocate loop.** `allocateResources()` first updates the container requests
  (`missing = target - pending - running`), then calls the RM, which returns the containers it granted
  since the previous call and the containers that completed. The loop polls every 200 ms while
  requests or loss-reason queries are pending, doubling up to the 3 s heartbeat, and every 3 s
  otherwise; the driver asking for more executors wakes it at once (`resetAllocatorInterval`).
* **RM grants.** The CapacityScheduler hands out at most one OFF_SWITCH container per NodeManager
  heartbeat (1 s), on nodes with room left by other tenants and not on the AM's excluded node list
  (`spark.yarn.exclude.nodes`, sent by `YarnAllocatorNodeHealthTracker` when the allocator is created).
  YARN asks are absolute: the AM re-sends its whole outstanding count whenever its request table
  changed, *before* it matched the containers in the same response, so the RM can grant a few
  containers more than needed. Spark releases them ("Received 2
  containers from YARN, launching executors on 1 of them."), and they use up container numbers, exactly
  as on our reference cluster (`reference/yarn-logs`, container 000005 of ref01).
* **Container lifecycle.** A launched container registers its executor after JVM start-up. The
  NodeManager checks its process-tree RSS every 3 s (`model.memory`) and kills it when it exceeds the
  container size (exit status -104, diagnostics in Hadoop's wording, SIGTERM → exit code 143). It reports
  completed containers at its next heartbeat; the AM sees them at its next allocate call and tells the
  driver why the executor is gone (`YarnAllocator.processCompletedContainers`). Containers the driver
  asked to kill (dynamic allocation) are released at the next allocate call and stopped by their
  NodeManager at its next heartbeat.

Everything that happens is recorded (`AppRun.yarn_log`) for the container logs; the event log only sees
what reaches the driver.
"""

from __future__ import annotations

import heapq
import math
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ..cost import calibration as cal
from ..model.cluster import Cluster, Executor, ExecutorSizing, Host
from ..model.memory import ContainerMemory, long2string
from ..yarn import MiB, YarnApp, unified_memory_max

if TYPE_CHECKING:
    from .runtime import AppRun

# [spark] ApplicationMaster: spark.yarn.scheduler.heartbeat.interval-ms / initial-allocation.interval
AM_HEARTBEAT_MS = 3_000
AM_INITIAL_ALLOCATION_MS = 200
# [spark] ContainerExitStatus
PMEM_EXCEEDED = -104
VMEM_EXCEEDED = -103
# [spark] yarn.nodemanager.vmem-pmem-ratio (only shown in the diagnostics: the vmem check is off)
VMEM_PMEM_RATIO = 2.1


def nm_date(ms: int) -> str:
    """`ContainerImpl` diagnostics timestamp: yyyy-MM-dd HH:mm:ss.SSS (NodeManager's time zone: UTC)."""
    d = datetime.fromtimestamp(ms / 1000, UTC)
    return d.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms % 1000:03d}"


@dataclass
class Completed:
    executor: Executor
    reported_at: int  # NodeManager heartbeat that told the RM


class ResourceManager:
    """Our application's view of the RM: free memory per NodeManager, heartbeats, one ask, grants."""

    def __init__(self, cluster: Cluster, rng: random.Random, container_mb: int,
                 excluded: frozenset[str] = frozenset()):
        self.cluster = cluster
        self.free = dict(cluster.free_mb)
        self.excluded = excluded  # the AM's excluded node list (updateBlacklist): never granted to us
        self.phase = {h.name: rng.randrange(cal.NM_HEARTBEAT_MS) for h in cluster.hosts}
        self.mb = container_mb
        self.ask = 0  # outstanding containers the RM still tries to place for us
        self.next_n = 2  # container numbers per app attempt; 000001 is the AM
        self.completed: list[Completed] = []

    def next_heartbeat(self, host: str, t: int) -> int:
        """First NodeManager heartbeat strictly after `t`."""
        hb = cal.NM_HEARTBEAT_MS
        return t + 1 + (self.phase[host] - t - 1) % hb

    def grants(self, t_from: int, t_to: int) -> list[tuple[int, Host, int]]:
        """Containers granted at NodeManager heartbeats in (t_from, t_to]: (time, host, container n)."""
        if self.ask <= 0:
            return []
        heap = [(self.next_heartbeat(h.name, t_from), i, h) for i, h in enumerate(self.cluster.hosts)]
        heapq.heapify(heap)
        out = []
        while heap and self.ask > 0:
            t, i, h = heapq.heappop(heap)
            if t > t_to:
                break
            if self.free[h.name] >= self.mb and h.name not in self.excluded:
                self.free[h.name] -= self.mb
                self.ask -= 1
                out.append((t, h, self.next_n))
                self.next_n += 1
            heapq.heappush(heap, (t + cal.NM_HEARTBEAT_MS, i, h))
        return out

    def container_exited(self, ex: Executor, t_exit: int) -> None:
        self.completed.append(Completed(ex, self.next_heartbeat(ex.host.name, t_exit)))

    def release(self, host: str) -> None:
        self.free[host] += self.mb


@dataclass
class YarnAllocator:
    """Spark's YarnAllocator + the AM's allocation thread (one resource profile)."""

    run: AppRun
    rm: ResourceManager
    app: YarnApp
    sizing: ExecutorSizing
    rng: random.Random
    target: int
    pending: int = 0  # container requests the AM has outstanding
    dirty: bool = False  # request table changed since the last allocate call
    running: dict[str, Executor] = field(default_factory=dict)  # executor id -> container (launched)
    released: set[str] = field(default_factory=set)  # container ids the driver asked to kill
    to_release: list[Executor] = field(default_factory=list)
    pending_loss: set[str] = field(default_factory=set)  # executors the driver asked about
    next_exec_id: int = 1
    next_interval: int = AM_INITIAL_ALLOCATION_MS
    last_call: int | None = None
    token: int = 0
    stopped: bool = False

    @property
    def container_mb(self) -> int:
        return self.sizing.container_mb(self.run.cluster.spec.min_allocation_mb)

    # ------------------------------------------------------------------ the allocation thread

    def start(self, t: int) -> None:
        self.run.sim.at(t, self._call, self.token)

    def stop(self) -> None:
        self.stopped = True

    def wake(self) -> None:
        """`resetAllocatorInterval()`: notifyAll wakes the allocation thread now."""
        self.next_interval = AM_INITIAL_ALLOCATION_MS
        self.token += 1
        self.run.sim.after(0, self._call, self.token)

    def _call(self, token: int) -> None:
        if token != self.token or self.stopped:
            return
        self.allocate()
        if self.pending > 0 or self.pending_loss:
            sleep = min(AM_HEARTBEAT_MS, self.next_interval)
            self.next_interval = sleep * 2
        else:
            self.next_interval = AM_INITIAL_ALLOCATION_MS
            sleep = AM_HEARTBEAT_MS
        self.token += 1
        self.run.sim.after(sleep, self._call, self.token)

    def set_target(self, n: int) -> None:
        """`requestTotalExecutors` from the driver (dynamic allocation)."""
        if n == self.target:
            return
        up = n > self.target
        self.target = n
        self.run.yarn_log.append((self.run.sim.now, "am_target", n))
        if up:
            self.wake()

    # ------------------------------------------------------------------ allocateResources()

    def allocate(self) -> None:
        now = self.run.sim.now
        log = self.run.yarn_log
        missing = self.target - self.pending - len(self.running)
        if missing > 0:
            log.append((now, "am_request", (missing, self.container_mb, self.sizing.cores)))
            self.pending += missing
            self.dirty = True
        elif missing < 0 and self.pending > 0:
            n = min(-missing, self.pending)
            log.append((now, "am_cancel", (n, self.target)))
            self.pending -= n
            self.dirty = True

        grants = self.rm.grants(self.last_call, now) if self.last_call is not None else []
        if self.dirty:  # AMRMClient sends the absolute outstanding count with this call
            self.rm.ask = self.pending
            self.dirty = False
        self.last_call = now
        for ex in self.to_release:  # releaseAssignedContainer: the RM stops it at the NM's next heartbeat
            self.run.sim.at(self.rm.next_heartbeat(ex.host.name, now), self.run._nm_stop_container, ex)
        self.to_release.clear()

        if grants:
            matched, extra = grants[:self.pending], grants[self.pending:]
            for t_alloc, host, n in matched:
                self.pending -= 1  # removeContainerRequest: sent with the next call
                self.dirty = True
                if len(self.running) < self.target:
                    self._launch(t_alloc, host, n)
                else:
                    log.append((now, "am_skip_launch", (len(self.running), self.target)))
                    self.rm.release(host.name)
            for _t, host, _n in extra:  # no request left to match: released unused
                self.rm.release(host.name)
            log.append((now, "am_received", (len(grants), len(matched))))

        done = [c for c in self.rm.completed if c.reported_at <= now]
        if done:
            self.rm.completed = [c for c in self.rm.completed if c.reported_at > now]
            for c in sorted(done, key=lambda c: (c.reported_at, c.executor.container_n)):
                self._completed(c.executor)

    def _launch(self, t_alloc: int, host: Host, n: int) -> None:
        now = self.run.sim.now
        rng = self.rng
        eid = str(self.next_exec_id)
        self.next_exec_id += 1
        launch = now + rng.randint(5, 60)
        registered = launch + int(rng.randint(*cal.EXECUTOR_START_MS) / host.cpu_speed)  # JVM start-up is CPU
        sz = self.sizing
        ex = Executor(
            id=eid, host=host, container_n=n, container_id=self.app.container_id(n), cores=sz.cores,
            heap_bytes=sz.heap_bytes, allocated_at=t_alloc, registered_at=registered,
            block_manager_at=registered + rng.randint(*cal.EXECUTOR_BLOCK_MANAGER_MS),
            block_manager_port=rng.randint(30000, 60999), max_memory=unified_memory_max(sz.heap_bytes),
            launched_at=launch, container_mb=self.container_mb, pid=rng.randint(9_000, 3_900_000),
        )
        self.running[eid] = ex
        self.run.yarn_log.append((now, "am_launch", ex))
        self.run._container_launched(ex)

    def kill_executor(self, ex: Executor) -> None:
        """`YarnAllocator.killExecutor`: release the container (sent with the next allocate call)."""
        self.released.add(ex.container_id)
        self.running.pop(ex.id, None)
        self.to_release.append(ex)

    def _completed(self, ex: Executor) -> None:
        """`processCompletedContainers` for one of our executors' containers."""
        now = self.run.sim.now
        self.rm.release(ex.host.name)
        if ex.container_id in self.released:
            self.released.discard(ex.container_id)
            reason, by_app = f"Container {ex.container_id} exited from explicit termination request.", False
            self.run.yarn_log.append((now, "am_completed_released", ex))
        else:
            self.running.pop(ex.id, None)
            reason, by_app = exit_reason(ex)
            self.run.yarn_log.append((now, "am_completed", (ex, reason, by_app)))
        self.pending_loss.discard(ex.id)
        self.run._executor_loss_reason(ex, reason, by_app, released=ex.killed_by_driver)


def exit_reason(ex: Executor) -> tuple[str, bool]:
    """YarnAllocator's message for a container that exited on its own (exitCausedByApp, reason)."""
    import re

    on_host = f" on host: {ex.host.name}"
    status = ex.exit_status
    if status == PMEM_EXCEEDED:
        m = re.search(r"[0-9.]+ [KMG]B of [0-9.]+ [KMG]B physical memory used", ex.diagnostics)
        diag = m.group(0) + "." if m else ""
        return (f"Container killed by YARN for exceeding physical memory limits. {diag} "
                "Consider boosting spark.executor.memoryOverhead."), True
    if status == -102:  # PREEMPTED
        return f"Container {ex.container_id}{on_host} was preempted.", False
    return (f"Container from a bad node: {ex.container_id}{on_host}. Exit status: {status}. "
            f"Diagnostics: {ex.diagnostics}."), True


# --------------------------------------------------------------------------- NodeManager memory monitor


def container_memory(ex: Executor, sizing: ExecutorSizing, rng: random.Random,
                     python_rss: int = 0, native_per_task: int = 0) -> ContainerMemory:
    return ContainerMemory(heap_max=sizing.heap_bytes, host_ram=ex.host.memory_mb * MiB, cores=ex.cores,
                           limit=ex.container_mb * MiB, python_rss=python_rss, native_per_task=native_per_task,
                           growth_core_s=cal.G1_HEAP_GROWTH_CORE_S * math.exp(rng.gauss(0, cal.G1_HEAP_GROWTH_SIGMA)),
                           native_factor=math.exp(rng.gauss(0, cal.NATIVE_FOOTPRINT_SIGMA)))


def pmem_diagnostics(ex: Executor, rss: int, limit: int, t_kill: int, t_exit: int, java_home: str,
                     command: str) -> str:
    """The container diagnostics the AM receives for a physical-memory kill (Hadoop 3.3.6 wording)."""
    vmem = int(ex.heap_bytes + 3.2 * 1024 * MiB + (rss - ex.heap_bytes) * 0.4)
    vlimit = int(limit * VMEM_PMEM_RATIO)
    usage = (f"{long2string(rss)}B of {long2string(limit)}B physical memory used; "
             f"{long2string(vmem)}B of {long2string(vlimit)}B virtual memory used")
    bash = ex.pid - 2
    page = 4096
    tree = ("\t|- PID PPID PGRPID SESSID CMD_NAME USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
            "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"
            f"\t|- {bash} {bash - 31} {bash} {bash} (bash) 0 0 10035200 693 /bin/bash -c {command}\n"
            f"\t|- {ex.pid} {bash} {bash} {bash} (java) {int((t_kill - ex.launched_at) * ex.cores * 0.78)} "
            f"{int((t_kill - ex.launched_at) * 0.05)} {vmem} {(rss - 2_838_528) // page} "
            f"{java_home}/bin/java {command.split('/bin/java ', 1)[-1].split(' 1>')[0]}\n")
    msg = (f"Container [pid={bash},containerID={ex.container_id}] is running {rss - limit}B beyond the "
           f"'PHYSICAL' memory limit. Current usage: {usage}. Killing container.\n"
           f"Dump of the process-tree for {ex.container_id} :\n{tree}")
    return (f"[{nm_date(t_kill)}]{msg}\n"
            f"[{nm_date(t_exit)}]Container killed on request. Exit code is 143\n"
            f"[{nm_date(t_exit)}]Container exited with a non-zero exit code 143. \n")
