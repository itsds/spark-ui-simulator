"""Run a PySpark DataFrame application on the simulated YARN cluster.

`AppRun` wires the pieces together and plays the driver:

* YARN places the AM; Spark's YarnAllocator asks the ResourceManager for executor
  containers (`engine.yarn_am`), executors register over a few seconds and
  scheduling starts once 80 % of the initial target have (minRegisteredResourcesRatio).
  NodeManagers watch each container's physical memory and kill it when it exceeds
  the container size; the driver then fails the executor's running tasks
  (ExecutorLostFailure), re-runs them elsewhere and YARN starts a replacement.
  With `spark.dynamicAllocation.enabled` the executor count follows the task
  backlog and idle executors are released (`engine.dynalloc`).
* Each query is a DataFrame action planned by `sql.dataframe` and run by
  `sql.aqe.AdaptiveExecution` the way Spark 3.5 runs it with AQE on (the
  default): every shuffle exchange is materialized by its own map-stage job;
  independent ones are submitted together, a broadcast job follows a few ms
  later on its own thread, a stage that probes a broadcast waits for it; after
  each query stage AQE re-plans, coalesces the next shuffle read from the real
  partition sizes, and the last job re-creates the finished map stages as
  **skipped** stages. The SQL tab's events (plans, AQE updates, driver-side
  metrics) are recorded as they happen, and every task reports its SQL metrics.
* DAGScheduler bookkeeping: job, stage, task, RDD, scope and accumulator ids in
  allocation order; `Stage IDs` in Scala HashSet order; properties as the
  driver posts them.
* Write jobs create real files on the simulated HDFS (replica 1 on the writing
  executor's host), so later scans of them are planned from what was written.

Everything that happens is recorded with its simulated time; `engine.emit`
turns the records into the event log.
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

import numpy as np

from ..cost import calibration as cal
from ..cost.model import StageContext, TaskEnv, TaskPlan, TaskResult, killed_metrics, simulate_task
from ..environment import CLUSTER_SPARK_DEFAULTS, PySparkYarnApp, spark_properties
from ..eventlog import (
    SHUFFLE_READ_ACCUMS,
    ExecutorLostFailure,
    ExecutorMetrics,
    IdAllocator,
    RDDInfo,
    TaskMetrics,
)
from ..eventlog.ordering import scala_int_hashset_order
from ..model.cluster import (
    Cluster,
    ClusterSpec,
    Executor,
    ExecutorSizing,
    Host,
    build_cluster,
)
from ..model.files import max_split_bytes, plan_scan
from ..model.hdfs import Hdfs
from ..model.shuffle import ShuffleExchange, coalesce_partitions, key_shares, row_shares_split
from ..model.workload import ScanSource, ShuffleSink, ShuffleSource, StageSpec, WriteSink
from ..rng import lognormal, numpy_rng, substream
from ..sched.scheduler import LOCALITY_NAMES, Launch, SpeculationConf, TaskScheduler, TaskSet
from ..sched.sim import Signal, Sim, any_of
from ..sql import metrics as sqlm
from ..sql import nodes as sqln
from ..sql.aqe import AdaptiveExecution
from ..sql.dataframe import PlanContext, PlanIds, SparkSession, SqlQuery
from ..sql.explain import explain_formatted
from ..sql.expr import ExprIds
from ..sql.planinfo import plan_info
from ..sql.scopes import ScopeSimulator
from ..yarn import YarnApp
from . import callsites
from .dynalloc import DynAllocConf, ExecutorAllocationManager
from .yarn_am import ResourceManager, YarnAllocator, container_memory, pmem_diagnostics

MiB = 1024 * 1024


def iso_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).astimezone(UTC).timestamp() * 1000)


# --------------------------------------------------------------------------- spec


@dataclass(frozen=True)
class InputDataset:
    """A table directory that exists before the app runs (written by upstream jobs).

    `keys`: how the values of the columns the queries join or group on are distributed in this data
    (distinct values, rows per key, hot keys). The engine doesn't read them; the app's queries take
    their `KeySpace`s from here, which is where key-skew faults change the data (`sparksim.scenario`).
    """

    path: str
    files: int
    mean_file_bytes: int
    file_size_sigma: float
    bytes_per_row: float  # Parquet bytes per row (all columns)
    row_group_bytes: int = 96 * MiB  # compressed Parquet row-group size of the upstream writer
    keys: dict = field(default_factory=dict, compare=False)  # column -> KeySpace


@dataclass(frozen=True)
class AppSpec:
    seed: int
    app_name: str
    user: str
    script: str
    started_at: str  # ISO-8601 SparkContext start
    app_seq: int
    cluster: ClusterSpec
    sizing: ExecutorSizing
    inputs: tuple[InputDataset, ...]
    queries: tuple[SqlQuery, ...]
    conf: tuple[tuple[str, str], ...] = ()  # spark-submit --conf (in submission order)
    queue: str = "default"
    aqe: bool = True
    submit_to_start_ms: tuple[int, int] = (5_200, 8_400)  # spark-submit → AM container → SparkContext start
    stop_ms: tuple[int, int] = (600, 1_800)  # last action → SparkContext.stop()
    # Fault injection (sparksim.scenario.faults): inputs of the simulation, never its outputs.
    native_memory_per_task: int = 0  # bytes of native memory each running task holds in its container
    executor_kills: tuple[ExecutorKill, ...] = ()


@dataclass(frozen=True)
class ExecutorKill:
    """SIGKILL to executor `executor`'s JVM `at_ms` after the application started (exit code 137)."""

    executor: str
    at_ms: int


# --------------------------------------------------------------------------- records


@dataclass
class Broadcast:
    """A TorrentBroadcast the driver created: task binaries, the Parquet reader's Hadoop conf, relations."""

    id: int
    kind: str  # task_binary | hadoop_conf | relation
    created: int
    piece_bytes: int  # serialized (compressed) size, split in spark.broadcast.blockSize (4 MiB) pieces
    value_bytes: int  # deserialized size in the MemoryStore
    callsite: str

    @property
    def pieces(self) -> int:
        return max(1, -(-self.piece_bytes // (4 * MiB)))


@dataclass
class TaskRecord:
    stage_id: int
    attempt: int
    index: int
    task_id: int
    executor: Executor
    locality: str
    launch: int
    finish: int
    task_type: str
    result: TaskResult
    touched: frozenset
    exec_metrics: ExecutorMetrics = field(default_factory=ExecutorMetrics)
    sql: list[tuple[int, int]] = field(default_factory=list)  # SQL metric (accumulator id, update)
    task_attempt: int = 0  # TaskInfo.Attempt (retries of the same index)
    failure: ExecutorLostFailure | None = None  # set when its executor was lost while it ran
    speculative: bool = False  # TaskInfo.Speculative: a copy launched by speculation
    killed: str | None = None  # TaskKilled reason when the attempt was killed
    kill_reqs: list[tuple[int, str]] = field(default_factory=list)  # (driver asked at, reason)
    killed_metrics: TaskMetrics | None = None  # what the killed attempt reported (None: it had finished)
    interrupted: bool = False  # the kill interrupted its I/O ("Executor interrupted and killed")

    @property
    def duration(self) -> int:
        return self.finish - self.launch

    @property
    def ok(self) -> bool:
        return self.failure is None and self.killed is None


@dataclass
class StageRecord:
    stage_id: int
    attempt: int
    name: str
    details: str
    num_tasks: int
    rdds: list[RDDInfo]
    parent_ids: list[int]
    task_type: str
    query: int
    spec: StageSpec | None = None
    skipped: bool = False
    submitted: int | None = None
    completed: int | None = None
    props: dict[str, str] | None = None
    accum_base: int | None = None
    tasks: list[TaskRecord] = field(default_factory=list)
    by_index: dict[int, TaskRecord] = field(default_factory=dict)
    plans: list[TaskPlan] = field(default_factory=list)
    ctx: StageContext | None = None
    exchange: str | None = None  # shuffle this map stage writes
    top_scope: tuple[int, str] | None = None
    done: Signal | None = None
    plan_root: object = None  # the stage's physical plan (SQL metrics)
    taskset: TaskSet | None = None
    task_binary_bc: int | None = None
    scan_bcs: list[int] = field(default_factory=list)  # Hadoop conf broadcasts of its scans
    epoch: int = 0  # MapOutputTracker epoch its tasks carry


@dataclass
class JobRecord:
    job_id: int
    query: int
    submitted: int
    stage_ids: list[int]
    final_stage: int
    props: dict[str, str]
    completed: int | None = None
    done: Signal | None = None


@dataclass
class QueryRecord:
    execution_id: int
    description: str
    start: int
    end: int | None = None
    job_ids: list[int] = field(default_factory=list)


# --------------------------------------------------------------------------- run


class AppRun:
    def __init__(self, spec: AppSpec):
        self.spec = spec
        seed = spec.seed
        self.rng = substream(seed, "driver")
        self.cluster: Cluster = build_cluster(spec.cluster, seed)
        self.yapp = YarnApp(spec.cluster.cluster_timestamp, spec.app_seq, epoch=spec.cluster.rm_epoch)
        self.t0 = iso_ms(spec.started_at)
        self.submit_time = self.t0 - self.rng.randint(*spec.submit_to_start_ms)

        # YARN: the AM container (driver), then Spark's YarnAllocator asks the RM for executor containers.
        conf = {**CLUSTER_SPARK_DEFAULTS, **dict(spec.conf)}  # the cluster's spark-defaults.conf, then --conf
        yarn_rng = substream(seed, "yarn-allocation")
        min_mb = spec.cluster.min_allocation_mb
        exec_mb = spec.sizing.container_mb(min_mb)
        if exec_mb > spec.cluster.max_allocation_mb:
            raise ValueError(f"executor container {exec_mb} MB exceeds yarn.scheduler.maximum-allocation-mb")
        am_mb = spec.sizing.am_container_mb(min_mb)
        self.am_host: Host = yarn_rng.choice([h for h in self.cluster.hosts if self.cluster.free_mb[h.name] >= am_mb])
        self.am_request_time = self.t0 + yarn_rng.randint(*cal.AM_REQUEST_DELAY_MS)
        excluded = conf.get("spark.yarn.exclude.nodes", "")
        self.excluded_nodes = sorted(h.strip() for h in excluded.split(",") if h.strip())
        self.rm = ResourceManager(self.cluster, yarn_rng, exec_mb, frozenset(self.excluded_nodes))
        self.rm.free[self.am_host.name] -= am_mb
        self.dynalloc_conf = (DynAllocConf.from_conf(conf)
                              if conf.get("spark.dynamicAllocation.enabled", "false") == "true" else None)
        self.ess_enabled = conf.get("spark.shuffle.service.enabled", "false") == "true"
        initial = self.dynalloc_conf.initial_executors if self.dynalloc_conf else spec.sizing.instances
        self.allocator = YarnAllocator(self, self.rm, self.yapp, spec.sizing, yarn_rng, target=initial)
        self.min_registered = math.ceil(initial * cal.MIN_REGISTERED_RATIO)
        self.executors: list[Executor] = []  # every executor that registered, in registration order
        self.executor_by_id: dict[str, Executor] = {}
        self.yarn_log: list[tuple[int, str, object]] = []  # YARN/AM/driver happenings for the container logs
        self.conf = conf
        self.java_home = "/usr/lib/jvm/java-17-openjdk-amd64"
        self.nm_log_dir = "/data/1/yarn/container-logs"
        self.nm_local_dirs = "/data/1/yarn/nm,/data/2/yarn/nm,/data/3/yarn/nm"
        self.driver_bm_port = substream(seed, "driver-ports").randint(33000, 46000)
        self.rm_ip = "10.31.0.5"
        self.executor_java_opts = ""
        self.stopping = False
        self.scheduling_started: int | None = None
        self.hdfs = Hdfs(self.cluster, substream(seed, "hdfs"))
        for d in spec.inputs:
            self.hdfs.create_dataset(d.path, d.files, d.mean_file_bytes, d.file_size_sigma, d.bytes_per_row,
                                     _upstream_file_namer(substream(seed, f"names/{d.path}")),
                                     row_group_bytes=d.row_group_bytes)
        am = self.am_host
        self.env_app = PySparkYarnApp(
            app=self.yapp, app_name=spec.app_name, user=spec.user, script=spec.script, am_node=am.nm,
            rm_host=spec.cluster.rm_host, submit_time=self.submit_time, start_time=self.t0,
            driver_port=self.rng.randint(33000, 46000),
            executor_memory=f"{spec.sizing.memory_mb // 1024}g",
            driver_memory=f"{spec.sizing.driver_memory_mb // 1024}g",
            executor_cores=spec.sizing.cores, executor_instances=spec.sizing.instances or None,
            extra_conf=dict(spec.conf), rm_standby_hosts=spec.cluster.rm_standby_hosts,
        )
        # SQL jobs carry the whole Spark conf in their properties (SQLExecution.withSQLConfPropagated).
        self.base_props = spark_properties(self.env_app)
        self.sim = Sim(self.t0)
        self.ready = self.sim.signal()  # enough executors registered to start scheduling
        if self.min_registered == 0:
            self.ready.fire()
        self.speculation = SpeculationConf.from_conf(conf)
        self.kill_rng = substream(seed, "task-kills")
        self.sched = TaskScheduler(self.sim, substream(seed, "scheduler"), self.cluster.rack_of,
                                   speculation=self.speculation, kill_latency=self._kill_latency)
        self.dynalloc = (ExecutorAllocationManager(self, self.dynalloc_conf, spec.sizing.cores)
                         if self.dynalloc_conf else None)

        # DAGScheduler / SparkContext counters
        self.job_ids = IdAllocator(0)
        self.stage_ids = IdAllocator(0)
        self.rdd_ids = IdAllocator(0)
        self.scope_ids = IdAllocator(0)
        self.shuffle_ids = IdAllocator(0)
        self.accum_ids = IdAllocator(0)
        self.accum_ids.skip(1)  # PySpark's PythonAccumulatorV2 (ID 0) at SparkContext start

        # SQL: expression ids, plan ids, the session the apps' DataFrames are built with
        self.expr_ids = ExprIds(0)
        self.plan_ids = PlanIds(substream(seed, "plan-ids"))
        self.spark = SparkSession(self.expr_ids, {**self.base_props, **dict(spec.conf)})
        self.scope_sim = ScopeSimulator(self.scope_ids, self.rdd_ids, substream(seed, "scopes"))
        self.stage_stats: dict[str, tuple[int, int]] = {}  # query stage -> (sizeInBytes, rowCount)
        self.scan_parts: dict[str, list] = {}

        self.jobs: list[JobRecord] = []
        self.stages: dict[int, StageRecord] = {}
        self.queries: list[QueryRecord] = []
        self.exchanges: dict[str, ShuffleExchange] = {}
        self.map_stage_of: dict[str, StageRecord] = {}  # exchange -> the stage that wrote it
        self.log: list[tuple[int, str, object]] = []  # (time, kind, payload) in simulation order
        self.exec_launched: dict[str, int] = {}
        self.exec_stages: dict[str, set[int]] = {}  # stages that ran tasks on each executor
        self.task_records: dict[int, TaskRecord] = {}
        self.compile_end: dict[tuple[int, int, str], int] = {}
        self.stage_rng: dict[int, random.Random] = {}
        self.stage_nprng: dict[int, object] = {}
        self.fetch_batch_prop = False  # __fetch_continuous_blocks_in_batch_enabled sticks once set
        self.bc_ids = IdAllocator(0)
        self.broadcasts: dict[int, Broadcast] = {}
        self.scan_bc: dict[int, int] = {}  # id(FileSourceScan) -> its Hadoop conf broadcast
        self.relation_bc: dict[str, int] = {}  # broadcast exchange name -> relation broadcast
        self.bc_rng = substream(seed, "broadcasts")
        self.epoch = 0  # MapOutputTrackerMaster epoch: +1 per finished map stage and per lost executor
        self.scan_split_bytes: dict[str, tuple[int, int]] = {}  # path -> (maxSplitBytes, openCost)
        self.app_end: int | None = None

    # ------------------------------------------------------------------ helpers

    def _record(self, kind: str, payload, t: int | None = None) -> None:
        self.log.append((self.sim.now if t is None else t, kind, payload))

    def _jitter(self, lo_hi: tuple[int, int]) -> int:
        return self.rng.randint(*lo_hi)

    def _new_broadcast(self, kind: str, piece: int, values: int, callsite: str) -> int:
        b = Broadcast(self.bc_ids.next(), kind, self.sim.now, piece, values, callsite)
        self.broadcasts[b.id] = b
        return b.id

    def conf_bytes(self, key: str, default: int) -> int:
        """A size property as Spark parses it (`10m`, `256MB`, `10485760`, `-1`)."""
        v = self.conf.get(key)
        if v is None:
            return default
        v = v.strip().lower().removesuffix("b")
        units = {"k": 1024, "m": MiB, "g": 1024 * MiB, "t": 1024 ** 4}
        return int(float(v[:-1]) * units[v[-1]]) if v and v[-1] in units else int(v)

    def conf_bool(self, key: str, default: bool) -> bool:
        v = self.conf.get(key)
        return default if v is None else v.strip().lower() == "true"

    @property
    def default_parallelism(self) -> int:
        return max(2, sum(e.cores for e in self.sched.executors.values()))

    # ------------------------------------------------------------------ main

    def run(self) -> AppRun:
        self.allocator.start(self.am_request_time)
        if self.dynalloc is not None:
            self.dynalloc.start()
        for k in self.spec.executor_kills:
            self.sim.at(self.t0 + k.at_ms, self._sigkill, k)
        self.sim.spawn(self._driver())
        self.sim.run()
        assert self.app_end is not None, "driver program did not finish"
        return self

    def _driver(self):
        if not self.conf_bool("spark.sql.adaptive.enabled", True):
            raise NotImplementedError("spark.sql.adaptive.enabled=false: only AQE plans are modelled so far")
        self.sched.start_speculation_thread()  # TaskSchedulerImpl.start, while the SparkContext starts
        driver_ready = self.t0 + 1_500
        yield max(0, driver_ready - self.sim.now)
        if not self.ready.fired:  # YarnClusterSchedulerBackend: 80 % registered, or 30 s
            timeout = self.sim.signal()
            self.sim.at(driver_ready + cal.MAX_REGISTERED_WAIT_MS, timeout.fire)
            yield any_of(self.sim, [self.ready, timeout])
        self.scheduling_started = self.sim.now
        self.sched.start()
        for qi, q in enumerate(self.spec.queries):
            yield from self._run_query(qi, q)
        yield self._jitter(self.spec.stop_ms)
        self.stopping = True
        self.allocator.stop()
        self.app_end = self.sim.now

    # ------------------------------------------------------------------ executors (YARN containers)

    def _container_launched(self, ex: Executor) -> None:
        """YarnAllocator launched `ex`: it registers after JVM start-up; its NodeManager watches its memory."""
        self.sim.at(ex.registered_at, self._executor_added, ex)
        rng = substream(self.spec.seed, f"nm-monitor/{ex.container_id}")
        phase = rng.randrange(cal.NM_MONITOR_INTERVAL_MS)
        mem = container_memory(ex, self.spec.sizing, rng, native_per_task=self.spec.native_memory_per_task)
        self.sim.at(ex.launched_at + phase, self._nm_check, ex, mem)

    def _executor_added(self, ex: Executor) -> None:
        if ex.exit_at is not None or self.stopping:
            return
        self.executors.append(ex)
        self.executor_by_id[ex.id] = ex
        self.exec_launched.setdefault(ex.id, 0)
        self.exec_stages.setdefault(ex.id, set())
        self._record("executor_added", ex)
        self._record("block_manager_added", ex, t=ex.block_manager_at)
        self.sched.add_executor(ex)
        if self.dynalloc is not None:
            self.dynalloc.executor_added(ex)
        if len(self.executors) >= self.min_registered and not self.ready.fired:
            self.ready.fire()

    def _nm_check(self, ex: Executor, mem) -> None:
        """ContainersMonitorImpl: the container's process-tree RSS against its size, every 3 s."""
        if self.stopping or ex.nm_kill_at is not None:
            return
        now = self.sim.now
        exec_mem, fetching, running = 0.0, 0, 0
        for a in self.sched.running.get(ex.id, {}).values():
            rec = self.task_records[a.task_id]
            frac = (now - rec.launch) / max(1, rec.finish - rec.launch)
            exec_mem += rec.result.exec_memory * min(1.0, frac / 0.7)
            fetching += rec.result.fetching
            running += 1
        rss = mem.check(now, running, exec_mem, fetching, len(self.exec_stages.get(ex.id, ())))
        ex.extra["peak_rss"] = max(ex.extra.get("peak_rss", 0), rss)
        if rss > mem.limit:
            t_exit = now + self.rng.randint(*cal.JVM_SIGTERM_EXIT_MS)
            ex.nm_kill_at, ex.exit_status = now, -104
            ex.diagnostics = pmem_diagnostics(ex, rss, mem.limit, now, t_exit, self.java_home,
                                              self.executor_command(ex))
            self.yarn_log.append((now, "nm_kill_pmem", (ex, rss, mem.limit)))
            self.sim.at(t_exit, self._jvm_exit, ex)
            return
        self.sim.after(cal.NM_MONITOR_INTERVAL_MS, self._nm_check, ex, mem)

    def _sigkill(self, k) -> None:
        """SIGKILL from outside YARN (the kernel's OOM killer, an operator): the JVM dies at once, without
        shutdown hooks; the NodeManager reports exit code 137 (diagnostics as in ref05)."""
        if self.stopping:
            return
        ex = self.executor_by_id.get(k.executor)
        if ex is None or ex.exit_at is not None or ex.nm_kill_at is not None:
            raise ValueError(f"executor_killed: executor {k.executor} is not running "
                             f"{k.at_ms / 1000:.0f} s after the application started")
        now = self.sim.now
        d = _nm(now)
        ex.exit_status = 137
        ex.diagnostics = (f"[{d}]Container killed on request. Exit code is 137\n"
                          f"[{d}]Container exited with a non-zero exit code 137. \n"
                          f"[{d}]Killed by external signal\n")
        self.yarn_log.append((now, "sigkill", ex))
        self._jvm_exit(ex)

    def _nm_stop_container(self, ex: Executor) -> None:
        """The RM told the NodeManager to stop a container the AM released (SIGTERM)."""
        if self.stopping or ex.nm_kill_at is not None:
            return
        now = self.sim.now
        t_exit = now + self.rng.randint(*cal.JVM_SIGTERM_EXIT_MS)
        ex.nm_kill_at, ex.exit_status = now, 143
        ex.diagnostics = (f"[{_nm(now)}]Container killed by the ApplicationMaster.\n"
                          f"[{_nm(t_exit)}]Container killed on request. Exit code is 143\n"
                          f"[{_nm(t_exit)}]Container exited with a non-zero exit code 143. \n")
        self.yarn_log.append((now, "nm_stop", ex))
        self.sim.at(t_exit, self._jvm_exit, ex)

    def _jvm_exit(self, ex: Executor) -> None:
        if self.stopping:
            return
        ex.exit_at = self.sim.now
        self.rm.container_exited(ex, ex.exit_at)
        if ex.id in self.sched.executors:  # the driver notices the dropped RPC connection
            self.sim.after(self.rng.randint(2, 15), self._driver_disconnected, ex)

    def _driver_disconnected(self, ex: Executor) -> None:
        """DriverEndpoint.onDisconnected → disableExecutor → executorLost(LossReasonPending): the block
        manager is removed now; the tasks wait for the loss reason the AM will give."""
        if self.stopping:
            return
        self.sched.disable_executor(ex.id)
        self.allocator.pending_loss.add(ex.id)
        self.yarn_log.append((self.sim.now, "driver_disconnected", ex))
        self._record("block_manager_removed", ex)

    def _executor_loss_reason(self, ex: Executor, reason: str, by_app: bool, released: bool) -> None:
        """RemoveExecutor(reason) reaches the driver: fail the running tasks, post ExecutorRemoved."""
        if self.stopping or ex.id not in self.sched.executors:
            return
        self.epoch += 1  # DAGScheduler.handleExecutorLost
        if not self.ess_enabled and any(e.map_executor and ex.id in e.map_executor
                                        for e in self.exchanges.values()):
            raise NotImplementedError(
                f"executor {ex.id} lost its shuffle files: without spark.shuffle.service.enabled this needs "
                "FetchFailed / stage retry handling, which is not modelled yet")
        ex.removed_reason = "Executor killed by driver." if released else reason
        ex.exit_caused_by_app = by_app
        self._loss_reason = (ex, reason, by_app)
        lost = self.sched.remove_executor(ex.id, counts_as_failure=by_app and not released)
        self.yarn_log.append((self.sim.now, "executor_lost", (ex, reason, by_app, released, lost)))
        ex.removed_at = self.sim.now + self.rng.randint(1, 4)
        self._record("executor_removed", (ex, ex.removed_reason), t=ex.removed_at)
        if not released:  # the AM also sent RemoveExecutor itself: the second one finds no executor
            self._record("executor_removed", (ex, ex.removed_reason), t=ex.removed_at + 1)
        if self.dynalloc is not None:
            self.dynalloc.executor_removed(ex)

    def _dynalloc_target(self, target: int, old: int, backlogged: bool, delta: int = 0) -> None:
        self.yarn_log.append((self.sim.now, "da_target", (target, old, backlogged, delta)))
        self.allocator.set_target(target)

    def _dynalloc_kill(self, ids: list[str]) -> None:
        """client.killExecutors(ids, countFailures = false): no more tasks, the AM releases the containers."""
        self.yarn_log.append((self.sim.now, "da_kill", ids))
        for eid in ids:
            ex = self.sched.executors[eid]
            ex.killed_by_driver = True
            self.sched.disable_executor(eid)
            self.allocator.kill_executor(ex)

    @property
    def event_log_path(self) -> str:
        """Where SingleEventLogFileWriter writes (Hadoop Path prints hdfs:///x as hdfs:/x)."""
        d = self.base_props.get("spark.eventLog.dir", "hdfs:///spark-history").replace(":///", ":/")
        return f"{d.rstrip('/')}/{self.yapp.event_log_name}"

    def job_tracker_id(self, stage: StageRecord) -> str:
        """FileFormatWriter's jobTrackerID: yyyyMMddHHmmss of the write job + abs(Random.nextLong)."""
        job = next(j for j in self.jobs if j.final_stage == stage.stage_id)
        stamp = datetime.fromtimestamp(job.submitted / 1000, UTC).strftime("%Y%m%d%H%M%S")
        return f"{stamp}{substream(self.spec.seed, f'jobtracker/{stage.stage_id}').getrandbits(63)}"

    def executor_command(self, ex: Executor) -> str:
        """The container launch command (ExecutorRunnable.prepareCommand), as the NM process tree shows it."""
        sz = self.spec.sizing
        log_dir = f"{self.nm_log_dir}/{self.yapp.app_id}/{ex.container_id}"
        return (f"{self.java_home}/bin/java -server -Xmx{sz.memory_mb}m "
                f"{self.executor_java_opts}-Djava.io.tmpdir={{{{PWD}}}}/tmp "
                f"-Dspark.driver.port={self.env_app.driver_port} -Dspark.yarn.app.container.log.dir={log_dir} "
                "-XX:OnOutOfMemoryError=kill %p org.apache.spark.executor.YarnCoarseGrainedExecutorBackend "
                f"--driver-url spark://CoarseGrainedScheduler@{self.am_host.name}:{self.env_app.driver_port} "
                f"--executor-id {ex.id} --hostname {ex.host.name} --cores {ex.cores} --app-id {self.yapp.app_id} "
                f"--resourceProfileId 0 1>{log_dir}/stdout 2>{log_dir}/stderr")

    # ------------------------------------------------------------------ queries (AQE)

    def _run_query(self, qi: int, q: SqlQuery):
        """One DataFrame action = one SQL execution, run by AdaptiveSparkPlanExec (AQE on, the default)."""
        yield self._jitter(q.think_ms)
        qrec = QueryRecord(qi, q.description, self.sim.now)
        self.queries.append(qrec)
        logical = q.build(self.spark)
        aqe = AdaptiveExecution(logical, self._plan_context(), self._coalescer, name_prefix=f"e{qi}.")
        listed = sum(len(self.hdfs.listdir(s.relation.root_paths[0])) for s in _scans(aqe.root.initial))
        yield self._jitter(q.planning_ms) + int(listed * cal.FILE_LISTING_MS_PER_FILE)
        qrec.start = self.sim.now
        self.scope_sim.skip(cal.FIRST_EXECUTION_SCOPES if qi == 0 else self.rng.randint(*cal.EXECUTION_SCOPES))
        self._plan_event("sql_start", qrec, aqe)
        yield self._jitter(cal.SQL_START_TO_STAGES_MS)

        materialized: dict[int, Signal] = {}
        stages: dict[int, object] = {}
        while True:
            new = aqe.create_stages()
            if new:
                self._plan_event("sql_update", qrec, aqe)
                lineages = self.scope_sim.materialize(new)
                for st in new:  # broadcast stages first: their RDDs are built first (SPARK-33933)
                    self._stage_driver_metrics(qrec, st.plan)
                for st in new:
                    stages[id(st)] = st
                for st in [s for s in new if not s.broadcast]:
                    materialized[id(st)] = self._submit_job(qi, q, aqe, st, lineages[id(st)], qrec)
                bcasts = [s for s in new if s.broadcast]
                if bcasts:
                    yield self._jitter(cal.BROADCAST_JOB_LAG_MS)
                    for b in bcasts:
                        materialized[id(b)] = self._submit_job(qi, q, aqe, b, lineages[id(b)], qrec)
            if aqe.all_materialized:
                break
            waiting = [sig for sid, sig in materialized.items() if not sig.fired]
            if not waiting:
                raise RuntimeError(f"query {q.description!r}: AQE has no running stage to wait for")
            yield any_of(self.sim, waiting)
            yield self._jitter(cal.AQE_REPLAN_MS)
            for sid, sig in materialized.items():
                st = stages[sid]
                if sig.fired and not st.materialized:
                    st.stats = self.stage_stats[st.name]
                    if st.broadcast:
                        self.scope_sim.skip(cal.SCOPES_AFTER_BROADCAST)
            aqe.replan()
        aqe.finalize()
        self._plan_event("sql_update", qrec, aqe)
        lineage = self.scope_sim.materialize_write(aqe.root.current)
        self._stage_driver_metrics(qrec, aqe.root.current)
        final = self._submit_job(qi, q, aqe, None, lineage, qrec)
        yield final
        stage = self.stages[self.jobs[-1].final_stage]
        files = [t.result.output_file for t in stage.tasks if t.ok and t.result.output_file]
        commit = int(cal.JOB_COMMIT_FIXED_MS + len(files) * cal.JOB_COMMIT_MS_PER_FILE * self.rng.uniform(0.85, 1.2))
        yield commit
        cmd = aqe.root.current
        self._record("driver_accum", (qi, sqlm.write_driver_updates(
            cmd, len(files), sum(f[0] for f in files), sum(f[1] for f in files), 1 if files else 0, commit)))
        qrec.end = self.sim.now + self.rng.randint(3, 40)
        yield qrec.end - self.sim.now
        self._record("sql_end", qrec)

    def _plan_context(self) -> PlanContext:
        stats = {}
        for d in self.spec.inputs:
            stats[d.path] = sum(f.size for f in self.hdfs.listdir(d.path))
        threshold = self.conf_bytes("spark.sql.autoBroadcastJoinThreshold", 10 * MiB)
        return PlanContext(self.spark, self.plan_ids, dataset_stats=stats,
                           broadcast_threshold=threshold if threshold >= 0 else -1)

    def _plan_event(self, kind: str, qrec: QueryRecord, aqe: AdaptiveExecution) -> None:
        """SQLExecutionStart / SQLAdaptiveExecutionUpdate: the metric ids of new nodes are allocated now."""
        info = plan_info(aqe.root, self.accum_ids)
        details = callsites.py4j_callsite(self.spec.queries[qrec.execution_id].action)[1]
        self._record(kind, (qrec, explain_formatted(aqe.root), info, details))

    def _coalescer(self, stages) -> list[tuple[int, int]] | None:
        """CoalesceShufflePartitions over the map output sizes of the shuffles a stage reads."""
        exs = [self.exchanges[s.name] for s in stages]
        if len(exs) == 2:
            self._check_skew_join(exs)
        if len({e.num_partitions for e in exs}) != 1:
            return None
        if not self.conf_bool("spark.sql.adaptive.coalescePartitions.enabled", True):
            return None
        sizes = sum(e.bytes_by_partition() for e in exs)
        advisory = self.conf_bytes("spark.sql.adaptive.advisoryPartitionSizeInBytes", 64 * MiB)
        min_size = self.conf_bytes("spark.sql.adaptive.coalescePartitions.minPartitionSize", 1 * MiB)
        parallelism = (self.default_parallelism
                       if self.conf_bool("spark.sql.adaptive.coalescePartitions.parallelismFirst", True) else 1)
        ranges = coalesce_partitions(sizes, parallelism, advisory=advisory, min_partition_size=min_size)
        return ranges if len(ranges) < exs[0].num_partitions else None

    def _check_skew_join(self, exs: list[ShuffleExchange]) -> None:
        """AQE's OptimizeSkewedJoin (on by default) splits a join partition larger than
        max(skewedPartitionThresholdInBytes, skewedPartitionFactor x median). We don't model the split yet,
        so refuse to simulate a run where Spark would have done it rather than show a skew Spark wouldn't."""
        if not self.conf_bool("spark.sql.adaptive.skewJoin.enabled", True):
            return
        threshold = self.conf_bytes("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 256 * MiB)
        factor = float(self.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0"))
        for ex in exs:
            sizes = ex.bytes_by_partition()
            limit = max(threshold, float(np.median(sizes)) * factor)
            if (sizes > limit).any():
                raise NotImplementedError(
                    f"AQE OptimizeSkewedJoin would split {int((sizes > limit).sum())} partition(s) of shuffle "
                    f"{ex.name} (largest {int(sizes.max()):,} bytes > {int(limit):,}); skew-join splits are not "
                    "modelled yet: set spark.sql.adaptive.skewJoin.enabled=false for this run")

    def _stage_driver_metrics(self, qrec: QueryRecord, plan) -> None:
        """What the driver posts while it builds a stage's RDDs: scan files, AQEShuffleRead specs, partitions."""
        root = plan.children[0] if isinstance(plan, (sqln.ShuffleExchange, sqln.BroadcastExchange)) else plan
        site = (callsites.BROADCAST_SHORT if isinstance(plan, sqln.BroadcastExchange)
                else callsites.py4j_callsite(self.spec.queries[qrec.execution_id].action)[0])
        for scan in sqlm.scans_in(root):
            if id(scan) not in self.scan_bc:  # ParquetFileFormat.buildReaderWithPartitionValues
                r = self.bc_rng
                self.scan_bc[id(scan)] = self._new_broadcast("hadoop_conf", 40_300 + r.randint(0, 900),
                                                             474_000 + r.randint(0, 4_000), site)
            path = scan.relation.root_paths[0]
            files = self.hdfs.listdir(path)
            listing = max(1, int(len(files) * cal.FILE_LISTING_MS_PER_FILE * self.rng.uniform(0.7, 1.3)))
            self._record("driver_accum", (qrec.execution_id, sqlm.scan_driver_updates(
                scan, len(files), sum(f.size for f in files), listing, 1 if scan.partitioned else 0)))
        for read in sqlm.reads_in(root):
            sizes = self.exchanges[read.children[0].name].bytes_by_partition().tolist()
            self._record("driver_accum", (qrec.execution_id, sqlm.read_driver_updates(read, sizes)))
        if isinstance(plan, sqln.ShuffleExchange):
            self._record("driver_accum", (qrec.execution_id, sqlm.exchange_driver_updates(plan)))

    # ------------------------------------------------------------------ jobs and stages

    def _submit_job(self, qi: int, q: SqlQuery, aqe: AdaptiveExecution, qstage, lineage, qrec) -> Signal:
        """DAGScheduler.submitMapStage (a query stage) or submitJob (the result stage); returns 'materialized'."""
        now = self.sim.now
        job_id = self.job_ids.next()
        qrec.job_ids.append(job_id)
        spec = aqe.stage_spec(qstage) if qstage is not None else aqe.final_spec()
        plan_root = qstage.plan if qstage is not None else aqe.root.current

        # Re-create the stages of every shuffle this stage depends on (they were cleaned up when their own
        # map-stage jobs finished): new ids, ancestors first, all skipped since their outputs are available.
        recreated: list[StageRecord] = []

        def recreate(exchange: str) -> int:
            orig = self.map_stage_of[exchange]
            parents = [recreate(x) for x in orig.spec.shuffle_inputs]
            sid = self.stage_ids.next()
            rec = StageRecord(sid, 0, orig.name, orig.details, orig.num_tasks, orig.rdds, parents,
                              "ShuffleMapTask", qi, orig.spec, skipped=True, exchange=exchange)
            self.stages[sid] = rec
            recreated.append(rec)
            return sid

        parent_ids = [recreate(x) for x in spec.shuffle_inputs]
        stage = self._new_stage(qi, q, spec, lineage, parent_ids, plan_root)
        insertion = [stage.stage_id] + [r.stage_id for r in reversed(recreated)]
        stage_ids = scala_int_hashset_order(insertion)

        props = self._job_properties(q, qi, stage, spec)
        job = JobRecord(job_id, qi, now, stage_ids, stage.stage_id, props, done=self.sim.signal())
        self.jobs.append(job)
        self._record("job_start", job)

        materialized = self.sim.signal()
        stage.props = {**props, "resource.executor.cores": str(self.spec.sizing.cores)}

        def submit_stage():
            stage.submitted = self.sim.now
            stage.accum_base = self.accum_ids.take(35).start
            stage.epoch = self.epoch
            nodes = sum(1 for _ in plan_root.walk()) if plan_root is not None else 4
            piece = 3_600 + nodes * 2_900 + (90_000 if isinstance(spec.sink, WriteSink) else 0)
            piece = int(piece * self.bc_rng.uniform(0.93, 1.07))
            stage.task_binary_bc = self._new_broadcast("task_binary", piece, int(piece * self.bc_rng.uniform(2.3, 3.4)),
                                                       "broadcast at DAGScheduler.scala:1611")
            self._record("stage_submitted", stage)
            ts = TaskSet(stage.stage_id, 0, job_id, [p.preferred for p in stage.plans],
                         on_launch=lambda a, st=stage: self._on_launch(st, a),
                         on_finish=lambda a, st=stage: self._on_finish(st, a),
                         on_fail=lambda a, st=stage: self._on_fail(st, a),
                         on_kill=lambda a, st=stage: self._on_kill(st, a),
                         records_of=self._records_read)
            stage.taskset = ts
            ts.done = self.sim.signal()
            ts.done.wait(lambda: self.sim.after(self._jitter(cal.STAGE_COMPLETE_DELAY_MS), stage_completed))
            self.sched.submit(ts)
            if self.dynalloc is not None:
                self.dynalloc.stage_submitted()

        def stage_completed():
            stage.completed = self.sim.now
            self._record("stage_completed", stage)
            self.sim.after(self._jitter(cal.JOB_END_DELAY_MS), job_ended)

        def job_ended():
            job.completed = self.sim.now
            if stage.task_type == "ResultTask":  # "Cancelling potential speculative or zombie tasks for this job"
                self.sched.kill_all(stage.taskset, "Stage finished")
            self._record("job_end", job)
            job.done.fire()
            if spec.is_broadcast:
                self._broadcast_built(qrec, stage, job, spec, materialized)
            else:
                if qstage is not None:
                    ex = self.exchanges[spec.sink.exchange]
                    size = sum(t.result.sink_rows * t.result.sink_row_bytes for t in stage.tasks if t.ok)
                    self.stage_stats[spec.sink.exchange] = (size, int(ex.records.sum()))
                    self.epoch += 1  # DAGScheduler: mapOutputTracker.incrementEpoch() after a map stage
                materialized.fire()

        self.sim.after(self._jitter(cal.JOB_TO_STAGE_MS), submit_stage)
        return materialized

    def _broadcast_built(self, qrec, stage: StageRecord, job: JobRecord, spec: StageSpec, materialized: Signal):
        """BroadcastExchangeExec.relationFuture after the collect job: build the HashedRelation, broadcast it."""
        done = [t for t in stage.tasks if t.ok]
        rows = sum(t.result.result_rows for t in done)
        row_bytes = done[0].result.sink_row_bytes if done else 16
        build = int(cal.BROADCAST_BUILD_FIXED_MS + rows * cal.BROADCAST_BUILD_NS_PER_ROW / 1e6)
        page_need = rows * (row_bytes + 8)  # UnsafeRow + its 8-byte length/next-pointer word
        page = cal.HASHED_RELATION_MIN_BYTES
        while page < page_need:
            page *= 2
        data_size = page + rows * cal.HASHED_RELATION_BYTES_PER_ROW
        bcast = int(cal.BROADCAST_FIXED_MS + data_size / (cal.BROADCAST_MBPS * MiB) * 1e3)
        collect = job.completed - job.submitted
        self.stage_stats[spec.sink.name] = (data_size, rows)

        def done():
            self._record("driver_accum", (qrec.execution_id, sqlm.broadcast_driver_updates(
                spec.sink.node, data_size, rows, collect, build, bcast)))
            self.relation_bc[spec.sink.name] = self._new_broadcast(
                "relation", int(data_size * 0.38), data_size + 512, callsites.BROADCAST_SHORT)
            materialized.fire()

        self.sim.after(build + bcast, done)

    def _new_stage(self, qi: int, q: SqlQuery, spec: StageSpec, lineage, parent_ids: list[int],
                   plan_root) -> StageRecord:
        sid = self.stage_ids.next()
        if spec.is_broadcast:
            short, details = callsites.BROADCAST_SHORT, callsites.BROADCAST_LONG
        else:
            short, details = callsites.py4j_callsite(q.action)

        # ---- partitions
        plans: list[TaskPlan] = []
        scan = next((s for s in spec.sources if isinstance(s, ScanSource)), None)
        shuffle_srcs = [s for s in spec.sources if isinstance(s, ShuffleSource)]
        coalesced = False
        if scan is not None:
            for p in self._scan_partitions(scan):
                plans.append(TaskPlan(p.index, scan=p, preferred=tuple(p.preferred_locations())))
        else:
            exs = [self.exchanges[s.exchange] for s in shuffle_srcs]
            read = next((s.read for s in shuffle_srcs if s.read is not None), None)
            ranges = read.ranges if read is not None else [(i, i + 1) for i in range(exs[0].num_partitions)]
            coalesced = read is not None
            for i, (a, b) in enumerate(ranges):
                plans.append(TaskPlan(i, reads=tuple((s.exchange, a, b) for s in shuffle_srcs),
                                      preferred=self._reduce_preferred(exs, a, b)))

        rdds = lineage.rdd_infos(short, len(plans), substream(self.spec.seed, f"lineage/{sid}"))
        task_type = "ShuffleMapTask" if isinstance(spec.sink, ShuffleSink) else "ResultTask"
        rec = StageRecord(sid, 0, short, details, len(plans), rdds, parent_ids, task_type, qi, spec,
                          plans=plans, top_scope=lineage.top_scope)
        rec.scan_bcs = [self.scan_bc[id(s.node)] for s in spec.sources
                        if isinstance(s, ScanSource) and id(s.node) in self.scan_bc]
        rec.plan_root = plan_root
        out_ex = None
        if isinstance(spec.sink, ShuffleSink):
            sink = spec.sink
            distinct_rows = sink.unique_keys or (spec.ops and spec.ops[-1].kind == "partial_agg")
            if distinct_rows:
                shares = cluster = key_shares(sink.key, sink.num_partitions, self.spec.seed)
            else:
                shares, cluster = row_shares_split(sink.key, sink.num_partitions, self.spec.seed)
            out_ex = ShuffleExchange(sink.exchange, self.shuffle_ids.next(), len(plans), sink.num_partitions, shares,
                                     cluster_shares=cluster)
            self.exchanges[sink.exchange] = out_ex
            self.map_stage_of[sink.exchange] = rec
            rec.exchange = sink.exchange
        rec.ctx = StageContext(spec, self.exchanges, batch_fetch=coalesced, out_exchange=out_ex,
                               output_partitions=out_ex.num_partitions if out_ex else 0)
        if shuffle_srcs:
            self.fetch_batch_prop = True  # ShuffledRowRDD sets this thread-local property once created
        self.stages[sid] = rec
        self.stage_rng[sid] = substream(self.spec.seed, f"tasks/{sid}")
        self.stage_nprng[sid] = numpy_rng(self.spec.seed, f"blocks/{sid}")
        return rec

    def _scan_partitions(self, scan: ScanSource):
        path = scan.dataset.path
        if path not in self.scan_parts:
            mpb = self.conf_bytes("spark.sql.files.maxPartitionBytes", 128 * MiB)
            oc = self.conf_bytes("spark.sql.files.openCostInBytes", 4 * MiB)
            files = self.hdfs.listdir(path)
            self.scan_parts[path] = plan_scan(files, self.default_parallelism, max_partition_bytes=mpb, open_cost=oc)
            self.scan_split_bytes[path] = (max_split_bytes(files, mpb, oc, self.default_parallelism), oc)
        return self.scan_parts[path]

    def _reduce_preferred(self, exs: list[ShuffleExchange], a: int, b: int) -> tuple[str, ...]:
        """MapOutputTracker.getPreferredLocationsForShuffle: hosts holding ≥ 20 % of the partition's input,
        only for shuffles with < 1000 map and < 1000 reduce partitions (spark.shuffle.reduceLocality.enabled)."""
        out: list[str] = []
        for ex in exs:
            if ex.num_maps >= 1000 or ex.num_partitions >= 1000:
                continue
            seg = ex.bytes[:, a:b].sum(axis=1)
            total = float(seg.sum())
            if total <= 0:
                continue
            by_exec: dict[str, float] = {}
            for m, v in enumerate(seg.tolist()):
                by_exec[ex.map_executor[m]] = by_exec.get(ex.map_executor[m], 0.0) + v
            for eid, v in by_exec.items():
                if v / total >= 0.2:
                    h = self.executor_by_id[eid].host.name
                    if h not in out:
                        out.append(h)
        return tuple(out)

    def _job_properties(self, q: SqlQuery, qi: int, stage: StageRecord, spec: StageSpec) -> dict[str, str]:
        p = dict(self.base_props)
        top_id, top_name = stage.top_scope
        if not isinstance(spec.sink, WriteSink):
            p["spark.rdd.scope"] = f'{{"id":"{top_id}","name":"{top_name}"}}'
            p["spark.rdd.scope.noOverride"] = "true"
        p["spark.job.description"] = q.description
        p["spark.sql.execution.root.id"] = str(qi)
        p["spark.sql.execution.id"] = str(qi)
        if spec.is_broadcast:
            p["spark.job.interruptOnCancel"] = "true"
            run_id = uuid.UUID(int=self.rng.getrandbits(128), version=4)
            p["spark.job.tags"] = f"broadcast exchange (runId {run_id})"
        if self.fetch_batch_prop:
            p["__fetch_continuous_blocks_in_batch_enabled"] = "true"
        return p

    # ------------------------------------------------------------------ tasks

    def _on_launch(self, st: StageRecord, a: Launch) -> int:
        plan = st.plans[a.index]
        ex = a.executor
        if plan.scan is not None:
            prefs = plan.preferred
            if ex.host.name in prefs:
                data_loc = "NODE_LOCAL"
            elif any(self.cluster.rack_of(h) == ex.host.rack for h in prefs):
                data_loc = "RACK_LOCAL"
            else:
                data_loc = "ANY"
        else:
            data_loc = "NODE_LOCAL"
        cold = self.exec_launched[ex.id] < ex.cores
        self.exec_launched[ex.id] += 1
        key = (st.stage_id, st.attempt, ex.id)
        first = key not in self.compile_end
        wait = 0.0 if first else max(0.0, self.compile_end[key] - a.launch_time - 5)
        unified = (ex.heap_bytes - cal.RESERVED_MEMORY) * cal.MEMORY_FRACTION
        env = TaskEnv(ex, data_loc, cold, first, wait, int(unified / ex.cores))
        res = simulate_task(st.ctx, plan, env, self.stage_rng[st.stage_id], self.stage_nprng[st.stage_id])
        if first:
            self.compile_end[key] = (a.launch_time + res.deserialize_ms
                                     + int(cal.CODEGEN_COMPILE_MS / ex.host.cpu_factor))
        touched = SHUFFLE_READ_ACCUMS if st.spec.shuffle_inputs else frozenset()
        rec = TaskRecord(st.stage_id, st.attempt, a.index, a.task_id, ex, LOCALITY_NAMES[a.locality],
                         a.launch_time, a.launch_time + res.duration_ms, st.task_type, res, touched,
                         task_attempt=a.attempt, speculative=a.speculative)
        rec.sql = sqlm.task_updates(st.plan_root, res)
        st.tasks.append(rec)
        self.task_records[a.task_id] = rec
        self.exec_stages[ex.id].add(st.stage_id)
        self._record("task_start", rec, t=a.launch_time)
        if self.dynalloc is not None:
            self.dynalloc.task_started(ex.id)
        return res.duration_ms

    def _on_fail(self, st: StageRecord, a: Launch) -> None:
        """The task's executor was lost (TaskSetManager.executorLost → handleFailedTask)."""
        rec = self.task_records[a.task_id]
        ex, reason, by_app = self._loss_reason
        rec.finish = self.sim.now
        rec.failure = ExecutorLostFailure(ex.id, by_app, reason)
        rec.sql = []
        self._record("task_end", rec)

    def _kill_latency(self, a: Launch, interrupt: bool) -> int:
        """KillTask RPC to the executor, then the task thread notices the kill, cleans up and reports KILLED
        (CPU work on that host)."""
        r = self.kill_rng
        handling = lognormal(r, cal.KILL_HANDLING_SIGMA, cal.KILL_HANDLING_MEDIAN_MS) / a.executor.host.cpu_factor
        return r.randint(*cal.KILL_RPC_MS) + int(handling) + r.randint(1, 4)

    def _records_read(self, a: Launch) -> tuple[int, int]:
        """(records read, executor run time) of a successful attempt, for TaskProcessRateCalculator."""
        m = self.task_records[a.task_id].result.metrics
        return m.input.records_read + m.shuffle_read.records_read, m.executor_run_time

    def _on_kill(self, st: StageRecord, a: Launch) -> None:
        """A killed attempt reported KILLED: TaskKilled with the metrics it had so far (none if it had
        finished: "Finish but did not commit due to another attempt succeeded")."""
        rec = self.task_records[a.task_id]
        res = rec.result
        natural_end = rec.finish
        rec.finish = self.sim.now
        rec.killed = a.kill_reason
        rec.kill_reqs = list(a.kill_reqs)
        rec.sql = []  # SQL metrics don't count failed values
        if a.kill_reason.startswith("Finish but did not commit"):
            rec.killed_metrics = None
            return self._record("task_end", rec)
        run_start = rec.launch + max(1, int(res.scheduler_delay_ms * 0.3)) + res.deserialize_ms
        elapsed = max(0, min(res.run_ms, self.sim.now - 2 - run_start))
        rec.killed_metrics = killed_metrics(res, elapsed)
        # An interrupt (speculation's kill) lands in the task's I/O if that is what it was doing; a kill without
        # interrupt ("Stage finished") is noticed at the next record. The share of I/O in its run decides.
        m = res.metrics
        io = res.run_ms - (m.executor_cpu_time / 1e6) if res.run_ms else 0
        interrupt = any(r == "another attempt succeeded" for _, r in a.kill_reqs)
        rec.interrupted = interrupt and self.kill_rng.random() < max(0.0, io) / max(1, res.run_ms)
        assert self.sim.now <= natural_end or a.killed_by_other
        self._record("task_end", rec)

    def _on_finish(self, st: StageRecord, a: Launch) -> None:
        rec = self.task_records[a.task_id]
        assert rec.finish == a.finish_time
        st.by_index[a.index] = rec
        if self.dynalloc is not None:
            self.dynalloc.task_ended(a.executor.id)
        ex = a.executor
        if st.exchange is not None:
            self.exchanges[st.exchange].commit(a.index, ex.id, ex.host.name)
        sink = st.spec.sink
        if isinstance(sink, WriteSink) and rec.result.output_file:
            size, rows = rec.result.output_file
            self.hdfs.write(f"{sink.path}/{self._part_name(st, a.index)}", size, rows, writer=ex.host.name,
                            row_group_bytes=sink.row_group_bytes, clustered=self._clustering(st, a.index))
        self._record("task_end", rec)

    def _clustering(self, st: StageRecord, index: int) -> tuple[str, float] | None:
        """A write task that read reducers [a, b) of a hash shuffle holds all rows of those keys."""
        plan = st.plans[index]
        if not plan.reads:
            return None
        name, a, b = plan.reads[0]
        key = self.map_stage_of[name].spec.sink.key
        return key.name, self.exchanges[name].key_fraction(a, b)

    def _part_name(self, st: StageRecord, index: int) -> str:
        if not hasattr(st, "_job_uuid"):
            st._job_uuid = uuid.UUID(int=substream(self.spec.seed, f"jobuuid/{st.stage_id}").getrandbits(128),
                                     version=4)
        return f"part-{index:05d}-{st._job_uuid}.c000.snappy.parquet"


def _scans(plan) -> list:
    return [n for n in plan.walk() if isinstance(n, sqln.FileSourceScan)]


def _nm(ms: int) -> str:
    from .yarn_am import nm_date

    return nm_date(ms)


def _upstream_file_namer(rng: random.Random):
    job = uuid.UUID(int=rng.getrandbits(128), version=4)
    return lambda i: f"part-{i:05d}-{job}.c000.snappy.parquet"


def quantile(xs: list[float], q: float) -> float:
    s = sorted(xs)
    if not s:
        return math.nan
    k = (len(s) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)
