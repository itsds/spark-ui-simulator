"""Run a PySpark DataFrame application on the simulated YARN cluster.

`AppRun` wires the pieces together and plays the driver:

* YARN places the AM and the executor containers; executors register over a
  few seconds and scheduling starts once 80 % have (minRegisteredResourcesRatio).
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

from ..cost import calibration as cal
from ..cost.model import StageContext, TaskEnv, TaskPlan, TaskResult, simulate_task
from ..environment import PySparkYarnApp, spark_properties
from ..eventlog import (
    SHUFFLE_READ_ACCUMS,
    ExecutorMetrics,
    IdAllocator,
    RDDInfo,
)
from ..eventlog.ordering import scala_int_hashset_order
from ..model.cluster import (
    Cluster,
    ClusterSpec,
    Executor,
    ExecutorSizing,
    build_cluster,
    place_application,
    scheduling_start,
)
from ..model.files import plan_scan
from ..model.hdfs import Hdfs
from ..model.shuffle import ShuffleExchange, coalesce_partitions, key_shares, row_shares
from ..model.workload import ScanSource, ShuffleSink, ShuffleSource, StageSpec, WriteSink
from ..rng import numpy_rng, substream
from ..sched.scheduler import LOCALITY_NAMES, Launch, TaskScheduler, TaskSet
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

MiB = 1024 * 1024


def iso_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).astimezone(UTC).timestamp() * 1000)


# --------------------------------------------------------------------------- spec


@dataclass(frozen=True)
class InputDataset:
    """A table directory that exists before the app runs (written by upstream jobs)."""

    path: str
    files: int
    mean_file_bytes: int
    file_size_sigma: float
    bytes_per_row: float  # Parquet bytes per row (all columns)
    row_group_bytes: int = 96 * MiB  # compressed Parquet row-group size of the upstream writer


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


# --------------------------------------------------------------------------- records


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

    @property
    def duration(self) -> int:
        return self.finish - self.launch


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
        self.yapp = YarnApp(spec.cluster.cluster_timestamp, spec.app_seq)
        self.t0 = iso_ms(spec.started_at)
        self.submit_time = self.t0 - self.rng.randint(*spec.submit_to_start_ms)
        self.placement = place_application(self.cluster, spec.sizing, self.yapp, self.t0, seed)
        self.executors = self.placement.executors
        self.hdfs = Hdfs(self.cluster, substream(seed, "hdfs"))
        for d in spec.inputs:
            self.hdfs.create_dataset(d.path, d.files, d.mean_file_bytes, d.file_size_sigma, d.bytes_per_row,
                                     _upstream_file_namer(substream(seed, f"names/{d.path}")),
                                     row_group_bytes=d.row_group_bytes)
        am = self.placement.am_host
        self.env_app = PySparkYarnApp(
            app=self.yapp, app_name=spec.app_name, user=spec.user, script=spec.script, am_node=am.nm,
            rm_host=spec.cluster.rm_host, submit_time=self.submit_time, start_time=self.t0,
            driver_port=self.rng.randint(33000, 46000),
            executor_memory=f"{spec.sizing.memory_mb // 1024}g",
            driver_memory=f"{spec.sizing.driver_memory_mb // 1024}g",
            executor_cores=spec.sizing.cores, executor_instances=spec.sizing.instances,
            extra_conf=dict(spec.conf),
        )
        # SQL jobs carry the whole Spark conf in their properties (SQLExecution.withSQLConfPropagated).
        self.base_props = spark_properties(self.env_app)
        self.sim = Sim(self.t0)
        self.sched = TaskScheduler(self.sim, substream(seed, "scheduler"), self.cluster.rack_of)

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
        self.exec_launched: dict[str, int] = {e.id: 0 for e in self.executors}
        self.compile_end: dict[tuple[int, int, str], int] = {}
        self.stage_rng: dict[int, random.Random] = {}
        self.stage_nprng: dict[int, object] = {}
        self.fetch_batch_prop = False  # __fetch_continuous_blocks_in_batch_enabled sticks once set
        self.app_end: int | None = None

    # ------------------------------------------------------------------ helpers

    def _record(self, kind: str, payload, t: int | None = None) -> None:
        self.log.append((self.sim.now if t is None else t, kind, payload))

    def _jitter(self, lo_hi: tuple[int, int]) -> int:
        return self.rng.randint(*lo_hi)

    def conf_bytes(self, key: str, default: int) -> int:
        v = dict(self.spec.conf).get(key)
        if v is None:
            return default
        units = {"k": 1024, "m": MiB, "g": 1024 * MiB}
        return int(v[:-1]) * units[v[-1].lower()] if v[-1].lower() in units else int(v)

    @property
    def default_parallelism(self) -> int:
        return max(2, sum(e.cores for e in self.sched.executors.values()))

    # ------------------------------------------------------------------ main

    def run(self) -> AppRun:
        for ex in self.executors:
            self.sim.at(ex.registered_at, self._executor_added, ex)
        self.sim.spawn(self._driver())
        self.sim.run()
        assert self.app_end is not None, "driver program did not finish"
        return self

    def _executor_added(self, ex: Executor) -> None:
        self._record("executor_added", ex)
        self._record("block_manager_added", ex, t=ex.block_manager_at)
        self.sched.add_executor(ex)

    def _driver(self):
        ready = scheduling_start(self.placement, self.spec.sizing.instances, self.t0 + 1_500)
        yield max(0, ready - self.sim.now)
        self.sched.start()
        for qi, q in enumerate(self.spec.queries):
            yield from self._run_query(qi, q)
        yield self._jitter(self.spec.stop_ms)
        self.app_end = self.sim.now

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
        files = [t.result.output_file for t in stage.tasks if t.result.output_file]
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
        return PlanContext(self.spark, self.plan_ids, dataset_stats=stats)

    def _plan_event(self, kind: str, qrec: QueryRecord, aqe: AdaptiveExecution) -> None:
        """SQLExecutionStart / SQLAdaptiveExecutionUpdate: the metric ids of new nodes are allocated now."""
        info = plan_info(aqe.root, self.accum_ids)
        details = callsites.py4j_callsite(self.spec.queries[qrec.execution_id].action)[1]
        self._record(kind, (qrec, explain_formatted(aqe.root), info, details))

    def _coalescer(self, stages) -> list[tuple[int, int]] | None:
        """CoalesceShufflePartitions over the map output sizes of the shuffles a stage reads."""
        exs = [self.exchanges[s.name] for s in stages]
        if len({e.num_partitions for e in exs}) != 1:
            return None
        sizes = sum(e.bytes_by_partition() for e in exs)
        ranges = coalesce_partitions(sizes, self.default_parallelism)
        return ranges if len(ranges) < exs[0].num_partitions else None

    def _stage_driver_metrics(self, qrec: QueryRecord, plan) -> None:
        """What the driver posts while it builds a stage's RDDs: scan files, AQEShuffleRead specs, partitions."""
        root = plan.children[0] if isinstance(plan, (sqln.ShuffleExchange, sqln.BroadcastExchange)) else plan
        for scan in sqlm.scans_in(root):
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
            self._record("stage_submitted", stage)
            ts = TaskSet(stage.stage_id, 0, job_id, [p.preferred for p in stage.plans],
                         on_launch=lambda a, st=stage: self._on_launch(st, a),
                         on_finish=lambda a, st=stage: self._on_finish(st, a))
            ts.done = self.sim.signal()
            ts.done.wait(lambda: self.sim.after(self._jitter(cal.STAGE_COMPLETE_DELAY_MS), stage_completed))
            self.sched.submit(ts)

        def stage_completed():
            stage.completed = self.sim.now
            self._record("stage_completed", stage)
            self.sim.after(self._jitter(cal.JOB_END_DELAY_MS), job_ended)

        def job_ended():
            job.completed = self.sim.now
            self._record("job_end", job)
            job.done.fire()
            if spec.is_broadcast:
                self._broadcast_built(qrec, stage, job, spec, materialized)
            else:
                if qstage is not None:
                    ex = self.exchanges[spec.sink.exchange]
                    size = sum(t.result.sink_rows * t.result.sink_row_bytes for t in stage.tasks)
                    self.stage_stats[spec.sink.exchange] = (size, int(ex.records.sum()))
                materialized.fire()

        self.sim.after(self._jitter(cal.JOB_TO_STAGE_MS), submit_stage)
        return materialized

    def _broadcast_built(self, qrec, stage: StageRecord, job: JobRecord, spec: StageSpec, materialized: Signal):
        """BroadcastExchangeExec.relationFuture after the collect job: build the HashedRelation, broadcast it."""
        rows = sum(t.result.result_rows for t in stage.tasks)
        row_bytes = stage.tasks[0].result.sink_row_bytes if stage.tasks else 16
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
        rec.plan_root = plan_root
        out_ex = None
        if isinstance(spec.sink, ShuffleSink):
            sink = spec.sink
            distinct_rows = sink.unique_keys or (spec.ops and spec.ops[-1].kind == "partial_agg")
            shares = (key_shares if distinct_rows else row_shares)(sink.key, sink.num_partitions, self.spec.seed)
            out_ex = ShuffleExchange(sink.exchange, self.shuffle_ids.next(), len(plans), sink.num_partitions, shares)
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
            self.scan_parts[path] = plan_scan(
                self.hdfs.listdir(path), self.default_parallelism,
                max_partition_bytes=self.conf_bytes("spark.sql.files.maxPartitionBytes", 128 * MiB),
                open_cost=self.conf_bytes("spark.sql.files.openCostInBytes", 4 * MiB))
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
                    h = self.sched.executors[eid].host.name
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
            self.compile_end[key] = a.launch_time + res.deserialize_ms + int(cal.CODEGEN_COMPILE_MS)
        touched = SHUFFLE_READ_ACCUMS if st.spec.shuffle_inputs else frozenset()
        rec = TaskRecord(st.stage_id, st.attempt, a.index, a.task_id, ex, LOCALITY_NAMES[a.locality],
                         a.launch_time, a.launch_time + res.duration_ms, st.task_type, res, touched)
        rec.sql = sqlm.task_updates(st.plan_root, res)
        st.tasks.append(rec)
        st.by_index[a.index] = rec
        self._record("task_start", rec, t=a.launch_time)
        return res.duration_ms

    def _on_finish(self, st: StageRecord, a: Launch) -> None:
        rec = st.by_index[a.index]
        assert rec.finish == a.finish_time
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
        return key.name, float(self.exchanges[name].shares[a:b].sum())

    def _part_name(self, st: StageRecord, index: int) -> str:
        if not hasattr(st, "_job_uuid"):
            st._job_uuid = uuid.UUID(int=substream(self.spec.seed, f"jobuuid/{st.stage_id}").getrandbits(128),
                                     version=4)
        return f"part-{index:05d}-{st._job_uuid}.c000.snappy.parquet"


def _scans(plan) -> list:
    return [n for n in plan.walk() if isinstance(n, sqln.FileSourceScan)]


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
