"""Phase 2 smoke app: a tiny PySpark RDD job on YARN (1 job, 2 stages, 8 tasks, 2 executors).

    rdd = sc.parallelize(events, 4).map(lambda e: (e.key, 1))
    rdd.reduceByKey(add).count()          # key_counts.py:14

The RDD lineage, stage names, call sites, job properties and the set of
accumulators each task reports mirror the real job 0 of
`reference/event-logs/ref08-rdd-api`. That run used the same API on the same
Spark, so every structural field is grounded in a real log.

No metric is typed in. The numbers come from a small explicit model:
the workload (records, keys, bytes per record) → per-task work → a greedy
slot scheduler on 2 executors × 2 cores, with seeded lognormal noise. The
rates are the order of magnitude ref08 measured. This micro model is a stand-in:
Phase 3 replaces it with the real cluster/cost model (`calibration.py`) and the
discrete-event scheduler. What it already guarantees:

* conservation: shuffle bytes and records written by the map stage equal those
  read by the reduce stage, block by block;
* local vs remote shuffle reads follow from which executor ran each map task;
* time is simulated: launch = slot free, finish = launch + modeled phases,
  stage end = last task end;
* deterministic: same seed → byte-identical log.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ..cost import calibration as cal
from ..environment import PySparkYarnApp, environment_update
from ..eventlog import (
    SHUFFLE_READ_ACCUMS,
    SPILL_ACCUMS,
    ApplicationEnd,
    ApplicationStart,
    BlockManagerAdded,
    BlockManagerId,
    DriverLedger,
    ExecutorAdded,
    ExecutorInfo,
    ExecutorMetrics,
    ExecutorResourceRequest,
    IdAllocator,
    JobEnd,
    JobStart,
    JobSucceeded,
    LogStart,
    RDDInfo,
    ResourceProfileAdded,
    ShuffleReadMetrics,
    ShuffleWriteMetrics,
    StageCompleted,
    StageInfo,
    StageSubmitted,
    Success,
    TaskEnd,
    TaskInfo,
    TaskMetrics,
    TaskResourceRequest,
    TaskStart,
    rdd_scope,
    write_event_log,
)
from ..eventlog.ordering import java_properties
from ..eventlog.timeline import Timeline
from ..yarn import GiB, NodeManager, YarnApp, container_attributes, container_log_urls, unified_memory_max

# ------------------------------------------------------------------------------------------- spec


@dataclass(frozen=True)
class TinySpec:
    seed: int = 7
    # workload
    partitions: int = 4  # parallelize(..., 4); reduceByKey keeps the parent's partition count
    records_per_partition: int = 600_000
    distinct_keys: int = 2_000
    shuffle_bytes_per_record: int = 54  # pickled (key, count) batch after lz4
    # cluster
    executors: int = 2
    executor_cores: int = 2
    executor_heap: int = 4 * GiB
    driver_heap: int = 2 * GiB
    # identity
    app_name: str = "tiny-key-counts"
    user: str = "svc_etl"
    script: str = "key_counts.py"
    action_line: int = 14
    started_at: str = "2026-11-02T06:15:03.412+00:00"


# Rates: order of magnitude from ref08 job 0 (same API); they live in cost/calibration.py (PY_* constants).
PY_NS_PER_RECORD_MAP = cal.PY_NS_PER_RECORD_MAP
PY_NS_PER_RECORD_REDUCE = cal.PY_NS_PER_RECORD_REDUCE
JVM_CPU_SHARE = cal.PY_JVM_CPU_SHARE
PY_WORKER_MS = cal.PY_WORKER_MS
DESERIALIZE_MS = cal.PY_DESERIALIZE_MS
SCHEDULER_OVERHEAD_MS = cal.PY_SCHEDULER_OVERHEAD_MS
SHUFFLE_WRITE_FIXED_MS = cal.PY_SHUFFLE_WRITE_FIXED_MS
SHUFFLE_WRITE_BPS = cal.PY_SHUFFLE_WRITE_BPS
NETWORK_BPS = cal.PY_NETWORK_BPS
REMOTE_FETCH_LATENCY_MS = cal.PY_REMOTE_FETCH_LATENCY_MS
HOST_SPEED_SIGMA = cal.PY_HOST_SPEED_SIGMA
TASK_NOISE_SIGMA = cal.PY_TASK_NOISE_SIGMA

CLUSTER_TS = 1792541274210  # ResourceManager start (2026-10-21)
APP_SEQ = 4217


def _ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).astimezone(UTC).timestamp() * 1000)


def _split(total: int, weights: list[float]) -> list[int]:
    """Integer split of `total` proportional to `weights` (largest remainder): parts sum to `total` exactly."""
    w = sum(weights)
    raw = [total * x / w for x in weights]
    parts = [math.floor(r) for r in raw]
    for i in sorted(range(len(raw)), key=lambda i: raw[i] - parts[i], reverse=True)[: total - sum(parts)]:
        parts[i] += 1
    return parts


@dataclass
class _Task:
    index: int
    tid: int
    executor: str
    host: str
    launch: int
    finish: int
    metrics: TaskMetrics
    locality: str


# ------------------------------------------------------------------------------------------- build


def build_events(spec: TinySpec | None = None):
    spec = spec or TinySpec()
    rng = random.Random(spec.seed)
    noise = lambda: rng.lognormvariate(0.0, TASK_NOISE_SIGMA)  # noqa: E731

    yapp = YarnApp(CLUSTER_TS, APP_SEQ)
    am = NodeManager("wn012.hadoop.prod.internal", "10.31.4.12")
    nodes = [NodeManager("wn023.hadoop.prod.internal", "10.31.4.23"),
             NodeManager("wn041.hadoop.prod.internal", "10.31.5.41")]
    rm_host = "rm1.hadoop.prod.internal"
    t0 = _ms(spec.started_at)  # SparkContext start in the AM container
    submit = t0 - 6_840  # spark-submit → YARN accepted → AM container launched
    env_app = PySparkYarnApp(
        app=yapp, app_name=spec.app_name, user=spec.user, script=spec.script, am_node=am, rm_host=rm_host,
        submit_time=submit, start_time=t0, driver_port=rng.randint(33000, 46000),
        executor_memory=f"{spec.executor_heap // GiB}g", driver_memory=f"{spec.driver_heap // GiB}g",
        executor_cores=spec.executor_cores, executor_instances=spec.executors,
    )
    callsite = f"{env_app.am_work_dir}/{spec.script}:{spec.action_line}"

    # ---- preamble (fixed log order, see Timeline)
    driver_bm = BlockManagerId("driver", am.ip, rng.randint(33000, 46000))
    preamble = [
        LogStart(),
        ResourceProfileAdded(
            resource_profile_id=0,
            executor_resources={
                "cores": ExecutorResourceRequest("cores", spec.executor_cores),
                "memory": ExecutorResourceRequest("memory", spec.executor_heap // (1024 * 1024)),
                "offHeap": ExecutorResourceRequest("offHeap", 0),
            },
            task_resources={"cpus": TaskResourceRequest("cpus", 1.0)},
        ),
        BlockManagerAdded(driver_bm, unified_memory_max(spec.driver_heap), t0 + 951,
                          unified_memory_max(spec.driver_heap), 0),
        environment_update(env_app, namenode="nn1.hadoop.prod.internal"),
        ApplicationStart(
            app_name=spec.app_name, app_id=yapp.app_id, timestamp=t0, user=spec.user,
            app_attempt_id=str(yapp.attempt),
            driver_logs=container_log_urls(am, yapp.am_container_id, spec.user),
            driver_attributes=container_attributes(am, yapp.am_container_id, spec.user),
        ),
    ]
    tl = Timeline()

    # ---- executors: YARN allocates containers 000002.. on two NodeManagers
    executors = []
    t_exec = t0 + 5_640
    for i in range(spec.executors):
        eid, nm = str(i + 1), nodes[i % len(nodes)]
        cid = yapp.container_id(i + 2)
        t_exec += rng.randint(120, 420)
        info = ExecutorInfo(host=nm.host, total_cores=spec.executor_cores,
                            log_urls=container_log_urls(nm, cid, spec.user),
                            attributes=container_attributes(nm, cid, spec.user),
                            resources={}, resource_profile_id=0, registration_time=t_exec)
        tl.add(t_exec, ExecutorAdded(t_exec, eid, info))
        bm = BlockManagerId(eid, nm.host, rng.randint(33000, 46000))
        mem = unified_memory_max(spec.executor_heap)
        t_bm = t_exec + rng.randint(110, 160)  # BlockManager registers right after the executor
        tl.add(t_bm, BlockManagerAdded(bm, mem, t_bm, mem, 0))
        executors.append((eid, nm, rng.lognormvariate(0.0, HOST_SPEED_SIGMA)))
    t_ready = t_exec + 400

    # ---- DAG: mirrors ref08 job 0 (RDD ids, scopes, call sites)
    py4j_ctor = "\n".join([
        "org.apache.spark.rdd.RDD.<init>(RDD.scala:102)",
        "org.apache.spark.api.python.PairwiseRDD.<init>(PythonRDD.scala:126)",
        "java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)",
        "java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:77)",
        "java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)",
        "java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:500)",
        "java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:481)",
        "py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:247)",
        "py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)",
        "py4j.Gateway.invoke(Gateway.java:238)",
        "py4j.commands.ConstructorCommand.invokeConstructor(ConstructorCommand.java:80)",
        "py4j.commands.ConstructorCommand.execute(ConstructorCommand.java:69)",
        "py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)",
        "py4j.ClientServerConnection.run(ClientServerConnection.java:106)",
        "java.base/java.lang.Thread.run(Thread.java:840)",
    ])
    py4j_collect = "\n".join([
        "org.apache.spark.rdd.RDD.collect(RDD.scala:1048)",
        "org.apache.spark.api.python.PythonRDD$.collectAndServe(PythonRDD.scala:195)",
        "org.apache.spark.api.python.PythonRDD.collectAndServe(PythonRDD.scala)",
        "java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
        "java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)",
        "java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)",
        "java.base/java.lang.reflect.Method.invoke(Method.java:569)",
        "py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)",
        "py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)",
        "py4j.Gateway.invoke(Gateway.java:282)",
        "py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)",
        "py4j.commands.CallCommand.execute(CallCommand.java:79)",
        "py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)",
        "py4j.ClientServerConnection.run(ClientServerConnection.java:106)",
        "java.base/java.lang.Thread.run(Thread.java:840)",
    ])
    n = spec.partitions
    map_rdds = [
        RDDInfo(2, "PairwiseRDD", None, f"reduceByKey at {callsite}", [1], num_partitions=n),
        RDDInfo(1, "PythonRDD", None, f"reduceByKey at {callsite}", [0], num_partitions=n),
        RDDInfo(0, "ParallelCollectionRDD", rdd_scope(0, "parallelize"), "readRDDFromFile at PythonRDD.scala:289", [],
                num_partitions=n),
    ]
    reduce_rdds = [
        RDDInfo(5, "PythonRDD", None, f"count at {callsite}", [4], deterministic_level="UNORDERED", num_partitions=n),
        RDDInfo(3, "ShuffledRDD", rdd_scope(1, "partitionBy"), "partitionBy at NativeMethodAccessorImpl.java:0", [2],
                deterministic_level="UNORDERED", num_partitions=n),
        RDDInfo(4, "MapPartitionsRDD", rdd_scope(2, "mapPartitions"), "mapPartitions at PythonRDD.scala:160", [3],
                deterministic_level="UNORDERED", num_partitions=n),
    ]

    def stage_info(sid, name, rdds, parents, details, submitted=None, completed=None, accums=None):
        return StageInfo(sid, 0, name, n, rdds, parents, details, submitted, completed, None, accums or [])

    props = java_properties({
        "spark.rdd.scope": rdd_scope(3, "collect"),
        "spark.job.description": "count events per key",
        "callSite.short": f"count at {callsite}",
        "spark.rdd.scope.noOverride": "true",
    })
    stage_props = java_properties({**props, "resource.executor.cores": str(spec.executor_cores)})
    names = (f"reduceByKey at {callsite}", f"count at {callsite}")

    ledger = DriverLedger()
    accum_ids = IdAllocator(0)
    accum_ids.skip(1)  # PySpark registers its PythonAccumulatorV2 at SparkContext start (ID 0)
    task_ids = IdAllocator(0)

    job_submit = t_ready + 3_150  # driver-side Python between SparkContext ready and the action
    tl.add(job_submit, JobStart(0, job_submit, [stage_info(0, names[0], map_rdds, [], py4j_ctor),
                                                stage_info(1, names[1], reduce_rdds, [0], py4j_collect)],
                                [0, 1], props))

    # ---- scheduling helpers
    slots_free: dict[tuple[str, int], int] = {(e[0], c): 0 for e in executors for c in range(spec.executor_cores)}
    warm_cores: set[tuple[str, int]] = set()
    host_speed = {e[0]: e[2] for e in executors}
    host_of = {e[0]: e[1].host for e in executors}

    def assign(t_submit: int, first_delay: int):
        """Greedy slot assignment, one task index at a time (callers update slots_free between picks).

        First wave: one TaskSchedulerImpl.resourceOffers round (executors shuffled, cores round robin),
        a couple of ms apart. Later tasks launch on the core that frees first, a few ms after its
        previous task's status update reached the driver.
        """
        execs = [e[0] for e in executors]
        rng.shuffle(execs)
        order = [(e, c) for c in range(spec.executor_cores) for e in execs]
        for idx in range(n):
            slot = min(order, key=lambda s: (max(slots_free[s], t_submit), order.index(s)))
            if slots_free[slot] <= t_submit:
                launch = t_submit + first_delay + 2 * order.index(slot)
            else:
                launch = slots_free[slot] + rng.randint(2, 6)
            yield idx, slot, launch

    def submitted(stage_id: int, internal_base: int, info: StageInfo) -> StageSubmitted:
        """Stage submission registers the attempt's 35 task-metric accumulators (DAGScheduler.submitMissingTasks)."""
        ledger.register_stage_attempt(stage_id, 0, internal_base)
        return StageSubmitted(info, stage_props)

    def deserialize_ms(slot, first_in_stage: bool) -> int:
        if slot not in warm_cores:
            base = DESERIALIZE_MS["cold_jvm"]
        elif first_in_stage:
            base = DESERIALIZE_MS["cold_stage"]
        else:
            base = DESERIALIZE_MS["warm"]
        return max(1, round(base * noise()))

    # ---- stage 0: map side of reduceByKey
    s0_submit = job_submit + 27
    base0 = accum_ids.take(35).start
    tl.add(s0_submit, lambda: submitted(0, base0, stage_info(0, names[0], map_rdds, [], py4j_ctor, s0_submit)))
    map_tasks: list[_Task] = []
    shuffle_bytes = []
    seen: set = set()
    for idx, slot, launch in assign(s0_submit, 16):
        eid = slot[0]
        cold = slot not in warm_cores
        deser = deserialize_ms(slot, slot not in seen)
        seen.add(slot)
        warm_cores.add(slot)
        records = spec.records_per_partition
        compute_ms = (PY_WORKER_MS["cold"] + records * PY_NS_PER_RECORD_MAP / 1e6) * host_speed[eid] * noise()
        out_records = spec.distinct_keys  # map-side combine: every partition holds every key
        out_bytes = round(out_records * spec.shuffle_bytes_per_record * rng.uniform(0.97, 1.03))
        write_ns = round((SHUFFLE_WRITE_FIXED_MS * 1e6 + out_bytes / SHUFFLE_WRITE_BPS * 1e9) * noise())
        gc = round(rng.uniform(30, 80) if cold else (rng.uniform(15, 45) if rng.random() < 0.25 else 0))
        run = round(compute_ms + write_ns / 1e6 + gc)
        ser = rng.randint(0, 3)
        finish = launch + deser + run + ser + rng.randint(*SCHEDULER_OVERHEAD_MS) + (80 if cold else 0)
        m = TaskMetrics(
            executor_deserialize_time=deser,
            executor_deserialize_cpu_time=round(deser * 1e6 * (0.12 if cold else 0.35) * noise()),
            executor_run_time=run,
            executor_cpu_time=round(run * 1e6 * JVM_CPU_SHARE * noise()),
            result_size=1497 + 43 * rng.randint(0, 2),  # serialized MapStatus (size buckets)
            jvm_gc_time=gc,
            result_serialization_time=ser,
            shuffle_write=ShuffleWriteMetrics(out_bytes, write_ns, out_records),
        )
        slots_free[slot] = finish
        map_tasks.append(_Task(idx, task_ids.next(), eid, host_of[eid], launch, finish, m, "PROCESS_LOCAL"))
        shuffle_bytes.append(out_bytes)
    s0_done = max(t.finish for t in map_tasks) + 1

    # ---- shuffle: every map output holds every key; hash partitioning spreads keys over reducers
    keys_per_reducer = _split(spec.distinct_keys, [rng.uniform(0.9, 1.1) for _ in range(n)])
    blocks = [[(keys_per_reducer[j], b) for j, b in enumerate(_split(shuffle_bytes[i], keys_per_reducer))]
              for i in range(n)]  # blocks[map][reduce] = (records, bytes)

    # ---- stage 1: reduce side + count
    s1_submit = s0_done + 15
    base1 = accum_ids.take(35).start
    tl.add(s1_submit, lambda: submitted(1, base1, stage_info(1, names[1], reduce_rdds, [0], py4j_collect, s1_submit)))
    reduce_tasks: list[_Task] = []
    seen = set()
    for idx, slot, launch in assign(s1_submit, 14):
        eid = slot[0]
        deser = deserialize_ms(slot, slot not in seen)
        seen.add(slot)
        rd = ShuffleReadMetrics()
        records = 0
        for i, mt in enumerate(map_tasks):
            recs, b = blocks[i][idx]
            records += recs
            if mt.executor == eid:
                rd.local_blocks_fetched += 1
                rd.local_bytes_read += b
            else:
                rd.remote_blocks_fetched += 1
                rd.remote_bytes_read += b
        rd.records_read = records
        if rd.remote_blocks_fetched:
            rd.fetch_wait_time = round((REMOTE_FETCH_LATENCY_MS + rd.remote_bytes_read / NETWORK_BPS * 1e3) * noise())
            rd.remote_reqs_duration = round(rd.fetch_wait_time * rng.uniform(1.2, 1.8))
        compute_ms = (PY_WORKER_MS["warm"] + records * PY_NS_PER_RECORD_REDUCE / 1e6) * host_speed[eid] * noise()
        gc = round(rng.uniform(10, 40)) if rng.random() < 0.25 else 0
        run = round(compute_ms + rd.fetch_wait_time + gc)
        ser = rng.randint(0, 2)
        finish = launch + deser + run + ser + rng.randint(*SCHEDULER_OVERHEAD_MS)
        m = TaskMetrics(
            executor_deserialize_time=deser,
            executor_deserialize_cpu_time=round(deser * 1e6 * 0.3 * noise()),
            executor_run_time=run,
            executor_cpu_time=round(run * 1e6 * JVM_CPU_SHARE * noise()),
            result_size=2055 + 43 * rng.randint(0, 2),  # count() returns one long per partition
            jvm_gc_time=gc,
            result_serialization_time=ser,
            shuffle_read=rd,
        )
        slots_free[slot] = finish
        reduce_tasks.append(_Task(idx, task_ids.next(), eid, host_of[eid], launch, finish, m, "NODE_LOCAL"))
    s1_done = max(t.finish for t in reduce_tasks) + 1

    # ---- task events (accumulables are derived by the ledger in log order)
    def add_task(stage_id, task_type, t: _Task, touched):
        def info(finish=0, accums=None):
            return TaskInfo(t.tid, t.index, 0, t.index, t.launch, t.executor, t.host, t.locality,
                            finish_time=finish, accumulables=accums or [])
        tl.add(t.launch, TaskStart(stage_id, 0, info()))

        def end():
            accums = ledger.task_end(stage_id, 0, Success(), t.metrics, touched=touched)
            return TaskEnd(stage_id, 0, task_type, Success(), info(t.finish, accums), ExecutorMetrics(), t.metrics)
        tl.add(t.finish, end)

    for t in map_tasks:
        add_task(0, "ShuffleMapTask", t, SPILL_ACCUMS)
    tl.add(s0_done, lambda: StageCompleted(stage_info(0, names[0], map_rdds, [], py4j_ctor, s0_submit, s0_done,
                                                      ledger.stage_accumulables(0, 0))))
    for t in reduce_tasks:
        add_task(1, "ResultTask", t, SPILL_ACCUMS | SHUFFLE_READ_ACCUMS)
    tl.add(s1_done, lambda: StageCompleted(stage_info(1, names[1], reduce_rdds, [0], py4j_collect, s1_submit, s1_done,
                                                      ledger.stage_accumulables(1, 0))))
    job_end = s1_done + 3
    tl.add(job_end, JobEnd(0, job_end, JobSucceeded()))
    app_end = job_end + 1_210  # Python prints the result, SparkContext.stop()
    tl.add(app_end, ApplicationEnd(app_end))

    return yapp, tl.events(preamble)


def generate(out_dir: str | Path, spec: TinySpec | None = None) -> Path:
    yapp, events = build_events(spec)
    return write_event_log(Path(out_dir) / yapp.event_log_name, events)
