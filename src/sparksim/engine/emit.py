"""Turn a finished `AppRun` into Spark 3.5 listener events.

* Executor metrics: executors poll their memory/GC metrics at every heartbeat
  (`spark.executor.metrics.pollingInterval = 0`); a task's "Task Executor
  Metrics" are the per-metric peaks over the heartbeats that happened while it
  ran, and all zero if none did (as in the reference logs).
* Accumulables, stage accumulables and task-metric ids come from
  `DriverLedger`, built in log order by `Timeline`.
* Properties are written in `java.util.Properties` order, environment maps
  in Scala `Map` order (`eventlog.ordering`).
"""

from __future__ import annotations

import math

from ..cost import calibration as cal
from ..environment import environment_update
from ..eventlog import (
    ApplicationEnd,
    ApplicationStart,
    BlockManagerAdded,
    BlockManagerId,
    DriverLedger,
    ExecutorAdded,
    ExecutorInfo,
    ExecutorMetrics,
    ExecutorResourceRequest,
    JobEnd,
    JobStart,
    JobSucceeded,
    LogStart,
    ResourceProfileAdded,
    StageCompleted,
    StageInfo,
    StageSubmitted,
    Success,
    TaskEnd,
    TaskInfo,
    TaskResourceRequest,
    TaskStart,
)
from ..eventlog.events import Event
from ..eventlog.ordering import java_properties
from ..eventlog.timeline import Timeline
from ..rng import substream
from ..yarn import container_attributes, container_log_urls, unified_memory_max
from .runtime import AppRun, StageRecord, TaskRecord

MiB = 1024 * 1024


# --------------------------------------------------------------------------- executor metrics at heartbeats


def sample_executor_metrics(run: AppRun) -> None:
    by_exec: dict[str, list[TaskRecord]] = {}
    for st in run.stages.values():
        for t in st.tasks:
            by_exec.setdefault(t.executor.id, []).append(t)
    interval = cal.HEARTBEAT_INTERVAL_MS
    for ex in run.executors:
        tasks = sorted(by_exec.get(ex.id, []), key=lambda t: (t.launch, t.task_id))
        rng = substream(run.spec.seed, f"heartbeats/{ex.id}")
        pause = cal.YOUNG_GC_PAUSE_MS * math.exp(rng.gauss(0, 0.15))
        t = ex.registered_at + interval + rng.randrange(interval)  # Heartbeater initial delay
        i = 0
        active: list[TaskRecord] = []
        gc_finished = 0.0
        stages_seen: set[int] = set()
        while t < run.app_end:
            while i < len(tasks) and tasks[i].launch <= t:
                active.append(tasks[i])
                stages_seen.add(tasks[i].stage_id)
                i += 1
            still = []
            for tk in active:
                if tk.finish <= t:
                    gc_finished += tk.result.metrics.jvm_gc_time
                else:
                    still.append(tk)
            active = still
            if active:
                snap = _snapshot(ex, active, t, gc_finished, pause, len(stages_seen), rng)
                for tk in active:
                    tk.exec_metrics = _peak(tk.exec_metrics, snap)
            t += interval


def _snapshot(ex, active: list[TaskRecord], t: int, gc_finished: float, pause: float, stages: int,
              rng) -> ExecutorMetrics:
    exec_mem = 0
    gc_running = 0.0
    fetching = 0
    for tk in active:
        frac = (t - tk.launch) / max(1, tk.duration)
        exec_mem += int(tk.result.exec_memory * min(1.0, frac / 0.7))
        gc_running += tk.result.metrics.jvm_gc_time * frac
        fetching += tk.result.fetching
    storage = 1_100_000 + min(stages, 40) * 38_000 + rng.randrange(60_000)
    heap = (cal.JVM_BASE_HEAP + len(active) * cal.JVM_HEAP_PER_RUNNING_TASK * rng.uniform(0.5, 1.5)
            + exec_mem + storage + rng.uniform(0.0, 0.16) * ex.heap_bytes)
    gc_time = int(gc_finished + gc_running)
    return ExecutorMetrics(
        jvm_heap_memory=int(min(heap, ex.heap_bytes - 1)),
        jvm_off_heap_memory=int(cal.JVM_OFF_HEAP_BASE + stages * cal.JVM_OFF_HEAP_PER_STAGE
                                + rng.randrange(4 * MiB)),
        on_heap_execution_memory=exec_mem,
        on_heap_storage_memory=storage,
        on_heap_unified_memory=exec_mem + storage,
        direct_pool_memory=int(cal.DIRECT_POOL_BASE + fetching * cal.DIRECT_POOL_PER_FETCH + rng.randrange(3 * MiB)),
        minor_gc_count=int(gc_time / pause),
        minor_gc_time=gc_time,
        total_gc_time=gc_time,
    )


def _peak(a: ExecutorMetrics, b: ExecutorMetrics) -> ExecutorMetrics:
    return ExecutorMetrics(**{k: max(getattr(a, k), getattr(b, k)) for k in a.__dataclass_fields__})


# --------------------------------------------------------------------------- events


def stage_info(st: StageRecord, submitted: int | None = None, completed: int | None = None,
               accums=None) -> StageInfo:
    return StageInfo(st.stage_id, st.attempt, st.name, st.num_tasks, st.rdds, st.parent_ids, st.details,
                     submitted, completed, None, accums or [])


def build_events(run: AppRun) -> list[Event]:
    sample_executor_metrics(run)
    spec = run.spec
    yapp = run.yapp
    am = run.placement.am_host
    rng = substream(spec.seed, "emit")
    t0 = run.t0
    sizing = spec.sizing

    driver_bm = BlockManagerId("driver", am.ip, rng.randint(33000, 46000))
    driver_mem = unified_memory_max(sizing.driver_heap_bytes)
    t_bm = t0 + rng.randint(*cal.DRIVER_BLOCK_MANAGER_MS)
    preamble: list[Event] = [
        LogStart(),
        ResourceProfileAdded(
            resource_profile_id=0,
            executor_resources={
                "cores": ExecutorResourceRequest("cores", sizing.cores),
                "memory": ExecutorResourceRequest("memory", sizing.memory_mb),
                "offHeap": ExecutorResourceRequest("offHeap", 0),
            },
            task_resources={"cpus": TaskResourceRequest("cpus", 1.0)},
        ),
        BlockManagerAdded(driver_bm, driver_mem, t_bm, driver_mem, 0),
        environment_update(run.env_app, namenode=spec.cluster.namenode),
        ApplicationStart(
            app_name=spec.app_name, app_id=yapp.app_id, timestamp=t0, user=spec.user,
            app_attempt_id=str(yapp.attempt),
            driver_logs=container_log_urls(am.nm, yapp.am_container_id, spec.user),
            driver_attributes=container_attributes(am.nm, yapp.am_container_id, spec.user),
        ),
    ]

    ledger = DriverLedger()
    tl = Timeline()
    for t, kind, obj in run.log:
        if kind == "executor_added":
            ex = obj
            info = ExecutorInfo(host=ex.host.name, total_cores=ex.cores,
                                log_urls=container_log_urls(ex.host.nm, ex.container_id, spec.user),
                                attributes=container_attributes(ex.host.nm, ex.container_id, spec.user),
                                resources={}, resource_profile_id=0, registration_time=ex.registered_at)
            tl.add(t, ExecutorAdded(ex.registered_at, ex.id, info))
        elif kind == "block_manager_added":
            ex = obj
            bm = BlockManagerId(ex.id, ex.host.name, ex.block_manager_port)
            tl.add(t, BlockManagerAdded(bm, ex.max_memory, ex.block_manager_at, ex.max_memory, 0))
        elif kind == "job_start":
            job = obj
            infos = [stage_info(run.stages[s]) for s in job.stage_ids]
            tl.add(t, JobStart(job.job_id, job.submitted, infos, list(job.stage_ids), java_properties(job.props)))
        elif kind == "stage_submitted":
            tl.add(t, _submitted(ledger, obj))
        elif kind == "task_start":
            tk = obj
            tl.add(t, TaskStart(tk.stage_id, tk.attempt, _task_info(tk)))
        elif kind == "task_end":
            tl.add(t, _task_end(ledger, obj))
        elif kind == "stage_completed":
            tl.add(t, _completed(ledger, obj))
        elif kind == "job_end":
            tl.add(t, JobEnd(obj.job_id, obj.completed, JobSucceeded()))
        else:
            raise ValueError(kind)
    tl.add(run.app_end, ApplicationEnd(run.app_end))
    return tl.events(preamble)


def _task_info(tk: TaskRecord, finish: int = 0, accums=None) -> TaskInfo:
    return TaskInfo(tk.task_id, tk.index, tk.attempt, tk.index, tk.launch, tk.executor.id, tk.executor.host.name,
                    tk.locality, finish_time=finish, accumulables=accums or [])


def _submitted(ledger: DriverLedger, st: StageRecord):
    def make():
        ledger.register_stage_attempt(st.stage_id, st.attempt, st.accum_base)
        return StageSubmitted(stage_info(st, st.submitted), java_properties(st.props))
    return make


def _task_end(ledger: DriverLedger, tk: TaskRecord):
    def make():
        m = tk.result.metrics
        accums = ledger.task_end(tk.stage_id, tk.attempt, Success(), m, touched=tk.touched)
        return TaskEnd(tk.stage_id, tk.attempt, tk.task_type, Success(), _task_info(tk, tk.finish, accums),
                       tk.exec_metrics, m)
    return make


def _completed(ledger: DriverLedger, st: StageRecord):
    def make():
        return StageCompleted(stage_info(st, st.submitted, st.completed,
                                         ledger.stage_accumulables(st.stage_id, st.attempt)))
    return make
