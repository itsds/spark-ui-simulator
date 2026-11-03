"""The ApplicationMaster container's logs. In YARN cluster mode the driver runs inside the AM, so its
stderr is the driver log: AM and SparkContext start-up, YarnAllocator container requests and launches,
executor registrations, every job, stage and task as DAGScheduler / TaskSetManager log them, executor
losses and dynamic-allocation decisions, and the shutdown.

Start-up follows the real AM logs in `reference/yarn-logs` (the reference harness switches the driver to
WARN after start-up; everything later follows the Spark 3.5.9 source). All times, ids, hosts and counts
come from the run's records (`AppRun.log`, `AppRun.yarn_log`, `AppRun.broadcasts`) and from what the
executors did (`echo` from `executor.executor_stderr`).
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from ..eventlog.ordering import java_properties_order
from ..rng import substream
from ..yarn import container_work_dir, unified_memory_max
from .executor import ESS_PORT, JAVA_VERSION, OS_VERSION, _security_manager, _uuid, local_dirs
from .log4j import ContainerLog, bytes_to_string

if TYPE_CHECKING:
    from ..engine.runtime import AppRun, StageRecord

AMIP = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
UI_PATHS = ["/jobs", "/jobs/json", "/jobs/job", "/jobs/job/json", "/stages", "/stages/json", "/stages/stage",
            "/stages/stage/json", "/stages/pool", "/stages/pool/json", "/storage", "/storage/json",
            "/storage/rdd", "/storage/rdd/json", "/environment", "/environment/json", "/executors",
            "/executors/json", "/executors/threadDump", "/executors/threadDump/json", "/executors/heapHistogram",
            "/executors/heapHistogram/json", "/static", "/", "/api", "/jobs/job/kill", "/stages/stage/kill",
            "/metrics/json"]
YA = "YarnAllocator"
DRIVER_EP = "YarnSchedulerBackend$YarnDriverEndpoint"


def _stage_str(st: StageRecord) -> str:
    return f"{'ShuffleMapStage' if st.task_type == 'ShuffleMapTask' else 'ResultStage'} {st.stage_id}"


def _rdd_str(st: StageRecord) -> str:
    r = st.rdds[0]
    return f"{r.name}[{r.rdd_id}] at {r.callsite}"


def _vector(n: int) -> str:
    return "Vector(" + ", ".join(str(i) for i in range(min(15, n))) + ")"


def _launch_context(run: AppRun) -> str:
    s = run.spec
    app = run.yapp.app_id
    nn = f"hdfs://{s.cluster.namenode}:8020"
    staging = f"{nn}/user/{s.user}/.sparkStaging/{app}"
    opts = run.base_props.get("spark.executor.extraJavaOptions", "").split()
    cmd = ["{{JAVA_HOME}}/bin/java", "-server", f"-Xmx{s.sizing.memory_mb}m", *[f"'{o}'" for o in opts],
           "-Djava.io.tmpdir={{PWD}}/tmp", f"'-Dspark.driver.port={run.env_app.driver_port}'", "'-Dspark.ui.port=0'"]
    if run.ess_enabled:
        cmd.append(f"'-Dspark.shuffle.service.port={ESS_PORT}'")
    cmd += ["-Dspark.yarn.app.container.log.dir=<LOG_DIR>", "-XX:OnOutOfMemoryError='kill %p'",
            "org.apache.spark.executor.YarnCoarseGrainedExecutorBackend", "--driver-url",
            f"spark://CoarseGrainedScheduler@{run.am_host.name}:{run.env_app.driver_port}", "--executor-id",
            "<executorId>", "--hostname", "<hostname>", "--cores", str(s.sizing.cores), "--app-id", app,
            "--resourceProfileId", "0", "1><LOG_DIR>/stdout", "2><LOG_DIR>/stderr"]
    rng = substream(s.seed, "yarnlogs/launch-context")
    ts = run.submit_time - rng.randint(900, 2_400)

    def res(name, path, size, kind, vis, t):
        return (f'    {name} -> resource {{ scheme: "hdfs" host: "{s.cluster.namenode}" port: 8020 file: "{path}" }} '
                f"size: {size} timestamp: {t} type: {kind} visibility: {vis}")

    resources = [
        res("py4j-0.10.9.7-src.zip", f"/user/{s.user}/.sparkStaging/{app}/py4j-0.10.9.7-src.zip", 42424, "FILE",
            "PRIVATE", ts + 120),
        res("__spark_conf__", f"/user/{s.user}/.sparkStaging/{app}/__spark_conf__.zip", 251_000 + rng.randint(0, 9_000),
            "ARCHIVE", "PRIVATE", ts + 1_100),
        res("pyspark.zip", f"/user/{s.user}/.sparkStaging/{app}/pyspark.zip", 2437297, "FILE", "PRIVATE", ts + 90),
        res("__spark_libs__", "/" + run.env_app.yarn_archive.split("://", 1)[-1].lstrip("/").split("/", 1)[-1]
            if run.env_app.yarn_archive.startswith("hdfs:///") else run.env_app.yarn_archive,
            313712059, "ARCHIVE", "PUBLIC", run.spec.cluster.cluster_timestamp + 86_400_000),
        res(s.script, f"/user/{s.user}/.sparkStaging/{app}/{s.script}", 6_000 + rng.randint(0, 3_000), "FILE",
            "PRIVATE", ts + 60),
    ]
    env = [
        "    CLASSPATH -> {{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>"
        "{{PWD}}/__spark_conf__/__hadoop_conf__<CPS>$HADOOP_CONF_DIR<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/*"
        "<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/lib/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/*<CPS>"
        "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/*<CPS>"
        "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*",
        f"    SPARK_YARN_STAGING_DIR -> {staging}",
        f"    SPARK_USER -> {s.user}",
        "    PYTHONPATH -> {{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.9.7-src.zip",
        "    SPARK_PREFER_IPV6 -> false",
        f"    PYSPARK_PYTHON -> {run.env_app.python}",
    ]
    body = "\n".join(["", "=" * 79, "Default YARN executor launch context:", "  env:", *env, "",
                      "  command:", "    " + " \\ \n      ".join(cmd), "", "  resources:", *resources, "",
                      "=" * 79])
    return body


class _Driver:
    def __init__(self, run: AppRun, echo: list):
        self.run = run
        self.echo = echo
        self.log = ContainerLog()
        self.rng: random.Random = substream(run.spec.seed, "yarnlogs/driver")
        self.am = run.am_host.name
        self.port = run.env_app.driver_port
        self.ui_port = self.rng.randint(33000, 46000)

    # ------------------------------------------------------------------ start-up

    def startup(self) -> None:
        run, log, r = self.run, self.log, self.rng
        s = run.spec
        t0 = run.t0
        t = t0 - r.randint(1_700, 2_400)
        for sig in ("TERM", "HUP", "INT"):
            log.info(t, "SignalUtils", f"Registering signal handler for {sig}")
        t += r.randint(500, 800)
        log.info(t, "ApplicationMaster", f"ApplicationAttemptId: {run.yapp.attempt_id}")
        log.info(t, "ApplicationMaster", "Starting the user application in a separate Thread")
        log.info(t, "ApplicationMaster", "Waiting for spark context initialization...")
        t = t0
        log.info(t, "SparkContext", "Running Spark version 3.5.9")
        log.info(t, "SparkContext", f"OS info Linux, {OS_VERSION}, amd64")
        log.info(t, "SparkContext", f"Java version {JAVA_VERSION}")
        for msg in ("=" * 62, "No custom resources configured for spark.driver.", "=" * 62):
            log.info(t, "ResourceUtils", msg)
        log.info(t, "SparkContext", f"Submitted application: {s.app_name}")
        res = {"cores": s.sizing.cores, "memory": s.sizing.memory_mb, "offHeap": 0}
        if "spark.executor.memoryOverhead" in run.conf:
            res["memoryOverhead"] = s.sizing.executor_overhead_mb
        ereqs = ", ".join(f"{k} -> name: {k}, amount: {res[k]}, script: , vendor: "
                          for k in java_properties_order(list(res)))
        log.info(t, "ResourceProfile", f"Default ResourceProfile created, executor resources: Map({ereqs}), "
                 "task resources: Map(cpus -> name: cpus, amount: 1.0)")
        log.info(t, "ResourceProfile", f"Limiting resource is cpus at {s.sizing.cores} tasks per executor")
        log.info(t, "ResourceProfileManager", "Added ResourceProfile id: 0")
        _security_manager(log, t, s.user)
        t += r.randint(150, 400)
        log.info(t, "Utils", f"Successfully started service 'sparkDriver' on port {self.port}.")
        log.info(t, "SparkEnv", "Registering MapOutputTracker")
        log.info(t, "SparkEnv", "Registering BlockManagerMaster")
        log.info(t, "BlockManagerMasterEndpoint",
                 "Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information")
        log.info(t, "BlockManagerMasterEndpoint", "BlockManagerMasterEndpoint up")
        log.info(t, "SparkEnv", "Registering BlockManagerMasterHeartbeat")
        for d in local_dirs(run):
            log.info(t, "DiskBlockManager", f"Created local directory at {d}/usercache/{s.user}/appcache/"
                     f"{run.yapp.app_id}/blockmgr-{_uuid(r)}")
        dmem = unified_memory_max(s.sizing.driver_heap_bytes)
        log.info(t, "MemoryStore", f"MemoryStore started with capacity {bytes_to_string(dmem)}")
        log.info(t, "SparkEnv", "Registering OutputCommitCoordinator")
        t += r.randint(200, 500)
        log.info(t, "JettyUtils", "Start Jetty 0.0.0.0:0 for SparkUI")
        log.info(t, "Utils", f"Successfully started service 'SparkUI' on port {self.ui_port}.")
        log.info(t, "ServerInfo", f"Adding filter to /: {AMIP}")
        log.info(t, "YarnClusterScheduler", "Created YarnClusterScheduler")
        bm = run.driver_bm_port
        log.info(t, "Utils", "Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' "
                 f"on port {bm}.")
        log.info(t, "NettyBlockTransferService", f"Server created on {self.am}:{bm}")
        log.info(t, "BlockManager",
                 "Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy")
        if run.ess_enabled:
            log.info(t, "BlockManager", f"external shuffle service port = {ESS_PORT}")
        bmid = f"BlockManagerId(driver, {self.am}, {bm}, None)"
        log.info(t, "BlockManagerMaster", f"Registering BlockManager {bmid}")
        log.info(t, "BlockManagerMasterEndpoint", f"Registering block manager {self.am}:{bm} with "
                 f"{bytes_to_string(dmem)} RAM, {bmid}")
        log.info(t, "BlockManagerMaster", f"Registered BlockManager {bmid}")
        log.info(t, "BlockManager", f"Initialized BlockManager: {bmid}")
        log.info(t, "SingleEventLogFileWriter", f"Logging events to {run.event_log_path}.inprogress")
        for p in UI_PATHS:
            log.info(t, "ServerInfo", f"Adding filter to {p}: {AMIP}")
        t += r.randint(80, 250)
        if not s.cluster.rm_standby_hosts:
            rm = run.cluster.spec.rm_host
            log.info(t, "DefaultNoHARMFailoverProxyProvider", f"Connecting to ResourceManager at {rm}/"
                     f"{run.rm_ip}:8030")
        log.info(t, "YarnRMClient", "Registering the ApplicationMaster")
        log.info(t, "ApplicationMaster", "Preparing Local resources")
        t += r.randint(100, 300)
        _security_manager(log, t, s.user)
        log.info(t, "ApplicationMaster", _launch_context(run))
        t = run.am_request_time
        log.info(t, YA, "Resource profile 0 doesn't exist, adding it")
        log.info(t, "Configuration", "resource-types.xml not found")
        log.info(t, "ResourceUtils", "Unable to find 'resource-types.xml'.")
        log.info(t, "YarnSchedulerBackend$YarnSchedulerEndpoint",
                 f"ApplicationMaster registered as NettyRpcEndpointRef(spark://YarnAM@{self.am}:{self.port})")
        log.info(t + 2, "ApplicationMaster", "Started progress reporter thread with (heartbeat : 3000, initial "
                 "allocation : 200) intervals")

    # ------------------------------------------------------------------ YARN / executors

    def yarn(self) -> None:
        run, log = self.run, self.log
        for t, kind, p in run.yarn_log:
            if kind == "am_request":
                n, mb, cores = p
                log.info(t, YA, f"Will request {n} executor container(s) for  ResourceProfile Id: 0, each with "
                         f"{cores} core(s) and {mb} MB memory. with custom resources: <memory:{mb}, vCores:{cores}>")
                log.info(t, YA, f"Submitted {n} unlocalized container requests.")
            elif kind == "am_cancel":
                n, target = p
                log.info(t, YA, f"Canceling requests for {n} executor container(s) to have a new desired total "
                         f"{target} executors.")
            elif kind == "am_launch":
                ex = p
                log.info(t, YA, f"Launching container {ex.container_id} on host {ex.host.name} for executor with "
                         f"ID {ex.id} for ResourceProfile Id 0")
            elif kind == "am_received":
                n, k = p
                log.info(t, YA, f"Received {n} containers from YARN, launching executors on {k} of them.")
            elif kind == "am_skip_launch":
                running, target = p
                log.info(t, YA, f"Skip launching executorRunnable as running executors count: {running} reached "
                         f"target executors count: {target}.")
            elif kind == "am_target":
                log.info(t, YA, f"Driver requested a total number of {p} executor(s) for resource profile id: 0.")
            elif kind == "am_completed":
                ex, reason, by_app = p
                log.info(t, YA, f"Completed container {ex.container_id} on host: {ex.host.name} (state: COMPLETE, "
                         f"exit status: {ex.exit_status})")
                (log.warn if by_app else log.info)(t, YA, reason)
                log.warn(t, "YarnSchedulerBackend$YarnSchedulerEndpoint",
                         f"Requesting driver to remove executor {ex.id} for reason {reason}")
            elif kind == "driver_disconnected":
                ex = p
                log.info(t, DRIVER_EP, f"Disabling executor {ex.id}.")
                log.info(t, "DAGScheduler", f"Executor lost: {ex.id} (epoch {run_epoch_at(run, t)})")
                bmid = f"BlockManagerId({ex.id}, {ex.host.name}, {ex.block_manager_port}, None)"
                log.info(t, "BlockManagerMasterEndpoint", f"Trying to remove executor {ex.id} from "
                         "BlockManagerMaster.")
                log.info(t, "BlockManagerMasterEndpoint", f"Removing block manager {bmid}")
                log.info(t, "BlockManagerMaster", f"Removed {ex.id} successfully in removeExecutor")
            elif kind == "executor_lost":
                ex, reason, by_app, released, lost = p
                if released:
                    log.info(t, "YarnClusterScheduler", f"Executor {ex.id} on {ex.host.name} killed by driver.")
                else:
                    log.error(t, "YarnClusterScheduler", f"Lost executor {ex.id} on {ex.host.name}: {reason}")
                for a in lost:
                    tk = run.task_records[a.task_id]
                    why = ("caused by one of the running tasks" if by_app else "unrelated to the running tasks")
                    log.warn(t, "TaskSetManager", f"Lost task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}."
                             f"{tk.attempt} (TID {tk.task_id}) ({ex.host.name} executor {ex.id}): ExecutorLostFailure "
                             f"(executor {ex.id} exited {why}) Reason: {reason}")
                if run.dynalloc is not None:
                    st = run.dynalloc.removed_stats
                    log.info(t + 1, "ExecutorMonitor", f"Executor {ex.id} is removed. Remove reason statistics: ("
                             f"gracefully decommissioned: 0, decommision unfinished: 0, driver killed: "
                             f"{st['driver killed']}, unexpectedly exited: {st['unexpectedly exited']}).")
                if not released:
                    log.info(t + 2, DRIVER_EP, f"Asked to remove non-existent executor {ex.id}")
            elif kind == "da_target":
                target, old, backlogged, delta = p
                if backlogged:
                    word = "executor" if delta == 1 else "executors"
                    log.info(t, "ExecutorAllocationManager", f"Requesting {delta} new {word} because tasks are "
                             f"backlogged (new desired total will be {target} for resource profile id: 0)")
            elif kind == "da_kill":
                ids = ", ".join(p)
                log.info(t, "YarnClusterSchedulerBackend", f"Requesting to kill executor(s) {ids}")
                log.info(t, "YarnClusterSchedulerBackend", f"Actual list of executor(s) to be killed is {ids}")
                log.info(t + 1, "ApplicationMaster$AMEndpoint", f"Driver requested to kill executor(s) {ids}.")
                log.info(t + 2, "ExecutorAllocationManager", f"Executors {','.join(p)} removed due to idle timeout.")

    def executors(self) -> None:
        run, log, r = self.run, self.log, self.rng
        ready_logged = False
        for t, kind, obj in run.log:
            if kind == "executor_added":
                ex = obj
                log.info(t, DRIVER_EP, f"Registered executor NettyRpcEndpointRef(spark-client://Executor) "
                         f"({ex.host.ip}:{r.randint(38000, 60999)}) with ID {ex.id},  ResourceProfileId 0")
                if run.dynalloc is not None:
                    n = sum(1 for e in run.executors if e.registered_at <= t and (e.exit_at is None or e.exit_at > t))
                    log.info(t, "ExecutorMonitor", f"New executor {ex.id} has registered (new total is {n})")
            elif kind == "block_manager_added":
                ex = obj
                log.info(t, "BlockManagerMasterEndpoint", f"Registering block manager {ex.host.name}:"
                         f"{ex.block_manager_port} with {bytes_to_string(ex.max_memory)} RAM, BlockManagerId("
                         f"{ex.id}, {ex.host.name}, {ex.block_manager_port}, None)")
        t = run.scheduling_started
        if not ready_logged and t is not None:
            log.info(t, "YarnClusterSchedulerBackend", "SchedulerBackend is ready for scheduling beginning after "
                     "reached minRegisteredResourcesRatio: 0.8")
            log.info(t, "YarnClusterScheduler", "YarnClusterScheduler.postStartHook done")

    # ------------------------------------------------------------------ jobs, stages, tasks

    def broadcasts(self) -> None:
        run, log = self.run, self.log
        dmem = unified_memory_max(run.spec.sizing.driver_heap_bytes)
        used = 0
        for b in sorted(run.broadcasts.values(), key=lambda b: b.id):
            t = b.created
            used += b.value_bytes
            log.info(t, "MemoryStore", f"Block broadcast_{b.id} stored as values in memory (estimated size "
                     f"{bytes_to_string(b.value_bytes)}, free {bytes_to_string(dmem - used)})")
            per = max(1, b.piece_bytes // b.pieces)
            for i in range(b.pieces):
                used += per
                log.info(t, "MemoryStore", f"Block broadcast_{b.id}_piece{i} stored as bytes in memory (estimated "
                         f"size {bytes_to_string(per)}, free {bytes_to_string(dmem - used)})")
                log.info(t, "BlockManagerInfo", f"Added broadcast_{b.id}_piece{i} in memory on {self.am}:"
                         f"{run.driver_bm_port} (size: {bytes_to_string(per)}, free: {bytes_to_string(dmem - used)})")
            log.info(t, "SparkContext", f"Created broadcast {b.id} from {b.callsite}")
        for t, kind, p in self.echo:
            if kind == "bc_piece":
                bid, i, size, ex, free = p
                log.info(t, "BlockManagerInfo", f"Added broadcast_{bid}_piece{i} in memory on {ex.host.name}:"
                         f"{ex.block_manager_port} (size: {bytes_to_string(size)}, free: {bytes_to_string(free)})")
            elif kind == "map_output_ask":
                sid, ex = p
                log.info(t, "MapOutputTrackerMasterEndpoint", f"Asked to send map output locations for shuffle "
                         f"{sid} to {ex.host.ip}:{39000 + int(ex.id) * 7 % 20000}")

    def jobs(self) -> None:
        run, log, r = self.run, self.log, self.rng
        props_len = sum(len(k) + len(v) + 4 for k, v in run.base_props.items())
        done: dict[tuple[int, int], int] = {}
        running: set[int] = set()
        for t, kind, obj in run.log:
            if kind == "job_start":
                job = obj
                st = run.stages[job.final_stage]
                map_job = st.task_type == "ShuffleMapTask"
                if map_job and st.exchange is not None:
                    xch = run.exchanges[st.exchange]
                    log.info(t, "DAGScheduler", f"Registering RDD {st.rdds[0].rdd_id} ({st.rdds[0].callsite}) as "
                             f"input to shuffle {xch.shuffle_id}")
                what = "Got map stage job" if map_job else "Got job"
                log.info(t, "DAGScheduler", f"{what} {job.job_id} ({st.name}) with {st.num_tasks} output partitions")
                log.info(t, "DAGScheduler", f"Final stage: {_stage_str(st)} ({st.name})")
                parents = ", ".join(_stage_str(run.stages[p]) for p in st.parent_ids)
                log.info(t, "DAGScheduler", f"Parents of final stage: List({parents})")
                log.info(t, "DAGScheduler", "Missing parents: List()")
            elif kind == "stage_submitted":
                st = obj
                log.info(t, "DAGScheduler", f"Submitting {_stage_str(st)} ({_rdd_str(st)}), which has no missing "
                         "parents")
                log.info(t + 1, "DAGScheduler", f"Submitting {st.num_tasks} missing tasks from {_stage_str(st)} "
                         f"({_rdd_str(st)}) (first 15 tasks are for partitions {_vector(st.num_tasks)})")
                log.info(t + 1, "YarnClusterScheduler", f"Adding task set {st.stage_id}.{st.attempt} with "
                         f"{st.num_tasks} tasks resource profile 0")
                running.add(st.stage_id)
            elif kind == "task_start":
                tk = obj
                st = run.stages[tk.stage_id]
                plan = st.plans[tk.index]
                files = plan.scan.files if plan.scan is not None else ()
                size = 4_700 + props_len + sum(len(pf.file.path) + 64 for pf in files) + 40 * len(plan.reads)
                log.info(t, "TaskSetManager", f"Starting task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}."
                         f"{tk.attempt} (TID {tk.task_id}) ({tk.executor.host.name}, executor {tk.executor.id}, "
                         f"partition {tk.index}, {tk.locality}, {size} bytes) ")
            elif kind == "task_end":
                tk = obj
                if not tk.ok:
                    continue
                st = run.stages[tk.stage_id]
                key = (tk.stage_id, tk.attempt)
                done[key] = done.get(key, 0) + 1
                log.info(t, "TaskSetManager", f"Finished task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}."
                         f"{tk.attempt} (TID {tk.task_id}) in {tk.finish - tk.launch} ms on {tk.executor.host.name} "
                         f"(executor {tk.executor.id}) ({done[key]}/{st.num_tasks})")
                if done[key] == st.num_tasks:
                    log.info(t, "YarnClusterScheduler", f"Removed TaskSet {tk.stage_id}.{tk.attempt}, whose tasks have "
                             "all completed, from pool ")
            elif kind == "stage_completed":
                st = obj
                running.discard(st.stage_id)
                secs = (st.completed - st.submitted) / 1000
                log.info(t, "DAGScheduler", f"{_stage_str(st)} ({st.name}) finished in {secs:.3f} s")
                if st.task_type == "ShuffleMapTask":
                    log.info(t, "DAGScheduler", "looking for newly runnable stages")
                    log.info(t, "DAGScheduler", "running: " + _set(run, running))
                    log.info(t, "DAGScheduler", "waiting: Set()")
                    log.info(t, "DAGScheduler", "failed: Set()")
            elif kind == "job_end":
                job = obj
                st = run.stages[job.final_stage]
                if st.task_type == "ResultTask":
                    log.info(t, "DAGScheduler", f"Job {job.job_id} is finished. Cancelling potential speculative or "
                             "zombie tasks for this job")
                    log.info(t, "YarnClusterScheduler", f"Killing all running tasks in stage {st.stage_id}: Stage "
                             "finished")
                    took = (t - job.submitted) / 1000 + r.uniform(0.0002, 0.004)
                    log.info(t, "DAGScheduler", f"Job {job.job_id} finished: {st.name}, took {took:.6f} s")
                    if st.spec is not None and type(st.spec.sink).__name__ == "WriteSink":
                        self._write_commit(t, st)

    def _write_commit(self, t: int, st: StageRecord) -> None:
        run, log, r = self.run, self.log, self.rng
        # WriteJobDescription.uuid: its own UUID (files and the staging dir use the commit protocol's job id)
        job_uuid = _uuid(substream(run.spec.seed, f"yarnlogs/write-job/{st.stage_id}"))
        q = next(q for q in run.queries if q.end is not None and q.start <= t <= q.end)
        commit = max(1, q.end - t - r.randint(3, 40))
        log.info(t + 1, "FileFormatWriter", f"Start to commit write Job {job_uuid}.")
        log.info(t + commit - 2, "FileFormatWriter", f"Write Job {job_uuid} committed. Elapsed time: {commit - 3} ms.")
        log.info(t + commit, "FileFormatWriter", f"Finished processing stats for write job {job_uuid}.")

    def queries(self) -> None:
        run, log, r = self.run, self.log, self.rng
        seen: set[str] = set()
        for _t, kind, obj in run.log:
            if kind == "stage_submitted":
                st = obj
                for src in st.spec.sources if st.spec is not None else ():
                    ds = getattr(src, "dataset", None)
                    if ds is not None and ds.path not in seen and ds.path in run.scan_split_bytes:
                        seen.add(ds.path)
                        ms, oc = run.scan_split_bytes[ds.path]
                        log.info(st.submitted - r.randint(30, 160), "FileSourceScanExec",
                                 f"Planning scan with bin packing, max size: {ms} bytes, open cost is considered as "
                                 f"scanning {oc} bytes.")

    # ------------------------------------------------------------------ shutdown

    def shutdown(self) -> None:
        run, log, r = self.run, self.log, self.rng
        t = run.app_end
        log.info(t, "SparkContext", "SparkContext is stopping with exitCode 0.")
        log.info(t + r.randint(5, 30), "SparkUI", f"Stopped Spark web UI at http://{self.am}:{self.ui_port}")
        t += r.randint(30, 60)
        log.info(t, "YarnClusterSchedulerBackend", "Shutting down all executors")
        log.info(t, DRIVER_EP, "Asking each executor to shut down")
        t += r.randint(40, 120)
        log.info(t, "MapOutputTrackerMasterEndpoint", "MapOutputTrackerMasterEndpoint stopped!")
        log.info(t + 5, "MemoryStore", "MemoryStore cleared")
        log.info(t + 5, "BlockManager", "BlockManager stopped")
        log.info(t + 7, "BlockManagerMaster", "BlockManagerMaster stopped")
        log.info(t + 9, "OutputCommitCoordinator$OutputCommitCoordinatorEndpoint", "OutputCommitCoordinator stopped!")
        t += r.randint(30, 90)
        log.info(t, "SparkContext", "Successfully stopped SparkContext")
        t += r.randint(80, 300)
        log.info(t, "ApplicationMaster", "Final app status: SUCCEEDED, exitCode: 0")
        log.info(t + 3, "ApplicationMaster", "Unregistering ApplicationMaster with SUCCEEDED")
        log.info(t + 15, "AMRMClientImpl", "Waiting for application to be successfully unregistered.")
        t += r.randint(100, 250)
        s = run.spec
        log.info(t, "ApplicationMaster", f"Deleting staging directory hdfs://{s.cluster.namenode}:8020/user/{s.user}/"
                 f".sparkStaging/{run.yapp.app_id}")
        log.info(t + 20, "ShutdownHookManager", "Shutdown hook called")
        work = container_work_dir(local_dirs(run)[0], s.user, run.yapp, run.yapp.am_container_id)
        log.info(t + 21, "ShutdownHookManager", f"Deleting directory {work}/tmp/spark-{_uuid(r)}")
        log.info(t + 22, "ShutdownHookManager",
                 f"Deleting directory {local_dirs(run)[0]}/usercache/{s.user}/appcache/{run.yapp.app_id}/"
                 f"spark-{_uuid(r)}")


def _set(run: AppRun, stage_ids: set[int]) -> str:
    return "Set(" + ", ".join(_stage_str(run.stages[s]) for s in sorted(stage_ids)) + ")"


def run_epoch_at(run: AppRun, t: int) -> int:
    """The MapOutputTracker epoch at time t: finished map stages + executors lost before t."""
    maps = sum(1 for st in run.stages.values() if st.task_type == "ShuffleMapTask" and st.completed is not None
               and st.completed <= t and not st.skipped)
    lost = sum(1 for e in run.executors if e.removed_at is not None and e.removed_at < t)
    return maps + lost


def am_stderr(run: AppRun, echo: list | None = None) -> str:
    d = _Driver(run, echo or [])
    d.startup()
    d.yarn()
    d.executors()
    d.queries()
    d.broadcasts()
    d.jobs()
    d.shutdown()
    return d.log.text()


def am_stdout(run: AppRun) -> str:
    """The Python driver's own output. The scenario scripts print nothing."""
    return ""
