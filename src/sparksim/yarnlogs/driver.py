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


def _task_str(tk) -> str:
    """`ShuffleMapTask.toString` / `ResultTask.toString`."""
    return f"{tk.task_type}({tk.stage_id}, {tk.partition})"


def _rdd_str(st: StageRecord) -> str:
    r = st.rdds[0]
    return f"{r.name}[{r.rdd_id}] at {r.callsite}"


def _vector(n: int, parts: list[int] | None = None) -> str:
    parts = list(range(n)) if parts is None else parts
    return "Vector(" + ", ".join(str(i) for i in parts[:15]) + ")"


def _launch_context(run: AppRun) -> str:
    s = run.spec
    app = run.yapp.app_id
    nn = f"hdfs://{s.cluster.namenode}:8020"
    staging = f"{nn}/user/{s.user}/.sparkStaging/{app}"
    opts = run.base_props.get("spark.executor.extraJavaOptions", "").split()
    lib = run.base_props.get("spark.executor.extraLibraryPath")
    cmd = [f'LD_LIBRARY_PATH=\\"{lib}:$LD_LIBRARY_PATH\\"'] if lib else []  # ExecutorRunnable.prepareCommand
    cmd += ["{{JAVA_HOME}}/bin/java", "-server", f"-Xmx{s.sizing.memory_mb}m", *[f"'{o}'" for o in opts],
            "-Djava.io.tmpdir={{PWD}}/tmp", f"'-Dspark.driver.port={run.env_app.driver_port}'", "'-Dspark.ui.port=0'"]
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
    for path, size in s.dist_resources:  # --jars / --files already on HDFS: localized from where they are
        rel = "/" + path.split("://", 1)[-1].split("/", 1)[-1]
        uploaded = s.cluster.cluster_timestamp + substream(s.seed, f"dist/{path}").randint(1, 9) * 86_400_000
        resources.append(res(rel.rsplit("/", 1)[-1], rel, size, "FILE", "PRIVATE", uploaded))
    env = [
        # with-Hadoop Spark build: spark.yarn.populateHadoopClasspath is false (as in the reference AM logs)
        "    CLASSPATH -> {{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>"
        "{{PWD}}/__spark_conf__/__hadoop_conf__",
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
        if run.speculation.enabled:  # TaskSchedulerImpl.start (ref09)
            log.info(t, "YarnClusterScheduler", "Starting speculative execution thread")
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
        if run.excluded_nodes:  # YarnAllocatorNodeHealthTracker, created with the allocator: spark.yarn.exclude.nodes
            log.info(t, "YarnAllocatorNodeHealthTracker", "adding nodes to YARN application master's excluded node "
                     f"list: List({', '.join(run.excluded_nodes)})")
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
            elif kind in ("driver_disconnected", "dag_executor_lost"):
                ex = p
                if kind == "driver_disconnected":
                    log.info(t, DRIVER_EP, f"Disabling executor {ex.id}.")
                epoch = ex.extra["loss_epoch"] if "loss_epoch" in ex.extra else run_epoch_at(run, t)
                log.info(t, "DAGScheduler", f"Executor lost: {ex.id} (epoch {epoch})")
                bmid = f"BlockManagerId({ex.id}, {ex.host.name}, {ex.block_manager_port}, None)"
                log.info(t, "BlockManagerMasterEndpoint", f"Trying to remove executor {ex.id} from "
                         "BlockManagerMaster.")
                log.info(t, "BlockManagerMasterEndpoint", f"Removing block manager {bmid}")
                log.info(t, "BlockManagerMaster", f"Removed {ex.id} successfully in removeExecutor")
            elif kind == "shuffle_files_lost":  # removeExecutorAndUnregisterOutputs(fileLost = true)
                ex, epoch, _n = p
                log.info(t, "DAGScheduler", f"Shuffle files lost for executor: {ex.id} (epoch {epoch})")
            elif kind == "resubmitted":  # DAGScheduler.handleTaskCompletion(Resubmitted)
                tk = p
                log.info(t, "DAGScheduler", f"Resubmitted {_task_str(tk)}, so marking it as still running.")
            elif kind == "fetch_failed":
                tk, reason, first, latest, st, map_base = p
                bm = reason.block_manager_address
                name = f"task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}.{tk.attempt} (TID {tk.task_id})"
                log.warn(t, "TaskSetManager", f"Lost {name} ({tk.executor.host.name} executor {tk.executor.id}): "
                         f"FetchFailed(BlockManagerId({bm.executor_id}, {bm.host}, {bm.port}, None), shuffleId="
                         f"{reason.shuffle_id}, mapIndex={reason.map_index}, mapId={reason.map_id}, reduceId="
                         f"{reason.reduce_id}, message=\n{reason.message}\n)")
                if not latest:
                    log.info(t, "DAGScheduler", f"Ignoring fetch failure from {_task_str(tk)} as it's from "
                             f"{_stage_str(st)} attempt {st.attempt} and there is a more recent attempt for that "
                             f"stage (attempt {st.root.next_attempt - 1}) running")
                elif first:
                    log.info(t, "DAGScheduler", f"Marking {_stage_str(st)} ({st.name}) as failed due to a fetch "
                             f"failure from {_stage_str(map_base)} ({map_base.name})")
                log.info(t, "TaskSetManager", f"{name} failed, but the task will not be re-executed (either because "
                         "the task failed with a shuffle data fetch failure, so the previous stage needs to be re-run, "
                         "or because a different copy of the task has already succeeded).")
                if first:
                    log.info(st.completed + 1, "DAGScheduler", f"Resubmitting {_stage_str(map_base)} ({map_base.name}) "
                             f"and {_stage_str(st)} ({st.name}) due to fetch failure")
            elif kind == "resubmit_failed_stages":
                log.info(t, "DAGScheduler", "Resubmitting failed stages")
            elif kind == "map_stage_missing":  # processShuffleMapStageCompletion: outputs lost meanwhile
                base, missing = p
                log.info(t, "DAGScheduler", f"Resubmitting {_stage_str(base)} ({base.name}) because some of its tasks "
                         f"had failed: {', '.join(str(m) for m in missing)}")
            elif kind == "executor_lost":
                ex, reason, by_app, released, lost = p
                if released:
                    log.info(t, "YarnClusterScheduler", f"Executor {ex.id} on {ex.host.name} killed by driver.")
                else:
                    log.error(t, "YarnClusterScheduler", f"Lost executor {ex.id} on {ex.host.name}: {reason}")
                for a in lost:
                    tk = run.task_records[a.task_id]
                    if not by_app and not run.ess_enabled:  # handleFailedTask: not counted towards maxFailures
                        log.info(t, "TaskSetManager", f"task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}."
                                 f"{tk.attempt} (TID {tk.task_id}) failed because while it was being computed, its "
                                 "executor exited for a reason unrelated to the task. Not counting this failure "
                                 "towards the maximum number of failures for the task.")
                        continue
                    why = ("caused by one of the running tasks" if by_app else "unrelated to the running tasks")
                    log.warn(t, "TaskSetManager", f"Lost task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}."
                             f"{tk.attempt} (TID {tk.task_id}) ({ex.host.name} executor {ex.id}): ExecutorLostFailure "
                             f"(executor {ex.id} exited {why}) Reason: {reason}")
                    if run.stage_rec(tk.stage_id, tk.attempt).abort_tid == tk.task_id:
                        log.error(t, "TaskSetManager", f"Task {tk.index} in stage {tk.stage_id}.{tk.attempt} failed "
                                  f"{run.sched.max_failures} times; aborting job")
                if run.dynalloc is not None:
                    st = run.dynalloc.removed_stats
                    log.info(t + 1, "ExecutorMonitor", f"Executor {ex.id} is removed. Remove reason statistics: ("
                             f"gracefully decommissioned: 0, decommision unfinished: 0, driver killed: "
                             f"{st['driver killed']}, unexpectedly exited: {st['unexpectedly exited']}).")
                if not released:
                    log.info(t + 2, DRIVER_EP, f"Asked to remove non-existent executor {ex.id}")
            elif kind == "stage_cancelled":  # TaskSchedulerImpl.cancelTasks
                st, why = p
                log.info(t, "YarnClusterScheduler", f"Cancelling stage {st.stage_id}")
                log.info(t, "YarnClusterScheduler", f"Killing all running tasks in stage {st.stage_id}: Stage "
                         f"cancelled: {why}")
                log.info(t, "YarnClusterScheduler", f"Stage {st.stage_id} was cancelled")
            elif kind == "job_cancel_asked":
                log.info(t, "DAGScheduler", f"Asked to cancel job {p.job_id}")
            elif kind == "taskset_removed":  # an aborted task set's last running attempt has ended
                log.info(t, "YarnClusterScheduler", f"Removed TaskSet {p.stage_id}.{p.attempt}, whose tasks have "
                         "all completed, from pool ")
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
        # The TaskSet leaves the pool once it is a zombie (every task succeeded) and its last attempt, maybe a
        # killed one, has reported: the last task_end of each stage attempt.
        last_end: dict[tuple[int, int], int] = {}
        for i, (_t, kind, obj) in enumerate(run.log):
            if kind == "task_end":
                last_end[(obj.stage_id, obj.attempt)] = i
        for t, sid, satt, index, host, threshold, n in run.sched.speculation_log:  # checkSpeculatableTasks
            log.info(t, "TaskSetManager", f"Marking task {index} in stage {sid}.{satt} (on {host}) as speculatable "
                     f"because it ran more than {threshold:.0f} ms({n} speculatable tasks in this taskset now)")
        losers: dict[int, list] = {}  # winner task id -> the attempts its success killed
        for tk in run.task_records.values():
            if tk.killed is not None:
                st = run.stage_rec(tk.stage_id, tk.attempt)
                winner = next((w for w in st.tasks if w.index == tk.index and w.ok), None)
                if winner is not None and any(rq == "another attempt succeeded" for _, rq in tk.kill_reqs):
                    losers.setdefault(winner.task_id, []).append(tk)
        for i, (t, kind, obj) in enumerate(run.log):
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
                missing = ", ".join(_stage_str(run.stages[p]) for p in job.missing_parents)
                log.info(t, "DAGScheduler", f"Missing parents: List({missing})")
            elif kind == "stage_submitted":
                st = obj
                log.info(t, "DAGScheduler", f"Submitting {_stage_str(st)} ({_rdd_str(st)}), which has no missing "
                         "parents")
                log.info(t + 1, "DAGScheduler", f"Submitting {st.num_tasks} missing tasks from {_stage_str(st)} "
                         f"({_rdd_str(st)}) (first 15 tasks are for partitions {_vector(st.num_tasks, st.partitions)})")
                log.info(t + 1, "YarnClusterScheduler", f"Adding task set {st.stage_id}.{st.attempt} with "
                         f"{st.num_tasks} tasks resource profile 0")
                running.add(st.stage_id)
            elif kind == "task_start":
                tk = obj
                st = run.stage_rec(tk.stage_id, tk.attempt)
                plan = st.plans[tk.partition]
                files = plan.scan.files if plan.scan is not None else ()
                size = 4_700 + props_len + sum(len(pf.file.path) + 64 for pf in files) + 40 * len(plan.reads)
                log.info(t, "TaskSetManager", f"Starting task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}."
                         f"{tk.attempt} (TID {tk.task_id}) ({tk.executor.host.name}, executor {tk.executor.id}, "
                         f"partition {tk.partition}, {tk.locality}, {size} bytes) ")
            elif kind == "task_end":
                tk = obj
                st = run.stage_rec(tk.stage_id, tk.attempt)
                key = (tk.stage_id, tk.attempt)
                if tk.killed is not None:  # handleFailedTask(TaskKilled)
                    name = f"task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}.{tk.attempt} (TID {tk.task_id})"
                    log.warn(t, "TaskSetManager", f"Lost {name} ({tk.executor.host.name} executor {tk.executor.id}): "
                             f"TaskKilled ({tk.killed})")
                    winner = st.by_index.get(tk.index)
                    if winner is not None and winner.finish <= tk.finish:  # its index already succeeded
                        log.info(t, "TaskSetManager", f"{name} failed, but the task will not be re-executed (either "
                                 "because the task failed with a shuffle data fetch failure, so the previous stage "
                                 "needs to be re-run, or because a different copy of the task has already "
                                 "succeeded).")
                elif tk.ok:
                    for lost in sorted(losers.get(tk.task_id, ()), key=lambda x: x.task_id):
                        log.info(t, "TaskSetManager", f"Killing attempt {lost.task_attempt} for task {lost.index}."
                                 f"{lost.task_attempt} in stage {lost.stage_id}.{lost.attempt} (TID {lost.task_id}) on "
                                 f"{lost.executor.host.name} as the attempt {tk.task_attempt} succeeded on "
                                 f"{tk.executor.host.name}")
                    done[key] = done.get(key, 0) + 1
                    log.info(t, "TaskSetManager", f"Finished task {tk.index}.{tk.task_attempt} in stage "
                             f"{tk.stage_id}.{tk.attempt} (TID {tk.task_id}) in {tk.finish - tk.launch} ms on "
                             f"{tk.executor.host.name} (executor {tk.executor.id}) ({done[key]}/{st.num_tasks})")
                elif not st.fetch_failed:
                    continue  # ExecutorLostFailure: logged with the executor loss (yarn())
                if last_end.get(key) == i and (st.fetch_failed or (done.get(key, 0) == st.num_tasks
                                                                    and st.failure is None)):
                    log.info(t, "YarnClusterScheduler", f"Removed TaskSet {tk.stage_id}.{tk.attempt}, whose tasks have "
                             "all completed, from pool ")
            elif kind == "stage_completed":
                st = obj
                running.discard(st.stage_id)
                secs = (st.completed - st.submitted) / 1000
                if st.failure is not None:  # markStageAsFinished(stage, Some(reason))
                    log.info(t, "DAGScheduler", f"{_stage_str(st)} ({st.name}) failed in {secs:.3f} s due to "
                             f"{st.failure}")
                    continue
                log.info(t, "DAGScheduler", f"{_stage_str(st)} ({st.name}) finished in {secs:.3f} s")
                if st.task_type == "ShuffleMapTask":
                    log.info(t, "DAGScheduler", "looking for newly runnable stages")
                    log.info(t, "DAGScheduler", "running: " + _set(run, running))
                    log.info(t, "DAGScheduler", "waiting: Set()")
                    log.info(t, "DAGScheduler", "failed: Set()")
            elif kind == "job_end":
                job = obj
                st = run.attempts_of(run.stages[job.final_stage])[-1]
                if job.failure is not None:  # DAGScheduler.runJob's caller logs it (result stages only)
                    if st.task_type == "ResultTask" and not job.cancelled:
                        took = (t - job.submitted) / 1000 + r.uniform(0.0002, 0.004)
                        log.info(t, "DAGScheduler", f"Job {job.job_id} failed: {st.name}, took {took:.6f} s")
                    continue
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
        rp = substream(run.spec.seed, "yarnlogs/scan-planning")  # its own stream: the other lines keep theirs
        for q in run.queries:
            self._scan_planning(q, rp)
        seen: set[str] = set()
        for _t, kind, obj in run.log:
            if kind == "stage_submitted":
                st = obj
                for src in st.spec.sources if st.spec is not None else ():
                    ds = getattr(src, "dataset", None)
                    if ds is not None and ds.path not in seen and ds.path in run.scan_split_bytes:
                        seen.add(ds.path)
                        ms, oc = run.scan_split_bytes[ds.path]
                        t = st.submitted - r.randint(30, 160)
                        scan = getattr(src, "node", None)
                        if scan is not None and scan.partition_filters:
                            self._partitions_selected(t - rp.randint(1, 4), scan)
                        log.info(t, "FileSourceScanExec",
                                 f"Planning scan with bin packing, max size: {ms} bytes, open cost is considered as "
                                 f"scanning {oc} bytes.")

    def _scan_planning(self, q, rp: random.Random) -> None:
        """What the driver logs while it reads a query's tables and plans it (Spark 3.5.9 at INFO, checked on a
        local run): one listing line per `spark.read` (the InMemoryFileIndex lists every leaf file under the path,
        `FILE_LISTING_MS_PER_FILE`); then FileSourceStrategy's lines for each scan, in planning order."""
        from ..cost import calibration as cal

        log = self.log
        t = q.planned_at
        for _scan, files in q.scans:
            took = max(1, int(files * cal.FILE_LISTING_MS_PER_FILE))
            t += took
            log.info(t, "InMemoryFileIndex", f"It took {took} ms to list leaf files for 1 paths.")
        t = max(t, q.start - rp.randint(20, 60))
        for scan, _ in q.scans:
            if scan.partitioned:  # DataSourceStrategy.getPushedDownFilters
                log.info(t, "DataSourceStrategy", "Pruning directories with: "
                         + ",".join(str(f) for f in scan.partition_filters))
            log.info(t, "FileSourceStrategy", "Pushed Filters: " + ",".join(scan.pushed_filters))
            log.info(t, "FileSourceStrategy", "Post-Scan Filters: " + ",".join(str(f) for f in scan.data_filters))
            t += rp.randint(0, 2)

    def _partitions_selected(self, t: int, scan) -> None:
        """PartitioningAwareFileIndex.prunePartitions (only with partition filters): selected out of listed."""
        run = self.run
        total = len(run.hdfs.partition_dirs(scan.relation.root_paths[0]))
        selected = len(run.selected_partitions(scan))
        pruned = "0" if total == 0 else f"{_java_double((1 - selected / total) * 100)}%"
        self.log.info(t, "InMemoryFileIndex", f"Selected {selected} partitions out of {total}, pruned {pruned} "
                      "partitions.")

    # ------------------------------------------------------------------ shutdown

    def shutdown(self) -> None:
        run, log, r = self.run, self.log, self.rng
        t = run.app_end
        failed = run.final_status != "SUCCEEDED"
        if failed:  # the Py4JJavaError ended the Python driver; the shutdown hook stops the SparkContext (ref10)
            t_exit = next(tt for tt, kind, _ in run.yarn_log if kind == "user_app_exited")
            why = f"User application exited with status {run.exit_code}"
            log.error(t_exit, "ApplicationMaster", why)
            log.info(t_exit, "ApplicationMaster", f"Final app status: FAILED, exitCode: {run.exit_code}, (reason: "
                     f"{why})")
            log.info(t, "SparkContext", "Invoking stop() from shutdown hook")
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
        s = run.spec
        staging = (f"Deleting staging directory hdfs://{s.cluster.namenode}:8020/user/{s.user}/.sparkStaging/"
                   f"{run.yapp.app_id}")
        if failed:  # only the last attempt cleans up and unregisters; otherwise YARN starts the next one
            t += r.randint(1, 6)
            if run.last_attempt:
                log.info(t, "ApplicationMaster", staging)
                log.info(t + r.randint(4, 12), "ApplicationMaster", f"Unregistering ApplicationMaster with FAILED "
                         f"(diag message: {why})")
                log.info(t + r.randint(14, 30), "AMRMClientImpl", "Waiting for application to be successfully "
                         "unregistered.")
                t += r.randint(100, 250)
        else:
            t += r.randint(80, 300)
            log.info(t, "ApplicationMaster", "Final app status: SUCCEEDED, exitCode: 0")
            log.info(t + 3, "ApplicationMaster", "Unregistering ApplicationMaster with SUCCEEDED")
            log.info(t + 15, "AMRMClientImpl", "Waiting for application to be successfully unregistered.")
            t += r.randint(100, 250)
            log.info(t, "ApplicationMaster", staging)
        log.info(t + 20, "ShutdownHookManager", "Shutdown hook called")
        work = container_work_dir(local_dirs(run)[0], s.user, run.yapp, run.yapp.am_container_id)
        log.info(t + 21, "ShutdownHookManager", f"Deleting directory {work}/tmp/spark-{_uuid(r)}")
        log.info(t + 22, "ShutdownHookManager",
                 f"Deleting directory {local_dirs(run)[0]}/usercache/{s.user}/appcache/{run.yapp.app_id}/"
                 f"spark-{_uuid(r)}")


def _java_double(x: float) -> str:
    """`Double.toString` of a percentage between 0 and 100 (the same shortest round trip as Python's repr)."""
    return repr(float(x))


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


# PySpark 3.5.9's frames between the script's action and the Py4J error (ref10's AM stdout).
_PY_FRAMES = [
    ("pyspark.zip/pyspark/sql/readwriter.py", 1721, "parquet"),
    ("py4j-0.10.9.7-src.zip/py4j/java_gateway.py", 1322, "__call__"),
    ("pyspark.zip/pyspark/errors/exceptions/captured.py", 179, "deco"),
    ("py4j-0.10.9.7-src.zip/py4j/protocol.py", 326, "get_return_value"),
]


def am_stdout(run: AppRun) -> str:
    """The Python driver's own output: the scenario scripts print nothing, but an action that fails raises a
    Py4JJavaError nobody catches, and Python prints its traceback before it exits with status 1 (the AM's
    PythonRunner copies the process's output to the container's stdout; ref10). The Java part is the
    SparkException's toString and its stack trace: the DAGScheduler frames of a map-stage job."""
    from ..engine.callsites import ABORT_STAGE_STACK, java_frame

    q = run.failed_query
    if q is None:
        return ""
    spec = run.spec.queries[q.execution_id]
    work = container_work_dir(local_dirs(run)[0], run.spec.user, run.yapp, run.yapp.am_container_id)
    # Py4J numbers the JVM objects the script got back: the DataFrameWriter of a query's write
    obj = 120 + 47 * q.execution_id + substream(run.spec.seed, "py4j-ids").randint(0, 12)
    lines = ["Traceback (most recent call last):",
             f'  File "{work}/{run.spec.script}", line {spec.source_line}, in <module>',
             f"    {spec.source}"]
    lines += [f'  File "{work}/{path}", line {n}, in {fn}' for path, n, fn in _PY_FRAMES]
    lines.append(f"py4j.protocol.Py4JJavaError: An error occurred while calling o{obj}.{spec.action}.")
    lines.append(f": org.apache.spark.SparkException: {q.error}")
    lines += [f"\tat {java_frame(*f)}" for f in ABORT_STAGE_STACK]
    return "\n".join(lines) + "\n\n"
