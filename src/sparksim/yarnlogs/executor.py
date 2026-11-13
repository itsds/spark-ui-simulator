"""An executor container's stderr: what `YarnCoarseGrainedExecutorBackend` and its task threads log.

Every line is derived from what the simulation says this executor did: its container launch and
registration, the tasks it ran (ids, attempts, timings, the file splits a scan read, the shuffle blocks
a reduce task fetched, the files a write task produced, spills), the broadcasts it fetched, and how it
ended (driver shutdown, or SIGTERM from its NodeManager). Message texts follow the real Spark 3.5.9
executor logs in `reference/yarn-logs` and the Spark source; the order inside one task follows the code
path (deserialize → broadcast fetch → codegen → read → write → commit → result).
"""

from __future__ import annotations

import heapq
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

from ..cost import calibration as cal
from ..cost.model import page_size
from ..model.workload import ScanSource, ShuffleSource, WriteSink
from ..rng import substream
from ..sql import nodes as sqln
from ..yarn import container_work_dir
from .log4j import ContainerLog, bytes_to_string

if TYPE_CHECKING:
    from ..engine.runtime import AppRun, StageRecord, TaskRecord
    from ..model.cluster import Executor

MiB = 1024 * 1024
ESS_PORT = 7337
OS_VERSION = "5.15.0-1068-generic"
JAVA_VERSION = "17.0.20.1"
EPOCH_DAY = date(1970, 1, 1)


@dataclass
class _State:
    """What the executor JVM has cached so far."""

    epoch: int = 0
    stages: set[int] = field(default_factory=set)
    broadcasts: set[int] = field(default_factory=set)
    shuffles: set[int] = field(default_factory=set)  # map statuses fetched in the current epoch
    storage: int = 0  # MemoryStore bytes held by broadcast blocks
    decompressors: int = 0
    compressors: int = 0
    connections: set[str] = field(default_factory=set)
    echo: list | None = None  # what the driver logs in reaction (block updates, map status requests)
    threads: dict[int, int] = field(default_factory=dict)  # task id → id of the task launch thread running it


def _hex(rng: random.Random, n: int = 8) -> str:
    return f"{rng.getrandbits(4 * n):0{n}x}"


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def local_dirs(run: AppRun) -> list[str]:
    return [d.strip() for d in run.nm_local_dirs.split(",")]


# --------------------------------------------------------------------------- start-up


def _startup(log: ContainerLog, run: AppRun, ex: Executor, rng: random.Random) -> None:
    spec = run.spec
    user = spec.user
    span = max(800, ex.registered_at - ex.launched_at)
    t = ex.launched_at + int(span * 0.42)
    log.info(t, "CoarseGrainedExecutorBackend", f"Started daemon with process name: {ex.pid}@{ex.host.name}")
    for sig in ("TERM", "HUP", "INT"):
        log.info(t, "SignalUtils", f"Registering signal handler for {sig}")
    t += int(span * 0.18)
    for _ in range(2):
        _security_manager(log, t, user)
        t += rng.randint(20, 160)
        log.info(t, "TransportClientFactory", f"Successfully created connection to {run.am_host.name}/"
                 f"{run.am_host.ip}:{run.env_app.driver_port} after {rng.randint(2, 40)} ms (0 ms spent in bootstraps)")
        t += rng.randint(15, 90)
    for d in local_dirs(run):
        log.info(t, "DiskBlockManager", f"Created local directory at {d}/usercache/{user}/appcache/"
                 f"{run.yapp.app_id}/blockmgr-{_uuid(rng)}")
    log.info(t, "MemoryStore", f"MemoryStore started with capacity {bytes_to_string(ex.max_memory)}")
    t = max(t + 1, ex.registered_at - rng.randint(60, 200))
    log.info(t, "YarnCoarseGrainedExecutorBackend",
             f"Connecting to driver: spark://CoarseGrainedScheduler@{run.am_host.name}:{run.env_app.driver_port}")
    for msg in ("=" * 62, "No custom resources configured for spark.executor.", "=" * 62):
        log.info(t, "ResourceUtils", msg)
    t = ex.registered_at
    log.info(t, "YarnCoarseGrainedExecutorBackend", "Successfully registered with driver")
    log.info(t, "Executor", f"Starting executor ID {ex.id} on host {ex.host.name}")
    log.info(t, "Executor", f"OS info Linux, {OS_VERSION}, amd64")
    log.info(t, "Executor", f"Java version {JAVA_VERSION}")
    bm = f"BlockManagerId({ex.id}, {ex.host.name}, {ex.block_manager_port}, None)"
    log.info(t, "Utils", "Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' "
             f"on port {ex.block_manager_port}.")
    log.info(t, "NettyBlockTransferService", f"Server created on {ex.host.name}:{ex.block_manager_port}")
    log.info(t, "BlockManager", "Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication "
             "policy")
    if run.ess_enabled:
        log.info(t, "BlockManager", f"external shuffle service port = {ESS_PORT}")
        log.info(t + rng.randint(5, 40), "BlockManager", "Registering executor with local external shuffle service.")
    t = ex.block_manager_at
    log.info(t, "BlockManagerMaster", f"Registering BlockManager {bm}")
    log.info(t, "BlockManagerMaster", f"Registered BlockManager {bm}")
    log.info(t, "BlockManager", f"Initialized BlockManager: {bm}")
    work = container_work_dir(local_dirs(run)[0], user, run.yapp, ex.container_id)
    log.info(t, "Executor", "Starting executor with user classpath (userClassPathFirst = false): "
             f"'file:{work}/__app__.jar,file:{work}/__app__.jar'")
    log.info(t, "Executor", f"Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@"
             f"{_hex(rng)} for default.")


def _security_manager(log: ContainerLog, t: int, user: str) -> None:
    log.info(t, "SecurityManager", f"Changing view acls to: {user}")
    log.info(t, "SecurityManager", f"Changing modify acls to: {user}")
    log.info(t, "SecurityManager", "Changing view acls groups to: ")
    log.info(t, "SecurityManager", "Changing modify acls groups to: ")
    log.info(t, "SecurityManager", "SecurityManager: authentication disabled; ui acls disabled; users with view "
             f"permissions: {user}; groups with view permissions: EMPTY; users with modify permissions: {user}; "
             "groups with modify permissions: EMPTY")


# --------------------------------------------------------------------------- broadcasts, map statuses


def _read_broadcast(log: ContainerLog, run: AppRun, ex: Executor, st: _State, bid: int, t: int,
                    rng: random.Random) -> int:
    """TorrentBroadcast.readBroadcastBlock: fetch the pieces, store them and the value. Returns end time."""
    if bid in st.broadcasts:
        return t
    st.broadcasts.add(bid)
    b = run.broadcasts[bid]
    log.info(t, "TorrentBroadcast", f"Started reading broadcast variable {bid} with {b.pieces} pieces "
             f"(estimated total size {bytes_to_string(b.pieces * 4 * MiB)})")
    peer = f"{run.am_host.name}/{run.am_host.ip}:{run.driver_bm_port}"
    if peer not in st.connections:
        st.connections.add(peer)
        log.info(t + 1, "TransportClientFactory", f"Successfully created connection to {peer} after "
                 f"{rng.randint(1, 12)} ms (0 ms spent in bootstraps)")
    took = rng.randint(4, 45) + int(b.piece_bytes / (60 * MiB) * 1e3)
    per_piece = max(1, b.piece_bytes // b.pieces)
    for i in range(b.pieces):
        st.storage += per_piece
        tp = t + took * (i + 1) // (b.pieces + 1)
        log.info(tp, "MemoryStore",
                 f"Block broadcast_{bid}_piece{i} stored as bytes in memory (estimated size "
                 f"{bytes_to_string(per_piece)}, free {bytes_to_string(ex.max_memory - st.storage)})")
        if st.echo is not None:  # pieces are reported to the BlockManagerMaster (tellMaster)
            st.echo.append((tp + 1, "bc_piece", (bid, i, per_piece, ex, ex.max_memory - st.storage)))
    log.info(t + took, "TorrentBroadcast", f"Reading broadcast variable {bid} took {took} ms")
    st.storage += b.value_bytes
    done = t + took + rng.randint(0, 3) + int(b.value_bytes / (400 * MiB) * 1e3)
    log.info(done, "MemoryStore", f"Block broadcast_{bid} stored as values in memory (estimated size "
             f"{bytes_to_string(b.value_bytes)}, free {bytes_to_string(ex.max_memory - st.storage)})")
    return done


def _map_statuses(log: ContainerLog, run: AppRun, ex: Executor, st: _State, shuffle_id: int, t: int) -> None:
    if shuffle_id in st.shuffles:
        return
    st.shuffles.add(shuffle_id)
    if st.echo is not None:
        st.echo.append((t + 1, "map_output_ask", (shuffle_id, ex)))
    log.info(t, "MapOutputTrackerWorker", f"Don't have map outputs for shuffle {shuffle_id}, fetching them")
    log.info(t, "MapOutputTrackerWorker", "Doing the fetch; tracker endpoint = NettyRpcEndpointRef("
             f"spark://MapOutputTracker@{run.am_host.name}:{run.env_app.driver_port})")
    log.info(t + 2, "MapOutputTrackerWorker", "Got the map output locations")


# --------------------------------------------------------------------------- one task


def parquet_predicate(pushed: list[str]) -> str | None:
    """ParquetFilters: Spark's pushed source filters as the Parquet FilterApi predicate (left-folded `and`)."""
    preds = []
    for f in pushed:
        if f.startswith("IsNotNull(") and f.endswith(")"):
            preds.append(f"noteq({f[10:-1]}, null)")
        elif f.startswith("Not(EqualTo(") and f.endswith("))"):
            col, val = f[12:-2].split(",", 1)
            preds.append(f"noteq({col}, {_parquet_value(val)})")
        elif f.startswith("EqualTo(") and f.endswith(")"):
            col, val = f[8:-1].split(",", 1)
            preds.append(f"eq({col}, {_parquet_value(val)})")
        elif f.startswith("IsNull(") and f.endswith(")"):
            preds.append(f"eq({f[7:-1]}, null)")
    if not preds:
        return None
    out = preds[0]
    for p in preds[1:]:
        out = f"and({out}, {p})"
    return out


def _parquet_value(v: str) -> str:
    if v in ("true", "false") or v.lstrip("-").replace(".", "", 1).isdigit():
        return v
    return 'Binary{"' + v + '"}'


def _partition_values(path: str) -> str:
    """FileScanRDD prints the partition values InternalRow: a date partition as days since the epoch."""
    for part in path.split("/"):
        if part.startswith("dt=") and len(part) == 13:
            return f"[{(date.fromisoformat(part[3:]) - EPOCH_DAY).days}]"
    return "[empty row]"


@dataclass
class _Work:
    """One task between its header lines (launch order) and its body lines (the order work really starts)."""

    tk: TaskRecord
    stage: StageRecord
    recv: int
    run_start: int
    run_end: int
    first_in_stage: bool
    work_start: int = 0


def _task_header(log: ContainerLog, run: AppRun, ex: Executor, stage: StageRecord, tk: TaskRecord, st: _State,
                 barrier: dict[int, int], rng: random.Random) -> _Work:
    """Receive the task, deserialize it: the first task of a stage on this JVM fetches the task binary;
    the stage's other tasks wait for it and for the first task's code generation (one class cache)."""
    res = tk.result
    recv = tk.launch + max(1, int(res.scheduler_delay_ms * 0.3))
    log.info(recv, "YarnCoarseGrainedExecutorBackend", f"Got assigned task {tk.task_id}")
    log.info(recv, "Executor", f"Running {_name(tk)}")
    if stage.epoch > st.epoch:
        log.info(recv, "MapOutputTrackerWorker", f"Updating epoch to {stage.epoch} and clearing cache")
        st.epoch = stage.epoch
        st.shuffles.clear()
    t = recv
    first = tk.stage_id not in st.stages
    st.stages.add(tk.stage_id)
    if stage.task_binary_bc is not None:
        t = _read_broadcast(log, run, ex, st, stage.task_binary_bc, recv + 1, rng)
    run_start = max(t, recv + res.deserialize_ms)
    w = _Work(tk, stage, recv, run_start, run_start + res.run_ms, first)
    if first:
        n = _codegen_classes(stage)
        # Janino compiles on the task's thread: a throttled host's CPU stretches it like the task's own work
        total = max(n, int(res.run_ms * 0.08 if res.run_ms < 2_000
                           else 260 * rng.uniform(0.6, 1.4) / ex.host.cpu_factor))
        t = run_start
        for _ in range(n):
            ms = total / n * rng.uniform(0.3, 1.7)
            t += int(ms)
            log.info(t, "CodeGenerator", f"Code generated in {ms:.6f} ms")
        barrier[tk.stage_id] = t
        w.work_start = t
    else:
        w.work_start = min(max(run_start, barrier.get(tk.stage_id, run_start)), w.run_end)
    return w


class _Until:
    """A killed task's view of the container log: nothing it would have logged after the kill took effect."""

    def __init__(self, log: ContainerLog, until: int):
        self.log, self.until = log, until

    def info(self, t: int, logger: str, msg: str) -> None:
        if t <= self.until:
            self.log.info(t, logger, msg)

    def warn(self, t: int, logger: str, msg: str) -> None:
        if t <= self.until:
            self.log.warn(t, logger, msg)

    def error(self, t: int, logger: str, msg: str) -> None:
        if t <= self.until:
            self.log.error(t, logger, msg)

    def raw(self, t: int, text: str) -> None:
        if t <= self.until:
            self.log.raw(t, text)


def _kill_lines(log: ContainerLog, tk: TaskRecord, rng: random.Random) -> int:
    """`Executor.killTask` per kill request ("trying to kill", the latest reason wins), then the task thread's
    exit: TaskKilledException ("Executor killed") or an interrupted call ("Executor interrupted and killed")
    (ref09). Returns when the task stopped logging."""
    end = tk.finish - rng.randint(1, 3)  # KILLED status update → the driver
    for t_req, reason in tk.kill_reqs:
        log.info(min(end - 1, t_req + rng.randint(1, 6)), "Executor", f"Executor is trying to kill {_name(tk)}, "
                 f"reason: {reason}")
    how = "Executor interrupted and killed" if tk.interrupted else "Executor killed"
    log.info(end, "Executor", f"{how} {_name(tk)}, reason: {tk.killed}")
    return end


def _name(tk: TaskRecord) -> str:
    return f"task {tk.index}.{tk.task_attempt} in stage {tk.stage_id}.{tk.attempt} (TID {tk.task_id})"


def _task_body(log: ContainerLog, run: AppRun, ex: Executor, w: _Work, st: _State, rng: random.Random) -> None:
    tk = w.tk
    if tk.killed is not None and tk.killed_metrics is not None:  # killed while running: cut its work short
        _task_work(_Until(log, _kill_lines(log, tk, rng) - 1), run, ex, w, st, rng, finished=False)
        return
    # Finished normally (also "Finish but did not commit": the kill found no running task, and
    # Executor.killTask logs nothing for an unknown task id).
    _task_work(log, run, ex, w, st, rng, finished=True)


def _task_work(log, run: AppRun, ex: Executor, w: _Work, st: _State, rng: random.Random, finished: bool) -> None:
    tk, stage = w.tk, w.stage
    res = tk.result
    t, run_end = w.work_start, w.run_end
    spec = stage.spec
    plan = stage.plans[tk.index]
    span = max(1, run_end - t)
    for src in spec.sources:
        if isinstance(src, ScanSource) and plan.scan is not None:
            t = _scan(log, run, ex, stage, src, plan.scan, st, t, span, rng)
        elif isinstance(src, ShuffleSource):
            t = _shuffle_fetch(log, run, ex, src, plan, st, t, rng)
    for name_dep in spec.depends_on:  # the broadcast relation a BroadcastHashJoin probes
        bid = run.relation_bc.get(name_dep)
        if bid is not None:
            t = _read_broadcast(log, run, ex, st, bid, t + 1, rng)
    m = res.metrics
    windows = _fill_windows(res)
    to_wall = _wall_clock(t, run_end, res.run_ms)
    if m.memory_bytes_spilled > 0:
        thread = st.threads.get(tk.task_id, THREAD_ID_BASE)
        page = page_size(ex.heap_bytes, ex.cores)
        _spill_lines(log, thread, page, res.exec_memory, windows, to_wall)
        if res.shuffle_sorter is not None and res.shuffle_sorter.spill > 0:
            _shuffle_spill_lines(log, tk.task_id, thread, page, res, windows, to_wall)
    if isinstance(spec.sink, WriteSink) and res.output_file:
        # FileFormatWriter.executeTask sets the task's committer up first; the first output file opens when the
        # first row reaches the writer, i.e. once the sort before the write has consumed all of its input.
        sorts = [b for op, _, b in windows if op.kind == "sort"]
        opened = to_wall(sorts[-1]) if sorts else t + rng.randint(2, 30)
        _write(log, run, ex, stage, tk, st, t + rng.randint(1, 6), max(t + 2, opened), run_end, rng)
    if finished:
        log.info(run_end + res.serialize_ms, "Executor", f"Finished {_name(tk)}. {m.result_size} bytes result sent "
                 "to driver")


def _wall_clock(t0: int, run_end: int, run_ms: int):
    """Maps a point of the task's cost breakdown (ms of run time) onto the log's clock between t0 and run_end."""
    span = max(1.0, float(run_ms))
    return lambda x: t0 + int((run_end - t0) * min(x, span) / span)


def _fill_windows(res) -> list[tuple[object, float, float]]:
    """(operator, start, end) in ms of the task's run time: when each operator consumes its input.

    The sorts below a join fill while the task reads its shuffle input; an operator after the join fills
    while the rows of the operators before it stream in. The windows come from the task's cost breakdown.
    """
    scale = res.wall_per_cpu_ms
    read_ms = sum(s.cpu_ns / 1e6 * scale + s.io_ms + s.fetch_wait_ms for s in res.sources)
    joined = any(op.kind == "smj" for op in res.trace)
    windows = []
    side_end = read_ms
    seen_join = not joined
    clock = 0.0
    for op in res.trace:
        dur = (op.cpu_ns + op.spill_cpu_ns) / 1e6 * scale + op.spill_ms
        if not seen_join:  # below the join: fills together with the shuffle read
            side_end += dur
            windows.append([op, 0.0, None])
            if op.kind == "smj":
                seen_join = True
            continue
        if op.kind == "smj":
            seen_join = True
        if clock == 0.0:
            clock = side_end if joined else read_ms
        windows.append([op, clock, clock + dur])
        clock += dur
    return [(op, a, side_end if b is None else b) for op, a, b in windows]


def _spill_lines(log: ContainerLog, thread: int, page: int, cap: int, windows, to_wall) -> None:
    """`UnsafeExternalSorter.spill`: one line per spill file, spread over the sorter's fill window.

    A sorter spills when it can't get another page within the task's execution memory (`cap`), so each spill
    file holds about that much. Every spilling operator has its own sorter, so its own count; the count is
    the number of spill files before this one (`spillWriters.size()`: "0  time", "1  time", "2  times"). The
    size is the sorter's `getMemoryUsage()`: whole data pages (`spark.buffer.pageSize`) plus its pointer
    array, a LongArray that starts at 2 x 4096 longs and doubles.
    """
    for op, a, b in windows:
        if op.spill <= 0:
            continue
        spills = max(1, math.ceil(op.spill / max(1, cap)))
        each = op.spill / spills
        pointers = each * min(1.0, op.rows_in * cal.SORT_POINTER_BYTES / op.mem) if op.mem else 0.0
        array = 1 << max(16, math.ceil(math.log2(max(1.0, pointers))))
        usage = max(1, int((each - array) // page)) * page + array  # never more than the sorter held
        for i in range(spills):
            ts_ = to_wall(a + (b - a) * (i + 1) / (spills + 1))
            log.info(ts_, "UnsafeExternalSorter", f"Thread {thread} spilling sort data of "
                     f"{bytes_to_string(usage)} to disk ({i} {' times' if i > 1 else ' time'} so far)")


def _shuffle_spill_lines(log: ContainerLog, task_id: int, thread: int, page: int, res, windows, to_wall) -> None:
    """`ShuffleExternalSorter.spill` of an UnsafeShuffleWriter: one line per spill file, while the map output
    streams in (after the operators below a join have sorted their input). Its count reads "0  times",
    "1  time", "2  times" (`!= 1`, unlike UnsafeExternalSorter), and it names the task (a real 3.5.9 run)."""
    sorter = res.shuffle_sorter
    spills = max(1, math.ceil(sorter.spill / max(1.0, sorter.held)))
    records = sorter.records / (spills + 0.5)  # every spill file holds a full buffer, the last run half of one
    array = 1 << max(15, math.ceil(math.log2(max(1.0, records * cal.SHUFFLE_POINTER_BYTES))))
    usage = max(1, int((sorter.held - array) // page)) * page + array
    joined = any(op.kind == "smj" for op, _, _ in windows)
    a = max((b for _, a0, b in windows if a0 == 0.0), default=0.0) if joined else 0.0  # the join's sorts are done
    b = res.run_ms - sorter.spill_ms / 2  # the merge into the output file comes last
    for i in range(spills):
        ts_ = to_wall(a + (b - a) * (i + 1) / (spills + 1))
        log.info(ts_, "ShuffleExternalSorter", f"Task {task_id} on Thread {thread} spilling sort data of "
                 f"{bytes_to_string(usage)} to disk ({i} {' time' if i == 1 else ' times'} so far)")


def _codegen_classes(stage: StageRecord) -> int:
    root = stage.plan_root
    if root is None:
        return 2
    wscg = sum(1 for n in root.walk() if isinstance(n, sqln.WholeStageCodegen))
    return max(2, min(14, 2 * wscg + 1))


THREAD_ID_BASE = 60  # the JVM's thread ids in use when the first task arrives (main, RPC, netty, block manager)
THREAD_KEEP_ALIVE_MS = 60_000  # Executor's cached task launch pool: idle workers exit after 60 s


def task_threads(tasks: list[TaskRecord], rng: random.Random) -> dict[int, int]:
    """Which "Executor task launch worker" thread runs each task (task id → thread id).

    The executor's task launch pool is a cached thread pool: a task goes to an idle worker if there is one
    (a SynchronousQueue hand-off, so the worker that went idle last), else to a new thread with the JVM's
    next thread id. Other threads (shuffle clients, block transfers) start in between, so ids have gaps.
    """
    out: dict[int, int] = {}
    busy: list[tuple[int, int]] = []  # (finish, thread id)
    idle: list[tuple[int, int]] = []  # (idle since, thread id)
    next_id = THREAD_ID_BASE
    for tk in sorted(tasks, key=lambda t: (t.launch, t.task_id)):
        while busy and busy[0][0] <= tk.launch:
            idle.append(heapq.heappop(busy))
        idle = sorted((s, tid) for s, tid in idle if tk.launch - s < THREAD_KEEP_ALIVE_MS)
        if idle:
            tid = idle.pop()[1]
        else:
            tid = next_id
            next_id += rng.choice((1, 1, 1, 2, 3))
        out[tk.task_id] = tid
        heapq.heappush(busy, (tk.finish, tid))
    return out


def _scan(log, run, ex, stage, src, part, st: _State, t: int, span: int, rng) -> int:
    pred = parquet_predicate(src.node.pushed_filters) if src.node is not None else None
    total = sum(pf.length for pf in part.files) or 1
    cur = t
    bid = next(iter(stage.scan_bcs), None)
    for i, pf in enumerate(part.files):
        log.info(cur, "FileScanRDD", f"Reading File path: {pf.file.path}, range: {pf.start}-{pf.start + pf.length}, "
                 f"partition values: {_partition_values(pf.file.path)}")
        if i == 0 and bid is not None:
            cur = _read_broadcast(log, run, ex, st, bid, cur + 1, rng)
        if i == 0:  # a UDF that opens its own resource in each task (its library's log line, first row)
            for f in stage.native_funcs:
                if f.native_log is not None:
                    log.info(cur + 2, *f.native_log)
        if pred is not None:
            log.info(cur + 1, "FilterCompat", f"Filtering using predicate: {pred}")
        if st.decompressors < ex.cores:
            st.decompressors += 1
            log.info(cur + 3, "CodecPool", "Got brand-new decompressor [.snappy]")
        cur += int(span * 0.9 * pf.length / total)
    return t + 2


def _shuffle_fetch(log, run, ex, src, plan, st: _State, t: int, rng) -> int:
    xch = run.exchanges[src.exchange]
    _map_statuses(log, run, ex, st, xch.shuffle_id, t)
    read = next((r for r in plan.reads if r[0] == src.exchange), None)
    if read is None:
        return t
    _name, a, b = read
    rec = xch.records[:, a:b]
    byt = xch.bytes[:, a:b]
    blocks = (rec.sum(axis=1) > 0).astype(int) if src.read is not None else (rec > 0).sum(axis=1)
    seg_bytes = byt.sum(axis=1)
    loc = [0, 0]
    hl = [0, 0]
    rem = [0, 0]
    remote_hosts: dict[str, int] = {}
    for mi in range(xch.num_maps):
        nb = int(blocks[mi])
        if not nb:
            continue
        sz = int(seg_bytes[mi])
        if xch.map_executor[mi] == ex.id:
            loc[0] += nb
            loc[1] += sz
        elif xch.map_host[mi] == ex.host.name:
            hl[0] += nb
            hl[1] += sz
        else:
            rem[0] += nb
            rem[1] += sz
            remote_hosts[xch.map_host[mi]] = remote_hosts.get(xch.map_host[mi], 0) + sz
    n = loc[0] + hl[0] + rem[0]
    size = loc[1] + hl[1] + rem[1]
    t += rng.randint(1, 8)
    log.info(t, "ShuffleBlockFetcherIterator",
             f"Getting {n} ({bytes_to_string(size)}) non-empty blocks including {loc[0]} ({bytes_to_string(loc[1])}) "
             f"local and {hl[0]} ({bytes_to_string(hl[1])}) host-local and 0 (0.0 B) push-merged-local and "
             f"{rem[0]} ({bytes_to_string(rem[1])}) remote blocks")
    per_req = 48 * MiB // 5  # spark.reducer.maxSizeInFlight / 5 per FetchRequest
    reqs = sum(max(1, math.ceil(v / per_req)) for v in remote_hosts.values())
    started = rng.randint(1, 6) + reqs // 8
    log.info(t + started, "ShuffleBlockFetcherIterator", f"Started {reqs} remote fetches in {started} ms")
    return t + started


def _write(log, run, ex, stage, tk, st: _State, t: int, opened: int, run_end: int, rng) -> None:
    """Committer set-up at `t` (task start), the Parquet writer when its first row arrives (`opened`)."""
    sink = stage.spec.sink
    cmd = _write_command(stage)
    table_root = cmd.output_path if cmd is not None else sink.path.rsplit("/", 1)[0]
    job_uuid = getattr(stage, "_job_uuid", None)
    dynamic = run.conf.get("spark.sql.sources.partitionOverwriteMode", "static") == "dynamic"
    out = f"{table_root}/.spark-staging-{job_uuid}" if dynamic else table_root
    for _ in range(2):
        log.info(t, "FileOutputCommitter", "File Output Committer Algorithm version is 1")
        log.info(t, "FileOutputCommitter", "FileOutputCommitter skip cleanup _temporary folders under output "
                 "directory:false, ignore cleanup failures: false")
        log.info(t, "SQLHadoopMapReduceCommitProtocol",
                 "Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter"
                 if _ == 0 else "Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter")
    t = opened
    log.info(t, "CodecConfig", "Compression: SNAPPY")
    log.info(t, "CodecConfig", "Compression: SNAPPY")
    log.info(t, "ParquetOutputFormat",
             "ParquetRecordWriter [block size: 134217728b, row group padding size: 8388608b, validating: false]")
    if cmd is not None:
        log.info(t, "ParquetWriteSupport", write_support_message(cmd, stage))
    if st.compressors < ex.cores:
        st.compressors += 1
        log.info(t + 1, "CodecPool", "Got brand-new compressor [.snappy]")
    commit_ms = max(1, int(tk.result.commit_ms))
    tc = run_end - commit_ms
    jt = run.job_tracker_id(stage)
    att = f"attempt_{jt}_{stage.stage_id:04d}_m_{tk.index:06d}_{tk.task_id}"
    log.info(tc, "FileOutputCommitter", f"Saved output of task '{att}' to {out}/_temporary/0/"
             f"task_{jt}_{stage.stage_id:04d}_m_{tk.index:06d}")
    log.info(run_end, "SparkHadoopMapRedUtil", f"{att}: Committed. Elapsed time: {commit_ms} ms.")


def _write_command(stage: StageRecord):
    root = stage.plan_root
    if root is None:
        return None
    for n in root.walk():
        if isinstance(n, sqln.DataWritingCommand):
            return n.cmd
    return None


_SPARK_TYPE = {"bigint": "long", "int": "integer", "string": "string", "double": "double", "boolean": "boolean",
               "timestamp": "timestamp", "date": "date", "float": "float", "smallint": "short", "tinyint": "byte"}
_PARQUET_TYPE = {"bigint": "int64 {n}", "int": "int32 {n}", "string": "binary {n} (STRING)", "double": "double {n}",
                 "boolean": "boolean {n}", "timestamp": "int96 {n}", "date": "int32 {n} (DATE)", "float": "float {n}",
                 "smallint": "int32 {n} (INTEGER(16,true))", "tinyint": "int32 {n} (INTEGER(8,true))"}


def write_support_message(cmd, stage: StageRecord) -> str:
    """ParquetWriteSupport.init: the Catalyst schema (prettyJson) and the Parquet message type."""
    root = stage.plan_root
    child = None
    for n in root.walk():
        if isinstance(n, sqln.DataWritingCommand):
            child = n.children[0]
            while isinstance(child, sqln.WriteFiles):
                child = child.children[0]
            break
    parts = {a.name for a in cmd.partition_columns}
    attrs = [a for a in (child.output() if child is not None else []) if a.name not in parts]
    fields = []
    for a in attrs:
        fields.append("{\n"
                      f'    "name" : "{a.name}",\n'
                      f'    "type" : "{_SPARK_TYPE.get(a.dtype, a.dtype)}",\n'
                      f'    "nullable" : {"true" if a.nullable else "false"},\n'
                      '    "metadata" : { }\n'
                      "  }")
    schema = '{\n  "type" : "struct",\n  "fields" : [ ' + ", ".join(fields) + " ]\n}"
    msg = "\n".join(f"  {'optional' if a.nullable else 'required'} "
                    f"{_PARQUET_TYPE.get(a.dtype, 'binary {n}').format(n=a.name)};" for a in attrs)
    return (f"Initialized Parquet WriteSupport with Catalyst schema:\n{schema}\n"
            f"and corresponding Parquet message type:\nmessage spark_schema {{\n{msg}\n}}\n\n       ")


# --------------------------------------------------------------------------- the whole container


def executor_stderr(run: AppRun, ex: Executor, echo: list | None = None) -> str:
    """The executor's stderr. `echo` collects what the driver logs in reaction (see `driver.am_stderr`)."""
    rng = substream(run.spec.seed, f"yarnlogs/{ex.container_id}")
    log = ContainerLog()
    _startup(log, run, ex, rng)
    tasks = sorted((t for t in run.task_records.values() if t.executor is ex), key=lambda t: (t.launch, t.task_id))
    st = _State(echo=echo, threads=task_threads(tasks, substream(run.spec.seed, f"yarnlogs/{ex.container_id}/threads")))
    if ex.exit_at is not None:  # the JVM stops logging task work once SIGTERM arrives, or dies with SIGKILL
        log.cutoff = ex.nm_kill_at if ex.nm_kill_at is not None else ex.exit_at
    barrier: dict[int, int] = {}
    work = [_task_header(log, run, ex, run.stages[tk.stage_id], tk, st, barrier, rng) for tk in tasks]
    for w in sorted(work, key=lambda w: (w.work_start, w.tk.task_id)):
        _task_body(log, run, ex, w, st, rng)
    if ex.nm_kill_at is not None:
        t = ex.nm_kill_at
        log.cutoff = None
        log.error(t, "CoarseGrainedExecutorBackend", "RECEIVED SIGNAL TERM")
        log.info(t + rng.randint(2, 30), "DiskBlockManager", "Shutdown hook called")
        log.info(t + rng.randint(30, 80), "ShutdownHookManager", "Shutdown hook called")
    elif ex.exit_at is not None:
        pass  # SIGKILL: no signal handler, no shutdown hooks; the log just stops
    else:
        t = run.app_end + rng.randint(40, 400)
        log.info(t, "YarnCoarseGrainedExecutorBackend", "Driver commanded a shutdown")
        log.info(t + rng.randint(5, 40), "MemoryStore", "MemoryStore cleared")
        log.info(t + rng.randint(40, 60), "BlockManager", "BlockManager stopped")
        log.info(t + rng.randint(60, 120), "ShutdownHookManager", "Shutdown hook called")
    return log.text()
