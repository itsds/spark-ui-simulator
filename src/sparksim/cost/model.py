"""Cost model v1: one task's work → its phases → `TaskMetrics`.

Given what a task reads (a file partition, or reducer ranges of shuffle
exchanges), where it runs (executor, host speed, data locality) and whether
the executor is warm, it pushes rows through the stage's operators and adds
up the phases Spark measures:

    deserialize │ run = HDFS read + decode + operators + shuffle fetch wait + local shuffle read
                │       + spill + shuffle/Parquet write + codegen compile (first task per executor) + GC
                │ result serialization │ scheduler delay (RPC + driver handling)

Only task-level facts come out of here; the driver's bookkeeping (accumulator
values, stage totals) is derived later by `eventlog.DriverLedger`.

Durations are fixed at launch: no dynamic contention between concurrent tasks
(v1 limitation, see ADR-014).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from ..eventlog import InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics
from ..model.cluster import Executor
from ..model.files import FilePartition
from ..model.shuffle import ShuffleExchange
from ..model.workload import (
    BroadcastSink,
    Op,
    ResultSink,
    ScanSource,
    ShuffleSink,
    ShuffleSource,
    StageSpec,
    WriteSink,
    expected_distinct,
)
from ..rng import lognormal
from . import calibration as cal

MiB = 1024 * 1024


@dataclass(frozen=True)
class TaskPlan:
    """The static description of one task: its partition and its preferred hosts."""

    index: int
    scan: FilePartition | None = None
    reads: tuple[tuple[str, int, int], ...] = ()  # (exchange, first reducer, end reducer) per shuffle source
    preferred: tuple[str, ...] = ()


@dataclass
class StageContext:
    spec: StageSpec
    exchanges: dict[str, ShuffleExchange]
    batch_fetch: bool = False  # AQE coalesced reads fetch contiguous blocks of one map as one batch
    out_exchange: ShuffleExchange | None = None
    output_partitions: int = 0  # for MapStatus size

    def exchange_for(self, name: str) -> ShuffleExchange:
        return self.exchanges[name]


@dataclass(frozen=True)
class TaskEnv:
    executor: Executor
    data_locality: str  # NODE_LOCAL | RACK_LOCAL | ANY, physical distance to the HDFS replica
    cold_core: bool  # first task on this core of a fresh JVM
    first_on_executor: bool  # first task of this stage on this executor
    compile_wait_ms: float  # waiting for another thread's codegen compile of this stage
    memory_per_task: int  # execution memory a task can get on this executor


@dataclass
class OpTrace:
    kind: str
    rows_in: int
    rows_out: int
    node: object = None  # the physical plan node (SQL metrics)
    cpu_ns: float = 0.0  # this operator's CPU work, before host speed / noise / GC
    mem: float = 0.0  # execution memory it holds (sort pages, hash map)
    spill: float = 0.0  # bytes of that memory it spilled to disk (Spill (Memory) share)
    spill_ms: float = 0.0  # writing and reading back its spill files
    spill_cpu_ns: float = 0.0  # serializing + compressing what it spilled (CPU, before host speed / noise / GC)


@dataclass
class SourceRead:
    """One source of a task, kept for the SQL metrics of its plan node."""

    node: object  # FileSourceScan or the upstream ShuffleExchange
    rows: int
    cpu_ns: float  # decode / deserialize
    io_ms: float  # disk + network read time (before fetch wait)
    batches: int = 0  # columnar batches (scans)
    stats: object = None  # model.shuffle.ReadStats (shuffle sources)
    remote_reqs_ms: int = 0
    fetch_wait_ms: int = 0


@dataclass
class TaskResult:
    metrics: TaskMetrics
    deserialize_ms: int
    run_ms: int
    serialize_ms: int
    scheduler_delay_ms: int
    exec_memory: int  # peak execution memory held (for executor heartbeat snapshots)
    fetching: bool
    output_file: tuple[int, int] | None = None  # (bytes, rows) of the Parquet file a write task produced
    result_rows: int = 0
    trace: list[OpTrace] = field(default_factory=list)
    sources: list[SourceRead] = field(default_factory=list)
    wall_per_cpu_ms: float = 1.0  # wall ms per ms of nominal CPU work (host speed, noise, GC)
    sink_rows: int = 0  # rows the sink consumed
    sink_row_bytes: int = 0  # their UnsafeRow size
    sink_cpu_ns: float = 0.0  # per-row work of the sink while the pipeline runs (serialization, encoding)
    commit_ms: float = 0.0  # task commit of a write task
    spill_op: int | None = None  # index in trace of the operator that spilled
    spill_ms: float = 0.0

    @property
    def duration_ms(self) -> int:
        return self.deserialize_ms + self.run_ms + self.serialize_ms + self.scheduler_delay_ms


def page_size(heap_bytes: int, cores: int) -> int:
    """`spark.buffer.pageSize` default (MemoryManager.defaultPageSizeBytes)."""
    max_exec = (heap_bytes - cal.RESERVED_MEMORY) * cal.MEMORY_FRACTION
    size = max_exec / cores / 16
    p = 1 << max(0, math.ceil(math.log2(max(1.0, size))))
    return int(min(cal.PAGE_SIZE_MAX, max(cal.PAGE_SIZE_MIN, p)))


def _pages(nbytes: float, page: int) -> int:
    return 0 if nbytes <= 0 else int(math.ceil(nbytes / page) * page)


def _op_memory(op: Op, rows_in: int, rows_out: int, row_bytes: int, page: int) -> float:
    if op.kind == "sort":
        return _pages(rows_in * row_bytes, page) + rows_in * cal.SORT_POINTER_BYTES
    if op.kind in ("partial_agg", "final_agg"):
        groups = rows_out
        cap = 1 << max(6, math.ceil(math.log2(max(1, groups) * 2)))  # BytesToBytesMap capacity, load 0.5
        return _pages(groups * (op.buffer_bytes + cal.HASH_MAP_BYTES_PER_GROUP), page) + cap * 16
    return 0.0


def simulate_task(ctx: StageContext, plan: TaskPlan, env: TaskEnv, rng: random.Random, nprng) -> TaskResult:
    spec = ctx.spec
    ex = env.executor
    host = ex.host
    noise = lognormal(rng, cal.TASK_NOISE_SIGMA)
    io_speed = host.disk_speed  # local disks
    net = host.net_factor  # its NIC (1.0 unless the node is degraded)
    page = page_size(ex.heap_bytes, ex.cores)
    trace: list[OpTrace] = []

    cpu_ns = 0.0
    io_ms = 0.0
    fetch_wait_ms = 0.0
    m = TaskMetrics()
    fetching = False
    sides: list[tuple[int, int]] = []  # (rows, row_bytes) per source
    sources: list[SourceRead] = []

    # ---------------------------------------------------------------- sources
    read_iter = iter(plan.reads)
    rd = ShuffleReadMetrics()
    for src in spec.sources:
        if isinstance(src, ScanSource):
            part = plan.scan
            assert part is not None
            # Selected column chunks of the row groups this split owns, plus each file's footer.
            nbytes = round(part.data_bytes * src.dataset.read_fraction) + len(part.files) * cal.PARQUET_FOOTER_READ
            rows = part.rows
            m.input = InputMetrics(nbytes, rows)
            remote = env.data_locality != "NODE_LOCAL"  # through a DataNode over the network
            mbps = cal.HDFS_READ_MBPS[env.data_locality] * io_speed * (net if remote else 1.0)
            src_io = nbytes / (mbps * MiB) * 1e3 + len(part.files) * cal.HDFS_READ_OPEN_MS
            src_cpu = nbytes * cal.PARQUET_DECODE_NS_PER_BYTE + rows * cal.SCAN_NS_PER_ROW
            io_ms += src_io
            cpu_ns += src_cpu
            sources.append(SourceRead(src.node, rows, src_cpu, src_io, part.batches()))
            sides.append((rows, src.dataset.row_bytes))
        elif isinstance(src, ShuffleSource):
            name, start, end = next(read_iter)
            st = ctx.exchange_for(name).read(start, end, host.name, ctx.batch_fetch)
            fetching = fetching or st.remote_bytes > 0
            rd.records_read += st.records
            rd.local_bytes_read += st.local_bytes
            rd.remote_bytes_read += st.remote_bytes
            rd.local_blocks_fetched += st.local_blocks
            rd.remote_blocks_fetched += st.remote_blocks
            src_cpu = st.bytes * cal.SHUFFLE_DESERIALIZE_NS_PER_BYTE
            src_io = st.local_bytes / (cal.SHUFFLE_LOCAL_READ_MBPS * io_speed * MiB) * 1e3
            cpu_ns += src_cpu
            io_ms += src_io
            sr = SourceRead(src.node, st.records, src_cpu, src_io, stats=st)
            if st.remote_blocks:
                requests = max(st.remote_hosts, math.ceil(st.remote_blocks / cal.SHUFFLE_BLOCKS_PER_REQUEST))
                transfer = st.remote_bytes / (cal.SHUFFLE_REMOTE_MBPS * net * MiB) * 1e3
                latency = requests * cal.SHUFFLE_REQUEST_LATENCY_MS / 5  # up to 5 requests in flight
                fetch_wait_ms += transfer * (1 - cal.SHUFFLE_FETCH_OVERLAP) + latency
                reqs = round((transfer * 3 + requests * cal.SHUFFLE_REQUEST_LATENCY_MS) * lognormal(rng, 0.2))
                rd.remote_reqs_duration += reqs
                sr.remote_reqs_ms = reqs
            sources.append(sr)
            sides.append((st.records, src.row_bytes))
    if any(isinstance(s, ShuffleSource) for s in spec.sources):
        m.shuffle_read = rd

    # ---------------------------------------------------------------- operators
    mem_side = 0.0
    side_ix: list[int] = []  # trace entries of the operators below the join (they hold memory together)
    for i, (rows, rb) in enumerate(sides):
        for op in (spec.side_ops[i] if i < len(spec.side_ops) else ()):
            out = _apply(op, rows, None, ctx, plan)
            cpu_ns += rows * op.ns_per_row
            mem = _op_memory(op, rows, out, rb, page)
            mem_side += mem
            side_ix.append(len(trace))
            trace.append(OpTrace(op.kind, rows, out, op.node, rows * op.ns_per_row, mem))
            rows, rb = out, op.out_row_bytes or rb
        sides[i] = (rows, rb)
    rows, rb = sides[0]
    mem_after = 0.0
    after_ix: list[int] = []
    for op in spec.ops:
        if op.kind == "smj":
            right_rows = sum(r for r, _ in sides[1:])
            cpu_ns += (rows + right_rows) * op.ns_per_row
            out = round(rows * op.selectivity)
            trace.append(OpTrace(op.kind, rows + right_rows, out, op.node, (rows + right_rows) * op.ns_per_row))
            rows, rb = out, op.out_row_bytes or rb
            continue
        out = _apply(op, rows, rb, ctx, plan)
        cpu_ns += rows * op.ns_per_row
        mem = _op_memory(op, rows, out, rb, page)
        mem_after = max(mem_after, mem)
        after_ix.append(len(trace))
        trace.append(OpTrace(op.kind, rows, out, op.node, rows * op.ns_per_row, mem))
        rows, rb = out, op.out_row_bytes or rb
    mem_need = max(mem_side, mem_after)

    # ---------------------------------------------------------------- spill (sort / aggregation over the task's share)
    exec_mem = mem_need
    spill_op = None
    spill_ms = 0.0
    if mem_need > env.memory_per_task:
        # The sorts below a join fill up together; each operator after the join on its own.
        _spill_group(trace, side_ix, env.memory_per_task)
        for i in after_ix:
            _spill_group(trace, [i], env.memory_per_task)
        spilled = sum(t.spill for t in trace)
        m.memory_bytes_spilled = round(spilled)
        m.disk_bytes_spilled = round(spilled * cal.LZ4_SHUFFLE_RATIO)
        rate = cal.SHUFFLE_DISK_WRITE_MBPS * io_speed * MiB
        for t in trace:  # write the spill files, read them back for the merge
            t.spill_ms = t.spill * cal.LZ4_SHUFFLE_RATIO / rate * 1e3 * 2
            t.spill_cpu_ns = t.spill * cal.SHUFFLE_SERIALIZE_NS_PER_BYTE * 0.6
        spill_ms = sum(t.spill_ms for t in trace)
        spill_op = max(range(len(trace)), key=lambda i: trace[i].spill)
        cpu_ns += sum(t.spill_cpu_ns for t in trace)
        exec_mem = env.memory_per_task
    m.peak_execution_memory = round(exec_mem)

    # ---------------------------------------------------------------- sink
    write_ms = 0.0
    output_file = None
    result_bytes = cal.RESULT_BASE_BYTES + rng.randint(0, cal.RESULT_JITTER_BYTES)
    sink = spec.sink
    sink_rows, sink_rb, sink_cpu, commit_ms = rows, rb, 0.0, 0.0
    if isinstance(sink, ShuffleSink):
        uncompressed = rows * rb
        sink_cpu = uncompressed * cal.SHUFFLE_SERIALIZE_NS_PER_BYTE
        cpu_ns += sink_cpu
        bpr = rb * cal.LZ4_SHUFFLE_RATIO * lognormal(rng, 0.03)
        written, recs = ctx.out_exchange.write(plan.index, rows, bpr, nprng)
        disk_ms = written / (cal.SHUFFLE_DISK_WRITE_MBPS * io_speed * MiB) * 1e3 + cal.SHUFFLE_WRITE_FIXED_MS
        disk_ms *= lognormal(rng, 0.15)
        if not rows:  # [ref] an empty map task commits empty partitions without timing a write (ref01 stage 5)
            disk_ms = 0.0
        write_ms += disk_ms
        m.shuffle_write = ShuffleWriteMetrics(written, round(disk_ms * 1e6), recs)
        r = ctx.output_partitions
        result_bytes += r + 48 if r <= 2000 else 160 + r // 64  # Compressed / HighlyCompressedMapStatus
    elif isinstance(sink, WriteSink):
        nbytes = round(rows * sink.bytes_per_row * lognormal(rng, 0.02)) if rows else 0
        sink_cpu = rows * cal.PARQUET_ENCODE_NS_PER_ROW + nbytes * cal.PARQUET_ENCODE_NS_PER_BYTE
        cpu_ns += sink_cpu
        commit_ms = cal.TASK_COMMIT_MS * lognormal(rng, 0.3)
        write_ms += nbytes / (cal.HDFS_WRITE_MBPS * io_speed * net * MiB) * 1e3 + commit_ms  # 3-replica pipeline
        m.output = OutputMetrics(nbytes, rows)
        output_file = (nbytes, rows) if rows else None
        result_bytes += 3_900 + rng.randint(0, 600)  # WriteTaskResult: commit message + written file stats
    elif isinstance(sink, BroadcastSink):
        result_bytes += rows * rb // 2  # rows collected to the driver (UnsafeRow, compressed)
    elif isinstance(sink, ResultSink):
        result_bytes += round(rows * sink.bytes_per_row)

    # ---------------------------------------------------------------- time
    cpu_ms = cpu_ns / 1e6 / host.cpu_speed * noise
    slow_cpu = host.cpu_factor  # a degraded host's CPU also stretches the fixed CPU costs below
    compile_ms = 0.0
    if env.first_on_executor:
        compile_ms = cal.CODEGEN_COMPILE_MS * lognormal(rng, 0.3) / slow_cpu
    elif env.compile_wait_ms > 0:
        compile_ms = env.compile_wait_ms
    alloc_intensity = 1.25 if (fetching or mem_need > 0) else 1.0
    gc_ms = cpu_ms * cal.GC_SHARE_MEDIAN * alloc_intensity * lognormal(rng, cal.GC_SHARE_SIGMA)
    if env.cold_core:
        gc_ms += rng.uniform(*cal.GC_COLD_EXTRA_MS)
    compute_ms = cpu_ms + compile_ms + gc_ms
    # Remote fetches overlap computation; whatever doesn't fit behind it is exposed as fetch wait.
    if fetch_wait_ms:
        transfer_left = (rd.remote_bytes_read / (cal.SHUFFLE_REMOTE_MBPS * net * MiB) * 1e3) - compute_ms
        fetch_wait_ms = max(fetch_wait_ms, transfer_left) * lognormal(rng, 0.25)
        rd.fetch_wait_time = round(fetch_wait_ms)
    run_ms = compute_ms + io_ms + fetch_wait_ms + write_ms + spill_ms

    if env.cold_core:
        deser, share = cal.DESERIALIZE_COLD_JVM_MS, cal.DESERIALIZE_CPU_SHARE["cold"]
    elif env.first_on_executor:
        deser, share = cal.DESERIALIZE_NEW_STAGE_MS, cal.DESERIALIZE_CPU_SHARE["stage"]
    else:
        deser, share = cal.DESERIALIZE_WARM_MS, cal.DESERIALIZE_CPU_SHARE["warm"]
    deser_ms = max(1, round(deser * lognormal(rng, cal.DESERIALIZE_SIGMA) / slow_cpu))
    ser_ms = round(rng.randint(*cal.RESULT_SERIALIZATION_MS) / slow_cpu)
    sched_ms = max(2, round(lognormal(rng, cal.SCHEDULER_DELAY_SIGMA, cal.SCHEDULER_DELAY_MEDIAN_MS)
                            + result_bytes / (40 * MiB) * 1e3))

    run = max(1, round(run_ms))
    m.executor_deserialize_time = deser_ms
    m.executor_deserialize_cpu_time = round(deser_ms * 1e6 * share * lognormal(rng, 0.15))
    m.executor_run_time = run
    cpu_time_ms = (cpu_ms + compile_ms * 0.92 + 0.12 * (io_ms + write_ms)) * lognormal(rng, 0.02)
    m.executor_cpu_time = round(min(cpu_time_ms, run) * 1e6)
    m.jvm_gc_time = round(gc_ms)
    m.result_serialization_time = ser_ms
    m.result_size = round(result_bytes)
    # wall time per nominal CPU ms (host speed, noise and the GC that CPU work causes)
    nominal_ms = cpu_ns / 1e6
    wall_per_cpu = (cpu_ms + gc_ms) / nominal_ms if nominal_ms > 0 else 1.0
    if fetch_wait_ms and rd.remote_bytes_read:
        for sr in sources:
            if sr.stats is not None and sr.stats.remote_bytes:
                sr.fetch_wait_ms = round(rd.fetch_wait_time * sr.stats.remote_bytes / rd.remote_bytes_read)
    return TaskResult(m, deser_ms, run, ser_ms, sched_ms, round(exec_mem), fetching, output_file,
                      rows, trace, sources, wall_per_cpu, sink_rows, sink_rb, sink_cpu, commit_ms, spill_op, spill_ms)


def killed_metrics(res: TaskResult, run_elapsed_ms: int) -> TaskMetrics:
    """What a task attempt killed after `run_elapsed_ms` of its run reports (`TaskKilled`'s accumulators).

    `collectAccumulatorsAndResetStatusOnFailure` sets the run time and the GC time; deserialization time, CPU
    time, result size and serialization time are only set on success and stay 0 (ref09). Records read so
    far, the shuffle blocks fetched so far (a fetch runs ahead of the records it delivers), the memory held
    and the spills so far are the finished task's, scaled to how far it got; nothing is written: an
    UnsafeShuffleWriter counts records when it writes its sorted file, a Parquet writer's output metrics
    only when it commits.
    """
    m = res.metrics
    f = min(1.0, max(0.0, run_elapsed_ms / max(1, res.run_ms)))
    g = min(1.0, f + 0.15) if f > 0 else 0.0
    out = TaskMetrics()
    out.executor_run_time = int(run_elapsed_ms)
    out.jvm_gc_time = round(m.jvm_gc_time * f)
    out.peak_execution_memory = round(res.exec_memory * min(1.0, f / 0.7))
    out.memory_bytes_spilled = round(m.memory_bytes_spilled * f)
    out.disk_bytes_spilled = round(m.disk_bytes_spilled * f)
    if m.input is not None:
        out.input = InputMetrics(round(m.input.bytes_read * f), round(m.input.records_read * f))
    if m.shuffle_read is not None:
        r = m.shuffle_read
        out.shuffle_read = ShuffleReadMetrics(
            remote_blocks_fetched=round(r.remote_blocks_fetched * g),
            local_blocks_fetched=round(r.local_blocks_fetched * g),
            fetch_wait_time=round(r.fetch_wait_time * f), remote_bytes_read=round(r.remote_bytes_read * g),
            local_bytes_read=round(r.local_bytes_read * g), records_read=round(r.records_read * f),
            remote_reqs_duration=round(r.remote_reqs_duration * g))
    if m.shuffle_write is not None:
        out.shuffle_write = ShuffleWriteMetrics()
    if m.output is not None:
        out.output = OutputMetrics()
    return out


def _spill_group(trace: list[OpTrace], ix: list[int], available: float) -> None:
    """Memory consumers that hold memory at the same time, with `available` bytes for the task.

    UnsafeExternalSorter / the aggregation map spill when they can't get another page; TaskMemoryManager
    spills the biggest consumer first. A consumer that spills keeps on average half the task's memory
    for its last, in-memory run, so a single consumer of M > A spills M - A/2.
    """
    remaining = sum(trace[i].mem for i in ix)
    for i in sorted(ix, key=lambda i: -trace[i].mem):
        if remaining <= available:
            break
        x = min(trace[i].mem, remaining - 0.5 * available)
        trace[i].spill = x
        remaining -= x


def _apply(op: Op, rows: int, row_bytes, ctx: StageContext, plan: TaskPlan) -> int:
    """Output rows of one operator for this task."""
    if op.kind in ("filter", "bhj"):
        return round(rows * op.selectivity)
    if op.kind == "partial_agg":
        return expected_distinct(_groups_in_task(op, plan), rows)
    if op.kind == "final_agg":
        # The groups hashed to this task's reducers: every one of them shows up in its input.
        name, start, end = plan.reads[0]
        shares = ctx.exchange_for(name).shares
        groups = round(op.groups.distinct * float(shares[start:end].sum()))
        return min(groups, rows)
    return rows


def _groups_in_task(op: Op, plan: TaskPlan) -> float:
    """Groups a partial aggregation can meet in this task's input.

    Rows are drawn from all of the key's values, unless the scanned files were written clustered on one of
    the grouping columns (a write after a hash shuffle on it): then the task only holds the keys of the
    files it reads, and all of their rows (`HdfsFile.key_fraction`).
    """
    g = op.groups.distinct
    if plan.scan is None:
        return g
    cols = set(op.groups.name.split(","))
    frac = 0.0
    for pf in plan.scan.files:
        f = pf.file
        if f.clustered_on is None or f.clustered_on not in cols or not f.rows:
            return g
        frac += f.key_fraction * pf.rows / f.rows
    return g * frac
