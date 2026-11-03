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
    io_speed = math.sqrt(host.speed)
    page = page_size(ex.heap_bytes, ex.cores)
    trace: list[OpTrace] = []

    cpu_ns = 0.0
    io_ms = 0.0
    fetch_wait_ms = 0.0
    m = TaskMetrics()
    fetching = False
    sides: list[tuple[int, int]] = []  # (rows, row_bytes) per source

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
            mbps = cal.HDFS_READ_MBPS[env.data_locality] * io_speed
            io_ms += nbytes / (mbps * MiB) * 1e3 + len(part.files) * cal.HDFS_READ_OPEN_MS
            cpu_ns += nbytes * cal.PARQUET_DECODE_NS_PER_BYTE + rows * cal.SCAN_NS_PER_ROW
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
            cpu_ns += st.bytes * cal.SHUFFLE_DESERIALIZE_NS_PER_BYTE
            io_ms += st.local_bytes / (cal.SHUFFLE_LOCAL_READ_MBPS * io_speed * MiB) * 1e3
            if st.remote_blocks:
                requests = max(st.remote_hosts, math.ceil(st.remote_blocks / cal.SHUFFLE_BLOCKS_PER_REQUEST))
                transfer = st.remote_bytes / (cal.SHUFFLE_REMOTE_MBPS * MiB) * 1e3
                latency = requests * cal.SHUFFLE_REQUEST_LATENCY_MS / 5  # up to 5 requests in flight
                fetch_wait_ms += transfer * (1 - cal.SHUFFLE_FETCH_OVERLAP) + latency
                rd.remote_reqs_duration += round((transfer * 3 + requests * cal.SHUFFLE_REQUEST_LATENCY_MS)
                                                 * lognormal(rng, 0.2))
            sides.append((st.records, src.row_bytes))
    if any(isinstance(s, ShuffleSource) for s in spec.sources):
        m.shuffle_read = rd

    # ---------------------------------------------------------------- operators
    mem_side = 0.0
    for i, (rows, rb) in enumerate(sides):
        for op in (spec.side_ops[i] if i < len(spec.side_ops) else ()):
            out = _apply(op, rows, None, ctx, plan)
            cpu_ns += rows * op.ns_per_row
            mem_side += _op_memory(op, rows, out, rb, page)
            trace.append(OpTrace(op.kind, rows, out))
            rows, rb = out, op.out_row_bytes or rb
        sides[i] = (rows, rb)
    rows, rb = sides[0]
    mem_after = 0.0
    for op in spec.ops:
        if op.kind == "smj":
            right_rows = sum(r for r, _ in sides[1:])
            cpu_ns += (rows + right_rows) * op.ns_per_row
            out = round(rows * op.selectivity)
            trace.append(OpTrace(op.kind, rows + right_rows, out))
            rows, rb = out, op.out_row_bytes or rb
            continue
        out = _apply(op, rows, rb, ctx, plan)
        cpu_ns += rows * op.ns_per_row
        mem_after = max(mem_after, _op_memory(op, rows, out, rb, page))
        trace.append(OpTrace(op.kind, rows, out))
        rows, rb = out, op.out_row_bytes or rb
    mem_need = max(mem_side, mem_after)

    # ---------------------------------------------------------------- spill (sort / aggregation over the task's share)
    spill_ms = 0.0
    exec_mem = mem_need
    if mem_need > env.memory_per_task:
        spilled = mem_need - env.memory_per_task * 0.5
        m.memory_bytes_spilled = round(spilled)
        m.disk_bytes_spilled = round(spilled * cal.LZ4_SHUFFLE_RATIO)
        spill_ms = m.disk_bytes_spilled / (cal.SHUFFLE_DISK_WRITE_MBPS * io_speed * MiB) * 1e3 * 2
        cpu_ns += spilled * cal.SHUFFLE_SERIALIZE_NS_PER_BYTE * 0.6
        exec_mem = env.memory_per_task
    m.peak_execution_memory = round(exec_mem)

    # ---------------------------------------------------------------- sink
    write_ms = 0.0
    output_file = None
    result_bytes = cal.RESULT_BASE_BYTES + rng.randint(0, cal.RESULT_JITTER_BYTES)
    sink = spec.sink
    if isinstance(sink, ShuffleSink):
        uncompressed = rows * rb
        cpu_ns += uncompressed * cal.SHUFFLE_SERIALIZE_NS_PER_BYTE
        bpr = rb * cal.LZ4_SHUFFLE_RATIO * lognormal(rng, 0.03)
        written, recs = ctx.out_exchange.write(plan.index, rows, bpr, nprng)
        disk_ms = written / (cal.SHUFFLE_DISK_WRITE_MBPS * io_speed * MiB) * 1e3 + cal.SHUFFLE_WRITE_FIXED_MS
        disk_ms *= lognormal(rng, 0.15)
        write_ms += disk_ms
        m.shuffle_write = ShuffleWriteMetrics(written, round(disk_ms * 1e6), recs)
        r = ctx.output_partitions
        result_bytes += r + 48 if r <= 2000 else 160 + r // 64  # Compressed / HighlyCompressedMapStatus
    elif isinstance(sink, WriteSink):
        nbytes = round(rows * sink.bytes_per_row * lognormal(rng, 0.02)) if rows else 0
        cpu_ns += rows * cal.PARQUET_ENCODE_NS_PER_ROW + nbytes * cal.PARQUET_ENCODE_NS_PER_BYTE
        write_ms += nbytes / (cal.HDFS_WRITE_MBPS * io_speed * MiB) * 1e3 + cal.TASK_COMMIT_MS * lognormal(rng, 0.3)
        m.output = OutputMetrics(nbytes, rows)
        output_file = (nbytes, rows) if rows else None
        result_bytes += 3_900 + rng.randint(0, 600)  # WriteTaskResult: commit message + written file stats
    elif isinstance(sink, BroadcastSink):
        result_bytes += rows * rb // 2  # rows collected to the driver (UnsafeRow, compressed)
    elif isinstance(sink, ResultSink):
        result_bytes += round(rows * sink.bytes_per_row)

    # ---------------------------------------------------------------- time
    cpu_ms = cpu_ns / 1e6 / host.speed * noise
    compile_ms = 0.0
    if env.first_on_executor:
        compile_ms = cal.CODEGEN_COMPILE_MS * lognormal(rng, 0.3)
    elif env.compile_wait_ms > 0:
        compile_ms = env.compile_wait_ms
    alloc_intensity = 1.25 if (fetching or mem_need > 0) else 1.0
    gc_ms = cpu_ms * cal.GC_SHARE_MEDIAN * alloc_intensity * lognormal(rng, cal.GC_SHARE_SIGMA)
    if env.cold_core:
        gc_ms += rng.uniform(*cal.GC_COLD_EXTRA_MS)
    compute_ms = cpu_ms + compile_ms + gc_ms
    # Remote fetches overlap computation; whatever doesn't fit behind it is exposed as fetch wait.
    if fetch_wait_ms:
        transfer_left = (rd.remote_bytes_read / (cal.SHUFFLE_REMOTE_MBPS * MiB) * 1e3) - compute_ms
        fetch_wait_ms = max(fetch_wait_ms, transfer_left) * lognormal(rng, 0.25)
        rd.fetch_wait_time = round(fetch_wait_ms)
    run_ms = compute_ms + io_ms + fetch_wait_ms + write_ms + spill_ms

    if env.cold_core:
        deser, share = cal.DESERIALIZE_COLD_JVM_MS, cal.DESERIALIZE_CPU_SHARE["cold"]
    elif env.first_on_executor:
        deser, share = cal.DESERIALIZE_NEW_STAGE_MS, cal.DESERIALIZE_CPU_SHARE["stage"]
    else:
        deser, share = cal.DESERIALIZE_WARM_MS, cal.DESERIALIZE_CPU_SHARE["warm"]
    deser_ms = max(1, round(deser * lognormal(rng, cal.DESERIALIZE_SIGMA)))
    ser_ms = rng.randint(*cal.RESULT_SERIALIZATION_MS)
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
    return TaskResult(m, deser_ms, run, ser_ms, sched_ms, round(exec_mem), fetching, output_file,
                      rows, trace)


def _apply(op: Op, rows: int, row_bytes, ctx: StageContext, plan: TaskPlan) -> int:
    """Output rows of one operator for this task."""
    if op.kind in ("filter", "bhj"):
        return round(rows * op.selectivity)
    if op.kind == "partial_agg":
        return expected_distinct(op.groups.distinct, rows)
    if op.kind == "final_agg":
        # The groups hashed to this task's reducers: every one of them shows up in its input.
        name, start, end = plan.reads[0]
        shares = ctx.exchange_for(name).shares
        groups = round(op.groups.distinct * float(shares[start:end].sum()))
        return expected_distinct(groups, rows) if rows else 0
    return rows
