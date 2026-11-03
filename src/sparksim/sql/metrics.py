"""SQL metric values: what each task reports for the plan nodes of its stage, and what the driver posts.

Task side (`task_updates`): the accumulator updates in `TaskEnd."Task Info".Accumulables`, exactly the
ones Spark reports. Sum metrics are reported when non-zero; size, timing and nsTiming metrics start at
-1, so they are reported whenever the code path touched them, even with 0 (`Sort` spill size,
`fetch wait time`); average metrics only when set to a positive value. Observed in the reference logs:

    Scan parquet          number of output rows, scan time (scan time even for empty splits)
    ColumnarToRow         number of output rows, number of input batches
    Filter / BHJ          number of output rows
    SortMergeJoin         number of output rows, spill size
    Sort                  sort time, peak memory, spill size (always)
    HashAggregate         number of output rows, peak memory, time in aggregation build, avg hash probes
    WholeStageCodegen     duration (always)
    Exchange (map side)   data size, shuffle bytes/records written, shuffle write time (tasks with rows)
    Exchange (read side)  local/remote blocks and bytes, records read, fetch wait time, remote reqs
                          duration, reported on the *upstream* Exchange node by tasks that fetched blocks
    Execute Insert...     task commit time

Timings come from the cost model's per-operator CPU work, scaled to the task's wall clock (host speed,
noise, GC). They nest the way Spark measures them: a WholeStageCodegen's duration and an aggregate's
build time run from the pipeline's start, so they include everything upstream in the stage; sort time is
only the sort itself (`UnsafeExternalSorter.getSortTimeNanos`).

Driver side: scan file metrics when the scan's RDD is built, the Exchange's partition count with its
shuffle dependency, AQEShuffleRead's partition specs, the broadcast's sizes and times after its job, and
the write command's totals after the job commit, each in its `Map`'s iteration order.
"""

from __future__ import annotations

from ..cost.model import TaskResult
from .nodes import (
    AQEShuffleRead,
    BroadcastExchange,
    BroadcastHashJoin,
    ColumnarToRow,
    DataWritingCommand,
    FileSourceScan,
    Filter,
    HashAggregate,
    QueryStage,
    ShuffleExchange,
    Sort,
    SortMergeJoin,
    SparkPlan,
    WholeStageCodegen,
    WriteFiles,
)

SORT_TIME_SHARE = 0.35  # of a Sort's CPU work: the in-memory sort itself (the rest is inserting rows)


def task_updates(stage_root: SparkPlan, res: TaskResult) -> list[tuple[int, int]]:
    """`(accumulator id, update)` for every SQL metric this task reports."""
    scale = res.wall_per_cpu_ms
    trace = {id(t.node): (i, t) for i, t in enumerate(res.trace) if t.node is not None}
    srcs = {id(s.node): s for s in res.sources if s.node is not None}
    out: list[tuple[int, int]] = []
    m = res.metrics

    def put(node: SparkPlan, key: str, v: float, always: bool = False) -> None:
        v = int(round(v))
        if v != 0 or always:
            out.append((node.metric_id(key), v))

    def own_ms(n: SparkPlan) -> float:
        hit = trace.get(id(n))
        if hit is None:
            return 0.0
        i, t = hit
        return t.cpu_ns / 1e6 * scale + (res.spill_ms if res.spill_op == i else 0.0)

    def read_ms(n: SparkPlan) -> float:
        s = srcs.get(id(n))
        return 0.0 if s is None else s.cpu_ns / 1e6 * scale + s.io_ms + s.fetch_wait_ms

    def elapsed(n: SparkPlan) -> float:
        """Wall time from the start of the stage's pipeline until `n` is exhausted."""
        if isinstance(n, FileSourceScan):
            return read_ms(n)
        if isinstance(n, AQEShuffleRead):
            return read_ms(n.children[0].plan)
        if isinstance(n, QueryStage):
            return 0.0 if n.broadcast else read_ms(n.plan)
        return own_ms(n) + sum(elapsed(c) for c in n.children)

    def rows_of(n: SparkPlan) -> int:
        hit = trace.get(id(n))
        return hit[1].rows_out if hit else 0

    def visit(n: SparkPlan, parent: SparkPlan | None) -> None:
        if isinstance(n, FileSourceScan):
            s = srcs[id(n)]
            put(n, "numOutputRows", s.rows)
            put(n, "scanTime", s.cpu_ns / 1e6 * scale + s.io_ms, always=True)
        elif isinstance(n, ColumnarToRow):
            s = srcs[id(_scan_below(n))]
            put(n, "numOutputRows", s.rows)
            put(n, "numInputBatches", s.batches)
        elif isinstance(n, (Filter, BroadcastHashJoin)):
            put(n, "numOutputRows", rows_of(n))
        elif isinstance(n, SortMergeJoin):
            put(n, "numOutputRows", rows_of(n))
            put(n, "spillSize", _spill(res, n, trace), always=True)
        elif isinstance(n, Sort):
            i, t = trace[id(n)]
            put(n, "sortTime", t.cpu_ns / 1e6 * scale * SORT_TIME_SHARE, always=True)
            put(n, "peakMemory", min(t.mem, max(res.exec_memory, 0)) if t.mem else 0, always=True)
            put(n, "spillSize", _spill(res, n, trace), always=True)
        elif isinstance(n, HashAggregate):
            i, t = trace[id(n)]
            put(n, "numOutputRows", t.rows_out)
            put(n, "peakMemory", min(t.mem, max(res.exec_memory, 0)), always=True)
            put(n, "aggTime", elapsed(n), always=True)
            if t.rows_out:
                cap = 1 << max(6, (max(1, t.rows_out) * 2 - 1).bit_length())
                put(n, "avgHashProbe", 10 * (1.0 + t.rows_out / cap))
            if res.spill_op == i:
                put(n, "spillSize", res.metrics.memory_bytes_spilled)
        elif isinstance(n, WholeStageCodegen):
            d = elapsed(n.children[0])
            if isinstance(parent, (ShuffleExchange, WriteFiles, BroadcastExchange)):
                d += res.sink_cpu_ns / 1e6 * scale  # the sink pulls rows through the pipeline
            put(n, "pipelineTime", d, always=True)
        elif isinstance(n, ShuffleExchange) and res.sink_rows:
            w = m.shuffle_write
            put(n, "dataSize", res.sink_rows * res.sink_row_bytes, always=True)
            put(n, "shuffleBytesWritten", w.bytes_written, always=True)
            put(n, "shuffleRecordsWritten", w.records_written)
            put(n, "shuffleWriteTime", w.write_time, always=True)
        elif isinstance(n, DataWritingCommand):
            put(n, "taskCommitTime", res.commit_ms, always=True)
        elif isinstance(n, (AQEShuffleRead, QueryStage)):
            stage = n.children[0] if isinstance(n, AQEShuffleRead) else n
            if not stage.broadcast:
                _read_metrics(stage.plan, srcs.get(id(stage.plan)), put)
            return
        for c in n.children:
            visit(c, n)

    visit(stage_root, None)
    return out


def _scan_below(n: SparkPlan) -> FileSourceScan:
    while not isinstance(n, FileSourceScan):
        n = n.children[0]
    return n


def _spill(res: TaskResult, n: SparkPlan, trace) -> int:
    hit = trace.get(id(n))
    return res.metrics.memory_bytes_spilled if hit and res.spill_op == hit[0] else 0


def _read_metrics(ex: ShuffleExchange, s, put) -> None:
    if s is None or s.stats is None:
        return
    st = s.stats
    if st.local_blocks + st.remote_blocks == 0:
        return
    put(ex, "remoteBlocksFetched", st.remote_blocks)
    put(ex, "localBlocksFetched", st.local_blocks)
    if st.remote_blocks:
        put(ex, "remoteBytesRead", st.remote_bytes, always=True)
        put(ex, "remoteReqsDuration", s.remote_reqs_ms, always=True)
    if st.local_blocks:
        put(ex, "localBytesRead", st.local_bytes, always=True)
    put(ex, "fetchWaitTime", s.fetch_wait_ms, always=True)
    put(ex, "recordsRead", st.records)


# --------------------------------------------------------------------------- driver-side metric updates


def scan_driver_updates(scan: FileSourceScan, num_files: int, files_bytes: int, listing_ms: int,
                        num_partitions: int) -> list[tuple[int, int]]:
    values = {"numFiles": num_files, "metadataTime": listing_ms, "filesSize": files_bytes,
              "numPartitions": num_partitions, "pruningTime": 0}
    return [(scan.metric_id(k), values[k]) for k in scan.driver_metric_keys()]


def exchange_driver_updates(ex: ShuffleExchange) -> list[tuple[int, int]]:
    return [(ex.metric_id("numPartitions"), ex.num_partitions)]


def read_driver_updates(read: AQEShuffleRead, sizes: list[int]) -> list[tuple[int, int]]:
    out = [(read.metric_id("numPartitions"), len(read.ranges))]
    if read.coalesced:
        out.append((read.metric_id("numCoalescedPartitions"), sum(1 for a, b in read.ranges if b - a > 1)))
    pid = read.metric_id("partitionDataSize")
    out += [(pid, int(sum(sizes[a:b]))) for a, b in read.ranges]
    return out


def broadcast_driver_updates(bx: BroadcastExchange, data_size: int, rows: int, collect_ms: int, build_ms: int,
                             broadcast_ms: int) -> list[tuple[int, int]]:
    from ..eventlog.ordering import scala_map_order

    values = {"dataSize": data_size, "numOutputRows": rows, "collectTime": collect_ms, "buildTime": build_ms,
              "broadcastTime": broadcast_ms}
    return [(bx.metric_id(k), values[k]) for k in scala_map_order(values)]


def write_driver_updates(cmd: DataWritingCommand, files: int, nbytes: int, rows: int, parts: int,
                         commit_ms: int) -> list[tuple[int, int]]:
    values = {"numFiles": files, "numOutputBytes": nbytes, "numOutputRows": rows, "numParts": parts,
              "jobCommitTime": commit_ms}
    return [(cmd.metric_id(k), values[k]) for k in cmd.driver_metric_keys()]


def reads_in(plan: SparkPlan) -> list[AQEShuffleRead]:
    """AQEShuffleRead nodes of a stage, in the order their RDDs are built (left to right)."""
    out = []

    def visit(n: SparkPlan) -> None:
        if isinstance(n, AQEShuffleRead):
            out.append(n)
            return
        if isinstance(n, (QueryStage,)):
            return
        for c in n.children:
            visit(c)

    visit(plan)
    return out


def scans_in(plan: SparkPlan) -> list[FileSourceScan]:
    out = []

    def visit(n: SparkPlan) -> None:
        if isinstance(n, FileSourceScan):
            out.append(n)
            return
        if isinstance(n, QueryStage):
            return
        for c in n.children:
            visit(c)

    visit(plan)
    return out

