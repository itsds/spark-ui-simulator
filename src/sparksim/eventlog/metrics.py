"""Task metrics, executor metrics and the internal-accumulator layout.

`TaskMetrics` mirrors `JsonProtocol.taskMetricsToJson`. Each task metric is
also an accumulator named `internal.metrics.<name>`. A stage attempt registers
all of them in one go, in `TaskMetrics.nameToAccums` order, so their IDs are
consecutive: `base + INTERNAL_ACCUM_OFFSETS[name]`. `TASK_METRIC_ACCUMS` maps
each accumulator to its `TaskMetrics` attribute path. The real logs confirm
this (`tests/test_eventlog_golden.py`).
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields

from .codec import JsonObject, jf


@dataclass
class PushBasedShuffleReadMetrics(JsonObject):
    corrupt_merged_block_chunks: int = jf("Corrupt Merged Block Chunks", default=0)
    merged_fetch_fallback_count: int = jf("Merged Fetch Fallback Count", default=0)
    remote_merged_blocks_fetched: int = jf("Merged Remote Blocks Fetched", default=0)
    local_merged_blocks_fetched: int = jf("Merged Local Blocks Fetched", default=0)
    remote_merged_chunks_fetched: int = jf("Merged Remote Chunks Fetched", default=0)
    local_merged_chunks_fetched: int = jf("Merged Local Chunks Fetched", default=0)
    remote_merged_bytes_read: int = jf("Merged Remote Bytes Read", default=0)
    local_merged_bytes_read: int = jf("Merged Local Bytes Read", default=0)
    remote_merged_reqs_duration: int = jf("Merged Remote Requests Duration", default=0)


@dataclass
class ShuffleReadMetrics(JsonObject):
    remote_blocks_fetched: int = jf("Remote Blocks Fetched", default=0)
    local_blocks_fetched: int = jf("Local Blocks Fetched", default=0)
    fetch_wait_time: int = jf("Fetch Wait Time", default=0)  # ms
    remote_bytes_read: int = jf("Remote Bytes Read", default=0)
    remote_bytes_read_to_disk: int = jf("Remote Bytes Read To Disk", default=0)
    local_bytes_read: int = jf("Local Bytes Read", default=0)
    records_read: int = jf("Total Records Read", default=0)
    remote_reqs_duration: int = jf("Remote Requests Duration", default=0)  # ms
    push: PushBasedShuffleReadMetrics = jf("Push Based Shuffle", default_factory=PushBasedShuffleReadMetrics)

    @property
    def total_bytes_read(self) -> int:
        return self.remote_bytes_read + self.local_bytes_read

    @property
    def total_blocks_fetched(self) -> int:
        return self.remote_blocks_fetched + self.local_blocks_fetched


@dataclass
class ShuffleWriteMetrics(JsonObject):
    bytes_written: int = jf("Shuffle Bytes Written", default=0)
    write_time: int = jf("Shuffle Write Time", default=0)  # ns
    records_written: int = jf("Shuffle Records Written", default=0)


@dataclass
class InputMetrics(JsonObject):
    bytes_read: int = jf("Bytes Read", default=0)
    records_read: int = jf("Records Read", default=0)


@dataclass
class OutputMetrics(JsonObject):
    bytes_written: int = jf("Bytes Written", default=0)
    records_written: int = jf("Records Written", default=0)


@dataclass
class TaskMetrics(JsonObject):
    executor_deserialize_time: int = jf("Executor Deserialize Time", default=0)  # ms
    executor_deserialize_cpu_time: int = jf("Executor Deserialize CPU Time", default=0)  # ns
    executor_run_time: int = jf("Executor Run Time", default=0)  # ms
    executor_cpu_time: int = jf("Executor CPU Time", default=0)  # ns
    peak_execution_memory: int = jf("Peak Execution Memory", default=0)
    result_size: int = jf("Result Size", default=0)
    jvm_gc_time: int = jf("JVM GC Time", default=0)  # ms
    result_serialization_time: int = jf("Result Serialization Time", default=0)  # ms
    memory_bytes_spilled: int = jf("Memory Bytes Spilled", default=0)
    disk_bytes_spilled: int = jf("Disk Bytes Spilled", default=0)
    shuffle_read: ShuffleReadMetrics = jf("Shuffle Read Metrics", default_factory=ShuffleReadMetrics)
    shuffle_write: ShuffleWriteMetrics = jf("Shuffle Write Metrics", default_factory=ShuffleWriteMetrics)
    input: InputMetrics = jf("Input Metrics", default_factory=InputMetrics)
    output: OutputMetrics = jf("Output Metrics", default_factory=OutputMetrics)
    # Only filled with spark.taskMetrics.trackUpdatedBlockStatuses=true (default false): always [] in practice.
    updated_blocks: list[dict] = jf("Updated Blocks", default_factory=list)

    def get(self, path: str) -> int:
        obj = self
        for part in path.split("."):
            obj = getattr(obj, part)
        return obj


# `TaskMetrics.nameToAccums` registration order -> (accumulator name, attribute path).
# `updatedBlockStatuses` (offset 10) is a list accumulator that JsonProtocol never writes.
TASK_METRIC_ACCUMS: tuple[tuple[str, str | None], ...] = (
    ("executorDeserializeTime", "executor_deserialize_time"),
    ("executorDeserializeCpuTime", "executor_deserialize_cpu_time"),
    ("executorRunTime", "executor_run_time"),
    ("executorCpuTime", "executor_cpu_time"),
    ("resultSize", "result_size"),
    ("jvmGCTime", "jvm_gc_time"),
    ("resultSerializationTime", "result_serialization_time"),
    ("memoryBytesSpilled", "memory_bytes_spilled"),
    ("diskBytesSpilled", "disk_bytes_spilled"),
    ("peakExecutionMemory", "peak_execution_memory"),
    ("updatedBlockStatuses", None),
    ("shuffle.read.remoteBlocksFetched", "shuffle_read.remote_blocks_fetched"),
    ("shuffle.read.localBlocksFetched", "shuffle_read.local_blocks_fetched"),
    ("shuffle.read.remoteBytesRead", "shuffle_read.remote_bytes_read"),
    ("shuffle.read.remoteBytesReadToDisk", "shuffle_read.remote_bytes_read_to_disk"),
    ("shuffle.read.localBytesRead", "shuffle_read.local_bytes_read"),
    ("shuffle.read.fetchWaitTime", "shuffle_read.fetch_wait_time"),
    ("shuffle.read.recordsRead", "shuffle_read.records_read"),
    ("shuffle.push.read.corruptMergedBlockChunks", "shuffle_read.push.corrupt_merged_block_chunks"),
    ("shuffle.push.read.mergedFetchFallbackCount", "shuffle_read.push.merged_fetch_fallback_count"),
    ("shuffle.push.read.remoteMergedBlocksFetched", "shuffle_read.push.remote_merged_blocks_fetched"),
    ("shuffle.push.read.localMergedBlocksFetched", "shuffle_read.push.local_merged_blocks_fetched"),
    ("shuffle.push.read.remoteMergedChunksFetched", "shuffle_read.push.remote_merged_chunks_fetched"),
    ("shuffle.push.read.localMergedChunksFetched", "shuffle_read.push.local_merged_chunks_fetched"),
    ("shuffle.push.read.remoteMergedBytesRead", "shuffle_read.push.remote_merged_bytes_read"),
    ("shuffle.push.read.localMergedBytesRead", "shuffle_read.push.local_merged_bytes_read"),
    ("shuffle.read.remoteReqsDuration", "shuffle_read.remote_reqs_duration"),
    ("shuffle.push.read.remoteMergedReqsDuration", "shuffle_read.push.remote_merged_reqs_duration"),
    ("shuffle.write.bytesWritten", "shuffle_write.bytes_written"),
    ("shuffle.write.recordsWritten", "shuffle_write.records_written"),
    ("shuffle.write.writeTime", "shuffle_write.write_time"),
    ("input.bytesRead", "input.bytes_read"),
    ("input.recordsRead", "input.records_read"),
    ("output.bytesWritten", "output.bytes_written"),
    ("output.recordsWritten", "output.records_written"),
)
INTERNAL_ACCUM_COUNT = len(TASK_METRIC_ACCUMS)  # 35 IDs per stage attempt
INTERNAL_ACCUM_OFFSETS = {name: i for i, (name, _) in enumerate(TASK_METRIC_ACCUMS)}

# Accumulators that a code path "touches" (LongAccumulator.add), so they're reported even when 0.
# Shuffle readers set every read metric (ShuffleReadMetrics.setMergeValues); sort/aggregate
# paths (ExternalSorter, Python aggregation) report spill counters even without spilling.
SHUFFLE_READ_ACCUMS = frozenset(
    name for name, _ in TASK_METRIC_ACCUMS if name.startswith(("shuffle.read.", "shuffle.push.read."))
)
SPILL_ACCUMS = frozenset({"memoryBytesSpilled", "diskBytesSpilled"})


# --------------------------------------------------------------------------- executor metrics

# ExecutorMetricType.metricToOffset order. Written as "Task Executor Metrics" (per-task peaks)
# and "Executor Metrics" (SparkListenerStageExecutorMetrics).
@dataclass
class ExecutorMetrics(JsonObject):
    jvm_heap_memory: int = jf("JVMHeapMemory", default=0)
    jvm_off_heap_memory: int = jf("JVMOffHeapMemory", default=0)
    on_heap_execution_memory: int = jf("OnHeapExecutionMemory", default=0)
    off_heap_execution_memory: int = jf("OffHeapExecutionMemory", default=0)
    on_heap_storage_memory: int = jf("OnHeapStorageMemory", default=0)
    off_heap_storage_memory: int = jf("OffHeapStorageMemory", default=0)
    on_heap_unified_memory: int = jf("OnHeapUnifiedMemory", default=0)
    off_heap_unified_memory: int = jf("OffHeapUnifiedMemory", default=0)
    direct_pool_memory: int = jf("DirectPoolMemory", default=0)
    mapped_pool_memory: int = jf("MappedPoolMemory", default=0)
    process_tree_jvm_vmemory: int = jf("ProcessTreeJVMVMemory", default=0)
    process_tree_jvm_rss_memory: int = jf("ProcessTreeJVMRSSMemory", default=0)
    process_tree_python_vmemory: int = jf("ProcessTreePythonVMemory", default=0)
    process_tree_python_rss_memory: int = jf("ProcessTreePythonRSSMemory", default=0)
    process_tree_other_vmemory: int = jf("ProcessTreeOtherVMemory", default=0)
    process_tree_other_rss_memory: int = jf("ProcessTreeOtherRSSMemory", default=0)
    minor_gc_count: int = jf("MinorGCCount", default=0)
    minor_gc_time: int = jf("MinorGCTime", default=0)
    major_gc_count: int = jf("MajorGCCount", default=0)
    major_gc_time: int = jf("MajorGCTime", default=0)
    total_gc_time: int = jf("TotalGCTime", default=0)

    def is_zero(self) -> bool:
        return not any(getattr(self, f.name) for f in fields(self))


__all__ = [
    "INTERNAL_ACCUM_COUNT", "INTERNAL_ACCUM_OFFSETS", "SHUFFLE_READ_ACCUMS", "SPILL_ACCUMS", "TASK_METRIC_ACCUMS",
    "ExecutorMetrics", "InputMetrics", "OutputMetrics", "PushBasedShuffleReadMetrics", "ShuffleReadMetrics",
    "ShuffleWriteMetrics", "TaskMetrics", "field",
]
