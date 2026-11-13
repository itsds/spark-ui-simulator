"""Every throughput, overhead and noise constant the simulation uses, with where it came from.

Provenance tags:

* **[ref]** measured on our real Spark 3.5.9 / YARN reference runs
  (`reference/event-logs`, single node). We take overheads and shapes from
  them (deserialization, scheduler delay, GC share, result sizes, timing gaps),
  not throughputs: the reference box is one oversubscribed VM.
* **[norm]** a production order of magnitude for Spark 3.5 on JDK 17,
  x86 servers at 2–3 GHz, 10 GbE, HDFS on JBOD disks. These are the usual
  figures from Spark tuning guides and vendor sizing docs, rounded. Phase 6+
  scenarios may override a few of them per cluster (e.g. a degraded disk).
* **[spark]** a Spark/YARN/HDFS default or source-code constant.

Rates are per task thread (one core) unless noted. Times are ms, sizes bytes.
"""

from __future__ import annotations

MiB = 1024 * 1024
GiB = 1024 * MiB

# ============================================================================ scheduling / driver

# [ref] Launch time → finish time minus (deserialize + run + result serialization): median 11 ms,
# p10 6 ms, p90 45 ms over 1,100 reference tasks. Production drivers with ~150 concurrent tasks sit
# at the low end; we model the RPC + result handling as lognormal around this median.
SCHEDULER_DELAY_MEDIAN_MS = 9.0
SCHEDULER_DELAY_SIGMA = 0.55
# [ref] TaskEnd → next TaskStart on the same core: 0 ms in most cases (same status-update round), ≤ 40 ms.
RELAUNCH_GAP_MS = (0, 4)
# [spark] spark.scheduler.revive.interval
REVIVE_INTERVAL_MS = 1_000
# [spark] spark.locality.wait (process/node/rack all default to it)
LOCALITY_WAIT_MS = 3_000
# [ref] Killing a task attempt (speculation's "another attempt succeeded", a result stage's "Stage finished"):
# the driver's KillTask RPC reaches the executor in 1–6 ms; the task thread notices it at its next record
# (or its I/O is interrupted), closes its resources and reports KILLED. ref09: 7–136 ms on its healthy-ish
# executors (median ~50 ms), seconds on the CPU-throttled host. The notice-and-report part is CPU work, so
# it scales with the host's degradation (`Host.cpu_factor`).
KILL_RPC_MS = (1, 6)
KILL_HANDLING_MEDIAN_MS = 45.0
KILL_HANDLING_SIGMA = 0.8
# [ref] StageSubmitted → first TaskStart for a newly submitted stage: 2–25 ms (task binary broadcast).
STAGE_LAUNCH_DELAY_MS = (3, 22)
# Per task launched in one offer round: TaskSetManager serializes each task (~0.2 ms each).
LAUNCH_SPACING_MS = 0.25
# [ref] last TaskEnd → StageCompleted 1–3 ms; StageCompleted → JobEnd 1–5 ms.
STAGE_COMPLETE_DELAY_MS = (1, 3)
JOB_END_DELAY_MS = (1, 5)
# [ref] JobStart → StageSubmitted 2–25 ms.
JOB_TO_STAGE_MS = (2, 25)
# [ref] A job aborted by spark.task.maxFailures (ref10/ref11, both attempts each): the TaskEnd of the loss that
# was the last straw → the failed stage's StageCompleted 10–18 ms (the DAGScheduler's taskSetFailed); → its
# JobEnd 4–19 ms later; AQE then cancels the query's other running jobs: their StageCompleted ("Job N
# cancelled ") 13–17 ms after the failed job's end, their JobEnd 2–5 ms after that.
ABORT_STAGE_DELAY_MS = (10, 18)
ABORT_JOB_END_MS = (4, 19)
AQE_CANCEL_MS = (13, 17)
# [ref] The failed job's end → the query's SQLExecutionEnd (errorMessage) 116–240 ms (AQE cleans up, the
# Py4JJavaError reaches Python); → the AM's "User application exited with status 1", the shutdown hook's
# SparkContext.stop() and ApplicationEnd 154–244 ms after that.
QUERY_FAIL_TO_SQL_END_MS = (115, 240)
FAILED_DRIVER_EXIT_MS = (150, 245)
# [ref] A failed AM attempt's ApplicationEnd → the next attempt's SparkContext start: 2.6–3.7 s (ref10, ref11:
# the AM exits, the RM allocates and launches a new AM container, the Python driver starts again). A
# production RM with busy NodeManagers is a little slower.
AM_RETRY_TO_START_MS = (2_600, 5_200)
# [ref] AQE: query-stage job end → next query-stage job start 80–500 ms (re-optimization, new stages).
AQE_REPLAN_MS = (90, 420)
# [ref] Broadcast job starts ~50 ms after the shuffle jobs submitted with it.
BROADCAST_JOB_LAG_MS = (20, 80)

# ============================================================================ SQL driver [ref]/[spark]

# [norm] InMemoryFileIndex listing on the driver (NameNode listStatus + block locations), per file.
# Feeds the scan's "metadata time" and the planning gap before an execution starts.
FILE_LISTING_MS_PER_FILE = 0.06
# [ref] SQLExecutionStart → first query-stage job: 300–500 ms in ref01 (createQueryStages, AQE update,
# RDD building, task binary broadcast).
SQL_START_TO_STAGES_MS = (180, 480)
# [ref] RDDOperationScope ids the driver opens outside stage materialization (ref01/ref04/ref07): 2 before
# the app's first SQL execution, 4-7 before every later one, 4 after a broadcast query stage completes.
FIRST_EXECUTION_SCOPES = 2
EXECUTION_SCOPES = (4, 7)
SCOPES_AFTER_BROADCAST = 4
# BroadcastExchangeExec after its collect job: build the HashedRelation, then TorrentBroadcast it.
# [ref] ref01: 5,000 rows, "time to build" 71 ms (cold JIT), "time to broadcast" 7 ms.
BROADCAST_BUILD_FIXED_MS = 12.0
BROADCAST_BUILD_NS_PER_ROW = 400.0
BROADCAST_FIXED_MS = 4.0
BROADCAST_MBPS = 400.0
# [spark] LongToUnsafeRowMap.estimatedSize = key array (8 B per key, dense ids) + row page (1 MiB, doubling):
# ref01's 5,000-row relation is exactly 1,048,576 + 40,000 = 1,088,576 bytes ("data size").
HASHED_RELATION_BYTES_PER_ROW = 8
HASHED_RELATION_MIN_BYTES = 1024 * 1024

# [spark] spark.executor.heartbeatInterval: executor metrics are polled at heartbeats
# (spark.executor.metrics.pollingInterval=0), so a task's "Task Executor Metrics" are the peaks seen
# by the heartbeats that happened while it ran: all zero for tasks shorter than one heartbeat gap.
HEARTBEAT_INTERVAL_MS = 10_000

# ============================================================================ YARN / executor start-up

# [ref] ApplicationStart → driver BlockManagerAdded ≈ 1.0 s.
DRIVER_BLOCK_MANAGER_MS = (900, 1_100)
# [ref]/[norm] driver start → YarnAllocator's first container request (SparkContext + AM registration).
AM_REQUEST_DELAY_MS = (1_300, 2_100)
# [spark] yarn.resourcemanager.nodemanagers.heartbeat-interval-ms; one OFF_SWITCH container per node
# heartbeat (yarn.scheduler.capacity.per-node-heartbeat.maximum-offswitch-assignments = 1).
NM_HEARTBEAT_MS = 1_000
# [spark] spark.yarn.scheduler.initial-allocation.interval (AM allocate() polling while requests are pending)
AM_ALLOCATE_INTERVAL_MS = 200
# [ref]/[norm] container launch → executor registered with the driver: NM startContainer, localization
# of the (cached) spark.yarn.archive, JVM start, CoarseGrainedExecutorBackend registration.
# Reference: first executor 6–7 s after app start on a cold single node; production nodes ~3–5 s.
EXECUTOR_START_MS = (2_900, 4_800)
# [norm] NodeManager resource localization (--files / --jars on HDFS): one FSDownload per resource, a single
# HDFS read stream into the NM's usercache, ~150 MB/s on a busy production DataNode.
LOCALIZE_MBPS = 150
# [ref] ExecutorAdded → executor BlockManagerAdded 60–200 ms.
EXECUTOR_BLOCK_MANAGER_MS = (60, 200)
# [spark] spark.scheduler.minRegisteredResourcesRatio on YARN (0.8) and maxRegisteredResourcesWaitingTime.
MIN_REGISTERED_RATIO = 0.8
MAX_REGISTERED_WAIT_MS = 30_000

# [norm] Host-to-host CPU/disk speed spread inside one hardware generation (lognormal sigma).
HOST_SPEED_SIGMA = 0.035
# [norm] Per-task multiplicative noise on CPU work (JIT state, cache misses, co-tenant load).
TASK_NOISE_SIGMA = 0.07

# ============================================================================ task overheads

# [ref] Executor Deserialize Time. First task on each core of a fresh JVM: median 690 ms (class loading,
# first Kryo/closure deserialization). First task of a stage on an executor: task-binary broadcast fetch.
# Later tasks: a few ms on a warm production JVM (reference: 37 ms median on the overloaded box).
DESERIALIZE_COLD_JVM_MS = 520.0
DESERIALIZE_NEW_STAGE_MS = 70.0
DESERIALIZE_WARM_MS = 4.0
DESERIALIZE_SIGMA = 0.35
# Share of deserialize wall time that is CPU on the task thread.
DESERIALIZE_CPU_SHARE = {"cold": 0.25, "stage": 0.55, "warm": 0.7}
# [norm] Janino compilation of a stage's whole-stage-codegen classes, paid by the first task of the stage on
# each executor (the CodeGenerator cache is per JVM). Shows up in run time of first-wave tasks.
CODEGEN_COMPILE_MS = 260.0
# [ref] Result Serialization Time: 0–4 ms.
RESULT_SERIALIZATION_MS = (0, 3)
# [ref] DirectTaskResult size: accumulator updates + value. ShuffleMapTask ≈ 1.7 KB + MapStatus.
RESULT_BASE_BYTES = 1_640
RESULT_JITTER_BYTES = 120

# ============================================================================ CPU work [norm]

# Vectorized Parquet read: decompression (snappy) + decoding of the selected column chunks
# (string-heavy schemas: ~80 MB/s of compressed column data per core).
PARQUET_DECODE_NS_PER_BYTE = 12.0
# [spark] Parquet footer read per file opened by a scan task (FileMetaData; input bytes include it).
PARQUET_FOOTER_READ = 48 * 1024
# ColumnarToRow + UnsafeRow assembly for ~25 selected columns.
SCAN_NS_PER_ROW = 120.0
# Whole-stage-codegen operators (per input row of the operator).
FILTER_PROJECT_NS_PER_ROW = 25.0
BROADCAST_PROBE_NS_PER_ROW = 60.0
HASH_AGG_NS_PER_ROW = 85.0
SORT_NS_PER_ROW = 150.0  # UnsafeExternalSorter: prefix radix sort + record copy
SMJ_NS_PER_ROW = 60.0
# Shuffle write (UnsafeShuffleWriter): row copy, partition id, in-memory sort, LZ4 compression.
SHUFFLE_SERIALIZE_NS_PER_BYTE = 11.0  # per uncompressed UnsafeRow byte ≈ 90 MB/s
# Shuffle read: LZ4 decompression + UnsafeRow deserialization.
SHUFFLE_DESERIALIZE_NS_PER_BYTE = 3.0  # per compressed byte read
# Parquet write: dictionary/RLE encoding + snappy.
PARQUET_ENCODE_NS_PER_ROW = 120.0
PARQUET_ENCODE_NS_PER_BYTE = 4.0  # per output (compressed) byte

# ============================================================================ I/O [norm]

# Per task stream, on a *shared* production node: 12 JBOD disks (~1.5 GB/s) and one 10 GbE NIC serve
# ~40 concurrent tasks of every tenant, so one stream gets far less than a disk can do alone.
# HDFS reads: NODE_LOCAL uses short-circuit local reads; remote reads go through a DataNode; cross-rack
# traffic also crosses the oversubscribed spine.
HDFS_READ_MBPS = {"NODE_LOCAL": 95.0, "RACK_LOCAL": 72.0, "ANY": 55.0}
HDFS_READ_OPEN_MS = 6.0  # per file opened (NameNode getBlockLocations + DataNode connect)
# Shuffle write: disk throughput while flushing / merging spill files into the map output (Shuffle Write Time).
SHUFFLE_DISK_WRITE_MBPS = 120.0
SHUFFLE_WRITE_FIXED_MS = 5.0  # data + index file create, rename, fsync
# Shuffle read: local blocks come from local disk/page cache; remote blocks over the network.
SHUFFLE_LOCAL_READ_MBPS = 250.0
SHUFFLE_REMOTE_MBPS = 90.0  # one reduce task's fetch rate (NIC shared by every task on the node)
SHUFFLE_FETCH_OVERLAP = 0.78  # share of remote transfer hidden behind compute (spark.reducer.maxSizeInFlight)
SHUFFLE_REQUEST_LATENCY_MS = 1.5  # per remote request round trip
SHUFFLE_BLOCKS_PER_REQUEST = 40  # blocks batched per fetch request (≈ maxSizeInFlight / 5 / block size)
# Parquet write to HDFS: 3-replica pipeline.
HDFS_WRITE_MBPS = 70.0
TASK_COMMIT_MS = 22.0  # [ref] "task commit time" ≈ 20 ms (FileOutputCommitter v1 rename)
# [norm] Driver-side v1 job commit: one rename per task attempt directory, done serially by the driver.
JOB_COMMIT_MS_PER_FILE = 5.5
JOB_COMMIT_FIXED_MS = 350.0  # _SUCCESS marker + metastore partition update

# ============================================================================ JVM [ref]/[norm]

# [ref] JVM GC Time / Executor Run Time: median 2.8 % (p25 1.7 %, p75 4.7 %) on reference tasks > 1 s.
GC_SHARE_MEDIAN = 0.024
GC_SHARE_SIGMA = 0.45
GC_COLD_EXTRA_MS = (40, 140)  # heap warm-up on a fresh JVM's first tasks
YOUNG_GC_PAUSE_MS = 22.0  # average G1 young pause, for executor GC counts
# [spark] Memory: reserved 300 MiB, spark.memory.fraction 0.6, spark.memory.storageFraction 0.5.
RESERVED_MEMORY = 300 * MiB
MEMORY_FRACTION = 0.6
STORAGE_FRACTION = 0.5
# [spark] spark.buffer.pageSize default: nextPowerOf2(maxExecutionMemory / cores / 16), clamped to [1 MiB, 64 MiB].
PAGE_SIZE_MIN = 1 * MiB
PAGE_SIZE_MAX = 64 * MiB
# Bytes of sort-pointer array per record in UnsafeInMemorySorter (pointer + key prefix, x2 for radix).
SORT_POINTER_BYTES = 32
# [spark] UnsafeShuffleWriter (SortShuffleManager picks it over BypassMergeSortShuffleWriter above
# spark.shuffle.sort.bypassMergeThreshold reduce partitions when nothing is combined map-side, i.e. every SQL
# shuffle): ShuffleExternalSorter keeps each record serialized in execution-memory pages, behind a 4-byte
# length (UnsafeRowSerializer), and one 8-byte pointer per record in a LongArray that radix sort uses only
# half of (ShuffleInMemorySorter.getUsableCapacity). It spills when it can't get another page, and
# UnsafeShuffleWriter.stop() adds its peak to the task's Peak Execution Memory (verified on 3.5.9).
SHUFFLE_BYPASS_MERGE_THRESHOLD = 200
SHUFFLE_RECORD_LENGTH_BYTES = 4
SHUFFLE_POINTER_BYTES = 16
# Hash map overhead per group in BytesToBytesMap / UnsafeFixedWidthAggregationMap.
HASH_MAP_BYTES_PER_GROUP = 48
# [ref]/[norm] Executor heap shape for heartbeat snapshots.
JVM_BASE_HEAP = 160 * MiB
JVM_HEAP_PER_RUNNING_TASK = 55 * MiB  # transient young-gen allocation of a running task
JVM_OFF_HEAP_BASE = 105 * MiB  # metaspace + code cache + threads (reference: 72–131 MiB)
JVM_OFF_HEAP_PER_STAGE = 1_500_000  # metaspace growth per distinct stage run (generated classes)
DIRECT_POOL_BASE = 21 * MiB  # [ref] 12–34 MiB
DIRECT_POOL_PER_FETCH = 9 * MiB  # netty buffers per reduce task fetching

# ============================================================================ container physical memory [norm]/[spark]
# What the NodeManager's ContainersMonitor measures (process-tree RSS) against the container size
# (spark.executor.memory + memoryOverhead, rounded up to yarn.scheduler.minimum-allocation-mb).

# [spark] yarn.nodemanager.container-monitor.interval-ms
NM_MONITOR_INTERVAL_MS = 3_000
# [spark] JVM ergonomics: without -Xms (Spark sets only -Xmx) the initial heap is 1/64 of the host's RAM.
JVM_INITIAL_HEAP_FRACTION_OF_RAM = 1 / 64
# [norm] G1 grows the committed heap under allocation pressure (GCTimeRatio) and almost never gives it
# back while tasks run: committed = Xms + (Xmx - Xms) * (1 - exp(-busy core-seconds / tau)), and never
# below 1.25x the live heap. A busy executor commits its whole -Xmx within a few minutes.
G1_HEAP_GROWTH_CORE_S = 240.0
G1_HEAP_GROWTH_SIGMA = 0.35  # lognormal spread of tau between executor JVMs (allocation patterns differ)
G1_COMMIT_HEADROOM = 1.25
# [norm] Native memory outside the heap: G1 remembered sets, card table and mark bitmaps (~4-5 % of the
# committed heap), thread stacks actually touched, glibc malloc arenas used by task threads (compression
# codecs, JNI) per core. Metaspace + code cache and netty direct buffers are the JVM_OFF_HEAP_* and
# DIRECT_POOL_* figures above. Together ≈ 6-8 % of the heap: why Spark's default overhead is 10 %.
G1_NATIVE_FRACTION = 0.045
NATIVE_FOOTPRINT_SIGMA = 0.06
THREAD_STACKS_RSS = 48 * MiB
# [norm] A task's lookups into a memory-mapped file (an IP-geolocation database read by a UDF) touch most of its
# pages within the first third of the task's rows; touched pages stay resident (RSS) while the mapping lives.
UDF_NATIVE_RAMP = 0.3
MALLOC_ARENA_RSS_PER_CORE = 40 * MiB
# [spark] yarn.nodemanager.sleep-delay-before-sigkill.ms (SIGTERM → SIGKILL); a JVM running shutdown hooks
# exits after ~100-250 ms, so the container exits with 143 (SIGTERM).
NM_SIGKILL_DELAY_MS = 250
JVM_SIGTERM_EXIT_MS = (90, 240)

# ============================================================================ data formats [norm]

LZ4_SHUFFLE_RATIO = 0.42  # compressed / UnsafeRow bytes for typical mixed-type rows
SHUFFLE_BYTES_PER_EMPTY_BLOCK = 0

# ============================================================================ PySpark RDD API (Phase 2 tiny app)

# Order of magnitude from ref08 job 0 (same API on the same Spark).
PY_NS_PER_RECORD_MAP = 2_700  # Python lambda + PairwiseRDD batching, per input record
PY_NS_PER_RECORD_REDUCE = 9_000  # Python merge of (key, count) pairs, per shuffled record
PY_JVM_CPU_SHARE = 0.11  # JVM task-thread CPU / run time: the work runs in the Python worker process
PY_WORKER_MS = {"cold": 260, "warm": 140}  # per-task Python worker hand-off (fork on first use, then reuse)
PY_DESERIALIZE_MS = {"cold_jvm": 480, "cold_stage": 210, "warm": 9}  # first task per core / new stage / later
PY_SCHEDULER_OVERHEAD_MS = (18, 45)  # launch RPC + status update, uniform range
PY_SHUFFLE_WRITE_FIXED_MS = 32  # open, fsync, index file commit
PY_SHUFFLE_WRITE_BPS = 150e6
PY_NETWORK_BPS = 90e6  # one remote fetch stream
PY_REMOTE_FETCH_LATENCY_MS = 4
PY_HOST_SPEED_SIGMA = 0.05
PY_TASK_NOISE_SIGMA = 0.08
