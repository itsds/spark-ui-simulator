# Spark 3.5 event logs → Spark UI

How the Spark History Server turns an event log into every page of the UI, and
what our generator must emit. Field-level schemas are generated from the real
reference logs in [`event-log-schema.md`](event-log-schema.md). When this doc
and a real log disagree, **the real log wins**; fix this doc.

## 1. The data path

```
 production Spark app                                    Spark History Server (what we run)
 ────────────────────                                    ───────────────────────────────────
 DAGScheduler / TaskScheduler / SQL                       FsHistoryProvider scans spark.history.fs.logDirectory
        │  post SparkListenerEvent                                 │
        ▼                                                          ▼
 LiveListenerBus ──► EventLoggingListener ──► event log ──► ReplayListenerBus (reads the file line by line)
                     (spark.eventLog.enabled)   file               │
                                                                   ├─► AppStatusListener     → jobs, stages, tasks, executors, RDDs, env
                                                                   └─► SQLAppStatusListener  → SQL executions, plan graphs, SQL metrics
                                                                              │
                                                                              ▼
                                                                   KVStore (in memory, or LevelDB/RocksDB on disk)
                                                                              │
                                                              ┌───────────────┴────────────────┐
                                                              ▼                                ▼
                                                      UI pages (Jobs, Stages,          REST API /api/v1/...
                                                      Storage, Environment,            (same KVStore)
                                                      Executors, SQL / DataFrame)
```

**The consequence for us:** the UI holds no data that isn't in the event log.
Everything the UI computes during replay comes from task-level events: stage
totals, job progress bars, task-metric quantiles (min/25th/median/75th/max),
"Aggregated Metrics by Executor", executor totals, SQL metric
`total (min, med, max (stageId: taskId))` strings and timelines. So the
generator writes task-level facts, and the History Server does the
aggregation, exactly as in production.

## 2. File naming and location

| Aspect | Spark 3.5 behaviour |
|---|---|
| Name (YARN cluster mode) | `application_<clusterTimestamp>_<seq>_<attemptId>`, e.g. `application_1793676968612_0002_1` |
| Name (client mode / no attempt) | `application_<clusterTimestamp>_<seq>` (`app-<yyyyMMddHHmmss>-<seq>` on standalone, `local-<ts>` locally) |
| In progress | suffix `.inprogress`; the History Server shows it as incomplete |
| Compression | the extension names the codec: `.lz4`, `.lzf`, `.snappy`, `.zstd`. **Any other `.ext` breaks loading**, so don't put dots in file names |
| Format | UTF-8 JSON, one event per line (JsonProtocol). The first line is `SparkListenerLogStart` |
| Rolling logs | `eventlog_v2_<appId>/events_<n>_<appId>` when `spark.eventLog.rolling.enabled`. Not used by us |

The History Server reads the app ID and attempt from `SparkListenerApplicationStart`,
not from the file name. Reference logs are stored as `reference/event-logs/<ref-name>`.

## 3. Canonical event order (one app)

```
SparkListenerLogStart                     {"Spark Version": "3.5.9"}
SparkListenerResourceProfileAdded         default profile 0: executor cores/memory/offHeap, task cpus
SparkListenerBlockManagerAdded            driver block manager
SparkListenerEnvironmentUpdate            JVM info, Spark/Hadoop/System properties, classpath  → Environment tab
SparkListenerApplicationStart             App ID, attempt, user, start time, driver log URLs/attributes
SparkListenerExecutorAdded  (×N)          executor id, host, cores, YARN log URLs + attributes    → Executors tab, timeline
SparkListenerBlockManagerAdded (×N)       per-executor storage memory                             → Executors "Storage Memory"
  per SQL query (DataFrame action):
  SQLExecutionStart                       executionId, description, physicalPlanDescription, sparkPlanInfo → SQL tab
  SQLAdaptiveExecutionUpdate (0..n)       AQE re-plans: replaces plan text + graph
  SQLAdaptiveSQLMetricUpdates (0..n)      metrics of nodes AQE added (not observed in our reference logs)
    per Spark job:
    SparkListenerJobStart                 job id, Stage Infos (incl. ones that will be skipped), Properties
                                          (spark.sql.execution.id links job ↔ SQL; spark.job.description;
                                           spark.jobGroup.id; callSite.short/long)
      per stage attempt:
      SparkListenerStageSubmitted         Stage Info (RDD Info lineage + scopes → DAG visualization)
      SparkListenerTaskStart (×tasks)     Task Info at launch
      SparkListenerTaskGettingResult      only for results fetched from the block manager (big results)
      SparkListenerTaskEnd   (×tasks)     Task End Reason, Task Info (+Accumulables), Task Metrics,
                                          Task Executor Metrics
      SparkListenerStageCompleted         Stage Info with Submission/Completion Time, Accumulables, Failure Reason
    SparkListenerJobEnd                   JobSucceeded | JobFailed {Exception}
  DriverAccumUpdates                      driver-side SQL metrics (files read, metadata time, broadcast build…)
  SQLExecutionEnd
SparkListenerUnpersistRDD                 when an RDD/DataFrame is unpersisted
SparkListenerBlockManagerRemoved          lost executor's block manager (ref05)
SparkListenerExecutorRemoved              executor id + Removed Reason (YARN: exit codes, "Container killed…")
SparkListenerApplicationEnd
```

Optional events and their switches (production defaults **off**):

- `SparkListenerBlockUpdated`: `spark.eventLog.logBlockUpdates.enabled`. **Without it the History Server's Storage tab is empty**, even if the app cached data.
- `SparkListenerStageExecutorMetrics`: `spark.eventLog.logStageExecutorMetrics`. Per-stage executor memory peaks.
- `SparkListenerExecutorMetricsUpdate`: heartbeats are not logged by default.

## 4. Event → UI mapping

| UI element | Fed by | Key fields |
|---|---|---|
| History listing (app name, user, duration, Spark version) | `ApplicationStart`, `ApplicationEnd`, `LogStart` | `App Name`, `User`, `Timestamp`, `Spark Version` |
| Jobs tab table | `JobStart`, `JobEnd`, task/stage events | `Job ID`, `Submission Time`, `Completion Time`, `Properties.spark.job.description`, `callSite.short` |
| Jobs "Event Timeline" | `ExecutorAdded/Removed`, `JobStart/End` | executor `Timestamp`, `Removed Reason` |
| Job detail: stage list, skipped stages | `JobStart.Stage Infos` vs submitted stages | a stage in `Stage Infos` that is never submitted shows as **skipped** |
| DAG visualization (job and stage) | `Stage Info.RDD Info` | `RDD ID`, `Name`, `Scope` (JSON `{"id","name"}` → cluster boxes like `WholeStageCodegen (2)`, `Exchange`), `Callsite`, `Parent IDs` |
| Stages tab | `StageSubmitted/Completed` + task totals | `Stage Name`, `Number of Tasks`, `Details` (stack trace), `Failure Reason` |
| Stage "Summary Metrics" quantiles | every `TaskEnd` of the stage attempt | `Task Metrics.*`, `Task Info.Launch/Finish/Getting Result Time` |
| Stage "Aggregated Metrics by Executor" | `TaskEnd` grouped by `Task Info.Executor ID` | same |
| Stage task table | `TaskStart` + `TaskEnd` | `Index`, `Attempt`, `Locality`, `Host`, `Executor ID`, `Task End Reason` (errors column) |
| Stage "Event Timeline" (per-task colored bars) | `TaskEnd` timings | scheduler delay = duration − run − deserialize − result serialization − getting result; shuffle read/write time; compute |
| Executors tab | `ExecutorAdded/Removed`, `BlockManagerAdded`, `TaskEnd` | cores, `Maximum Memory`, `Task Metrics` totals, GC time (**cell turns red when GC > 10% of task time**), `Log Urls` |
| Executors "Peak memory" columns | `TaskEnd.Task Executor Metrics` (+ `StageExecutorMetrics` if logged) | `JVMHeapMemory`, `OnHeapExecutionMemory`, `ProcessTreeJVMRSSMemory`, … |
| Executor stdout/stderr links | `ExecutorAdded.Executor Info.Log Urls` / `Attributes` | YARN: `CONTAINER_ID`, `NM_HOST`, `NM_HTTP_PORT`, `USER`, `LOG_FILES`, `HTTP_SCHEME` |
| Storage tab | `BlockUpdated` (+ `StageSubmitted.RDD Info.Storage Level`) | only if block updates are logged |
| Environment tab | `EnvironmentUpdate` | `JVM Information`, `Spark Properties`, `Hadoop Properties`, `System Properties`, `Classpath Entries` |
| SQL / DataFrame list | `SQLExecutionStart/End`, job↔SQL link | `executionId`, `description`, `time`, jobs via `spark.sql.execution.id` |
| SQL plan graph + operator metrics | `sparkPlanInfo` (latest AQE update wins) + accumulator updates | node `metrics[].accumulatorId` ↔ `TaskEnd.Task Info.Accumulables[].ID` and `DriverAccumUpdates` |
| SQL "Details" text | `physicalPlanDescription` (latest AQE update wins) | `== Physical Plan ==` formatted explain |

## 5. Accumulators: how SQL metrics get their numbers

- Each SQL operator metric (e.g. `number of output rows`, `data size`,
  `spill size`, `shuffle bytes written`) is an **accumulator**. It's declared
  in `sparkPlanInfo` as `{name, accumulatorId, metricType}` with types `sum`,
  `size`, `timing`, `nsTiming` and `average`.
- Each `TaskEnd` carries that task's contribution in
  `Task Info.Accumulables` as `{ID, Name, Update, Value, Internal, Count Failed Values, Metadata: "sql"}`.
  `SQLAppStatusListener` aggregates the updates per metric across the
  execution's stages to render `total (min, med, max (stageId: taskId))`.
- Driver-side metrics such as `number of files read`, `metadata time`,
  `size of files read` and broadcast `time to build` arrive as
  `DriverAccumUpdates` (`accumUpdates: [[id, value], …]`).
- Task metrics also appear as accumulators named `internal.metrics.*`.
  `Stage Info.Accumulables` in `StageCompleted` holds the stage totals of
  both kinds. Spark writes these, so the generator writes them too, and they
  must equal the sum of the task updates.
- Accumulator IDs are unique per app and increase over its lifetime.

### 5.1 The driver's accumulator bookkeeping (verified on all 9 reference logs)

`sparksim.eventlog.driver.DriverLedger` implements these rules.
`tests/test_eventlog_golden.py` re-derives all 26,719 accumulables in
the reference logs from task metrics alone and matches every one.

| Rule | Detail |
|---|---|
| `Value` is a running total | `Task Info.Accumulables[].Value` = the driver's accumulator after merging this task (`DAGScheduler.updateAccumulators`). Task-metric accumulators are fresh per stage attempt, so the total is per attempt. SQL metrics belong to a plan node, so their total spans every task that updates them, across stages. |
| Number vs string | `internal.metrics.*` → JSON numbers. Every other accumulator (SQL metrics included) → strings, e.g. `"Update":"166666"`. Nameless updates inside `ExceptionFailure."Accumulator Updates"` → strings, `"Internal": false`. |
| Zero updates are dropped… | A task lists an accumulator only if its update is non-zero (`!updates.isZero`). |
| …unless the code path touched it | `LongAccumulator.add(0)` counts as non-zero. Every reduce task lists **all 17** shuffle-read and push-read metrics. Sort/aggregate paths list `memoryBytesSpilled`/`diskBytesSpilled` = 0. Size/timing SQL metrics start at -1, so a reported 0 (`spill size`, `fetch wait time`) is a real update. |
| Who merges | Only `Success`, `ExceptionFailure` and `TaskKilled` merge updates. `FetchFailed` and `ExecutorLostFailure` tasks have empty `Accumulables` and no `Task Metrics`. Failed tasks report no SQL metrics, because those are registered with `countFailedValues=false`. |
| Task-metric IDs | Each stage attempt registers 35 consecutive IDs at submission, in `TaskMetrics.nameToAccums` order (`metrics.TASK_METRIC_ACCUMS`). Offsets: 0 executorDeserializeTime … 4 resultSize, 5 jvmGCTime, 9 peakExecutionMemory, 10 updatedBlockStatuses (never written), 11–17 shuffle.read.*, 18–25 shuffle.push.read.*, 26 remoteReqsDuration, 27 remoteMergedReqsDuration, 28–30 shuffle.write.*, 31–32 input.*, 33–34 output.*. PySpark takes ID 0 at startup (PythonAccumulatorV2), so in an RDD-only app stage 0 uses 1–35 and stage 1 uses 36–70. |
| Order | `Accumulables` lists are sorted by ID (`JsonProtocol.accumulablesToJson`). `updatedBlockStatuses` is never written. |
| Stage accumulables | `StageCompleted` lists every accumulator the stage's tasks updated, with its latest `Value` and no `Update`. They are recorded on the stage's *latest* attempt (`stage.latestInfo`). |
| JSON bytes | Compact, raw UTF-8, `\uXXXX` with upper-case hex for other control characters, `/` not escaped (Jackson defaults). `tests/test_eventlog_roundtrip.py` reproduces all 3,327 reference lines byte for byte. |

### 5.2 Collection orders (verified on all 9 reference logs, `eventlog/ordering.py`)

| Field | Collection Spark iterates | Order |
|---|---|---|
| `JobStart."Stage IDs"`, `"Stage Infos"` | `DAGScheduler.jobIdToStageIds`: Scala 2.12 `mutable.HashSet[Int]` | open addressing on `byteswap32`, table 32 → 64 at 45 % load |
| `JobStart/StageSubmitted."Properties"` | JDK 17 `java.util.Properties` (a `ConcurrentHashMap`) | bins by `spread(String.hashCode)`; 16 bins, doubling at 75 %; insertion order inside a bin |
| `EnvironmentUpdate` maps | `Seq[(String, String)].toMap`: Scala immutable `HashMap` | hash-trie order (5 bits per level, `improve(hashCode)`); ≤ 4 entries: insertion order |
| `Stage Info."RDD Info"` | the stage RDD, then `getNarrowAncestors` (`HashSet[RDD]`, identity hash) | random per JVM run: seeded shuffle |

### 5.3 Other driver behaviour the generator follows

- **Job properties of DataFrame jobs** carry the whole Spark conf (every key of
  the Environment tab's Spark Properties), plus `spark.rdd.scope` (absent on
  write jobs), `spark.job.description`, `spark.sql.execution.id` /
  `.root.id`, `__fetch_continuous_blocks_in_batch_enabled` once a
  `ShuffledRowRDD` was created on the thread, and on broadcast jobs
  `spark.job.interruptOnCancel` + `spark.job.tags`. A job that names an
  execution id without its `SQLExecutionStart` makes the History Server's
  `/sql` endpoint answer 404 (ADR-015).
- **AQE** materializes each shuffle with its own map-stage job. The query's
  last job re-creates the finished map stages under new ids. They appear in
  `JobStart` but are never submitted, so the UI lists them as skipped and they
  reuse the original stage's RDD infos.
- **Task Executor Metrics** are the per-metric peaks of the executor's metric
  polls during the task. With `spark.executor.metrics.pollingInterval = 0` the
  polls happen at heartbeats (every 10 s, first one 10–20 s after start), so
  short tasks report all zeros.
- **Locality labels.** Tasks with no preferred location are dequeued from the
  `noPrefs` list and reported as `PROCESS_LOCAL`. `FilePartition` drops
  `localhost` from its block hosts, which is why the reference scans (HDFS on
  `localhost`) show PROCESS_LOCAL. Reduce tasks only get a preferred host when
  the shuffle has < 1000 map and < 1000 reduce partitions and one executor
  holds ≥ 20 % of the input.

## 6. YARN-specific fields (captured on real YARN, cluster mode)

- `ApplicationStart`: `App ID` = `application_…`, `App Attempt ID` = `"1"`, plus driver `Driver Logs`/`Driver Attributes` (the AM container).
- `ExecutorAdded.Executor Info`:
  `Log Urls: {stdout, stderr: http://<nm-host>:8042/node/containerlogs/<container>/<user>/<file>?start=-4096}`
  `Attributes: {CONTAINER_ID, NM_HOST, NM_PORT, NM_HTTP_PORT, NM_HTTP_ADDRESS, HTTP_SCHEME, USER, LOG_FILES, CLUSTER_ID}`.
  These attributes are the template variables for the History Server's
  `spark.history.custom.executor.log.url`, which we'll point at the mock
  NodeManager log server in Phase 5.
- Container IDs: `container_<clusterTs>_<appSeq>_<attempt:02>_<n:06>`. Container
  `000001` is the ApplicationMaster, which holds the driver in cluster mode.
  Executors come after it.
- `EnvironmentUpdate`: `spark.master=yarn`, `spark.submit.deployMode=cluster`,
  `spark.yarn.app.id`, `spark.yarn.archive`, AM filter properties, Hadoop
  properties from the cluster config.
- Executor removal reasons come in YARN wording (see `ref05-failures`).

## 7. PySpark tells

- Call sites (stage names, the Jobs tab "Description" fallback, `callSite.short`) come in two shapes:
  - **Actions with PySpark call-site capture** (`collect`, `count`,
    `toPandas`, RDD actions): `<action> at <python file>:<line>`. In YARN
    cluster mode the file is the script inside the AM container's working
    dir, e.g.
    `collect at /…/nm-local/usercache/root/appcache/application_1793676968612_0002/container_1793676968612_0002_01_000001/ref01_daily_etl_aqe.py:36`.
    On a production cluster that becomes
    `/<yarn.nodemanager.local-dirs>/usercache/<user>/appcache/<appId>/<AM container>/<job>.py:<line>`.
  - **Calls going straight through Py4J** (reads/writes such as
    `spark.read.parquet`, `df.write…parquet`, schema-inference/listing jobs):
    `parquet at NativeMethodAccessorImpl.java:0`.
  - AQE/broadcast jobs started on helper threads:
    `$anonfun$withThreadLocalCaptured$1 at FutureTask.java:264`.
- `callSite.short` is absent from `JobStart.Properties` for many jobs; the UI
  then falls back to the stage name.
- Python UDFs appear as `BatchEvalPython` (row UDF) or `ArrowEvalPython`
  (pandas UDF), and `mapInPandas` as `MapInPandas`. Python executors also
  carry Python-specific memory metrics.
- `spark.submit.pyFiles`, `spark.yarn.isPython=true` and
  `spark.executorEnv.PYTHONPATH` show up in the Environment tab.

## 8. Known artifacts of our reference cluster (don't copy them into the generator)

| Artifact | Why | Generator must instead |
|---|---|---|
| Host `localhost` everywhere | single-node YARN | use realistic multi-host NodeManager hostnames |
| Shuffle "remote" fetches are loopback | one host; the harness sets `spark.shuffle.readHostLocalDisk=false` so cross-executor reads go over the network (otherwise all reads are host-local **and a dead executor's shuffle files stay readable, so no FetchFailed**) | split remote/local by placement (≈ (N−1)/N remote across N hosts), realistic network fetch times |
| Tiny sizes and durations | small reference data | come from the cost model at production scale |
| `ref*` app names, `/data/ref` paths, `REF_*` env properties | reference harness | use the scenario's realistic job names, tables and properties |
| `Locality` mostly `NODE_LOCAL`/`PROCESS_LOCAL` | HDFS blocks on the only node | model locality from block placement |

## 9. Reference apps and what each one is for

| Log | Captures |
|---|---|
| `ref00-datagen` | partitioned parquet writes (`InsertIntoHadoopFsRelationCommand`, `WriteFiles`), 600-file write, write metrics |
| `ref01-daily-etl-aqe` | Spark 3.5 defaults: AQE coalesce, BroadcastHashJoin + SortMergeJoin, partition pruning, aggregation, partitioned write, `limit`/`collect` |
| `ref02-static-plan-spill` | AQE off: static plan, fixed partitions, memory and disk **spill**, window function |
| `ref03-skew-join` | skewed join without AQE skew handling (straggler task), then with `OptimizeSkewedJoin` (AQEShuffleRead skew metrics) |
| `ref04-cache-reuse` | recomputation vs cache, `BlockUpdated` events, Storage tab, `InMemoryTableScan`, `UnpersistRDD` |
| `ref05-failures` | task retry (`ExceptionFailure`), failed job, executor killed (YARN exit status) → `ExecutorLostFailure` / `FetchFailed` → stage attempt 1 |
| `ref06-python-udf` | `BatchEvalPython`, `ArrowEvalPython`, `MapInPandas`, native-expression baseline |
| `ref07-driver-bound` | big `collect()`/`toPandas()` (result size, getting-result time), driver-side gaps between jobs, small-files scan + listing |
| `ref08-rdd-api` | pure RDD jobs (no SQL tab), `groupByKey` vs `reduceByKey`, `sortByKey` sampling job, RDD join |

Regenerate everything: `reference/yarn/cluster.sh start && reference/capture.sh && uv run scripts/event_inventory.py`.
