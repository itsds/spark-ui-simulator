# Decision log (ADRs)

Append new decisions at the bottom. Don't rewrite old ones; supersede them.

---

## ADR-001 — Render with the real Spark History Server from synthetic event logs

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** The first idea was a static HTML replica of the Spark UI. The
  owner then made the requirement explicit: "production-level Spark UI at any
  cost without any compromise in my learning". A running local server is fine.
- **Decision:** Generate Spark event logs and render them with the real
  History Server. Build no UI of our own except a small scenario portal.
- **Consequences:** Every page, sort, tooltip, DAG, timeline and REST endpoint
  is the real thing. The hard part moves to producing event logs that are
  valid and internally consistent. That needs reference logs and round-trip
  tests (Phases 1–2).

## ADR-002 — Target Apache Spark 3.5.9, YARN cluster mode

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** The owner targets Spark on YARN. The 3.5 line is still the most
  common in YARN production estates (EMR, CDP, on-prem Hadoop). 3.5.9 is the
  latest 3.5 patch on PyPI as of this date.
- **Decision:** Event logs follow the Spark 3.5 JsonProtocol and are rendered
  by History Server 3.5.9. YARN cluster deploy mode.
- **Revisit:** History Server 4.x can usually read 3.5 logs. Rendering the same
  logs on 4.x is an optional later phase.

## ADR-003 — Simulation-first metrics

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** All metrics come from workload model → cost model → scheduler
  simulation, seeded. Faults are injected as causes. See `ARCHITECTURE.md`
  ("realism rules").
- **Consequences:** Adding a scenario means describing a workload and a fault,
  not writing numbers. The cost model needs calibration.

## ADR-004 — Project memory lives in the repo

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** The owner works across several Claude sessions. A
  session-specific memory tool (agent-memory plugin) failed to connect in the
  founding session.
- **Decision:** `CLAUDE.md` + `docs/ROADMAP.md` + `docs/SESSION_LOG.md` +
  this file are the memory. They travel with every clone and Claude Code loads
  `CLAUDE.md` automatically.

## ADR-005 — Structured Streaming deferred

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** Batch only (Spark Core + SQL/DataFrame). The Streaming tab and
  scenarios are in the backlog.

## ADR-006 — Each incident scenario ships a "last good run"

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** Each scenario generates a healthy previous run of the same job
  alongside the bad run, so the investigation can compare the two the way
  real on-call does.

## ADR-007 — Toolchain

- **Date:** 2026-11-03
- **Status:** accepted (finalized in Phase 1)
- **Decision:** Python 3.11 managed by `uv`. Generator package `sparksim`.
  Real PySpark 3.5.9 only for capturing reference logs and for running the
  History Server locally (its bundled Spark distribution has
  `start-history-server.sh`, `spark-class` and the `spark-yarn` jar).
  **JDK 17** everywhere (`scripts/env.sh` picks it). History Server via
  `services/history-server/run.sh`, or Docker Compose (`apache/spark:3.5.9-java17`).
- **Evidence:** JDK 21 runs plain Spark 3.5.9 jobs but **breaks Arrow**:
  pandas UDFs, `mapInPandas` and Arrow `toPandas` fail with
  `UnsupportedOperationException: sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available`.
  Spark 3.5 officially supports Java 8/11/17 only.
- **Not verified:** the Docker Compose path (the cloud dev container has no
  Docker daemon). The image tag exists on Docker Hub.

## ADR-008 — Reference logs come from real YARN in cluster mode

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** `local-cluster[...]` mode was tried first. Its teardown in
  3.5.9 races and logs dozens of bogus `ExecutorRemoved` events ("Shutdown
  hooks cannot be modified during shutdown") for executors that never
  existed. It also lacks every YARN-specific field (container IDs,
  NodeManager log URLs, executor attributes, YARN exit codes).
- **Decision:** Capture reference logs with `spark-submit --master yarn
  --deploy-mode cluster` on a single-node Hadoop 3.3.6 cluster (HDFS + YARN,
  `reference/yarn/`). Spark jars come via `spark.yarn.archive`, the
  physical-memory check is on, and container logs are retained and copied to
  `reference/yarn-logs/`. `local-cluster` stays available
  (`REF_MODE=local-cluster`) as a fallback for machines without Hadoop.
- **Consequences:** Reference logs are exactly what a YARN production
  cluster writes, except for single-node artifacts (one host, all shuffle
  reads local), listed in `EVENT_LOG_FORMAT.md` §8.

## ADR-009 — Reference artifacts are committed uncompressed

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** Commit `reference/event-logs/*` (plain JSON lines, about 10 MB
  total) and `reference/yarn-logs/**` (container stdout/stderr) uncompressed,
  so sessions can grep them directly. Git compresses them in its object store.
  Generated scenario logs (`event-logs/`) are never committed.

## ADR-010 — Typed event model, strict parser, byte-identical round trip

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** The History Server only shows what is in the event log, and it
  silently ignores or mis-renders fields that are slightly wrong (string vs
  number, a missing optional key). We need proof that our writer emits exactly
  what Spark 3.5 emits.
- **Decision:** One dataclass per Spark type and event (`sparksim/eventlog/`).
  The JSON key and field order are declared on the field (`jf("Stage ID")`), and
  a generic codec maps dataclasses to JSON and back. The parser is strict: an
  unknown key, a missing key or a wrong scalar type raises `DecodeError`.
  `jsonfmt.dumps` reproduces Jackson's bytes (compact, raw UTF-8, upper-case
  `\uXXXX`). The test: parse each of the 3,327 lines in the reference logs, then
  re-serialize it. The result must be **byte-identical**.
- **Consequences:** A field Spark writes but we don't model fails the round trip,
  so nothing can be dropped silently. Events no reference log contains yet
  (`StageExecutorMetrics`, `SQLAdaptiveSQLMetricUpdates`) are marked unverified
  until a capture includes them.

## ADR-011 — The driver's bookkeeping is derived, never supplied

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Some event fields are pure driver bookkeeping: accumulator
  `Value` (a running total), which zero-valued accumulables are listed,
  task-metric accumulator IDs, and the stage accumulables in `StageCompleted`.
  If generators wrote them by hand they would drift from the task data
  (non-negotiable 2).
- **Decision:** `DriverLedger` derives them the way `DAGScheduler` does.
  Generators supply only task-level facts: `TaskMetrics`, the set of "touched"
  zero metrics, SQL metric updates, and IDs from `IdAllocator`. `Timeline`
  builds each event at emission time, so running totals follow log order. The
  golden test re-derives all 26,719 accumulables in the reference logs and
  matches every one (EVENT_LOG_FORMAT.md §5.1).
- **Consequences:** Phases 3–4 (scheduler, SQL metrics) emit events through the
  ledger and never compute `Value` or stage totals themselves.

## ADR-012 — The Phase 2 tiny app is a stand-in micro model

- **Date:** 2026-11-03
- **Status:** accepted; partly superseded by ADR-013 (its rates now live in `cost/calibration.py`)
- **Context:** Phase 2 needs a synthetic app before the cost model and the
  scheduler exist. Non-negotiable 2 forbids hand-typed metrics.
- **Decision:** `sparksim/apps/tiny.py` copies the structure of the real ref08
  job 0 (same PySpark RDD API, so RDD lineage, call sites, properties and
  accumulable sets are grounded). Its numbers come from an explicit workload
  (records, keys, bytes per record), a few rates of ref08's order of magnitude,
  seeded lognormal noise and a greedy slot scheduler. Tests enforce
  conservation, no oversubscribed core, time consistency and determinism.
- **Consequences:** The rates are named module constants. Phase 3 moves them
  into `cost/calibration.py` and replaces the greedy scheduler with the
  discrete-event one. The tiny app has no HDFS input: no reference capture uses
  `sc.textFile`, and we don't guess its lineage.

## ADR-013 — Production-scale apps run on a layered engine: model → cost → scheduler → emit

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Phase 3 needs apps with tens of executors and ~15k tasks whose
  numbers all come from causes (non-negotiables 2 and 3). The Phase 2 tiny app
  hard-wires one RDD job and a greedy slot loop, which doesn't scale to several
  jobs, AQE, locality or concurrent stages.
- **Decision:** Four layers, each testable on its own:
  `model/` (YARN cluster and container placement, HDFS blocks and Parquet row
  groups, Spark's file-split algorithm, operator pipelines per stage, shuffle
  block matrices, AQE coalescing), `cost/` (`calibration.py` + a per-task phase
  model), `sched/` (a discrete-event kernel and a port of `TaskSchedulerImpl` /
  `TaskSetManager` delay scheduling, including the Spark 3.1+ timer-reset rule
  and noPref tasks reported as PROCESS_LOCAL), and `engine/` (the driver: AQE
  query stages as map-stage jobs, re-created skipped stages, RDD lineage and
  scopes, heartbeat-sampled executor metrics, emission through `Timeline` +
  `DriverLedger`). Where Spark has an algorithm we port it (maxSplitBytes,
  Next Fit Decreasing packing, block hosts, preferred locations, coalescing,
  delay scheduling) instead of approximating its output.
- **Consequences:** Scenario 00 (15,243 tasks) simulates and writes in ~10 s.
  numpy becomes a runtime dependency (block matrices). The tiny app stays on its
  own loop: it is a PySpark RDD app with Python-worker costs the engine doesn't
  model yet.

## ADR-014 — Cost model v1: per-stream rates on shared nodes, durations fixed at launch

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** The reference runs give overheads and shapes (deserialization,
  scheduler delay, GC share, result sizes) but not production throughputs: they
  ran on one oversubscribed VM.
- **Decision:** Throughputs are per task stream on a *shared* production node
  (other tenants use most of the cluster), tagged `[norm]` in
  `calibration.py`; overheads are tagged `[ref]` with the reference medians.
  A task's duration is computed once at launch from its data, host speed,
  locality and JVM warmth. There is no dynamic contention between concurrent
  tasks yet.
- **Consequences:** Healthy stages come out at max/median 1.25–1.45 with ~2 %
  GC. Faults that work through contention (a saturated disk, a noisy neighbour)
  must be modelled as host properties (`Host.speed`, per-host I/O rates) until a
  contention model exists.

## ADR-015 — `spark.sql.execution.id` is emitted together with the SQL events (Phase 4)

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Real DataFrame jobs carry `spark.sql.execution.id` and
  `spark.sql.execution.root.id` in their properties. Tested on History Server
  3.5.9: when jobs reference an execution id with no `SQLExecutionStart`, the
  `/api/v1/.../sql` endpoint answers 404 ("no such app").
- **Decision:** Phase 3 jobs carry everything else a SQL job carries (the whole
  Spark conf, `spark.rdd.scope`, `spark.job.description`, the broadcast job's
  tags, `__fetch_continuous_blocks_in_batch_enabled`) but not the execution ids.
  Phase 4 adds the ids and the SQL events in one step.

## ADR-016 — JVM collection iteration orders are reproduced, not guessed

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Several event-log fields come from iterating hash collections, so
  their order looks random: `JobStart."Stage IDs"` (Scala 2.12
  `mutable.HashSet[Int]`), `Properties` (JDK 17 `java.util.Properties`, backed
  by a `ConcurrentHashMap`), and the `EnvironmentUpdate` maps (Scala immutable
  `HashMap`, a hash trie).
- **Decision:** `eventlog/ordering.py` implements the three orders from the
  hash functions. `tests/test_ordering.py` checks them against every reference
  log: 34 multi-stage jobs, 178 property sets, 36 environment maps. Within a
  `ConcurrentHashMap` bin the order is insertion order, which we take from the
  caller. `RDD Info` order after the first entry depends on identity hash codes,
  which are random per JVM; we use a seeded shuffle there.

## ADR-017 — Scenario evidence lives in `solution.md` and is checked against the REST API

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** Each `solution.md` has an Evidence table with rows
  `| E<n> | where | what | `value` |`. `scripts/evidence.py` recomputes every
  value from a running History Server, formatted like the UI (`UIUtils`,
  `Utils.bytesToString`). The integration test fails when a number in the
  solution no longer matches what the server shows. The numbers are written in
  one place only, the solution. `uv run scripts/evidence.py <scenario>` shows
  the new values after a model change.

## ADR-018 — The SQL layer ports Spark's planner pieces and is golden-tested against a real execution

- **Date:** 2026-11-03
- **Status:** accepted; supersedes the hand-written stage pipelines of ADR-013 (`StageSpec`s are now derived)
- **Context:** The SQL tab shows a plan graph, an explain text and per-operator metrics, and AQE rewrites the plan
  several times during one execution. Real logs expose details that only come out right if the mechanism is the same:
  metric accumulator ids change exactly for the nodes AQE copies, explain operator ids are per node *object*,
  scope ids follow the order of `execute()`/`produce()` calls.
- **Decision:** `sparksim/sql/` models Spark's physical operators (node names, simple strings, explain fields, metric
  maps in definition order) and ports the algorithms around them: a small DataFrame API with the optimizer rules
  that shape plans (filter pushdown, `isnotnull` inference, column pruning, CollapseProject, join strategy,
  EnsureRequirements, partial/final aggregation, V1Writes sort), `createQueryStages` / `newQueryStage` /
  CoalesceShufflePartitions / ApplyColumnarRulesAndInsertTransitions / CollapseCodegenStages, re-planning after
  every completed stage (always adopted: every re-plan in ref01 was), `ExplainUtils.processPlan`,
  `SparkPlanInfo.fromSparkPlan` with lazily allocated metric ids (pre-order), and an RDD-scope simulator. The engine
  drives one `AdaptiveExecution` per query and runs the query stages it creates.
- **Proof:** `tests/test_sql_golden.py` writes `ref01-daily-etl-aqe` execution 0 with our DSL (only the analyzer's
  aggregate expression ids are pinned) and reproduces all 5 plan events byte for byte, explain text and SparkPlanInfo
  with every accumulator id, except `[plan_id=N]` (a JVM-wide counter of every SparkPlan Catalyst instantiates; we
  emit plausible increasing values). Scope and RDD ids of the single-threaded AQE rounds match exactly; the round
  where the broadcast runs on its own thread matches in shape and id set (the interleaving is seeded). Metric
  definitions are checked against all 300+ plan nodes in the 9 reference logs.
- **Not modelled yet:** exchange reuse, subqueries / dynamic partition pruning, skew-join splits and local shuffle
  reads in AQEShuffleRead, AQE's runtime SMJ→BHJ switch, collect/limit actions (TakeOrderedAndProject,
  CollectLimit), Python UDF nodes. The 4 scope ids the driver opens after a broadcast stage completes, and the 2 / 4-7
  before each execution, are taken from the reference logs, not from a mechanism.

## ADR-019 — SQL metric values: Spark's reporting rules, timings nested like Spark measures them

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** `sql/metrics.py` derives each task's SQL accumulator updates from the cost model's per-task
  result, which now keeps per-operator CPU work and memory, per-source reads and the sink's per-row work.
  Reporting follows `SQLMetric`: sum metrics when non-zero; size/timing/nsTiming whenever the code path touched them
  (they start at -1, so a 0 is reported); average metrics only when positive. Which operator touches what was read
  off the reference logs (EVENT_LOG_FORMAT.md §5.4). Shuffle-read metrics go to the *upstream* Exchange node.
  Timings nest: a WholeStageCodegen's `duration` and an aggregate's `time in aggregation build` run from the
  pipeline's start (everything upstream in the stage), `sort time` is only the sort (35 % of the Sort's work),
  `scan time` is the source's read + decode. Driver-side metrics are posted when Spark posts them, in the iteration
  order of the metric `Map`s. Tests check that SQL totals equal the task metrics they shadow (rows, shuffle bytes,
  write time, records read, written rows/files) and that timings fit inside the task's run time.
- **Consequences:** The History Server's "total (min, med, max)" strings, the plan graph's row counts and the stage
  pages agree with each other by construction.

## ADR-020 — Row sizes and read fractions come from table schemas; Scenario 00 re-baselined

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Phase 3 apps gave each operator an UnsafeRow size and each scan a read fraction by hand. With real
  schemas in the plan (`ReadSchema`, `Output [n]: [...]`), hand-picked sizes would contradict what the UI shows.
- **Decision:** Tables declare their columns with an average string length (UnsafeRow sizing: null bitset + 8 bytes
  per field + variable data rounded to 8) and their Parquet bytes per row (read fraction = columns read / all).
  Column pruning decides which columns a scan reads. Two model fixes surfaced by the SQL tab's row counts:
  (1) files written right after a hash shuffle record the key they are clustered on (`HdfsFile.key_fraction`), so a
  partial aggregation on that key meets only the keys of its files (Scenario 00's activity query: 4.06 B rows →
  61 M, not 3.8 B); (2) a final aggregation outputs every key of its reducers (`min(groups, rows)`), not a
  random-draw estimate. Also: an empty map task reports 0 shuffle write time (ref01 stage 5).
- **Consequences:** Scenario 00 changed: 9,448 tasks (was 15,243; `events_enriched` is 34.6 B/row, not 82, so the
  downstream scans have 600 tasks), 5.5 min, the customers scan reads 1.7 GiB of its 58.3 GiB (5 of 20 columns).
  `solution.md` was rewritten from `scripts/evidence.py`; the "2x tasks from file packing" lesson no longer occurs
  and was replaced by the SQL-tab lessons (E15–E19).

## ADR-021 — YARN allocation runs inside the simulation; Scenario 00 re-baselined

- **Date:** 2026-11-03
- **Status:** accepted; replaces the static `place_application` of ADR-013
- **Context:** Phase 3 placed every executor before the run. Executor loss, replacement containers and
  dynamic allocation need containers requested, granted and released *during* the run, by the same
  mechanism.
- **Decision:** `engine/yarn_am.py` ports Spark 3.5.9's `ApplicationMaster.allocationThreadImpl` +
  `YarnAllocator` (requests = target − pending − running; poll every 200 ms doubling to the 3 s heartbeat
  while requests or loss-reason queries are pending; woken when the driver raises the target) and models
  the RM side: one OFF_SWITCH container per NodeManager heartbeat on nodes with room, and YARN's absolute
  asks, re-sent before the AM matched the containers in the same response. That last rule reproduces the
  over-allocation seen on our reference cluster ("Received 1 containers from YARN, launching executors on 0
  of them", container 000005 of ref01) and YARN-1902 in production; the extras are released and use up
  container numbers. Container ids carry the RM epoch (`e23`), `CLUSTER_ID` the RM HA cluster id.
- **Consequences:** Scenario 00 shifted slightly (executors register over ~4 s instead of ~3 s): 9,447 tasks
  (was 9,448: one fewer coalesced partition), E6/E10/E12/E14/E18/E19 re-derived with `scripts/evidence.py`.
  Locality requests (`LocalityPreferredContainerPlacementStrategy`), preemption and queue limits are not
  modelled yet.

## ADR-022 — Container memory is a model of RSS; YARN kills follow from it

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Scenario 03 (memoryOverhead kills) needs YARN to kill containers *because* their physical
  memory exceeds the container size, not on a schedule.
- **Decision:** `model/memory.py` computes what the NodeManager measures every 3 s: committed heap (JVM
  default `-Xms` = RAM/64, growing towards `-Xmx` with busy core-seconds, never below 1.25x the live heap)
  + G1 native structures (4.5 % of committed) + metaspace/code cache + netty direct buffers + thread stacks
  + malloc arenas per core (+ Python workers), each JVM with a seeded spread (`[norm]` constants in
  `calibration.py`). Above the container size the container is killed with Hadoop's diagnostics; the
  driver path follows Spark (block manager removed at disconnect, ExecutorLostFailure when the AM gives the
  reason, ExecutorRemoved twice), the tasks re-run, YARN starts a replacement.
- **Consequences:** Scenario 00 peaks at 16.4–17.2 GiB of its 18 GiB containers (tested: busy executors commit
  their heap, nothing is killed). A 384 MiB overhead on a 512 MB allocation grid kills executors from
  minute 2 on (`tests/test_executor_loss.py`). Executor loss requires `spark.shuffle.service.enabled`
  (cluster default here); without it the engine stops with NotImplementedError until FetchFailed / stage
  retry (Scenario 12) exist. A task failing `spark.task.maxFailures` times raises `TaskSetAborted` (job
  abort is not modelled yet).

## ADR-023 — Dynamic allocation is a port of ExecutorAllocationManager

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** `engine/dynalloc.py`: the 100 ms `schedule()` loop, `maxNeeded = ceil((pending + running) ×
  ratio / cores)`, exponential ramp-up after `schedulerBacklogTimeout`, immediate target drop, idle timeout
  (60 s) with the `minExecutors`/target guards, `initializing` until the first stage; kills go through the
  AM (release → NodeManager SIGTERM → "Executor killed by driver."). Shuffle files don't pin executors
  because the external shuffle service serves them.
- **Consequences:** Scenario 14 (slow ramp / queue starvation) and dynamic-allocation thrash can be built
  from causes. Scenario 00 keeps `spark.dynamicAllocation.enabled=false`.

## ADR-024 — Container logs are derived from the run and served by a NodeManager mock

- **Date:** 2026-11-03
- **Status:** accepted
- **Context:** Phase 5's done-when: an executor's stderr link in the History Server opens a log that matches
  what that executor did. The links in the event log point at production NodeManager hosts.
- **Decision:** `sparksim/yarnlogs` writes `yarn-logs/<appId>/<containerId>/{stdout,stderr}` from the run's
  records only (tasks, timings, file splits, shuffle blocks, broadcasts, writes, spills, kills). Message
  texts come from the reference container logs and the Spark/Hadoop source; the driver's per-job INFO
  lines from the source (our capture runs the driver at WARN). `services/nm-logs` serves them under the
  NodeManager's `/node/containerlogs/<container>/<user>/<file>?start=-4096` paths, and the History Server's
  `spark.history.custom.executor.log.url` points every executor's and the driver's links at it. The mock
  NodeManager is not the Spark UI, so it is ours to build (non-negotiable 1 is about Spark's pages).
- **Proof:** `tests/test_scenario00_hs.py::test_executor_log_links_open_matching_logs` opens all 41 links
  through the real History Server and checks each executor's `Running task … (TID n)` set against the
  REST task list.
- **Consequences:** ~22 MB of logs for Scenario 00 (~5 s). Not modelled: GC logs, Python worker output,
  `ShufflePartitionsUtil` / `FileSourceStrategy` planning lines, ContextCleaner removals.

## ADR-025 — The Environment tab comes from a real driver's configuration plus a production site config

- **Date:** 2026-11-03
- **Status:** accepted
- **Decision:** Hadoop Properties = the 974 keys a real Spark 3.5.9 driver on Hadoop 3.3.6 logged (ref01),
  with the 19 keys our harness set reset to Hadoop's defaults (`scripts/extract_hadoop_defaults.py` →
  `data/hadoop-3.3.6-defaults.json`), plus the production cluster's site files derived from `ClusterSpec`
  (RM HA + recovery, NodeManager resources and dirs, `spark_shuffle` aux service, log aggregation,
  topology script). Spark Properties start from the cluster's `spark-defaults.conf`
  (`CLUSTER_SPARK_DEFAULTS`: shuffle service, native library paths, SHS address). System Properties have
  every key the reference AMs logged.
- **Consequences:** The configuration the learner reads (allocation grid, NodeManager memory, overhead,
  shuffle service, dirs) is the one the simulation ran with, and the container logs use the same dirs.
