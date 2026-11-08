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

## ADR-026 — Scenarios are YAML over Python workloads; incidents run the same job twice

- **Date:** 2026-11-04
- **Status:** accepted; supersedes the per-app `SPEC` of ADR-013/020 (`apps/daily_enrichment.py` is now a workload)
- **Context:** Phase 6 needs scenarios that differ by data, config, nodes and failures while the job's code
  stays the same, and every incident needs its last good run (ADR-006). DataFrame code doesn't belong in
  YAML; seeds, dates, cluster, spark-submit options and faults do.
- **Decision:** A *workload* (`apps/<module>.py`, `WORKLOAD = Workload(...)`) is the PySpark job: its params
  (the date), its usual input data (`datasets(params)`: files, sizes, and the key distributions of the columns
  its queries shuffle on) and its DataFrame queries, which take their `KeySpace`s from those datasets. A
  *scenario* (`scenarios/<id>/scenario.yaml`, pydantic schema in `sparksim/scenario/schema.py`) names the
  workload and gives the job (app name, user, queue, cluster, executors, `--conf` in order) and its runs:
  either one `baseline` (no faults) or `last_good` + `incident` (faults only in the incident, the last good
  run first). Each run has its app sequence number, seed, start time, params, data overrides and faults.
  The schema checks units, roles, fault fields and hosts; loading also builds every run's `AppSpec`, so
  unknown datasets, key columns or params fail at `sparksim validate`. `sparksim generate <id>` writes every
  run's event log and container logs; `scripts/evidence.py` takes app ids from the YAML.
- **Consequences:** Scenario 00's YAML reproduces the old `SPEC` byte for byte (checked before adding
  `spark.sql.adaptive.skewJoin.enabled=false` to the job's conf, which changes no number: all 19 evidence
  values held). Scenario 01's last good run *is* Scenario 00's run (`test_last_good_run_is_scenario_00`),
  so the learner compares the incident with the baseline they already studied. pydantic and PyYAML are
  runtime dependencies.

## ADR-027 — Skewed keys: Spark's own hash for hot keys, Zipf head by rank, tail by its second moment

- **Date:** 2026-11-04
- **Status:** accepted
- **Decision:** `KeySpace` gains `hot` (explicit values with a share of the rows), `dtypes` and `zipf`.
  A hot key goes to `pmod(Murmur3Hash(key, 42), n)`, a port of `Murmur3_x86_32` (int, bigint, string,
  date, multi-column folding) checked against PySpark's `F.hash` and `spark_partition_id()`
  (`customer_id = -1` → partition 1993 of 2400). A Zipf law places its top 4,096 ranks one by one (a seeded
  32-bit hash per rank, so a key keeps its partition for any n) and turns the rest into an equivalent
  rows-per-key CV for the existing gamma spread. Exchanges keep the tail's shares separately
  (`cluster_shares`): files written from a reducer range are clustered on the share of *distinct* keys,
  which a hot key barely changes. Unskewed keys take exactly the old code path (Scenario 00 unchanged).
- **Consequences:** Skew appears only in shuffles keyed on the skewed column, in the partition Spark would
  pick, and the dimension side of a join stays even. Partial aggregation still collapses a hot key (it
  writes distinct keys).

## ADR-028 — Spill is per operator; a straggler's sorters spill in their own windows

- **Date:** 2026-11-04
- **Status:** accepted; refines ADR-014's single-operator spill
- **Context:** A skewed sort-merge join partition is buffered twice in the same task: by the `Sort` under
  the join and by the `Sort` before the partitioned write. Spark spills both, and the SQL tab shows each
  node's `spill size`; the old model charged everything to the biggest operator.
- **Decision:** Memory consumers that hold memory at the same time (the sorts below a join) form a group;
  each operator after the join is its own group. Within a group the biggest consumer spills first, down to
  half the task's execution memory left in memory (a single consumer of M > A spills M − A/2, as before).
  Each operator keeps its spill bytes, its spill I/O time (write + read back) and its spill CPU
  (serialization + compression), which count in its SQL timings. Executor logs write one
  `UnsafeExternalSorter … spilling sort data … (n times so far)` sequence per sorter, spread over the
  time that sorter fills (from the task's cost breakdown).
- **Limitation:** a task's execution memory is fixed at (heap − 300 MiB) × 0.6 / cores. In Spark a task
  alone on its executor may grow to the whole pool, so a straggler's *peak execution memory* is
  under-reported (2.4 GiB instead of up to ~9 GiB); its spill volume barely changes (280 GiB either way).
  Dynamic memory sharing belongs with the contention model (ADR-014).

## ADR-029 — Fault mechanisms change inputs; what Spark would do and we don't model is refused

- **Date:** 2026-11-04
- **Status:** accepted
- **Decision:** Five fault types, each an input of the simulation (`sparksim/scenario/faults.py`):
  `key_skew` (ADR-027); `config` (set/unset `--conf`; `spark.executor.memory/cores/instances/memoryOverhead`
  change the containers); `slow_node` (a host's CPU, disk and NIC factors: CPU work, local disk I/O, remote
  shuffle fetch, non-local HDFS reads, HDFS writes); `memory_pressure` (native memory per running task in the
  container RSS model → NodeManager pmem kills); `executor_killed` (SIGKILL at a time: exit 137 with the
  NodeManager's "Killed by external signal" diagnostics from ref05, ExecutorLostFailure, re-run, replacement
  container, a log that stops without shutdown hooks). The engine now also reads
  `spark.sql.autoBroadcastJoinThreshold` and AQE's coalescing settings (enabled, advisory size, minimum
  size, parallelismFirst). Each mechanism has a test on a scaled Scenario 00 (`tests/test_faults.py`).
- **Guard:** Spark's default `spark.sql.adaptive.skewJoin.enabled=true` would split a join partition above
  max(256 MiB, 5 × median). The split isn't modelled yet, so the engine raises NotImplementedError instead of
  producing a skew Spark wouldn't show; `spark.sql.adaptive.enabled=false` is refused the same way.
- **Not yet:** task exceptions (ExceptionFailure, retries, job abort), node loss with lost shuffle files,
  the skew-join split itself (a "fixed run" of Scenario 01 needs it), GC pressure.

## ADR-030 — Scenario 01: a guest sentinel key after an upstream SDK change, AQE skew join off

- **Date:** 2026-11-04
- **Status:** accepted
- **Decision:** The incident is the next night of Scenario 00's job. Its only differences are data: +15 %
  events (logged-out traffic now tracked) and `customer_id = -1` on 13 % of them (0.15 / 1.15), the CRM's
  guest placeholder, which exists in `customers_snapshot` and so survives the inner join. The job's
  `--conf spark.sql.adaptive.skewJoin.enabled=false` (in both runs) is why AQE doesn't split the partition.
  Hot share and volume were chosen so the runtime lands where real skew incidents do (5.5 min → 1.6 h,
  one task of 1.5 h, 280 GiB spilled), and the ticket adds the realistic red herring of a node whose
  local dirs filled up (the straggler's spill). The solution's 17 evidence values cover both runs, the SQL
  tab and the straggler's container log.

## ADR-031 — The portal is a small server-rendered page set that never shows a metric

- **Date:** 2026-11-05
- **Status:** accepted
- **Context:** Phase 7 needs a place for the ticket, the hints and the solution, with links into the History
  Server (ADR-001: no UI of our own except a small scenario portal). The learner should not be able to read
  the answer by accident, and the portal must not become a second source of numbers.
- **Decision:** `sparksim/portal/`: a standard-library HTTP server in the package, pages rendered on the
  server from the scenario's Markdown (markdown-it-py: CommonMark + tables, inline HTML for `hints.md`'s
  `<details>` blocks), one CSS and one script file, no CDN. Cards show the number, difficulty, title, summary
  and run states; **topics are spoilers** (Scenario 01's say "data skew") and sit in a collapsed
  "Topics (spoiler)" element. Hints are revealed one at a time, the solution behind a button that asks for a
  second click while the diagnosis box is empty; progress and notes are per scenario in localStorage (no
  accounts, nothing on the server). Each run gets deep links into the History Server (Jobs, Stages, SQL,
  Executors, Environment) that open in one named browser tab. `/api/status` reports each app as generating /
  queued / not generated / loading / ready (and out of date), from the manifest (ADR-032) and the History
  Server's application list. The portal shows no metric: every number is read in the History Server.
- **Consequences:** Editing `ticket.md`/`hints.md`/`solution.md` shows on reload. The portal runs anywhere the
  package runs (no Node build). markdown-it-py is a runtime dependency.

## ADR-032 — One command: `sparksim up` without Docker, `docker compose up` with it; a fingerprint manifest

- **Date:** 2026-11-05
- **Status:** accepted; supersedes ADR-007's "Docker Compose path not verified"
- **Context:** "A fresh clone and one command give you Scenario 01 ready to debug." Generating every scenario
  takes ~30 s per two runs and ~1.4 GB of memory per run, so it must not happen on every start, and a
  `git pull` that changes a scenario or the simulator must not leave stale logs behind.
- **Decision:**
  - `sparksim prepare`: every run of every scenario is one app id; runs that are the same (Scenario 01's last
    good run = Scenario 00's run) share their app and are generated once, and two *different* runs claiming
    one app id are an error (`sparksim validate` checks it). An app's fingerprint is sha256 of the job and the
    run in `scenario.yaml` (the label excluded) and of the simulator's source (the package minus the portal
    and the CLI). `<event-logs>/.sparksim-manifest.json` records each log's fingerprint and a running
    prepare's progress; it is a hidden file, which the History Server skips. Apps whose event log, container
    logs or fingerprint is missing or different are regenerated, in sequence (memory). `sparksim generate`
    records into the same manifest.
  - `sparksim up` (needs uv + JDK 17): checks the ports, starts the log server and the History Server
    (`run.sh`, `JAVA_TOOL_OPTIONS` removed, the executor log URL pointed at the chosen port) as
    subprocesses with output in `build/logs/`, the portal in a thread, runs `prepare` meanwhile, waits until
    the History Server lists every app, prints `Ready.` with a link per scenario, and stops everything on
    Ctrl+C or SIGTERM.
  - `docker compose up` (needs Docker only): `generate` (runs `prepare` once, exits), `history-server`
    (`apache/spark:3.5.9-java17`), `nm-logs` and `portal` (the simulator image: `python:3.11-slim`, uv,
    `uv sync --frozen --no-default-groups`, no PySpark, non-root user). Generated logs live in named volumes,
    not bind mounts, so a container never leaves root-owned files in the clone. `pull_policy: build` rebuilds
    the image from cache on every `up`. Host ports move with `PORTAL_PORT`, `HS_PORT`, `NM_PORT`; the
    reference captures are a `reference` profile.
- **Evidence:** both paths were run from a fresh clone in the cloud container (which can run `dockerd` after
  all; images pull from Docker Hub) and pass `scripts/stack_check.py`; the event logs generated in the
  container are byte-identical to the local ones, and a second `docker compose up` regenerates nothing.
  `tests/test_up.py` runs `sparksim up` from empty directories to `Ready.` and checks the stack and the stop.
- **Consequences:** Adding a scenario needs no change to the stack: `prepare` and the portal discover
  `scenarios/*/scenario.yaml`. The sandbox's TLS-intercepting proxy needs a CA in the image's base and a
  relay for builds; that set-up is not part of the repo (SESSION_LOG gotchas).

## ADR-033 — Scenario 02: a migration drops the shuffle partitions; AQE stays on

- **Context:** The backlog's "too few shuffle partitions" asked for 200 partitions on a big shuffle with AQE
  off. AQE off isn't modelled (ADR-029), and Spark 3.5 runs with AQE on by default, so "AQE off" would also be
  the less common production story. The scenario should also not contradict Scenarios 00/01, which run the
  same job with `spark.sql.shuffle.partitions=2400` and whose ticket says its submit options last changed
  "three weeks" before 2026-11-04.
- **Decision:** Same job and cluster, three weeks earlier: last good run 2026-10-14 (app 44657), incident
  2026-10-15 (app 44836). The only fault is `config`: the job's move to the platform's Airflow DAG factory
  (DATAENG-2207) *unsets* `spark.sql.shuffle.partitions` ("AQE sizes partitions on Spark 3") and adds two
  harmless keys (`spark.yarn.maxAppAttempts=1`, `spark.yarn.tags`), so the Environment diff has noise; the
  migration was rolled back the next day (hence 00/01's conf). Unset rather than set to 200: the Environment
  tab then doesn't list the property at all, and the learner has to know the default and confirm it in the
  SQL tab (`number of partitions`, `hashpartitioning(…, 200)`). With AQE on, the scenario teaches why AQE
  didn't help: `CoalesceShufflePartitions` only merges, its start is `initialPartitionNum` (default: the
  shuffle partitions), and with every partition equally big none is "skewed". The ticket comes from Hadoop
  ops (disk alerts on the 40 executor hosts, other tenants degraded), with the HDFS balancer as a red herring;
  the job "only" went from 5.6 to 9.1 min, which is what 200 partitions cost on this data.
- **Planner:** `SparkSession.shuffle_partitions` follows `SQLConf.numShufflePartitions`: with AQE and its
  coalescing on, `spark.sql.adaptive.coalescePartitions.initialPartitionNum` (if set) replaces
  `spark.sql.shuffle.partitions`. The solution's durable fix (`initialPartitionNum=8000`, advisory 128 MiB →
  2,667 tasks, no spill) and tonight's fix (2400) are verified in `tests/test_scenario02.py`; 600 partitions
  would already avoid the spill.
- **Consequence:** the spill is the existing per-operator model (ADR-028): each of the 200 tasks sorts ~3.5 GiB
  of rows for the join and again for the write against ~2.35 GiB of execution memory. No disk contention
  between the 160 concurrent spilling tasks is modelled (ADR-014), so the slowdown is a lower bound.

## ADR-034 — Executor logs: task threads from a cached pool, spill lines as Spark prints them

- **Context:** Scenario 02 is the first with many tasks spilling at once on one executor, and its logs showed
  four concurrent tasks sharing two thread ids (the id was `60 + task id mod 16`), spill counts off by one
  (`(1  time so far)` for the first spill; real logs start at `(0  time so far)`, `reference/yarn-logs/ref02`),
  and the Parquet writer opened in the middle of the task, before the write sort's spills (ref02: committer
  set-up at the start, `ParquetRecordWriter` after the spills).
- **Decision:** (1) `yarnlogs.executor.task_threads` models the executor's task launch pool: a task takes the
  worker that went idle last (SynchronousQueue hand-off), else a new thread with the next JVM thread id (gaps of
  1–3 for other threads); idle workers exit after 60 s. (2) The count is `spillWriters.size()` before the spill.
  (3) A spill line's size is `getMemoryUsage()`: whole data pages (`spark.buffer.pageSize`, 64 MiB here) plus
  the pointer array (a power of two ≥ 64 KiB), never more than the sorter held; the number of spills per sorter
  is unchanged (spill bytes / the task's execution memory). (4) `FileFormatWriter.executeTask` order: committer
  set-up when the task starts, the first output file when the sort before the write has consumed its input
  (the end of that sort's fill window in the task's cost breakdown).
- **Consequence:** every container log changes (fingerprints regenerate all apps); event logs don't. Scenario
  01's evidence is unchanged (120 lines, 53 + 67, 2 sorters); its solution now quotes `Thread 62`.


## ADR-035 — ref09: speculation captured on a two-NodeManager YARN with a CPU-throttled node

- **Date:** 2026-11-08
- **Status:** accepted
- **Context:** Speculative execution is the job-side fix for a bad-node straggler, but no reference log had a
  speculative or killed task, and a single-node cluster can't produce one: a copy never runs on a host where
  its task already ran. Event shapes, kill reasons, metrics of killed attempts and the log lines had to come
  from a real run, not from reading the source alone.
- **Decision:** `REF_SECOND_NM=1 reference/yarn/cluster.sh start` adds a second NodeManager on host `slownode`
  (127.0.0.2 in `/etc/hosts`; own conf, local and log dirs, pid dir, localizer and web ports; both NMs offer
  4.5 GB so the 3 executors split around the AM). `reference/yarn/slow_node.sh` moves every executor JVM on
  `slownode` (and the Python workers it forks) into a cgroup v1 `cpu` group with a quota: a real throttled
  host, the cause Scenario 09 injects. `reference/jobs/ref09_speculation.py` runs a CPU-bound map stage, a
  sort + Parquet write and an aggregation with `spark.speculation=true`. Findings (EVENT_LOG_FORMAT.md §6.2):
  no `SpeculativeTaskSubmitted` in the log; killed attempts that report after their stage completed list no
  accumulables; "Stage finished" overwrites a pending "another attempt succeeded"; a 6-task stage the slow
  host dominates is never speculated. The golden test now re-derives ref09 too, including every killed task's
  own `Accumulator Updates` (`DriverLedger.failure_updates`, `stage_removed`).
- **Consequence:** `tests/test_reference_logs.py` still holds (YARN cluster mode); the reference set is 10 logs.

## ADR-036 — Speculative execution is a port of TaskSetManager; kills take effect when the executor acts

- **Date:** 2026-11-08
- **Status:** accepted
- **Decision:** `sched/scheduler.py` ports Spark 3.5.9's speculation: the 100 ms check (fixed delay from the
  SparkContext start), quantile / multiplier / minTaskRuntime / task.duration.threshold, `PercentileHeap`'s
  median, the 3.4+ efficiency rule (a running task's process rate is always 0 because heartbeats don't carry
  its run time, so it never vetoes), `runningTasksSet` iterated in Scala `mutable.HashSet` order
  (`ordering.ScalaHashSet`, with back-shift removal), separate speculatable pending lists, regular tasks
  first, never a host the task already ran on, revive after a check that marked something. The first
  attempt to succeed wins; the others get a kill request. A kill takes effect after `kill_latency`: the
  RPC plus the task thread noticing it and reporting, CPU work that scales with the host's `cpu_factor`
  (calibration `KILL_*`, ref09). Until then the attempt holds its core. A result stage's job end kills what
  is left with "Stage finished" (the latest reason wins); an attempt that finishes before its kill arrives
  ends as "Finish but did not commit due to another attempt succeeded" (no metrics). A killed attempt
  reports `cost.model.killed_metrics`: run time and GC so far, reads so far, nothing written. Map outputs
  are idempotent per map index (`ShuffleExchange.write`): a copy or re-run hashes the same rows into the
  same blocks. `spark.yarn.exclude.nodes` keeps the RM from granting containers on those hosts.
- **Consequence:** with `spark.speculation` unset nothing changes: Scenarios 00/01/02 regenerate byte for
  byte (event logs and container logs). Not modelled: speculation for barrier stages, executor
  decommissioning, `spark.speculation.task.duration.threshold` on multi-wave stages beyond Spark's rule.

## ADR-037 — Scenario 09: a power-capped node that only some nights' placement hits

- **Date:** 2026-11-08
- **Status:** accepted
- **Context:** The backlog said "one host with a degraded disk". In this job a slow node hardly shows unless
  it is very slow: Spark hands tasks to free cores, so a 3x slower executor just takes fewer of 5,000 tasks
  (5.5 → 5.8 min). The scenario needed a degradation that is real, large and modelled faithfully.
- **Decision:** the cause is CPU, not disk: `wn063` lost one of its two power supplies (HW-6612, three days
  earlier) and its BMC caps the power, so its CPUs run at ~1/20 of their speed (`slow_node cpu: 0.05`). A
  throttled CPU slows everything on the host that is CPU work, so `cpu_factor` now also stretches the fixed
  CPU costs: task deserialization, code generation, result serialization, JVM start-up (executor
  registration), kill handling (healthy hosts have factor 1.0: no other scenario changes). A degraded disk
  would mostly slow *other* hosts' reads and writes through that node's DataNode and shuffle service, which
  the cost model doesn't express yet; CPU keeps the effect on the node's own executor, where the evidence is.
  The fault is in the incident run only, but it stands for a node that was already bad the night before:
  YARN placed none of that night's containers on it, and `test_scenario09.py` proves the last good run is
  byte-identical with the fault. That is the "randomness": 40 of 120 nodes per night. Incident: executor 19
  on `wn063` registers 88 s late, runs 28 tasks (others 225–259), every one of them the longest of its stage
  at 13–24x the median, with normal input, GC share, no spill or failures; 5.5 → 9.3 min. The ticket's
  change calendar lists the PSU alert among red herrings (rack04 switch work, rack02 patching, a NameNode
  upgrade). Fixes, verified in the simulation: `spark.yarn.exclude.nodes` (5.5 min) and
  `spark.speculation=true` (5.7 min, 21 copies, 18 win); the solution explains why `spark.excludeOnFailure`
  would not help (nothing failed).
- **Not modelled:** the degraded node's DataNode and shuffle service serving other executors (its CPU
  throttling would slow HDFS write pipelines through it a little); disk faults remain available as a
  mechanism but no scenario uses them yet.
