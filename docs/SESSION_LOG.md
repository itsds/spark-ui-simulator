# Session log

Append-only handoff notes. Newest entry at the bottom. Each entry: date, phase,
what was done, what's next, gotchas.

---

## 2026-11-03 — Phase 0: foundation

- **Done:** Settled the approach: synthetic Spark 3.5 event logs rendered by the
  real Spark History Server (ADR-001), not a static replica. Wrote `CLAUDE.md`,
  architecture, ADRs 001–007, roadmap (Phases 0–8+), scenario backlog (00–15),
  README and `.gitignore`.
- **Next:** Phase 1. Set up the `uv` project, settle Java (the dev container has
  JDK 21; Spark 3.5 officially supports 8/11/17), get History Server 3.5.9
  running, and capture reference event logs from real PySpark 3.5.9 jobs.
- **Gotchas:**
  - The dev container has Python 3.13. Use `uv` to get Python 3.11 for PySpark 3.5.9.
  - Docker CLI is present in the cloud container; whether a daemon is available is unverified.
  - The agent-memory plugin failed to connect this session. Repo docs are the canonical memory (ADR-004).

## 2026-11-03 — Phase 1: reference capture + History Server harness

- **Done:**
  - `uv` project (Python 3.11, PySpark 3.5.9, pytest, ruff). `scripts/env.sh` selects **JDK 17**.
  - Real History Server 3.5.9 via `services/history-server/run.sh` (PySpark's bundled Spark), plus a
    `docker-compose.yml` that is still unverified (no Docker daemon in the cloud container).
  - Single-node **Hadoop 3.3.6 HDFS + YARN** (`reference/yarn/cluster.sh`). All nine reference jobs run with
    `--master yarn --deploy-mode cluster`. Logs are in `reference/event-logs/`, real container stdout/stderr
    in `reference/yarn-logs/`.
  - `docs/EVENT_LOG_FORMAT.md` (how logs feed every UI element) and the generated `docs/event-log-schema.md`.
  - `scripts/hs_api.py` (REST + `validate`), `scripts/screenshot.py` (Playwright), tests (unit + integration).
- **Next:** Phase 2, the event-log writer. Start from `docs/event-log-schema.md`. Golden-test it by re-emitting
  `ref01-daily-etl-aqe` from a model and diffing it field by field.
- **Gotchas (all hit for real this session):**
  - JDK 21 breaks Arrow in Spark 3.5 (`DirectByteBuffer.<init>(long, int) not available`). Use JDK 17.
  - `local-cluster` teardown logs dozens of bogus `ExecutorRemoved` events, so it's not usable as truth (ADR-008).
  - PySpark has no `StorageLevel.MEMORY_ONLY_SER` and no `TaskContext.stageAttemptNumber()`.
  - Single host plus the default `spark.shuffle.readHostLocalDisk=true` means a dead executor's shuffle files stay
    readable, so there's no FetchFailed. The harness sets it to `false`.
  - A failed driver in cluster mode is retried by YARN as attempt 2 (two event logs in the same dir).
  - Event-log file names must not contain dots (the extension is read as a compression codec).
  - `pkill -f <pattern>` also matches the invoking shell's own command line; use `pgrep` and kill by PID.
  - `.gitignore` rules must be root-anchored (`/event-logs/`). The unanchored form also ignored
    `reference/event-logs/`.
- **Small open items:** `scripts/screenshot.py` doesn't expand "Show Additional Metrics" on the stage page
  (selector needs fixing). The Docker Compose path is unverified.

## 2026-11-03 — Phase 2: event-log writer + tiny synthetic app

- **Done:**
  - `sparksim.eventlog`: typed dataclasses for every event type in the reference logs (22) plus 2 not yet verified,
    a generic strict codec, Jackson-identical JSON (`jsonfmt`), and read/write. **All 3,327 reference lines round-trip
    byte for byte** (ADR-010).
  - `DriverLedger` derives what the driver adds itself (running accumulator `Value`, dropped-zero and touched rules,
    35 task-metric IDs per stage attempt, stage accumulables). The golden test re-derives **all 26,719 accumulables** in
    the 9 reference logs, and planted bugs make it fail (ADR-011). Rules written up in EVENT_LOG_FORMAT.md §5.1.
  - `Timeline` (time-ordered emission with a fixed preamble), `IdAllocator`, `sparksim.yarn` (app and container IDs,
    NM log URLs and attributes, BlockManager max memory formula that matches the real 1536m and 2g values),
    `sparksim.environment` (PySpark-on-YARN Environment tab; the real Spark 3.5.9 jar list is in `sparksim/data/`).
  - Tiny app `sparksim generate tiny` (1 job, 2 stages, 8 tasks, 2 executors on 2 NodeManagers), modelled on ref08 job 0
    (ADR-012). It renders in every tab of the real HS. Integration test: generate → HS → REST.
  - `scripts/screenshot.py`: fixed "Show Additional Metrics" (`#additionalMetrics` + `#box-0`), added the stage DAG,
    aggregated metrics and a job-detail (DAG) shot.
  - Tests: 68 passed (`uv run pytest`), ruff clean.
- **Next:** Phase 3. Cluster model, workload model, `cost/calibration.py` (move the tiny app's rates there),
  the discrete-event scheduler (replaces the greedy `assign` in `apps/tiny.py`), then Scenario 00 at ~15k tasks.
  Emit through `Timeline` + `DriverLedger`. Check serializer speed at that scale (about 1 s per 3k lines today).
- **Gotchas:**
  - Accumulator `Value` is cumulative in log order, so build TaskEnds at emission time (`Timeline` takes callables).
  - Reduce tasks list all 17 shuffle-read and push-read accumulators even when they're 0. Python RDD stages list
    spill accumulators = 0. Pass them as `touched=` (`SHUFFLE_READ_ACCUMS`, `SPILL_ACCUMS`).
  - The log isn't strictly time-sorted. ApplicationStart (earliest timestamp) comes after the driver's
    BlockManagerAdded and EnvironmentUpdate.
  - The driver row's "Task Time" in the Executors tab equals the app duration. Real Spark does the same (checked on ref08).
  - Python RDD stages: JVM `Executor CPU Time` ≪ `Run Time`, because the work happens in the Python worker process.
  - The session harness required branch `claude/beautiful-mccarthy-dp7e07` rather than `phase-2/<slug>`.
  - `pgrep -f`/`pkill -f` with a pattern also kills the calling shell (exit 144). Kill by the PID from `ps`.
- **Still open:** the Docker Compose path is still unverified (no Docker daemon in the cloud container).

## 2026-11-03 — Phase 3: cluster, cost model, scheduler → Scenario 00

- **Done:**
  - `model/`: YARN cluster (6 racks × 20 NodeManagers, other tenants' load), container sizing (overhead, 1 GiB
    rounding), CapacityScheduler placement (one off-switch container per NM heartbeat), registration and the 80 % start
    rule; HDFS blocks with rack-aware replicas and Parquet row groups; a port of Spark's scan planning (maxSplitBytes,
    splitFiles, Next Fit Decreasing, block hosts, preferred locations); stage pipelines (`StageSpec`: sources →
    operators → sink); shuffle block matrices with host-local reads and empty-block skipping; AQE coalescing.
  - `cost/calibration.py` (`[ref]` overheads from the reference runs, `[norm]` per-stream production rates on shared
    nodes; the tiny app's rates moved here) and `cost/model.py` (per-task phases → `TaskMetrics`).
  - `sched/`: event kernel with generator processes; a port of `TaskSchedulerImpl` + `TaskSetManager` delay
    scheduling (locality levels, SPARK-18886 reset rule, noPref → PROCESS_LOCAL), FIFO, revive timer.
  - `engine/`: AQE driver loop (map-stage jobs, broadcast job on its own thread, re-created skipped stages), RDD
    lineage/scopes, heartbeat-sampled Task Executor Metrics, emission via `Timeline` + `DriverLedger`.
  - `eventlog/ordering.py`: Scala HashSet, JDK Properties and Scala Map iteration orders, verified on every reference
    log (ADR-016). The environment and the tiny app now use them.
  - Scenario 00 `clickstream-daily-enrichment`: 40 × 4 cores, 8 jobs, 15,243 tasks, 6.8 min, 641 GiB in, 405 GiB
    shuffled; ticket, hints, solution. `scripts/evidence.py` + `tests/test_scenario00_hs.py` check the 15 evidence
    numbers against the History Server REST API (ADR-017).
  - Tests: 124 passed (`uv run pytest`, ~55 s), ruff clean. ADRs 013–017.
- **Next:** Phase 4 (SQL layer). The stage pipelines in `apps/daily_enrichment.py` are the hand-written stand-in for
  a planner: Phase 4 builds the physical plan, derives the same `StageSpec`s and lineage from it, adds SQL metric
  accumulators per operator (`cost.model` already traces rows in/out per operator: `TaskResult.trace`), and adds
  `spark.sql.execution.id` to job properties together with `SQLExecutionStart/End` (ADR-015).
- **Gotchas:**
  - A job property `spark.sql.execution.id` without its `SQLExecutionStart` makes HS 3.5.9 answer 404 on `/sql`.
  - HS stage tables measure Duration from `firstTaskLaunchedTime`, not submission; the REST stage data has no locality
    summary (count `taskLocality` over the task list).
  - Next Fit Decreasing packs two small files into one split: the aggregation scans have a 2.2× max task that is
    input-size variance, not skew. The solution teaches it; `test_healthy_quantiles` checks time per byte instead.
  - At a scan stage's start some cores idle up to `spark.locality.wait` (3 s) while their hosts have no node-local
    task. That's Spark's delay scheduling, not a bug.
  - The harness required branch `claude/sharp-davinci-6fpjzj` (not `phase-3/<slug>`).
  - JDK 17 must be installed with apt in a fresh container (`openjdk-17-jdk-headless`).
- **Still open:** dynamic contention between concurrent tasks (ADR-014); the Docker Compose path is unverified.

## 2026-11-03 — Phase 4: SQL / DataFrame layer

- **Done:**
  - `sparksim/sql/`: a PySpark-like DataFrame API (`read`, `where`, `select`, `join`, `group_by().agg()`,
    `write_parquet`) and a physical planner (filter pushdown with `isnotnull` inference, column pruning,
    CollapseProject, BHJ vs SMJ, EnsureRequirements exchanges and sorts, partial/final and one-distinct aggregation,
    planned-write sort); Spark 3.5 operators with their node names, strings and metric maps; AQE
    (`createQueryStages`, CoalesceShufflePartitions → AQEShuffleRead, ColumnarToRow, CollapseCodegenStages,
    re-planning with Spark's node identity, final plan); formatted explain; SparkPlanInfo with lazily allocated
    metric ids; an RDD-scope simulator; per-task and driver-side SQL metrics.
  - Engine: one `AdaptiveExecution` per query drives the jobs; `StageSpec`s and lineage come from the query stages
    (`engine/lineage.py` and `workload.Query` removed). Events: SQLExecutionStart / AQE updates / DriverAccumUpdates /
    SQLExecutionEnd; jobs carry `spark.sql.execution.id`; TaskEnds list SQL accumulables via `DriverLedger`.
  - Golden test (`tests/test_sql_golden.py`, model in `tests/sql_ref01_model.py`): ref01 execution 0 re-planned with
    our DSL matches all 5 plan events (explain + SparkPlanInfo + every accumulator id; `plan_id` normalized) and the
    RDD scope/RDD ids; metric definitions match all 300+ reference plan nodes (ADR-018).
  - Scenario 00 rewritten as 3 DataFrame queries over table schemas; re-baselined (9,448 tasks, 5.5 min); evidence
    E15–E19 from the SQL tab; ticket/hints/solution updated (ADR-020). Model fixes found via the SQL tab: clustered
    files after a shuffle (partial aggregation), final aggregation = all keys of its reducers, 0 write time for empty
    map tasks.
  - `scripts/screenshot.py` also shoots the SQL list, the plan with Details expanded and the other executions.
  - Tests: 145 passed (`uv run pytest`, ~55 s), ruff clean. ADRs 018–020.
- **Next:** Phase 5 (YARN realism): mock NodeManager log server and per-container stdout/stderr, executor removal
  with YARN wording, Environment tab polish, dynamic allocation. Consider the schema-inference jobs PySpark runs at
  `spark.read.parquet` (ref01 jobs 0–2: 1 task, ParallelCollectionRDD, no execution id) for Jobs-tab realism.
- **Gotchas:**
  - The SQL REST endpoint needs the attempt id for YARN apps: `/applications/<id>/1/sql`; node names come back trimmed
    (`Scan parquet`, not `Scan parquet `).
  - Metric ids are allocated when the plan event is built, so record plan events in simulation order relative to
    stage submissions (each takes 35 task-metric ids).
  - The HS counts tasks that didn't report a SQL metric as 0, so `min` is 0 when some tasks had empty splits.
  - A plan event must precede the job whose tasks update its metrics, or the HS ignores the updates.
  - Spark source for checking: `https://raw.githubusercontent.com/apache/spark/v3.5.9/...` is reachable from the
    cloud container (GitHub archive downloads are not).
  - The harness required branch `claude/optimistic-dirac-8ewgqh` (not `phase-4/<slug>`).
- **Still open:** exchange reuse, subqueries/DPP, skew-join splits and local reads in AQEShuffleRead, AQE's runtime
  SMJ→BHJ switch, collect/limit actions, Python UDF nodes; MapStatus size quantization (AQE sees exact sizes);
  dynamic contention (ADR-014); the Docker Compose path is unverified.

## 2026-11-03 — Phase 5: YARN realism

- **Done:**
  - YARN allocation inside the simulation (`engine/yarn_am.py`): Spark's AM allocate loop (200 ms doubling to
    3 s, woken by a higher target), RM grants one OFF_SWITCH container per NodeManager heartbeat, YARN's absolute
    asks over-allocate (as on our reference cluster) and Spark releases the extras. Container ids with the RM
    epoch (`container_e23_…`), RM HA `CLUSTER_ID`, FQDN driver host. `place_application` removed (ADR-021).
  - Executor loss: container RSS model (`model/memory.py`: committed heap growth + G1/native footprint), the
    NodeManager's 3 s pmem check, Hadoop's kill diagnostics, Spark's driver path (BlockManagerRemoved at
    disconnect, ExecutorLostFailure when the AM gives the reason, ExecutorRemoved twice), re-run with the next
    attempt number, replacement containers (ADR-022). Scheduler: disable/remove executors, attempts, failures.
  - Dynamic allocation: ExecutorAllocationManager port (`engine/dynalloc.py`, ADR-023).
  - Container logs (`sparksim/yarnlogs`): executor stderr and the AM's driver log, derived from the run;
    `services/nm-logs` serves them like a NodeManager; the History Server's custom executor log URL points
    every link at it (ADR-024). `sparksim generate` writes `yarn-logs/` by default.
  - Environment tab: Hadoop 3.3.6 defaults from a real driver (`scripts/extract_hadoop_defaults.py`) + production
    site files, cluster spark-defaults (shuffle service on), JDK system properties (ADR-025). `count()` results
    are non-nullable; an explicit memoryOverhead is part of the default ResourceProfile.
  - Scenario 00 re-baselined (9,447 tasks; evidence E3/E6/E7/E10/E12/E14/E18/E19 updated), solution + hint on
    reading container logs.
  - Tests: 162 passed (`uv run pytest`), ruff clean. New: `test_yarn.py`, `test_executor_loss.py` (pmem kills,
    dynamic allocation, their logs), `test_environment.py`, the 41-link check in `test_scenario00_hs.py`.
- **Next:** Phase 6 (scenario DSL + fault injection → Scenario 01 join skew). The mechanisms Scenario 03 (pmem
  kills: `spark.executor.memoryOverhead` + a native-memory cause such as Python/Arrow workers, `python_rss` in
  `ContainerMemory`) and 14 (dynamic allocation / queue starvation) need now exist; they still need job abort
  after `spark.task.maxFailures` (`TaskSetAborted` is raised) and, for Scenario 12, lost shuffle files without
  the shuffle service (FetchFailed, stage retry: currently NotImplementedError).
- **Gotchas:**
  - A fixed-size allocation grid hides small overheads: on a 1024 MB grid a 16 GiB executor with a 384m or 512m
    overhead still gets a 17 GiB container, enough for the ~1 GiB native footprint. The loss test uses a 512 MB
    grid (16.5 GiB containers).
  - `spark.history.custom.executor.log.url` needs `{{FILE_NAME}}` (from `LOG_FILES`) to produce both stdout and
    stderr links; it also rewrites the driver's links. Tests pass it through `SPARK_HISTORY_OPTS`.
  - `ExecutorLostFailure` TaskEnds carry `JVMHeapMemory: -1` in Task Executor Metrics (`ExecutorMetrics(Array.empty)`).
  - The reference driver logs stop at WARN after start-up (`common.py` sets it): driver INFO texts came from the
    Spark 3.5.9 source (raw.githubusercontent.com is reachable; Hadoop sources at `rel/release-3.3.6`).
  - The harness required branch `claude/keen-shannon-045j1o` (not `phase-5/<slug>`).
- **Still open:** node-local container requests, preemption, queue capacity, app attempt 2, GC logs, Python
  worker output, `ShufflePartitionsUtil` / `FileSourceStrategy` planning lines; the Docker Compose path is
  unverified (now with the nm-logs service).
