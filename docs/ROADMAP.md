# Roadmap

Each phase is one branch/PR, sized for roughly one focused Claude session.
A phase is **done** when its "done when" checks pass and every box is ticked.

Status legend: ⬜ not started · 🟨 in progress · ✅ done

| Phase | Name | Status |
|---|---|---|
| 0 | Foundation and project memory | ✅ |
| 1 | Reference capture + real History Server harness | ✅ |
| 2 | Event-log writer (minimal synthetic app) | ✅ |
| 3 | Cluster, cost model, scheduler → Scenario 00 at production scale | ✅ |
| 4 | SQL / DataFrame layer (physical plans, SQL metrics, AQE) | ✅ |
| 5 | YARN realism: IDs, containers, executor logs, Environment tab | ✅ |
| 6 | Scenario DSL + fault injection → Scenario 01 (join skew) | ✅ |
| 7 | Scenario portal + one-command startup | ⬜ |
| 8+ | Scenario library (one or two scenarios per session) | ⬜ |

---

## Phase 0 — Foundation and project memory ✅

- [x] `CLAUDE.md` with non-negotiables and session protocol
- [x] `docs/ARCHITECTURE.md`, `DECISIONS.md`, `ROADMAP.md`, `SCENARIOS.md`, `SESSION_LOG.md`
- [x] `README.md`, `.gitignore`

## Phase 1 — Reference capture + real History Server harness ✅

Goal: real Spark 3.5.9 event logs that we can trust as format and calibration truth, plus a History Server we can start with one command.

- [x] `uv` project: Python 3.11, `pyproject.toml`, package skeleton `src/sparksim/`, `pytest` + `ruff`
- [x] Settle the Java version → **JDK 17** (JDK 21 breaks Arrow in Spark 3.5; ADR-007)
- [x] `services/history-server/`: `run.sh` (no Docker, uses PySpark's bundled Spark) + `docker-compose.yml` (`apache/spark:3.5.9-java17`, **not yet verified**: no Docker daemon in the cloud dev container)
- [x] Single-node **real YARN** cluster for capture: `reference/yarn/` (HDFS + YARN 3.3.6, cluster mode; ADR-008)
- [x] `reference/jobs/ref00–ref08`: datagen/partitioned writes, AQE ETL (BHJ + SMJ), static plan + spill, skew join (AQE skew off/on),
      cache vs recompute (+ block updates), failures (task retry, failed job, executor kill → stage retry), Python/pandas UDFs,
      driver-bound (collect/toPandas/gaps/small files), pure RDD API
- [x] `reference/event-logs/` (9 apps) + `reference/yarn-logs/` (real container stdout/stderr)
- [x] `docs/EVENT_LOG_FORMAT.md` (data path, file naming, event order, event → UI mapping, accumulators, YARN fields, PySpark tells, single-node artifacts)
      + generated `docs/event-log-schema.md` (`scripts/event_inventory.py`)
- [x] `scripts/hs_api.py`: REST helper + `validate` (log vs server counts); `scripts/screenshot.py` (Playwright)
- [x] Tests: `tests/test_reference_logs.py` (well-formed, YARN cluster mode) + integration replay test
- **Done when:** HS shows every reference app with no replay errors, and `hs_api.py` returns their stages. ✅

## Phase 2 — Event-log writer (minimal synthetic app) ✅

- [x] `sparksim/eventlog/`: typed builders for each event in Phase 1's catalog, serialized exactly as Spark 3.5 JsonProtocol
      (all 22 event types seen in the reference logs, plus `StageExecutorMetrics` and `SQLAdaptiveSQLMetricUpdates`, which
      no reference log contains yet and so are unverified; strict parser; byte-identical round trip of all 3,327 reference lines)
- [x] Golden test: re-emit a reference app from a model and diff it field-by-field against the real log
      (`DriverLedger` re-derives all 26,719 accumulables in the 9 reference logs from task metrics: running `Value`s,
      dropped-zero / touched rules, task-metric ID layout, stage accumulables; see EVENT_LOG_FORMAT.md §5.1)
- [x] Hand-specified tiny synthetic app (1 job, 2 stages, 8 tasks, 2 executors) renders in HS: `sparksim generate tiny`
- [x] Integration test: generate → start HS → REST round-trip assertions (`tests/test_tiny_app.py`)
- **Done when:** the synthetic app looks like a real tiny app in every tab, and the tests pass. ✅ (screenshots checked:
  Jobs + timeline, job DAG, stage DAG/timeline/all metrics/aggregated by executor, Executors, Environment)

## Phase 3 — Cluster, cost model, scheduler → Scenario 00 (healthy baseline) ✅

- [x] Cluster model: racks, NodeManagers with other tenants' load, executor/AM container sizing (overhead, rounding),
      CapacityScheduler placement (one off-switch container per NM heartbeat), executor registration, 80 % start rule
- [x] Workload model: HDFS blocks + rack-aware replicas, Parquet row groups, Spark's file splits (maxSplitBytes,
      Next Fit Decreasing, block hosts, preferred locations), operator pipelines per stage, key spaces, shuffle
      block matrices, AQE coalescing
- [x] Cost model v1 + `calibration.py` (`[ref]` overheads from the reference runs, `[norm]` production throughputs; ADR-014)
- [x] Discrete-event scheduler: core slots, FIFO, Spark 3.5 delay scheduling (locality levels, SPARK-18886 timer rule,
      noPref → PROCESS_LOCAL), stage dependencies, revive timer; AQE driver loop with skipped re-created stages
- [x] Invariant tests: conservation (block by block, rows across queries), no double-booked cores, no task before its
      parents, stage accumulables = task totals, healthy quantiles, determinism (byte-identical log)
- [x] Scenario 00: 40 executors × 4 cores, 8 jobs, 15,243 tasks, 641 GiB scanned, 405 GiB shuffled, max/median 1.26–1.44
      on the big stages; ticket, hints, solution with REST-checked evidence (ADR-017)
- **Done when:** Scenario 00 renders in HS, the invariant tests pass, and screenshots look like a real production app. ✅
  (checked: Jobs + timeline, stages with skipped stages, job DAG with greyed skipped stages, stage DAG / summary /
  aggregated by executor, Executors. The SQL tab arrives in Phase 4.)

## Phase 4 — SQL / DataFrame layer ✅

- [x] Physical plan builder with Spark 3.5 operators: FileScan parquet (PartitionFilters, PushedFilters, DataFilters,
      ReadSchema), Filter, Project, HashAggregate (partial/final, and the 4-aggregate plan for one distinct), Exchange,
      Sort, SortMergeJoin, BroadcastExchange/BroadcastHashJoin, WholeStageCodegen, ColumnarToRow, InputAdapter,
      AdaptiveSparkPlan, Shuffle/BroadcastQueryStage, AQEShuffleRead, WriteFiles, Execute InsertIntoHadoopFsRelationCommand.
      Apps write PySpark-like DataFrame code (`sql/dataframe.py`); the planner infers `isnotnull` filters, prunes
      columns, picks the join strategy and inserts exchanges/sorts; `StageSpec`s are derived from the query stages
- [x] SQL metrics as accumulators updated by tasks (Spark's reporting rules, nested timings from the cost model's
      per-operator work), plus driver accumulator updates (scan files, AQEShuffleRead specs, broadcast, write command)
- [x] AQE replanning events (`SparkListenerSQLAdaptiveExecutionUpdate`): createQueryStages, CoalesceShufflePartitions,
      columnar transitions, CollapseCodegenStages, re-planning with Spark's node identity (metric ids, operator ids)
- [x] `physicalPlanDescription` in Spark's formatted explain, incl. `== Current/Final Plan ==` / `== Initial Plan ==`
      and query-stage `Statistics(...)`
- [x] RDD operation scopes from the execute()/produce() calls of each stage, so stage DAGs show the codegen clusters
- [x] Golden test: `ref01-daily-etl-aqe` execution 0 re-planned with our DSL reproduces all 5 plan events (explain
      text + SparkPlanInfo with every accumulator id) and the RDD scope ids; every operator's metric definitions are
      checked against all 300+ plan nodes in the reference logs (ADR-018)
- [x] Scenario 00 rewritten as 3 DataFrame queries over real table schemas (re-baselined: 9,448 tasks, 5.5 min;
      ADR-020), SQL evidence rows E15–E19 checked against the REST API
- **Done when:** Scenario 00's SQL tab shows plan graphs with coherent metrics that link to its jobs and stages. ✅
  (checked in HS 3.5.9: SQL list, all three plan graphs with Details, job ↔ query links; screenshots reviewed)

## Phase 5 — YARN realism ✅

- [x] YARN app, attempt and container IDs (RM epoch `e23`, HA `CLUSTER_ID`); NodeManager hosts and ports;
      executor attributes; FQDN driver host. YARN allocation now runs inside the simulation (`engine/yarn_am.py`:
      Spark's AM allocate loop, RM grants per NodeManager heartbeat, YARN's absolute-ask over-allocation; ADR-021)
- [x] `services/nm-logs/`: mock NodeManager log server; `sparksim/yarnlogs` writes stdout/stderr per container that
      match its tasks (executors: tasks, broadcasts, file splits, shuffle fetches, writes, spills, kills; the AM
      container: the driver log with YarnAllocator / DAGScheduler / TaskSetManager lines); the History Server's
      `spark.history.custom.executor.log.url` points every link at the mock (ADR-024)
- [x] Executor removal with YARN wording and exit codes: container RSS model (`model/memory.py`), NodeManager pmem
      kills (-104, SIGTERM 143, Hadoop diagnostics), ExecutorLostFailure + re-run, replacement containers,
      ExecutorRemoved twice for unexpected exits (ADR-022)
- [x] Environment tab: full Hadoop 3.3.6 configuration from a real driver + production site files, the cluster's
      spark-defaults, JDK 17 system properties, real classpath (ADR-025)
- [x] Dynamic allocation: ExecutorAllocationManager port (backlog ramp-up, idle timeout, "Executor killed by
      driver."; ADR-023)
- **Done when:** clicking any executor's stderr in HS opens a log that matches what that executor did. ✅
  (`tests/test_scenario00_hs.py`: all 41 links of Scenario 00, opened through HS 3.5.9, list exactly the
  tasks the REST API shows for that executor; screenshots of the Executors tab, a log page and the
  Environment tab reviewed)

## Phase 6 — Scenario DSL + fault injection → Scenario 01 (join skew) ✅

- [x] `scenario.yaml` schema (pydantic) and validator: job (workload, cluster, executors, `--conf`) + runs
      (`baseline`, or `last_good` + `incident` with params, data overrides and faults); `sparksim validate`
      also builds every run against its workload (datasets, key columns, params). Workloads are Python
      (`apps/<module>.py: WORKLOAD`); Scenario 00 moved to YAML byte for byte (ADR-026)
- [x] Fault mechanisms, each tested on a scaled Scenario 00 (`tests/test_faults.py`, ADR-029): key
      distributions (hot keys in Spark's Murmur3 partition, Zipf; ADR-027), config (`--conf` set/unset,
      executor sizing; the engine reads the broadcast threshold and AQE coalescing settings), node faults
      (CPU/disk/NIC factors), memory pressure (native memory per task → pmem kills), failures (SIGKILL →
      exit 137 → ExecutorLostFailure). Spill is per operator now (ADR-028). AQE skew join on + a skewed join
      partition is refused (not modelled yet) instead of faked
- [x] Generator emits "last good run" + "bad run" per scenario (ADR-006): `sparksim generate 01-join-skew`
- [x] Scenario 01: ticket, hints, solution; skew comes **only** from the key distribution (+15 % events,
      13 % of them with `customer_id = -1`; the last good run is Scenario 00's run; ADR-030)
- [x] Solution-evidence test: the 17 numbers quoted in `solution.md` (both runs, SQL tab, the straggler's
      container log) are asserted via the HS REST API (`tests/test_scenario01_hs.py`)
- **Done when:** Scenario 01 can be investigated end to end in HS, and its evidence test passes. ✅
  (checked in HS 3.5.9: application list, Jobs timeline with the 1.5 h job, stage 5 summary metrics /
  task table / timeline / aggregated by executor, SQL plan metrics, Environment, executor 40's stderr)

## Phase 7 — Portal + one-command startup

- [ ] `docker compose up`: generate → HS → nm-logs → portal
- [ ] Portal: scenario cards (difficulty, topics), ticket, progressive hints, solution, deep links into HS
- [ ] README quickstart verified on a clean machine
- **Done when:** a fresh clone and one command give you Scenario 01 ready to debug.

## Phase 8+ — Scenario library

Work through `docs/SCENARIOS.md` in priority order. One branch per scenario: `scenario/<NN>-<slug>`.

## Backlog (later)

- Capstone scenarios with 2–3 combined faults and red herrings
- Optional rendering on Spark 4.x History Server
- YARN ResourceManager UI mock (queues, preemption, AM logs)
- Structured Streaming tab and scenarios (deferred, ADR-005)
- "Interview mode": timed investigation, then spoken-answer checklist
