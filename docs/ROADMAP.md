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
| 4 | SQL / DataFrame layer (physical plans, SQL metrics, AQE) | ⬜ |
| 5 | YARN realism: IDs, containers, executor logs, Environment tab | ⬜ |
| 6 | Scenario DSL + fault injection → Scenario 01 (join skew) | ⬜ |
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

## Phase 4 — SQL / DataFrame layer

- [ ] Physical plan builder with Spark 3.5 operators: FileScan parquet (PartitionFilters, PushedFilters), Filter, Project,
      HashAggregate (partial/final), Exchange, Sort, SortMergeJoin, BroadcastExchange/BroadcastHashJoin,
      WholeStageCodegen, AdaptiveSparkPlan, AQEShuffleRead, write commands
- [ ] SQL metrics as accumulators updated by tasks, plus driver accumulator updates
- [ ] AQE replanning events (`SparkListenerSQLAdaptiveExecutionUpdate`)
- [ ] `physicalPlanDescription` text in Spark's format
- [ ] RDD operation scopes so stage DAGs show the codegen clusters
- **Done when:** Scenario 00's SQL tab shows plan graphs with coherent metrics that link to its jobs and stages.

## Phase 5 — YARN realism

- [ ] YARN app, attempt and container IDs; NodeManager hosts and ports; executor attributes
- [ ] `services/nm-logs/`: mock NodeManager log server; generated stdout/stderr per container that match its tasks
- [ ] Executor removal with YARN wording and exit codes; memoryOverhead modeling
- [ ] Environment tab: realistic Spark, Hadoop and system properties and classpath
- [ ] Dynamic allocation (executors added and removed over time)
- **Done when:** clicking any executor's stderr in HS opens a log that matches what that executor did.

## Phase 6 — Scenario DSL + fault injection → Scenario 01 (join skew)

- [ ] `scenario.yaml` schema (pydantic) and validator
- [ ] Fault mechanisms: key distributions (hot keys, Zipf), config faults, node faults, memory pressure, failures
- [ ] Generator emits "last good run" + "bad run" per scenario (ADR-006)
- [ ] Scenario 01: ticket, hints, solution; skew comes **only** from the key distribution
- [ ] Solution-evidence test: the numbers quoted in `solution.md` are asserted via the HS REST API
- **Done when:** Scenario 01 can be investigated end to end in HS, and its evidence test passes.

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
