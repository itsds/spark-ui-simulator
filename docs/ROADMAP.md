# Roadmap

Each phase is one branch/PR, sized for roughly one focused Claude session.
A phase is **done** when its "done when" checks pass and every box is ticked.

Status legend: ⬜ not started · 🟨 in progress · ✅ done

| Phase | Name | Status |
|---|---|---|
| 0 | Foundation and project memory | ✅ |
| 1 | Reference capture + real History Server harness | ⬜ |
| 2 | Event-log writer (minimal synthetic app) | ⬜ |
| 3 | Cluster, cost model, scheduler → Scenario 00 at production scale | ⬜ |
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

## Phase 1 — Reference capture + real History Server harness

Goal: real Spark 3.5.9 event logs that we can trust as format and calibration truth, plus a History Server we can start with one command.

- [ ] `uv` project: Python 3.11, `pyproject.toml`, package skeleton `src/sparksim/`, `pytest` + `ruff`
- [ ] Settle the Java version (ADR-007) and record it
- [ ] `services/history-server/`: start HS 3.5.9 pointing at a log dir (Docker Compose service + no-Docker script)
- [ ] `reference/jobs/*.py`: small real PySpark jobs with `spark.eventLog.enabled=true`, covering:
      parquet scan + filter + agg, sort-merge join, broadcast join, AQE coalesce + skew join,
      cache/persist, write to partitioned parquet, a task failure and retry, a stage retry (fetch failure if feasible),
      a Python UDF, `collect()` to the driver
- [ ] `reference/event-logs/`: captured logs (small; committed)
- [ ] `docs/EVENT_LOG_FORMAT.md`: every event type seen, its fields, and **which UI element it feeds**
- [ ] `scripts/hs_api.py`: helper to query the HS REST API (apps, jobs, stages, taskList, executors, sql)
- **Done when:** HS shows every reference app with no replay errors, and `hs_api.py` returns their stages.

## Phase 2 — Event-log writer (minimal synthetic app)

- [ ] `sparksim/eventlog/`: typed builders for each event in Phase 1's catalog, serialized exactly as Spark 3.5 JsonProtocol
- [ ] Golden test: re-emit a reference app from a model and diff it field-by-field against the real log
- [ ] Hand-specified tiny synthetic app (1 job, 2 stages, 8 tasks, 2 executors) renders in HS
- [ ] Integration test: generate → start HS → REST round-trip assertions
- **Done when:** the synthetic app looks like a real tiny app in every tab, and the tests pass.

## Phase 3 — Cluster, cost model, scheduler → Scenario 00 (healthy baseline)

- [ ] Cluster model: YARN nodes, executors, cores, memory, overhead (static allocation first)
- [ ] Workload model: tables (rows, width, files, partitions), stage shapes
- [ ] Cost model v1 + `calibration.py` (constants sourced from reference runs and documented norms)
- [ ] Discrete-event scheduler: core slots, FIFO order, locality wait, stage dependencies
- [ ] Invariant tests: conservation, no double-booked cores, no task before parents finish, determinism
- [ ] Scenario 00: about 40 executors × 4 cores, several jobs, ~15k tasks, healthy quantiles (max ≈ 1.5–2× median)
- **Done when:** Scenario 00 renders in HS, the invariant tests pass, and screenshots look like a real production app.

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
