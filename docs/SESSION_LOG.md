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
