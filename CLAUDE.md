# CLAUDE.md — spark-ui-simulator

This file is the project's memory. Every Claude session working in this repo
starts here. Keep it short and current; put detail in `docs/`.

## What this project is

A practice environment for **debugging production Spark jobs in the real Spark
UI**. The owner is preparing for Data Engineering interviews ("how do you debug
a slow/failing Spark job?") and knows the theory well. What's missing is hands-on
time with the UI on production-scale jobs that have real problems in them.

We generate **synthetic Spark event logs** for production-scale applications
(tens of executors, thousands to tens of thousands of tasks, TB of
input and shuffle) that each contain a planted problem. Then we load them into
the **real Spark History Server**. The owner opens a scenario with only an
incident ticket and investigates the same way they would at work.

## Non-negotiables

1. **The UI is the real Spark History Server.** We never build, copy or patch
   Spark UI HTML. We produce event logs that Spark itself renders.
2. **No hand-typed metrics.** Every number comes from the chain
   workload model → cost model → scheduler simulation → event log. Totals
   (stage, job, executor, app) are computed by the History Server from
   task-level events, just as in production.
3. **Faults are injected as causes, never painted on as symptoms.** Skew is a
   hot key in the data distribution. Spill comes from partition size compared
   with execution memory. A straggler comes from a slow node. The symptoms in
   the UI follow from the simulation.
4. **Deterministic.** Same scenario + seed → byte-identical event log.
5. **Realistic, not random.** Variance is seeded, bounded and physically
   plausible (per-task noise, per-host speed, GC jitter), and it's calibrated
   against real reference runs in `reference/`.
6. **Target: Apache Spark 3.5.9 on YARN (cluster mode), Hadoop 3.x.**
   Structured Streaming is out of scope for now (backlog).
7. Every scenario ships a ticket, hints and a solution. The solution's
   evidence numbers must be checked by a test against the History Server
   REST API.

## Where things are

| Need | File |
|---|---|
| Current phase, task checklist, status | `docs/ROADMAP.md` |
| How the pipeline works | `docs/ARCHITECTURE.md` |
| Why we chose X over Y | `docs/DECISIONS.md` |
| Scenario backlog and design notes | `docs/SCENARIOS.md` |
| Handoff notes between sessions | `docs/SESSION_LOG.md` |

## Session protocol (every session)

1. Read `docs/ROADMAP.md` (find the first phase that isn't done) and the
   **latest entry** in `docs/SESSION_LOG.md`.
2. Work on a branch named `phase-<N>/<short-slug>` (scenarios:
   `scenario/<NN>-<slug>`). One phase or one scenario per branch/PR.
3. Before ending the session:
   - run the test suite;
   - tick the completed items in `docs/ROADMAP.md`;
   - append an entry to `docs/SESSION_LOG.md` (date, done, next, gotchas);
   - record any design choice in `docs/DECISIONS.md` as a new ADR;
   - update this file only if a non-negotiable, the layout or a command changed.
4. Never hand-edit generated output (`event-logs/`, `yarn-logs/`, `build/`).
5. Don't stop at a guess when the question is "what would real Spark emit?":
   check a real event log in `reference/` or the Spark 3.5 source
   (`JsonProtocol.scala`, `AppStatusListener.scala`, `SQLAppStatusListener.scala`).

## Repo layout

```
src/sparksim/          Python package: eventlog/ (typed events, JsonProtocol bytes, DriverLedger, Timeline, JVM orders),
                       model/ (cluster, container memory, HDFS, file splits, workload, shuffle), cost/ (calibration.py,
                       model.py), sched/ (event kernel, TaskScheduler), sql/ (DataFrame API + planner, AQE, explain,
                       plan info, RDD scopes, SQL metrics), engine/ (driver runtime, yarn_am = YARN allocation +
                       NodeManager monitor, dynalloc, emit), yarnlogs/ (container stdout/stderr from the run),
                       apps/ (workloads: daily_enrichment = clickstream_enrichment; tiny smoke app), scenario/
                       (scenario.yaml schema, loader, fault mechanisms), prepare.py (fingerprints + manifest:
                       generate only what is missing/stale), portal/ (scenario portal: server, Markdown, status,
                       static/), up.py (`sparksim up`), yarn.py, environment.py, cli.py,
                       data/ (Spark archive jars, Hadoop 3.3.6 defaults)
scenarios/NN-slug/     scenario.yaml (job + runs + faults), ticket.md, hints.md, solution.md (Evidence checked by tests)
reference/jobs/        real PySpark reference jobs (ref00–ref08) + common.py
reference/event-logs/  REAL Spark 3.5.9 event logs captured on YARN cluster mode: format truth
reference/yarn-logs/   REAL container stdout/stderr for those apps (driver = container ..._000001)
reference/yarn/        single-node HDFS+YARN config + cluster.sh
reference/capture.sh   runs reference jobs on YARN, collects logs
services/history-server/  run.sh + spark-defaults.conf (custom executor log URL → nm-logs)
Dockerfile, docker-compose.yml   the simulator image (prepare, portal, nm-logs) + the stack (HS = apache/spark image)
services/nm-logs/      mock YARN NodeManager log server (stdlib) for the generated container logs, port 8042
scripts/               env.sh, hs_api.py (REST + validate), evidence.py, event_inventory.py, screenshot.py,
                       stack_check.py (smoke-check a running stack), extract_hadoop_defaults.py
docs/                  ROADMAP, ARCHITECTURE, DECISIONS, SCENARIOS, SESSION_LOG,
                       EVENT_LOG_FORMAT (how logs feed the UI), event-log-schema (generated)
tests/                 unit + integration (marker `integration` starts a real History Server)
event-logs/, yarn-logs/, build/   GENERATED, gitignored
```

Read `docs/EVENT_LOG_FORMAT.md` before writing any event-log code. Emit events through
`sparksim.eventlog` (`Timeline` + `DriverLedger`): never hand-compute accumulator `Value`s or stage totals.
Container logs are derived from the run's records (`sparksim.yarnlogs`), never written by hand.
A scenario is `scenarios/<id>/scenario.yaml` over a workload (`apps/<module>.py: WORKLOAD`: params, the usual
input datasets with their key distributions, PySpark-like DataFrame queries over table schemas). Faults change
simulation inputs only (`scenario/faults.py`); incidents have a `last_good` and an `incident` run. Every
throughput or overhead constant goes in `cost/calibration.py` with its provenance tag. Check plan/explain changes against
`tests/test_sql_golden.py` (ref01 re-planned with our DSL must match the real log).

## Tooling and commands

Python 3.11 via `uv` (`uv sync` installs dev + PySpark 3.5.9). **JDK 17** is
required: JDK 21 breaks Arrow in Spark 3.5 (ADR-007). `scripts/env.sh` picks
JDK 17 and sets `SPARK_HOME` (PySpark's bundled Spark) and the Hadoop vars.

| Task | Command |
|---|---|
| Whole stack, one command (generate what's missing + HS + nm-logs + portal on :8000) | `uv run sparksim up` (or `docker compose up`) |
| Smoke-check a running stack | `uv run scripts/stack_check.py` |
| Lint | `uv run ruff check .` |
| Generate every scenario's missing/stale apps (manifest `event-logs/.sparksim-manifest.json`) | `uv run sparksim prepare` |
| Generate a scenario, every run (+ container logs in `yarn-logs/`) | `uv run sparksim generate 01-join-skew` (00: ~15 s, 01: ~30 s) |
| List / validate scenarios | `uv run sparksim scenarios` / `uv run sparksim validate` |
| Generate the tiny synthetic app | `uv run sparksim generate tiny --out event-logs` |
| Check the event model on a log (byte round trip) | `uv run sparksim roundtrip <event-log>...` |
| Unit tests | `uv run pytest -m "not integration"` |
| All tests (starts a real History Server) | `uv run pytest` |
| Portal alone | `uv run sparksim portal [--event-logs DIR] [--port 8000]` |
| History Server on a log dir | `services/history-server/run.sh [LOG_DIR] [PORT]` (default `event-logs/`, 18080) |
| Mock NodeManager logs (executor stdout/stderr links) | `services/nm-logs/run.sh [LOG_DIR] [PORT]` (default `yarn-logs/`, 8042) |
| Validate replay (log vs server counts) | `uv run scripts/hs_api.py validate --log-dir <dir>` |
| Query the REST API | `uv run scripts/hs_api.py stages <app-id>` / `summary <app-id> <stage>` / `get <app-id> <path>` / `sql <app-id>` |
| Check a solution's evidence numbers | `uv run scripts/evidence.py 01-join-skew` (against a running HS + nm-logs) |
| Screenshot key UI pages | `uv run scripts/screenshot.py <app-id> [--stage N]` → `build/screenshots/` |
| Start reference YARN cluster | `reference/yarn/cluster.sh start` (needs Hadoop 3.3.x at `/opt/hadoop`) |
| Re-capture reference logs | `reference/capture.sh [ref0N_name ...]` (YARN cluster mode) |
| Regenerate event schema doc | `uv run scripts/event_inventory.py` |

Environment notes (cloud dev container): JDK 17 comes from apt
(`apt-get install -y openjdk-17-jdk-headless`; not preinstalled). Hadoop comes from
`downloads.apache.org` (`dlcdn.apache.org` is blocked). The sandbox sets
`JAVA_TOOL_OPTIONS` (proxy); scripts unset it for local-only JVMs. Docker works
after starting the daemon (`dockerd &`); Docker Hub pulls work, ghcr.io blobs
don't, and image builds need the proxy's CA (see SESSION_LOG 2026-11-05).
