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
