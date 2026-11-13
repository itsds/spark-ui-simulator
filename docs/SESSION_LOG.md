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

## 2026-11-04 — Phase 6: scenario DSL + fault injection → Scenario 01 (join skew)

- **Done:**
  - `scenarios/<id>/scenario.yaml` + `sparksim/scenario/` (pydantic schema, loader, `faults.py`): the job
    (workload, cluster, executors, ordered `--conf`) and its runs (`baseline`, or `last_good` + `incident`
    with params, data overrides and faults). Workloads are Python (`apps/base.py: Workload`;
    `apps/daily_enrichment.py: WORKLOAD = clickstream_enrichment`, queries take their key spaces from the
    run's datasets). Scenario 00 moved to YAML byte for byte; `daily_enrichment.SPEC/build/generate` are gone
    (tests use `load_scenario(...).spec/build/generate`). CLI: `sparksim generate <scenario> [--run ROLE]`,
    `sparksim scenarios`, `sparksim validate` (ADR-026).
  - Key skew: `model/murmur3.py` (Spark's Murmur3_x86_32 + `pmod`, checked against PySpark), `KeySpace.hot /
    dtypes / zipf`, `row_shares_split` (tail shares for file clustering; unskewed keys unchanged; ADR-027).
  - Spill per operator (`cost.model._spill_group`): concurrent consumers below a join, each operator after it;
    per-op spill bytes, I/O and CPU in the SQL metrics and in per-sorter executor log lines placed in each
    sorter's fill window (ADR-028).
  - Fault mechanisms (ADR-029): `key_skew`, `config` (+ the engine reads `autoBroadcastJoinThreshold` and AQE
    coalescing settings), `slow_node` (`Host.cpu/disk/net_factor`), `memory_pressure` (native memory per task
    in `ContainerMemory`), `executor_killed` (SIGKILL → exit 137, ref05 diagnostics, log stops). Guards: AQE
    skew join on + a skewed join partition, and AQE off, raise NotImplementedError.
  - Scenario 01 `01-join-skew` (ADR-030): last good run = Scenario 00's app 48213; incident app 48391
    (dt=2026-11-03): +15 % events, `customer_id = -1` on 13 %, skew join off in the job's conf → 5.5 min →
    1.6 h, task 1993 (= `pmod(hash(-1L), 2400)`) runs 1.5 h, 44.5 GiB shuffle read, 280.7 GiB spill. Ticket
    (with the "bad node?" red herring: the straggler's spill), 8 hints, solution with 17 evidence values;
    `scripts/evidence.py` takes app ids from the YAML and can read container logs through the log links.
  - Scenario 00's job conf gained `spark.sql.adaptive.skewJoin.enabled=false` (no evidence value changed).
  - Tests: new `test_faults.py` (11), `test_scenario_dsl.py`, `test_scenario01.py`, `test_scenario01_hs.py`;
    `uv run pytest` all green, ruff clean. ADRs 026–030.
- **Next:** Phase 7 (portal + one-command startup). `scenario.yaml` already carries what the portal cards need
  (title, difficulty, topics, summary, run labels, app ids). Then Phase 8 scenarios; the cheapest next ones
  with today's mechanisms: 02 (too few partitions → spill: `config` + the per-op spill), 09 (bad node:
  `slow_node`), 05 (missed broadcast: `config` autoBroadcastJoinThreshold, the hint must go), 03 (pmem kills:
  `memory_pressure`, but "failed after 4 attempts" needs job abort).
- **Gotchas:**
  - AQE coalescing changes the reduce task count at small scales: tests on scaled data must look up a task's
    reducer range (`stage.plans[i].reads`), not assume index = partition.
  - The stage page's Event Timeline only shows the task table's current page (100 tasks): sort by Duration
    before expanding it to see a straggler.
  - The Summary Metrics table rounds durations (`2.8 s` → `3 s`); the evidence quotes both the UI strings and
    the REST values.
  - Spark's `OptimizeSkewedJoin` target piece size is max(advisory, mean non-skewed partition), not the
    advisory size: ~130 MiB pieces for Scenario 01.
  - Python 3.11: no nested same-quote f-strings (PEP 701 is 3.12); PIL isn't installed, use Playwright element
    screenshots to zoom into a page.
  - The harness required branch `claude/gifted-johnson-brvuae` (not `phase-6/<slug>`).
- **Still open:** AQE skew-join split (a "fixed run"), task exceptions / job abort, lost shuffle files without
  the shuffle service, dynamic per-task memory (a straggler's peak execution memory is under-reported,
  ADR-028), GC pressure model; the Docker Compose path is unverified.

## 2026-11-05 — Phase 7: scenario portal + one-command startup

- **Done:**
  - `sparksim prepare` (`src/sparksim/prepare.py`): every run of every scenario is one app id; Scenario 01's last
    good run and Scenario 00's run are one app, generated once; two different runs on one app id are refused
    (also by `sparksim validate`). Fingerprint = sha256(job + run from `scenario.yaml`, label excluded; the
    package source minus portal/CLI). Manifest `event-logs/.sparksim-manifest.json` (hidden, the HS skips it)
    holds fingerprints and a running prepare's progress. `sparksim generate` records into it too and clears
    an app's old container-log dir before writing.
  - Portal (`src/sparksim/portal/`): stdlib server, markdown-it-py rendering of ticket/hints/solution on every
    request, cards with topics behind a spoiler, hints one at a time, notes + solution reveal in localStorage,
    per-run deep links (Jobs, Stages, SQL, Executors, Environment) into one named tab, `/api/status`
    (generating / queued / not generated / loading / ready / out of date; HS and log server up). ADR-031.
  - `sparksim up` (`src/sparksim/up.py`): port check, nm-logs + HS subprocesses (output in `build/logs/`),
    portal thread, prepare, wait until the HS lists every app, `Ready.` + links, Ctrl+C/SIGTERM stop all.
  - `Dockerfile` (python:3.11-slim + uv, `--no-default-groups`, non-root) + `.dockerignore` (allowlist) +
    `docker-compose.yml` (generate, history-server, nm-logs, portal; named volumes; `pull_policy: build`;
    `reference` profile). ADR-032.
  - `scripts/stack_check.py`: smoke-checks a running stack (portal pages, HS deep links, driver + executor
    log links through the REST API).
  - Tickets 00/01 now point at `sparksim up` / `docker compose up` instead of `run.sh` (no evidence changed).
  - Verified from fresh clones: `uv run sparksim up` (new venv) and `docker compose up` (no image, no build
    cache) both pass `stack_check.py`; Docker-generated logs are byte-identical to local ones; a second
    `up` regenerates nothing. Screenshots of the portal (light, dark, 390 px) reviewed.
  - Tests: new `test_prepare.py` (7), `test_portal.py` (6), `test_up.py` (integration: `sparksim up` from
    empty dirs to `Ready.`, stack check, Ctrl+C stops every server). `uv run pytest`: 212 passed (~2.8 min);
    ruff clean.
- **Next:** Phase 8, the scenario library. Cheapest with today's mechanisms (see the 2026-11-04 entry): 02 (too
  few shuffle partitions → spill: `config`), 09 (bad node: `slow_node`), 05 (missed broadcast: `config`
  `autoBroadcastJoinThreshold`), 03 (pmem kills: `memory_pressure`, needs job abort for "failed after 4
  attempts"). A new scenario needs nothing in the stack: `prepare` and the portal discover
  `scenarios/*/scenario.yaml`; give each run a new `app_seq`.
- **Gotchas:**
  - Docker *does* work in the cloud container: `dockerd > /tmp/dockerd.log 2>&1 &`. Docker Hub pulls work
    (python:3.11-slim, apache/spark:3.5.9-java17); ghcr.io blobs return 403 (so the Dockerfile installs uv
    with pip). Image builds need two sandbox-only tweaks, not in the repo: (1) a local `python:3.11-slim`
    rebuilt with the proxy CA (`/root/.ccr/ca-bundle.crt` appended to `/etc/ssl/certs/ca-certificates.crt`,
    `ENV PIP_CERT=… SSL_CERT_FILE=… UV_SYSTEM_CERTS=1`; keep the original as `python:3.11-slim-original`);
    (2) build containers can't reach the proxy on the host's 127.0.0.1: relay `172.17.0.1:42852` →
    `127.0.0.1:<proxy port>` (a 20-line Python socket relay) and set `~/.docker/config.json`
    `{"proxies": {"default": {"httpsProxy": "http://172.17.0.1:42852"}}}`. Compose v5 always builds with bake,
    which refuses `build.network: host` without `--allow`.
  - JDK 17 is not preinstalled in this container image: `apt-get install -y openjdk-17-jdk-headless`.
  - A process started in the background by a non-interactive shell (`(cmd &)`) has SIGINT ignored, so Python
    never raises KeyboardInterrupt: `sparksim up` installs its own SIGINT/SIGTERM handler.
  - A CSS class rule beats an element rule: `.wrap { padding: 0 16px }` silently zeroed `main`'s vertical
    padding; use `main.wrap`.
  - The sandbox exports `UV_NATIVE_TLS` (uv warns it is deprecated); harmless.
  - The harness required branch `claude/sharp-cray-dn2d2m` (not `phase-7/<slug>`).
- **Still open:** links from the solution's evidence rows to the exact HS page; interview mode (backlog);
  native Windows (WSL2 works the same as Linux); HS heap stays at Spark's default 1 GiB (enough for today's
  ~65 MB logs, revisit for bigger scenarios).

## 2026-11-05 — Phase 8: Scenario 02 (too few shuffle partitions → spill)

- **Done:**
  - `scenarios/02-shuffle-partitions-spill` (ADR-033): the same job three weeks before 00/01. Last good run
    2026-10-14 (app 44657), incident 2026-10-15 (app 44836). One `config` fault: the move to the Airflow DAG
    factory unsets `spark.sql.shuffle.partitions` (→ 200) and adds `spark.yarn.maxAppAttempts=1` +
    `spark.yarn.tags` (Environment noise). AQE stays on: coalescing can't split. Symptoms from the simulation:
    stage 5 has 200 tasks instead of 2,400, min/med/max 2.0/2.3/2.6 min, every task spills (1451.9 GiB memory /
    609.8 GiB disk), Peak Execution Memory pinned at 2.4 GiB, 160 + 40 tasks on 160 cores; app 5.6 → 9.1 min.
    Ticket from Hadoop ops (disk alerts on the 40 executor hosts; HDFS balancer as red herring), 8 hints,
    solution with 15 evidence values (`scripts/evidence.py` S02, `tests/test_scenario02_hs.py`).
  - Planner: `SparkSession.shuffle_partitions` follows `SQLConf.numShufflePartitions` (AQE's
    `coalescePartitions.initialPartitionNum` when coalescing is on). The solution's fixes are verified in
    `tests/test_scenario02.py`: 2400 → the last good shape; `initialPartitionNum=8000` + advisory 128m → 2,667
    tasks, no spill; 600 partitions already avoid the spill.
  - Executor logs (ADR-034): task threads from a cached-pool model (`task_threads`; concurrent tasks shared
    thread ids before), spill counts from 0 (`(0  time so far)`, as in ref02), spill sizes = whole 64 MiB pages
    + pointer array, committer set-up at task start and the Parquet writer after the write sort's spills.
    Scenario 00/01 event logs are byte-identical to Phase 7 (checked); their container logs change. 01's
    solution now quotes `Thread 62`; `evidence._spill_log` counts sorters from `(0  time so far)`.
  - `evidence.fmt_bytes_js`: the stage page's Summary Metrics and task tables format bytes in utils.js
    (`1.5 GiB`, `3 GiB`), not with `Utils.bytesToString` (`1544.4 MiB`); summary values use it now.
  - `uv run pytest`: 220 passed (~3.3 min); ruff clean; `sparksim up` + `stack_check.py` pass with 3 scenarios.
- **Next:** 09 (bad-node straggler: `slow_node` exists; speculation off; the evidence is Aggregated Metrics by
  Executor + the same host slow on every stage), then 05 (missed broadcast: `config`
  `autoBroadcastJoinThreshold`, the workload's `broadcast=True` hint has to become a parameter), then 03
  (pmem kills; needs task failure → job abort).
- **Gotchas:**
  - Faults apply after the job conf: to try a fix on an incident, append a `config` fault (as
    `tests/test_scenario02.py::with_fix` does); changing `job.conf` is undone by the incident's `unset`.
  - AQE coalescing is greedy: with `initialPartitionNum=4800` and a 128 MiB advisory size, 64.4 MiB partitions
    never pair up (128.8 MiB > 128 MiB) and you get 3,985 tasks, not 2,400.
  - The local `origin/main` ref was stale (Phase 1) in this container: compare against `HEAD`/`main`, or fetch.
  - Executor logs have no thread names (log4j pattern): attribute spill lines to an executor's stage, not to a
    task, in evidence.
  - The harness required branch `claude/clever-lovelace-n4pa0z` (not `scenario/02-…`).
- **Still open:** a second scenario this session was not started; disk contention between concurrently
  spilling tasks (ADR-014) would make Scenario 02's slowdown larger and is still a lower bound.

## 2026-11-08 — Phase 9: speculative execution + Scenario 09 (bad-node straggler)

- **Done:**
  - Real reference `ref09-speculation` (ADR-035): `REF_SECOND_NM=1 reference/yarn/cluster.sh start` runs a second
    NodeManager on host `slownode` (127.0.0.2), `reference/yarn/slow_node.sh start 50` puts its executors into a
    cgroup CPU quota, `reference/jobs/ref09_speculation.py` runs with `spark.speculation=true`. What it showed:
    no `SpeculativeTaskSubmitted` in the log, killed attempts after their stage completed list no accumulables,
    "Stage finished" overwrites a pending "another attempt succeeded", a killed task's `Accumulator Updates` are
    nameless with `Internal: false` (+ `resultSize` always). `DriverLedger.stage_removed` / `failure_updates`;
    the golden test re-derives all of it. EVENT_LOG_FORMAT.md §6.2.
  - Speculation in `sched/scheduler.py` (ADR-036): port of TaskSetManager/TaskSchedulerImpl (`SpeculationConf`,
    100 ms check, `PercentileHeap` median, efficiency rule, speculatable lists, `ScalaHashSet` running-set order,
    kills with `kill_latency`, `kill_all` "Stage finished" at a result stage's job end, "Finish but did not
    commit"). Engine: `TaskRecord.speculative/killed/killed_metrics`, `cost.model.killed_metrics`, idempotent
    `ShuffleExchange.write`; emit: speculative TaskStarts, TaskKilled TaskEnds, stage removal at job end
    (`Timeline` factories may return None). Logs: driver "Starting speculative execution thread", "Marking task
    … as speculatable", "Killing attempt …", "Lost task … TaskKilled (…)", TaskSet removed after its last killed
    attempt; executor "Executor is trying to kill …" / "Executor (interrupted and) killed …", work cut at the kill.
    `spark.yarn.exclude.nodes` in the RM model + the AM's `YarnAllocatorNodeHealthTracker` line.
  - `slow_node`'s CPU factor also stretches deserialization, codegen (and its log lines), result serialization,
    JVM start-up (registration), kill handling. Healthy hosts have factor 1.0: Scenarios 00/01/02 regenerate
    byte for byte, event logs and container logs (checked against `main` in a worktree).
  - `scenarios/09-bad-node-straggler` (ADR-037): last good 2026-10-27 (app 46958), incident 2026-10-28 (app
    47131). `wn063` power-capped after a PSU failure (`cpu: 0.05`); executor 19 lands on it in the incident only
    → 5.5 → 9.3 min, its tasks the longest of every stage (13–24x the median), 28 tasks vs 225–259, registered
    88 s late. Ticket (BI SLA; change calendar with the PSU alert among red herrings), 8 hints, solution with 14
    evidence values (`scripts/evidence.py` S09, `tests/test_scenario09_hs.py`). `tests/test_scenario09.py`: the
    last good run is byte-identical with the fault (the node was already bad), both fixes verified
    (`spark.yarn.exclude.nodes` 5.5 min, `spark.speculation=true` 5.7 min). `tests/test_speculation.py`: rules,
    events vs ref09's shapes, logs, excluded nodes.
  - `uv run pytest`: 240 passed (~4.9 min, integration included); ruff clean.
- **Next:** 05 (missed broadcast: `config` `autoBroadcastJoinThreshold`, the workload's `broadcast=True` hint has
  to become a parameter), then 03 (pmem kills; needs task failure → job abort). With speculation in place, a
  "speculation can't fix skew" variant of 01 is cheap (a test or a capstone red herring).
- **Gotchas:**
  - Hadoop isn't in the container image: `downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz`
    (730 MB, sha512 in the same dir) → `/opt/hadoop`. JDK 17 needs `apt-get update` first.
  - The second NodeManager needs `127.0.0.2 slownode` in `/etc/hosts`; cgroup v1 `cpu` is writable here, so a
    quota is a real slow node. A single-node YARN can't run a speculative copy (never on the same host).
  - The reference harness sets the driver log to WARN: speculation's INFO lines (Marking/Killing) come from the
    source, the WARN `Lost task … TaskKilled` and every executor line from ref09.
  - A slow node hardly hurts this job unless it is very slow: Spark's dynamic task assignment absorbs it (3x
    slower → +5 %). The tail of each stage is where it shows.
  - The schema forbids faults on a last good run; a "condition that already existed" is shown by building that
    run with the fault and comparing the event logs.
  - A Bash heredoc with JavaScript tripped the sandbox's rm safety check; write such scripts with the Write tool.
  - The harness required branch `claude/phase-9-6f71me` (not `scenario/09-…`).
- **Still open:** the degraded node's DataNode / shuffle service serving others (write pipelines through it);
  disk-fault scenarios; speculation with barrier stages or decommissioning; the BypassMergeSortShuffleWriter's
  exception lines on a killed map task (our map stages use UnsafeShuffleWriter).

## 2026-11-13 — Phase 10: Scenario 05 (missed broadcast join) + the shuffle writer's memory

- **Done:**
  - `scenarios/05-missed-broadcast-join` (ADR-038): the job in September, before its `F.broadcast(pages)`
    (new workload param `broadcast_hint`, default true). Last good 2026-09-22 (app 40741), incident 2026-09-23
    (app 40919), on the RM before the 2026-10-13 restart (`cluster_timestamp` 1787640083508, epoch 22). One
    `config` fault: the platform's new gateway defaults (PLAT-1184) set `spark.sql.autoBroadcastJoinThreshold=-1`
    (+ `spark.driver.maxResultSize=512m`). The page catalog join becomes a sort-merge join on `page_id`; page
    popularity (`PAGE` key space, Zipf 0.85, part of the usual data) skews it: 5.5 → 22 min, stage 5 max 17 min
    vs median 4 s. Ticket (BI SLA; CMS migration of the catalog as red herring, a colleague says "skew, turn on
    AQE skew join"), 8 hints, solution with 19 evidence values (`scripts/evidence.py` S05,
    `tests/test_scenario05_hs.py`). `tests/test_scenario05.py`: cause, plan (auto-broadcast = hinted plan, byte
    for byte), skew from page popularity only in the page join, both fixes (5.5 min).
  - UnsafeShuffleWriter memory (ADR-039), found while checking the straggler against real Spark: a local 3.5.9
    run with forced writer spills + `javap` of `UnsafeShuffleWriter.stop()`. Map tasks with > 200 reduce
    partitions report their ShuffleExternalSorter's peak in Peak Execution Memory (was 0), and spill in the writer
    when their output outgrows the task's memory (log lines in Spark's format, "Task <TID> on Thread …",
    "(0  times so far)"). Scenarios 00/01/02/09 regenerate with different map-stage peak memory and executor
    memory snapshots; their timings and every evidence value are unchanged (HS tests pass).
  - Docs: ROADMAP, SCENARIOS (05 row + workload params), README, ARCHITECTURE (shuffle writer), ADR-038/039.
  - `uv run pytest`: 250 passed (~5.7 min, integration included); ruff clean.
- **Next:** 03 (YARN memoryOverhead kills: `memory_pressure` exists; needs task failure → "failed after 4
  attempts" → job abort, and a failed application in HS). Then 04 (small files: `data` overrides of the events'
  files) or 13 (missing cache: needs a workload reusing a DataFrame and the Storage tab).
- **Gotchas:**
  - A scenario dated before 2026-10-13 needs the earlier RM's `cluster_timestamp` and `rm_epoch: 22`.
  - `sparksim generate` takes `--logs-out`, not `--logs` (argparse accepts the prefix, so `--logs` works too).
  - Map stages with > 200 reduce partitions now spill in the writer when a single map task's output exceeds
    ~2.35 GiB (16g executors, 4 cores): a skewed *map* stage (any join before another shuffle) shows writer
    spills, not only sort spills.
  - Spark's peak execution memory is the sum of every consumer's peak; the model still combines operator peaks
    with max (ADR-039). Fixing it would change Scenario 02's E7 narrative ("pinned at 2.4 GiB").
  - `apt-get install -y openjdk-17-jdk-headless` again (not in the image); `javap` from it reads Spark's jars in
    `.venv/lib/python3.11/site-packages/pyspark/jars/` when the source question is "what does 3.5.9 do?".
  - The harness required branch `claude/admiring-goldberg-haqy9f` (not `scenario/05-…`).
- **Still open:** summing operator peaks like Spark; AQE skew-join split and AQE's runtime SMJ → BHJ conversion
  (05's solution discusses both without simulated numbers); the reading sorter's forced spill when the shuffle
  writer needs memory.
