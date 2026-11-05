# Architecture

## Pipeline

```
scenarios/<id>/scenario.yaml + apps/<workload>.py
   cluster (YARN nodes, executors, cores, memory), Spark configs,
   tables (schemas, file layout, key distributions), the DataFrame queries,
   per run: date, data changes, fault injections, seed
        │
        ▼
 planner     DataFrame code → physical plan (Spark 3.5 operators, AQE query stages,
             codegen stages) → stage pipelines per query stage
             → RDD lineage, operation scopes, call sites, SQL plan events
        │
        ▼
 cost model  per-task work from data: input bytes/rows, shuffle bytes/records,
             CPU time, spill (partition size vs execution memory), GC (heap
             pressure), (de)serialization, fetch wait, scheduler delay
        │
        ▼
 scheduler   discrete-event simulation of executors and cores on YARN:
             task launch/finish times, locality waits, failures and retries,
             executor loss, speculation, dynamic allocation
        │
        ▼
 eventlog    Spark 3.5 JsonProtocol events, one JSON object per line
             (+ yarnlogs: per-container stdout/stderr that match the run)
        │
        ▼
 event-logs/application_<clusterTs>_<seq>_1[.zstd]
        │
        ├──► Spark History Server 3.5.9      http://localhost:18080   ← the UI you debug in
        ├──► mock YARN NodeManager logs      http://localhost:8042    ← executor stdout/stderr links (yarn-logs/)
        └──► scenario portal                 http://localhost:8000    ← tickets, hints, solutions
```

## The eventlog layer (built in Phase 2)

```
generator (apps/, later sched/)          sparksim.eventlog
  task-level facts only:                   Timeline       sorts by simulated time, builds events at emission
  TaskMetrics, outcome, touched zeros,  →  DriverLedger   adds the driver's bookkeeping: running accumulator Value,
  SQL metric updates, IDs (IdAllocator)                   task-metric IDs, StageCompleted accumulables
                                           events/common  one dataclass per JsonProtocol type, key order = field order
                                           jsonfmt + io   Jackson-identical bytes → application_<ts>_<seq>_<attempt>
```

Proof: every reference line round-trips byte for byte (ADR-010), and the
ledger re-derives every real accumulable (ADR-011, EVENT_LOG_FORMAT.md §5.1).

## The simulation engine (built in Phase 3)

```
apps/<app>.py  AppSpec: cluster, executor sizing, input datasets, queries = PySpark-like DataFrame code (sql/)
      │
      ▼
model/   cluster.py   racks, NodeManagers (+ other tenants' load), container sizing, YARN placement, registration
         hdfs.py      files, 128 MiB blocks, rack-aware replicas, Parquet row groups
         files.py     Spark's scan planning: maxSplitBytes, splitFiles, Next Fit Decreasing, preferred locations
         workload.py  StageSpec = sources → operators (rows, bytes, CPU, memory) → sink; KeySpace
         shuffle.py   block matrices records[map, reducer], local/remote reads, AQE coalescing
cost/    calibration.py ([ref]/[norm] constants), model.py: one task's phases → TaskMetrics
sched/   sim.py (event heap, signals, generator processes), scheduler.py (TaskSchedulerImpl + TaskSetManager)
engine/  runtime.py   the driver: executors register, one AdaptiveExecution per query, its query stages → jobs,
                      skipped stages, SQL events and driver metrics, write → new HDFS files; executor loss
                      (disconnect → loss reason → ExecutorLostFailure, re-run), broadcasts, MapOutputTracker epoch
         yarn_am.py   YARN in the simulation (Phase 5): RM grants at NodeManager heartbeats (absolute asks,
                      over-allocation), Spark's YarnAllocator loop (200 ms doubling to 3 s), container launch,
                      NodeManager memory monitor (pmem kill, Hadoop diagnostics), completed containers → reasons
         dynalloc.py  ExecutorAllocationManager + ExecutorMonitor (backlog ramp-up 1,2,4,8…, idle timeout)
         emit.py      heartbeat-sampled executor metrics; events via Timeline + DriverLedger
model/   memory.py    container RSS: committed heap (G1 growth) + native footprint vs the container size
```

## The YARN container logs (built in Phase 5)

```
AppRun records (log, yarn_log, task_records, broadcasts)
      │
      ▼
yarnlogs/  executor.py  one executor's stderr: start-up, per task Got assigned / Running / broadcast fetch /
                        codegen / Reading File path / ShuffleBlockFetcherIterator / Parquet writer + commit /
                        spill / Finished, then "Driver commanded a shutdown" or "RECEIVED SIGNAL TERM"
           driver.py    the AM container's stderr = the driver log: AM + SparkContext start-up, YarnAllocator,
                        executor registrations, DAGScheduler / TaskSetManager lines, executor loss, dynamic
                        allocation, shutdown (+ what the driver logs in reaction to executors: block updates,
                        map-status requests)
      │
      ▼
yarn-logs/<appId>/<containerId>/{stdout,stderr}  ──►  services/nm-logs (mock NodeManager, :8042)
                                                       ◄── History Server executor links
                                                           (spark.history.custom.executor.log.url)
```

Lines are placed on the simulated clock: a task's lines sit between its launch and finish, tasks of a
stage that wait for the first one's broadcast fetch and code generation log after it, and a killed
container writes nothing after its SIGTERM.

## The SQL layer (built in Phase 4)

```
sql/  dataframe.py  read / where / select / join / group_by().agg() / write_parquet → logical plan;
                    planner: filter pushdown + isnotnull inference, column pruning, BHJ vs SMJ, exchanges, sorts,
                    partial/final aggregation, planned-write sort. Re-entrant: AQE re-plans around finished stages
      aqe.py        AdaptiveSparkPlanExec: createQueryStages, CoalesceShufflePartitions (AQEShuffleRead),
                    ColumnarToRow, CollapseCodegenStages, re-planning, final plan; query stage → engine StageSpec
      nodes.py      operators: node names, simple strings, explain fields, SQL metric maps (definition order)
      explain.py    explain("formatted") = physicalPlanDescription
      planinfo.py   SparkPlanInfo, metric accumulator ids allocated lazily in pre-order
      scopes.py     RDD scopes + lineage from execute()/produce() call order (stage DAG boxes)
      metrics.py    per-task SQL metric updates from the cost model's per-operator work; driver-side updates
```

Node identity is modelled on purpose: a node keeps its metric ids and its explain operator id for as long as
the same object stays in the plan, and rules that change a child copy the node, as Catalyst's `makeCopy` does.
That is what makes the AQE updates match real logs (ADR-018).

Data flows forward only: a write stage creates files on the simulated HDFS
(first replica on the writer's host), and later queries plan their scans from
those files, so task counts, input sizes and locality downstream follow from
what upstream wrote.

## The realism rules ("numbers must make sense")

1. **Conservation.** Rows and bytes flow through operators. The shuffle write
   of a map stage equals the total shuffle read of its reduce stage. The
   output rows of one operator are the input rows of the next. Tests enforce
   this.
2. **Time is simulated, not invented.** A task's launch time is when a core
   became free. Its duration is the sum of its modeled phases. A stage ends
   when its last task ends, and a job ends when its last stage ends.
3. **One source per metric.** We write only what Spark itself writes (task
   metrics, accumulator updates, executor metric peaks, plan info). Every
   total is derived by the History Server.
4. **Seeded, physically plausible variance.** Per-task lognormal noise (small
   sigma), per-host speed factors, GC jitter. No uniform random numbers on
   output fields.
5. **Calibrated constants in one place.** Throughput and overhead constants
   (scan MB/s per core, shuffle write/read rates, serialization cost, GC
   behavior) live in `src/sparksim/cost/calibration.py`. Each constant carries
   a note on where it came from: a reference run or a cited production norm.
6. **Causes, not symptoms.** Faults change inputs to the simulation
   (distribution, config, node health, memory), and the UI symptoms follow.

## YARN specifics we model

- App IDs `application_<clusterTimestamp>_<seq>`, attempt ID `1`; cluster
  deploy mode (the driver runs in the ApplicationMaster container).
- Container IDs `container_e<epoch>_<clusterTs>_<seq>_<attempt>_<n>` (the RM
  epoch after work-preserving restarts; HA cluster id as `CLUSTER_ID`),
  NodeManager hosts and ports in the executor attributes and log URLs. Container
  numbers follow the RM's grants, including the extra containers YARN's
  absolute asks over-allocate and Spark releases unused.
- Executor log links open generated container logs through a mock NodeManager
  log server (`services/nm-logs`), via the History Server's
  `spark.history.custom.executor.log.url` (verified on HS 3.5.9, driver row included).
- Executor loss in YARN's wording: physical-memory kills from the container
  memory model (`spark.executor.memoryOverhead` vs the JVM's native footprint;
  exit status -104, SIGTERM 143, Hadoop's diagnostics), "Container from a bad
  node" for other exits, ExecutorLostFailure for the running tasks, a
  replacement container; dynamic allocation with the external shuffle service
  (idle executors: "Executor killed by driver.").
- Environment tab: the full Hadoop 3.3.6 configuration (~1,000 keys) with the
  production site files, the cluster's spark-defaults, JDK 17 system properties
  and the real Spark 3.5.9 classpath.
- Not yet: lost shuffle files without the shuffle service (FetchFailed, stage
  retry), node-local container requests, preemption, queue limits, app attempt 2.

## Scenario packaging (built in Phase 6)

Each incident scenario generates **two applications**: the last good run of
the job and the bad run. On-call reality is "it took 6 min yesterday and
1.6 h today", and comparing the two is part of the skill (ADR-006).

```
scenarios/01-join-skew/
  scenario.yaml   job (workload, cluster, executors, --conf) + runs (last_good, incident: params, data, faults)
  ticket.md       what the on-call engineer is told (symptom only)
  hints.md        progressive hints (where to look, not the answer)
  solution.md     root cause, click path, evidence numbers, fix, 60-second interview answer
```

```
scenario.yaml ──► sparksim.scenario.schema   pydantic models: units (16g, 6m30s), run roles, fault types
      │
      ▼
apps/<workload>.py   WORKLOAD: params (dt), datasets(params) = the usual input data (files, sizes,
      │              key distributions per column), queries(params, datasets) = DataFrame code
      ▼
sparksim.scenario    the run's data overrides, then its faults (faults.py) → engine.runtime.AppSpec
      │              key_skew → KeySpace.hot / .zipf of a dataset column   (shuffle partition sizes)
      │              config → --conf / executor sizing                     (whatever Spark reads)
      │              slow_node → Host.cpu/disk/net factors                 (every task on that host)
      │              memory_pressure → native memory per running task      (container RSS → pmem kills)
      │              executor_killed → SIGKILL at a time                   (exit 137 → ExecutorLostFailure)
      ▼
engine → event log (+ container logs) per run: `sparksim generate <id>` writes all of them
```

Hot keys go to the partition Spark's hash partitioning picks
(`model/murmur3.py`: `pmod(Murmur3Hash(key, 42), n)`, checked against PySpark), Zipf
heads rank by rank, the tail by its second moment. A partition's size decides its
task's work and, against the task's execution memory, its spill (each sorter or
aggregation map spills its own share, `cost.model._spill_group`).

## The portal and the one-command stack (built in Phase 7)

```
                        ┌─ sparksim prepare ─ plan_apps: every run of every scenario → one app id (shared runs
                        │                     once); fingerprint = sha256(job + run in scenario.yaml, simulator
                        │                     source); generate what is missing/stale → event-logs/ + yarn-logs/
                        │                     + event-logs/.sparksim-manifest.json (hidden: HS skips it)
sparksim up  ───────────┼─ services/nm-logs/server.py       :8042   (subprocess, build/logs/nm-logs.log)
(no Docker)             ├─ services/history-server/run.sh    :18080  (subprocess, build/logs/history-server.log)
                        └─ sparksim.portal (thread)          :8000
docker compose up       the same four as services: generate (runs once, exits), history-server
                        (apache/spark:3.5.9-java17), nm-logs and portal (the simulator image, Dockerfile);
                        named volumes event-logs + yarn-logs
```

The portal (`sparksim/portal/`, standard-library HTTP server) renders each scenario's `ticket.md`,
`hints.md` (one hint per `<details>` block) and `solution.md` with markdown-it-py on every request, so
edits show without a restart. It links every run into the History Server (`/history/<app>/1/jobs/`, stages,
SQL, executors, environment) and polls its own `/api/status`, which combines the manifest (generated?
being generated?), the History Server's application list (listed yet?) and whether the log server answers.
Hints revealed, the learner's notes and the solution reveal live in the browser's localStorage. The portal
never shows a metric: every number is read in the History Server (ADR-031). `scripts/stack_check.py`
checks a running stack the way a learner uses it.

## Testing strategy

- **Unit:** conservation invariants, scheduler correctness (no core runs two
  tasks at once, no task before its parent stage completes), determinism
  (same hash every run).
- **Format:** synthetic events checked field-by-field against real events in
  `reference/` for the same event types.
- **Integration:** start History Server 3.5.9, load the generated log, assert
  there are no replay errors, and query `/api/v1/applications/...` to check
  the values match the model and each scenario's `solution.md` evidence.
- **Visual smoke:** Playwright opens key pages (Jobs, Stage detail, Executors,
  SQL) and saves screenshots for review.
