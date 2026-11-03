# Architecture

## Pipeline

```
scenarios/<id>/scenario.yaml
   cluster (YARN nodes, executors, cores, memory), Spark configs,
   tables (rows, row width, partitioning, file layout, key distributions),
   the query/job shape, fault injections, seed
        │
        ▼
 planner     logical plan → physical plan (Spark 3.5 operator names, AQE)
             → stage DAG split at Exchanges (like DAGScheduler)
             → RDD lineage, operation scopes, call sites
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
        ├──► mock YARN NodeManager logs      http://localhost:8042    ← executor stdout/stderr links
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
apps/<app>.py  AppSpec: cluster, executor sizing, input datasets, queries = stage pipelines (Phase 4: from a planner)
      │
      ▼
model/   cluster.py   racks, NodeManagers (+ other tenants' load), container sizing, YARN placement, registration
         hdfs.py      files, 128 MiB blocks, rack-aware replicas, Parquet row groups
         files.py     Spark's scan planning: maxSplitBytes, splitFiles, Next Fit Decreasing, preferred locations
         workload.py  StageSpec = sources → operators (rows, bytes, CPU, memory) → sink; KeySpace
         shuffle.py   block matrices records[map, reducer], local/remote reads, AQE coalescing
cost/    calibration.py ([ref]/[norm] constants), model.py: one task's phases → TaskMetrics
sched/   sim.py (event heap, signals, generator processes), scheduler.py (TaskSchedulerImpl + TaskSetManager)
engine/  runtime.py   the driver: executors register, AQE query stages → jobs, skipped stages, write → new HDFS files
         lineage.py   RDD lineage + operation scopes per stage (stage DAG)
         emit.py      heartbeat-sampled executor metrics; events via Timeline + DriverLedger
```

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
- Container IDs `container_e<epoch>_<clusterTs>_<seq>_<attempt>_<n>`, plus
  NodeManager hosts and ports in the executor attributes and log URLs.
- Executor log links point at a mock NodeManager log server. The plan is to
  use the History Server's `spark.history.custom.executor.log.url`. This
  still needs checking in Phase 5.
- Executor loss reasons in YARN's wording (container killed for exceeding
  memory limits, exit codes 137/143, lost node), `spark.executor.memoryOverhead`
  sizing, queue and resource limits, dynamic allocation with external shuffle
  service.
- Environment tab: realistic Spark, Hadoop and system properties and classpath
  for a Hadoop 3.x YARN cluster.

## Scenario packaging

Each incident scenario generates **two applications**: the last good run of
the job and the bad run. On-call reality is "it took 40 min yesterday and
3 h today", and comparing the two is part of the skill.

```
scenarios/01-join-skew/
  scenario.yaml   workload + fault + seed
  ticket.md       what the on-call engineer is told (symptom only)
  hints.md        progressive hints (where to look, not the answer)
  solution.md     root cause, click path, evidence numbers, fix, 60-second interview answer
```

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
