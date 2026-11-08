# Scenario backlog

Priority order. Each scenario = workload + **injected cause**. The symptoms
must come out of the simulation (see ARCHITECTURE.md, realism rules).
"Evidence" lists where a good engineer finds the proof in the real Spark UI;
`solution.md` must quote the exact numbers, and a test checks them.

| # | Scenario | Injected cause | Ticket (symptom) | Evidence in Spark UI | Fix |
|---|---|---|---|---|---|
| 00 ✅ | Healthy baseline | none | (onboarding: record the baseline) | Learn what normal looks like: task max ≈ 1.5–2× median, low GC, no spill, even executors; SQL tab: BHJ + SMJ plan, row counts per operator, pre-clustered partial aggregation (`scenarios/00-healthy-baseline`) | — |
| 01 ✅ | Join-key skew | hot key `customer_id = -1` (guest placeholder) on 13 % of the events after an upstream SDK change; AQE skew join off in the job's conf (`scenarios/01-join-skew`) | "Nightly enrichment ran 1h35m, usually 6 min, dashboards stale, no code change" | Stage detail: max shuffle read ≫ median; Event Timeline one long bar; 2399/2400 tasks done; one task spills 280 GiB; SQL: SortMergeJoin, Sort spill size max = total; Environment: `spark.sql.adaptive.skewJoin.enabled=false` | AQE skew join, salting, isolate hot key, broadcast small side |
| 02 ✅ | Shuffle partitions too few → spill | an Airflow migration drops `--conf spark.sql.shuffle.partitions=2400` ("AQE sizes partitions on Spark 3") → default 200 on ~300 GiB of join shuffle; AQE on (coalescing can't split) (`scenarios/02-shuffle-partitions-spill`) | "Disk alerts on 40 worker nodes 02:14–02:19, job 5.6 → 9.1 min" | Stage 5: 200 tasks instead of 2,400, min/med/max all ~2.3 min, every task spills (1.4 TiB memory / 610 GiB disk), Peak Execution Memory pinned at 2.4 GiB, 160 + 40 tasks on 160 cores; SQL: Exchange `number of partitions` 200, no AQEShuffleRead; Environment: `spark.sql.shuffle.partitions` missing (+ two harmless Airflow keys) | Restore the count; better `coalescePartitions.initialPartitionNum` high + advisory size, AQE coalesces down |
| 03 | YARN memoryOverhead kills | off-heap usage (Arrow/netty/Python) > `memoryOverhead` | "Job failed after 4 attempts" | Failed tasks `ExecutorLostFailure`; Executors tab dead executors; stderr: "Container killed by YARN for exceeding physical memory limits", exit 137 | Raise `spark.executor.memoryOverhead`, fewer cores/executor, smaller partitions |
| 04 | Small files / tiny tasks | input table of 120k small files | "Simple filter job takes 50 min" | 120k tasks, median task 40 ms, scheduler delay & deserialization dominate; SQL scan: number of files read | Compaction, `maxPartitionBytes`/`openCostInBytes`, coalesce |
| 05 | Missed broadcast join | stats missing / AQE off → SMJ on 20 MB dim | "Job got slower after table migration" | SQL: SortMergeJoin with tiny side, big Exchange on fact side; Environment broadcast threshold | ANALYZE TABLE, broadcast hint, AQE |
| 06 | Partition pruning failure | filter casts partition column (`cast(dt as date)`) | "Daily job reads whole table" | SQL scan: `PartitionFilters: []`, files/size read = whole table; input size per stage | Fix predicate type, avoid functions on partition column |
| 07 | Exploding join (duplicate keys) | dimension has duplicate business keys | "Output 50× bigger than expected, job slow" | SQL: join output rows ≫ input rows; shuffle write of downstream stage | Dedupe dimension, fix join key |
| 08 | GC pressure | huge executor heap + object-heavy rows / cached deserialized | "Job slow, CPU busy" | Executors tab task time red (GC > 10%); stage GC time column; Peak JVM memory | Serialized caching, smaller executors, G1 tuning, avoid object churn |
| 09 ✅ | Bad-node straggler | one NodeManager (`wn063`) power-capped after a PSU failure: CPU at ~1/20; YARN puts an executor on it on some nights only; speculation off (`scenarios/09-bad-node-straggler`) | "The nightly enrichment is randomly slow" (5.5 → 9.3 min) | Stage 2: max 22x the median with a normal input split; the longest tasks of every stage on executor 19 (13–24x the median); Aggregated Metrics by Executor / Executors: 28 tasks vs 225–259, Add Time 88 s late; GC, spill, failures normal; rack neighbours fine; last good run had no executor on that host; stderr: 53 s JVM start, slow codegen | `spark.yarn.exclude.nodes` / drain the node; `spark.speculation=true` (verified); NM health check; `excludeOnFailure` would not help (nothing failed) |
| 10 | Python UDF bottleneck | row-at-a-time Python UDF in hot path | "PySpark job 5× slower than Scala version" | SQL: `BatchEvalPython`; high task duration with low shuffle; stderr Python worker lines | Native functions, pandas UDF (Arrow) |
| 11 | Driver bottleneck | `collect()`/`toPandas()` + driver-side loop | "Cluster idle but job takes hours" | Jobs Event Timeline: long gaps between jobs; driver result size; many tiny jobs | Push work to executors, avoid collect, write output |
| 12 | Fetch failure / stage retry | executor lost during shuffle, no ESS | "Flaky job, sometimes 2× slower" | Stages: attempt 1 (retry), `FetchFailedException`; executor removed | External shuffle service, decommissioning, fix root executor loss |
| 13 | Missing cache / recomputation | DataFrame reused by 3 actions without cache | "Same input read 3 times" | Jobs show identical stage DAGs, no skipped stages; Storage tab empty | cache/persist at the right point, unpersist |
| 14 | YARN queue starvation / slow ramp | queue at capacity; dynamic allocation can't get containers | "Job sometimes waits 30 min to start working" | Jobs Event Timeline: executors trickle in; tasks pending; few active executors | Queue capacity, min executors, schedule window |
| 15 | Skewed / small-file output | `repartition(low-cardinality col)` before write | "Write stage takes forever, downstream reads 40k files" | Final stage: a few huge tasks; output size/records per task | Repartition by (col, salt), `maxRecordsPerFile`, AQE rebalance |
| ★ | Capstone(s) | 2–3 faults combined + red herrings | realistic multi-cause incidents | — | — |

Fault mechanisms available in `scenario.yaml` (Phase 6, ADR-029): `key_skew` (hot keys, Zipf), `config`,
`slow_node` (CPU/disk/NIC; the CPU factor also stretches deserialization, codegen, JVM start-up, ADR-037),
`memory_pressure` (native memory per task), `executor_killed` (SIGKILL, exit 137). The engine also models
speculative execution (`spark.speculation`, ADR-036) and `spark.yarn.exclude.nodes`.
The planner also follows `spark.sql.adaptive.coalescePartitions.initialPartitionNum` (Scenario 02's durable
fix). Still to build for the backlog: task exceptions + job abort (03's "failed after 4 attempts"), lost
shuffle files without the shuffle service (12), AQE's skew-join split (a "fixed run" for 01), GC pressure
(08), Python UDF nodes (10), queue capacity (14).

Later: Structured Streaming (ADR-005), dynamic-allocation thrash, broadcast timeout / driver OOM, "AQE working as intended" (to recognize success).

Also later: **driver failure → YARN app attempt 2.** In cluster mode a failed driver is retried by
YARN (`yarn.resourcemanager.am.max-attempts`, default 2), so the History Server lists two attempts
and the second may "succeed" or fail differently. Seen for real while capturing `ref04` (a driver
`AttributeError` produced `container_…_02_000001`). Evidence: attempt selector on the app page,
attempt 1 shows partial jobs, AM stdout shows the Python traceback.
