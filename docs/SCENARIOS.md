# Scenario backlog

Priority order. Each scenario = workload + **injected cause**. The symptoms
must come out of the simulation (see ARCHITECTURE.md, realism rules).
"Evidence" lists where a good engineer finds the proof in the real Spark UI;
`solution.md` must quote the exact numbers, and a test checks them.

| # | Scenario | Injected cause | Ticket (symptom) | Evidence in Spark UI | Fix |
|---|---|---|---|---|---|
| 00 | Healthy baseline | none | — | Learn what normal looks like: task max ≈ 1.5–2× median, low GC, no spill, even executors | — |
| 01 | Join-key skew | hot key (e.g. `customer_id = -1` guest checkouts) in fact table; AQE skew join off | "Nightly enrichment took 3h12m, usually 40m, no code change" | Stage detail: max shuffle read ≫ median; Event Timeline one long bar; 399/400 tasks done; SQL: SortMergeJoin; Environment: `spark.sql.adaptive.skewJoin.enabled=false` | AQE skew join, salting, isolate hot key, broadcast small side |
| 02 | Shuffle partitions too few → spill | 200 shuffle partitions on ~2 TB shuffle | "Job slow, disk-usage alerts on workers" | Every task spills (memory & disk); ~10 GB/partition; Environment: `spark.sql.shuffle.partitions=200`, AQE off | Raise partitions / AQE coalesce with advisory size |
| 03 | YARN memoryOverhead kills | off-heap usage (Arrow/netty/Python) > `memoryOverhead` | "Job failed after 4 attempts" | Failed tasks `ExecutorLostFailure`; Executors tab dead executors; stderr: "Container killed by YARN for exceeding physical memory limits", exit 137 | Raise `spark.executor.memoryOverhead`, fewer cores/executor, smaller partitions |
| 04 | Small files / tiny tasks | input table of 120k small files | "Simple filter job takes 50 min" | 120k tasks, median task 40 ms, scheduler delay & deserialization dominate; SQL scan: number of files read | Compaction, `maxPartitionBytes`/`openCostInBytes`, coalesce |
| 05 | Missed broadcast join | stats missing / AQE off → SMJ on 20 MB dim | "Job got slower after table migration" | SQL: SortMergeJoin with tiny side, big Exchange on fact side; Environment broadcast threshold | ANALYZE TABLE, broadcast hint, AQE |
| 06 | Partition pruning failure | filter casts partition column (`cast(dt as date)`) | "Daily job reads whole table" | SQL scan: `PartitionFilters: []`, files/size read = whole table; input size per stage | Fix predicate type, avoid functions on partition column |
| 07 | Exploding join (duplicate keys) | dimension has duplicate business keys | "Output 50× bigger than expected, job slow" | SQL: join output rows ≫ input rows; shuffle write of downstream stage | Dedupe dimension, fix join key |
| 08 | GC pressure | huge executor heap + object-heavy rows / cached deserialized | "Job slow, CPU busy" | Executors tab task time red (GC > 10%); stage GC time column; Peak JVM memory | Serialized caching, smaller executors, G1 tuning, avoid object churn |
| 09 | Bad-node straggler | one NodeManager host with degraded disk | "Random slowness" | Aggregated Metrics by Executor: one host slow on all tasks; speculation off | Speculation, exclude node (`spark.excludeOnFailure`), report node |
| 10 | Python UDF bottleneck | row-at-a-time Python UDF in hot path | "PySpark job 5× slower than Scala version" | SQL: `BatchEvalPython`; high task duration with low shuffle; stderr Python worker lines | Native functions, pandas UDF (Arrow) |
| 11 | Driver bottleneck | `collect()`/`toPandas()` + driver-side loop | "Cluster idle but job takes hours" | Jobs Event Timeline: long gaps between jobs; driver result size; many tiny jobs | Push work to executors, avoid collect, write output |
| 12 | Fetch failure / stage retry | executor lost during shuffle, no ESS | "Flaky job, sometimes 2× slower" | Stages: attempt 1 (retry), `FetchFailedException`; executor removed | External shuffle service, decommissioning, fix root executor loss |
| 13 | Missing cache / recomputation | DataFrame reused by 3 actions without cache | "Same input read 3 times" | Jobs show identical stage DAGs, no skipped stages; Storage tab empty | cache/persist at the right point, unpersist |
| 14 | YARN queue starvation / slow ramp | queue at capacity; dynamic allocation can't get containers | "Job sometimes waits 30 min to start working" | Jobs Event Timeline: executors trickle in; tasks pending; few active executors | Queue capacity, min executors, schedule window |
| 15 | Skewed / small-file output | `repartition(low-cardinality col)` before write | "Write stage takes forever, downstream reads 40k files" | Final stage: a few huge tasks; output size/records per task | Repartition by (col, salt), `maxRecordsPerFile`, AQE rebalance |
| ★ | Capstone(s) | 2–3 faults combined + red herrings | realistic multi-cause incidents | — | — |

Later: Structured Streaming (ADR-005), dynamic-allocation thrash, broadcast timeout / driver OOM, "AQE working as intended" (to recognize success).
