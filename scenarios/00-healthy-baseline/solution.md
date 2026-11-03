# Solution: the healthy baseline of `clickstream-daily-enrichment`

Nothing is wrong with this run. It is the reference you compare against when the
job pages you. Every number below is what the History Server shows, and
`tests/test_scenario00_hs.py` checks each one against its REST API
(`uv run scripts/evidence.py 00-healthy-baseline` prints them for a running server).

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | Jobs tab, header | Total Uptime | `5.5 min` |
| E2 | Jobs tab / Stages tab | Jobs, completed and skipped stages | `8 jobs, 8 completed + 4 skipped stages` |
| E3 | Stages tab | Tasks over all completed stages | `9,447` |
| E4 | Executors tab | Executors | `40 x 4 cores on 40 hosts` |
| E5 | Stages tab, stage 2 (events scan) | Tasks, duration | `5,200 tasks, 3.7 min` |
| E6 | Stage 2, Summary Metrics, Duration | Median / max | `7.0 s / 10.2 s (1.46x)` |
| E7 | Stage 2 header | Input, shuffle write | `517.7 GiB in, 296.6 GiB shuffle write` |
| E8 | Stage 2 header, Locality Level Summary | Tasks per locality level | `NODE_LOCAL 3,684, RACK_LOCAL 1,516` |
| E9 | Stages tab, stage 5 (join + write) | Shuffle read, output | `303.2 GiB read, 130.7 GiB written by 2,400 tasks` |
| E10 | Stage 5, Summary Metrics, Duration | Median / max | `2.8 s / 3.7 s (1.32x)` |
| E11 | Stages tab, stage 8 (daily_funnel final) | Tasks reading 2,400 shuffle partitions | `166 tasks` |
| E12 | Executors tab, Task Time (GC Time) | GC share per executor, min – max | `1.8 % – 2.1 %` |
| E13 | Stage pages | Spill (memory + disk), all stages | `0 B` |
| E14 | Executors tab, Complete Tasks | Tasks per executor, min – max | `224 – 261` |
| E15 | SQL / DataFrame tab | Queries and their durations | `3 queries: 4.7 min, 13 s, 21 s` |
| E16 | SQL query 0, events `Scan parquet` → `Filter` | number of output rows | `4,426,997,388 → 4,117,107,532` |
| E17 | SQL query 0, `BroadcastExchange` | data size, rows | `4.4 MiB, 46,261 rows` |
| E18 | SQL query 0, `SortMergeJoin` | number of output rows | `4,055,350,906 rows` |
| E19 | SQL query 2, `Scan parquet` → `HashAggregate` (partial) → `HashAggregate` (final) | number of output rows | `4,055,350,906 → 61,000,003 → 60,962,709` |

## Click path and what each number means

**Jobs tab.** The app ran **5.5 min** (E1) with **8 jobs** for 3 writes (E2).
Spark 3.5 runs queries with AQE, so each shuffle is materialized by its own
job, and the broadcast of the page catalog is a job of its own (stage 1,
`$anonfun$withThreadLocalCaptured$1 at FutureTask.java:264`: broadcasts run on
a helper thread). The last job of each query lists the finished map stages
again under new ids and skips them: the **4 skipped stages** (E2) are those,
greyed out in each final job's DAG. Not lost work, not failures. Each job page
links back to its SQL query ("Associated SQL Query").

**Executors.** 40 executors x 4 cores (E4), 16 GiB heap each
(`spark.executor.memory`, Environment tab), static allocation. YARN placed one
container per NodeManager, so they sit on 40 of the cluster's 120 hosts. All
registered within ~2.5 s, and the first job started once the driver had them.

**Stage 2, the events scan (the biggest stage).** 5,200 tasks, 3.7 min (E5).
Spark cut 4,300 Parquet files at `spark.sql.files.maxPartitionBytes = 256m` and
packed the small tails together, so most tasks read one 256 MiB split. Task
times are tight: median 7.0 s, max 10.2 s, **1.46x** (E6). That's the shape of
a healthy stage: max within ~1.5–2x the median, no long tail. The stage reads
517.7 GiB of the selected columns and shuffles 296.6 GiB to the join (E7).
**Locality:** 3,684 tasks NODE_LOCAL and 1,516 RACK_LOCAL (E8). Each HDFS
block has 3 replicas, but our executors run on only 40 of 120 nodes, so for
some splits no replica host has an executor. After `spark.locality.wait`
(3 s) the scheduler runs those on another host in the replica's rack:
RACK_LOCAL reads cross a switch and those tasks take a little longer. That's
normal. A sudden jump in ANY would not be. A few dozen tasks read 0 records:
their split held no Parquet row-group midpoint, so the row groups were read by
the neighbouring split. Also normal.

**Stage 5, join + write.** Reads both shuffles (303.2 GiB, E9) into 2,400
tasks (`spark.sql.shuffle.partitions = 2400`), sorts both sides, sort-merge
joins on `customer_id`, sorts by `dt` for the partitioned write, and writes
130.7 GiB of Parquet. Median 2.8 s, max 3.7 s (**1.32x**, E10): partition sizes
are even because `customer_id` has ~200 M distinct values and no hot key. The
Locality column says PROCESS_LOCAL for every task. A reduce task has no
preferred host when its input is spread over thousands of map outputs, and
Spark reports such tasks as PROCESS_LOCAL. Nothing is cached.

**What AQE changed.** The funnel aggregation shuffles only 1.4 GiB into 2,400
partitions. AQE coalesced them into **166 tasks** (E11): the final stage's DAG
and the SQL plan have an `AQEShuffleRead` box (`number of partitions: 166`).
The activity aggregation got the same treatment. The join stage has no
`AQEShuffleRead`, because its ~130 MiB partitions were already above the
coalescing target.

**JVM and memory.** GC is **1.8–2.1 %** of task time on every executor (E12),
far below the 10 % at which the Executors tab turns the cell red. **No spill**
anywhere (E13): the largest per-task sort buffer (~500 MiB `peak memory` of the
write's `Sort` in the SQL plan) fits in the ~2.4 GiB of execution memory each
task can use. Work is evenly spread: **224–261 tasks** per executor (E14).

**SQL / DataFrame tab.** One row per DataFrame action: **3 queries**, 4.7 min,
13 s and 21 s (E15). Click query 0 to see its plan as Spark ran it (AQE's final
plan; *Details* below the graph has the same plan as text, with the initial
plan underneath). Blue boxes are `WholeStageCodegen (n)` stages: the operators
inside run as one generated function, and its `duration` covers all of them.
`Exchange` boxes are shuffles, and they carry both sides: shuffle bytes written
by the map stage and records/bytes read by the next stage. Read the row counts
from the top:

* the events scan returns **4.43 billion** rows and the `Filter`
  (`NOT is_bot`, plus `isnotnull` on both join keys, which Spark infers) keeps
  **4.12 billion** (E16): 7 % of the traffic was bots;
* the page catalog is a `BroadcastExchange` of **4.4 MiB, 46,261 rows** (E17),
  far below `spark.sql.autoBroadcastJoinThreshold` (10 MiB), so the
  `BroadcastHashJoin` keeps every event without a shuffle;
* customers are too big to broadcast (`size of files read` 58.3 GiB), so both
  sides are shuffled on `customer_id`, sorted, and the `SortMergeJoin` outputs
  **4.06 billion** rows (E18): 1.5 % of the events have no customer in the
  snapshot (accounts created after it was taken) and drop out of the inner join.

Also worth noticing: the scan nodes show `size of files read` (whole files,
1195.7 GiB for events) while the stage page's Input (517.7 GiB) counts only the
columns read. Parquet reads 11 of the 24 event columns and 5 of the 20
customer columns (`ReadSchema` in *Details*).

**Why the activity aggregation shuffles so little** (E19). Query 2 groups 4.06
billion rows by `customer_id`, a key with 61 million values, yet its partial
`HashAggregate` (before the `Exchange`) already cuts them to **61.0 million**,
almost exactly the final count (**60.96 million**). Partial aggregation usually
helps little on a high-cardinality key. It works here because of how query 0
wrote `events_enriched`: each write task got one hash range of `customer_id`
from the join's shuffle, so every file holds all the events of its customers,
and a scan task meets each of its customers in full. Data written right after
a shuffle on the key you later group by is pre-clustered. Losing that layout,
for example by repartitioning before the write, would make this shuffle ~60x
bigger.

**Executor logs.** On the Executors tab each row has `stdout` / `stderr` links (YARN container logs,
served here by the NodeManager mock on port 8042). A healthy executor's stderr reads like this: start-up
(registration with the driver, `MemoryStore started with capacity 9.4 GiB`), then per task
`Running task … (TID n)`, the broadcasts it fetched, `Reading File path: …` lines for each file split a scan
read, `Getting N … non-empty blocks including … local and … remote blocks` for each shuffle it read,
`Saved output of task …` for writes, and `Finished task … bytes result sent to driver`; at the end
`Driver commanded a shutdown`. No WARN or ERROR lines, no `spilling sort data`, no `RECEIVED SIGNAL TERM`.
The driver's row links to the ApplicationMaster container (the driver runs inside it in cluster mode): its
stderr has every `Starting task` / `Finished task … (k/n)` line and the YarnAllocator's container requests.

## The baseline to remember

| | Healthy value |
|---|---|
| Runtime | ~5.5 min, 8 jobs (AQE: one job per shuffle), 3 SQL queries |
| Biggest stage | events scan, ~5k tasks, ~4 min, max/median ≈ 1.5 |
| Join + write | 2,400 tasks, max/median ≈ 1.4, no spill |
| SQL plan | BroadcastHashJoin (page catalog) + SortMergeJoin (customers), ~4.1 B rows in and out |
| Locality | ~70 % NODE_LOCAL, rest RACK_LOCAL on scans; PROCESS_LOCAL on reduce stages |
| Logs | executor stderr: only INFO lines; containers end with `Driver commanded a shutdown`, none removed |
| GC | ~2 % of task time |
| Spill | none |
| Executors | 40 x 4 cores, even task counts |

## 60-second interview answer

*"What does a healthy Spark job look like in the UI?"*

> I start on the Jobs tab: total runtime against the usual, and the event timeline to see whether
> jobs ran back to back or left gaps. With AQE you expect several jobs per query and some skipped
> stages, which is fine. Then I sort the Stages tab by duration and open the biggest stage. In the
> summary metrics I compare the max task to the median: a healthy stage is within about 1.5 to 2x
> with no long tail, shuffle read is spread evenly, and spill is zero. If one task is far above the
> median I check its input and shuffle-read size. More data across the board points to file sizes or
> partitioning; one huge shuffle partition points to key skew. Then the Executors tab: GC under 10 %
> of task time, tasks spread evenly over executors, no failed tasks or dead executors. Locality on
> scans is mostly NODE_LOCAL, with some RACK_LOCAL when executors don't sit on the replica hosts.
> Last, the SQL tab ties it to the code: which join strategies Spark picked, whether AQE coalesced the
> shuffle reads, and the row counts through each operator, so I can see where data grows or shrinks.
> Once I know these numbers for a job, an incident is just "which of them moved".
