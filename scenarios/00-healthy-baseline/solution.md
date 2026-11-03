# Solution: the healthy baseline of `clickstream-daily-enrichment`

Nothing is wrong with this run. It is the reference you compare against when the
job pages you. Every number below is what the History Server shows, and
`tests/test_scenario00_hs.py` checks each one against its REST API
(`uv run scripts/evidence.py 00-healthy-baseline` prints them for a running server).

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | Jobs tab, header | Total Uptime | `6.8 min` |
| E2 | Jobs tab / Stages tab | Jobs, completed and skipped stages | `8 jobs, 8 completed + 4 skipped stages` |
| E3 | Stages tab | Tasks over all completed stages | `15,243` |
| E4 | Executors tab | Executors | `40 x 4 cores on 40 hosts` |
| E5 | Stages tab, stage 2 (events scan) | Tasks, duration | `5,200 tasks, 3.7 min` |
| E6 | Stage 2, Summary Metrics, Duration | Median / max | `6.8 s / 9.8 s (1.44x)` |
| E7 | Stage 2 header | Input, shuffle write | `502.4 GiB in, 283.7 GiB shuffle write` |
| E8 | Stage 2 header, Locality Level Summary | Tasks per locality level | `NODE_LOCAL 3,684, RACK_LOCAL 1,516` |
| E9 | Stages tab, stage 5 (join + write) | Shuffle read, output | `289.0 GiB read, 309.7 GiB written by 2,400 tasks` |
| E10 | Stage 5, Summary Metrics, Duration | Median / max | `4.2 s / 5.3 s (1.26x)` |
| E11 | Stages tab, stage 8 (daily_funnel final) | Tasks reading 2,400 shuffle partitions | `167 tasks` |
| E12 | Executors tab, Task Time (GC Time) | GC share per executor, min – max | `1.8 % – 2.0 %` |
| E13 | Stage pages | Spill (memory + disk), all stages | `0 B` |
| E14 | Executors tab, Complete Tasks | Tasks per executor, min – max | `364 – 404` |
| E15 | Stage 9 (customer activity scan), Summary Metrics | Median / max | `2.8 s / 6.2 s (2.21x)` |

## Click path and what each number means

**Jobs tab.** The app ran **6.8 min** (E1) with **8 jobs** for 3 writes (E2).
Spark 3.5 runs queries with AQE, so each shuffle is materialized by its own
job, and the broadcast of the page catalog is a job of its own (stage 1,
`$anonfun$withThreadLocalCaptured$1 at FutureTask.java:264`: broadcasts run on
a helper thread). The last job of each query lists the finished map stages
again under new ids and skips them: the **4 skipped stages** (E2) are those,
greyed out in each final job's DAG. Not lost work, not failures.

**Executors.** 40 executors x 4 cores (E4), 16 GiB heap each
(`spark.executor.memory`, Environment tab), static allocation. YARN placed one
container per NodeManager, so they sit on 40 of the cluster's 120 hosts. All
registered within ~2.5 s, and the first job started once the driver had them.

**Stage 2, the events scan (the biggest stage).** 5,200 tasks, 3.7 min (E5).
Spark cut 4,300 Parquet files at `spark.sql.files.maxPartitionBytes = 256m` and
packed the small tails together, so most tasks read one 256 MiB split. Task
times are tight: median 6.8 s, max 9.8 s, **1.44x** (E6). That's the shape of
a healthy stage: max within ~1.5–2x the median, no long tail. The stage reads
502.4 GiB of selected columns and shuffles 283.7 GiB to the join (E7).
**Locality:** 3,684 tasks NODE_LOCAL and 1,516 RACK_LOCAL (E8). Each HDFS
block has 3 replicas, but our executors run on only 40 of 120 nodes, so for
some splits no replica host has an executor. After `spark.locality.wait`
(3 s) the scheduler runs those on another host in the replica's rack:
RACK_LOCAL reads cross a switch and those tasks take a little longer. That's
normal. A sudden jump in ANY would not be. A few dozen tasks read 0 records:
their split held no Parquet row-group midpoint, so the row groups were read by
the neighbouring split. Also normal.

**Stage 5, join + write.** Reads both shuffles (289.0 GiB, E9) into 2,400
tasks (`spark.sql.shuffle.partitions = 2400`), sorts both sides, sort-merge
joins on `customer_id`, and writes 309.7 GiB of Parquet. Median 4.2 s, max
5.3 s (**1.26x**, E10): partition sizes are even because `customer_id` has
~200 M distinct values and no hot key. The Locality column says PROCESS_LOCAL
for every task. A reduce task has no preferred host when its input is spread
over thousands of map outputs, and Spark reports such tasks as PROCESS_LOCAL.
Nothing is cached.

**What AQE changed.** The funnel aggregation shuffles only 3.4 GiB into 2,400
partitions. AQE coalesced them into **167 tasks** (E11): the final stage's DAG
starts with an `AQEShuffleRead` box. The join stage has no `AQEShuffleRead`
because its ~120 MiB partitions were already above the coalescing target.

**JVM and memory.** GC is **1.8–2.0 %** of task time on every executor (E12),
far below the 10 % at which the Executors tab turns the cell red. **No spill**
anywhere (E13): the largest per-task sort buffers (~440 MiB peak execution memory in the join) fit in
the ~2.4 GiB of execution memory each task can use. Work is evenly spread:
**364–404 tasks** per executor (E14).

**The 2x tasks in the aggregation scans** (E15: 2.8 s / 6.2 s, **2.21x**).
Sort stage 9's tasks by Duration: the slowest twenty read ~53 MiB while the
median task reads ~28 MiB. Spark packed **two** smaller `events_enriched` files
into one split (Next Fit Decreasing up to 256 MiB), so those tasks have twice
the data. Time per MB is the same as everywhere else. That's input-size
variance, **not skew**. Skew looks different: one or a few tasks with
far more *shuffle read* (records), on a reduce stage, at many times the median.

## The baseline to remember

| | Healthy value |
|---|---|
| Runtime | ~7 min, 8 jobs (AQE: one job per shuffle) |
| Biggest stage | events scan, ~5k tasks, ~4 min, max/median ≈ 1.4 |
| Join + write | 2,400 tasks, max/median ≈ 1.3, no spill |
| Locality | ~70 % NODE_LOCAL, rest RACK_LOCAL on scans; PROCESS_LOCAL on reduce stages |
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
> Once I know these numbers for a job, an incident is just "which of them moved".
