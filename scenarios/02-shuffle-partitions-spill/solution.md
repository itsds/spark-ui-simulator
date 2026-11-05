# Solution: 200 shuffle partitions for 300 GiB of shuffle data (every task spilled)

**Root cause.** The Airflow migration (DATAENG-2207) dropped `--conf spark.sql.shuffle.partitions=2400`
from the job's submission: the PR treated it as legacy tuning, "AQE sizes the partitions on Spark 3". Without
it, every shuffle the planner inserts gets Spark's default of **200** partitions. The data was the same as the
night before: the events scan wrote ~295 GiB of shuffle data for the join, as usual. But instead of 2,400 reduce
tasks reading ~130 MiB each, 200 tasks read **1.5 GiB each**, about 3.5 GiB of rows to sort. One task can
hold at most ~2.35 GiB of execution memory (a 16 GiB heap with 4 cores), so **every** task spilled both of
its sorts: 1.4 TiB in memory, 610 GiB written to the YARN local dirs and read back for merging, on all 40
executor hosts at once. That is the disk alert. AQE could not help: its partition coalescing only *merges*
partitions that are too small and never splits partitions that are too big, and it starts from the same 200.
The job took 9.1 min instead of 5.6 min: 4x the task time for the same work in the join stage, plus a
second, mostly idle wave of 40 tasks on 160 cores.

Every number below is what the History Server shows for the two runs, and
`tests/test_scenario02_hs.py` checks each one against its REST API (and an executor's container log through
its log link). `uv run scripts/evidence.py 02-shuffle-partitions-spill` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.6 min → 9.1 min` |
| E2 | Jobs tab, job 3 (`events_enriched …`, the join + write) | Duration, last good → incident | `44 s → 4.6 min` |
| E3 | Stages tab, stage 5 (join + write) | Tasks and duration, last good → incident | `2,400 tasks, 44 s → 200 tasks, 4.6 min` |
| E4 | Incident, stage 5, Summary Metrics, Duration | Min / median / max | `2.0 min / 2.3 min / 2.6 min (REST: 120.4 s / 137.2 s / 154.2 s, max 1.12x the median)` |
| E5 | Stage 5, Summary Metrics, Shuffle Read Size / Records | Median, last good → incident | `129.5 MiB / 1805725 → 1.5 GiB / 21564064` |
| E6 | Incident, stage 5 header + Summary Metrics | Spill (Memory) / Spill (Disk), per task | `1451.9 GiB / 609.8 GiB; every task spilled (7.1 GiB to 7.3 GiB in memory per task)` |
| E7 | Stage 5, Summary Metrics (Show Additional Metrics), Peak Execution Memory | Incident vs last good | `median 2.4 GiB, max 2.4 GiB (last good: max 501.1 MiB)` |
| E8 | Incident, stage 5, task list / Event Timeline | How 200 tasks used 160 cores | `160 tasks at once, the other 40 from 2.0 min later; at most 40 of 160 cores busy for the last 2.0 min` |
| E9 | Stage 2 (events scan), last good → incident | Input and shuffle write | `518.2 GiB in, 296.8 GiB shuffle write → 515.4 GiB in, 295.0 GiB shuffle write` |
| E10 | SQL query 0, both `Exchange` nodes, last good → incident | number of partitions | `2,400 → 200` |
| E11 | Incident, SQL query 0, `Sort` under `SortMergeJoin` (events side) | spill size, total (min, med, max) | `629.4 GiB (3.1 GiB, 3.2 GiB, 3.2 GiB (stage 5.0: task 5577))` |
| E12 | Environment tab, Spark Properties, last good → incident | Every property that differs (without per-run values) | `spark.sql.shuffle.partitions: 2400 → not set; spark.yarn.maxAppAttempts: not set → 1; spark.yarn.tags: not set → airflow,dag_id=clickstream_daily,task_id=enrich_events` |
| E13 | Executors tab, total Task Time | Last good → incident | `12.6 h → 18.3 h` |
| E14 | `stderr` of the executor that ran stage 5's task 0 (Executors tab) | `UnsafeExternalSorter … spilling sort data` lines | `executor 22: 6 tasks of stage 5, 24 lines (12 x 1600.0 MiB, 12 x 2.0 GiB)` |
| E15 | SQL query 0, `Execute InsertIntoHadoopFsRelationCommand` | number of written files, written output | `2,400 files, 130.8 GiB → 200 files, 130.1 GiB` |

## Click path and what each number means

**Application list → Jobs.** The incident run took **9.1 min**, the night before **5.6 min** (E1). On the
Jobs tab, expand the Event Timeline: the alert window 02:14–02:19 is exactly job 3, the result stage of the
enrichment (`events_enriched dt=2026-10-14 …`), from 02:14:01 to 02:18:37. The same job took **44 s** the night
before and **4.6 min** now (E2). Everything else is unchanged: the events scan (job 2) took 3.7 min in both
runs, and the two aggregations on the SQL tab (queries 1 and 2) took 13 s and 22 s in both.

**Stage 5, the join + write.** The header already tells the story when you put the two runs side by side
(E3): **200 tasks instead of 2,400**, and **Total Time Across All Tasks 7.6 h instead of 1.9 h** for the same
rows. The incident's header also shows **Spill (Memory) 1451.9 GiB / Spill (Disk) 609.8 GiB** (E6); the last
good run's stage 5 has no spill at all.

**Summary Metrics: everything, not one task.** Min, median and max duration are **2.0 / 2.3 / 2.6 min**, the
max only 1.12x the median (E4). Compare Scenario 01: there the median stayed at 3 s and one task ran 1.5 h. Here
the whole distribution moved, by ~50x, and it is tight. That is the signature of tasks that are all too big,
not of skew. The input says the same: the median task read **1.5 GiB / 21.6 M records** of shuffle data,
against 129.5 MiB / 1.8 M the night before (E5), 12x more per task, the ratio of 2,400 to 200. **Every task
spilled**, 7.1 to 7.3 GiB each (E6).

**Why every task spilled.** Tick *Peak Execution Memory* under *Show Additional Metrics*: median and max are
both **2.4 GiB**, against at most 501.1 MiB the night before (E7). That is a ceiling, not a coincidence. An
executor's unified memory is (16 GiB heap − 300 MiB reserved) x `spark.memory.fraction` 0.6 ≈ **9.4 GiB** (the
executor logs print `free 9.4 GiB` when they store a broadcast), shared by its 4 running tasks: each task can
get at most 1/4 of it, **~2.35 GiB**. A reduce task now had to sort ~3.5 GiB of rows for the join (the events
`Exchange` reports a *data size* of 702.4 GiB of rows for 200 partitions), and then sort the joined rows again
by `dt` for the partitioned write. Neither fits in 2.35 GiB, so both sorters spill until the input is consumed,
then merge their spill files. With 2,400 partitions a task sorted ~300 MiB and nothing spilled.

**The disks.** Spill (Disk), **609.8 GiB**, is what those tasks wrote (LZ4-compressed) into the executors' YARN
local dirs, `/data/*/yarn/nm/usercache/svc_analytics/appcache/<app>/blockmgr-*`, and read back during the merge.
All 160 first-wave tasks did it at the same time: ~15 GiB written and read again per node in under five minutes, on
top of fetching 1.5 GiB of shuffle blocks per task, on the same disks the co-located HBase region servers use.
The executors map one-to-one onto the 40 nodes that alerted (Executors tab: 40 executors on 40 hosts).

**The waves.** 40 executors x 4 cores = 160 task slots for 200 tasks: **160 tasks started at once and the other
40 two minutes later, when the first ones finished; for the last 2.0 min at most 40 of the 160 cores were
busy** (E8). Sort the task list by Launch Time, or look at page 2 of the stage's Event Timeline. A partition count
that is not a multiple of the slots leaves a partial last wave; 200 is the worst kind: 1.25 waves of 2-minute
tasks. 2,400 is 15 full waves of 3-second tasks.

**Same data?** Yes. The events scan (stage 2) read **518.2 GiB / 515.4 GiB** and wrote **296.8 GiB / 295.0 GiB** of
shuffle data in the two runs (E9). The map side did the same work; only the number of reduce partitions changed.

**SQL tab.** Open query 0 in both runs. The plan is the same (events and customers shuffled and sorted on
`customer_id`, `SortMergeJoin`, a `Sort` on `dt` before the write) except for one number: both `Exchange` nodes
report **number of partitions 2,400 → 200** (E10), and the Details text says `hashpartitioning(customer_id#…, 200)`.
The `Sort` under the join reports **spill size 629.4 GiB (3.1 GiB, 3.2 GiB, 3.2 GiB (stage 5.0: task 5577))**
(E11): total, min, median and max all large, unlike the one-task spill of Scenario 01. The `Sort` before the
write spilled another 822.5 GiB (4.0 / 4.1 / 4.2 GiB). There is no `AQEShuffleRead` above the exchanges in
either run: AQE didn't change these partitions at all.

**Why AQE didn't save it.** Spark 3.5's adaptive execution re-plans after each map stage, but:

* `CoalesceShufflePartitions` only **merges** adjacent small partitions up to the advisory size
  (`spark.sql.adaptive.advisoryPartitionSizeInBytes`, 64 MiB); it never splits a partition. 200 partitions of
  1.5 GiB are all far above the target, so there was nothing to merge.
* It starts from `spark.sql.adaptive.coalescePartitions.initialPartitionNum`, which defaults to
  `spark.sql.shuffle.partitions`: 200 here.
* `OptimizeSkewedJoin` is the only rule that splits partitions, and only partitions that are larger than 5x the
  *median* (and 256 MiB). When every partition is too big, none is skewed (and this job turns skew join off).

So "AQE handles partition sizing" is half true: AQE can bring a too-high partition count down, never a too-low
one up.

**What changed: the Environment tab.** Diff the Spark Properties of the two runs, leaving out the values every
run has its own of (app id, start and submit time, driver host and port, log dir, proxy URI). Three properties
differ (E12): `spark.yarn.maxAppAttempts=1` and `spark.yarn.tags=airflow,…` are new (Airflow retries the task
itself, and tags the YARN application; neither affects execution), and **`spark.sql.shuffle.partitions` is
gone**. The Environment tab only lists properties that were set; a missing one means Spark's default, which for
`spark.sql.shuffle.partitions` is **200**. That matches the SQL tab and the task count.

**Ruling out the rest.** No task failed, no executor was lost, GC stayed at 1.5–2.3 % of task time on every
executor. The cluster did the same work slower: total task time on the Executors tab went from **12.6 h to
18.3 h** (E13), all of the difference in stage 5. The HDFS balancer had finished at 17:40 the day before and
touched only rack `/r03`; the alerts were on every node with an executor, and only during stage 5.

**The executor log.** Open any executor's `stderr`, e.g. executor 22 (it ran stage 5's task 0): it ran **6 tasks
of stage 5** and logged **24 `UnsafeExternalSorter` lines, 12 x `1600.0 MiB` and 12 x `2.0 GiB`** (E14). Each
task's thread logs `spilling sort data of 1600.0 MiB to disk (0  time so far)`, then `(1  time so far)`: the
sort on `customer_id` below the join, filling while the task fetches its shuffle blocks. Then a second sorter,
the `Sort` on `dt` for the write, starts its own count with `2.0 GiB … (0  time so far)`. Only after that the
task opens its Parquet file (`ParquetRecordWriter`) and writes. Four concurrent tasks, four thread ids. (The
count is the number of spill files *before* this one, which is why it starts at 0.)

**A side effect worth knowing.** The write after the shuffle produced **200 files instead of 2,400** for the same
130 GiB (E15): ~666 MiB per file instead of ~56 MiB. Bigger files are not a problem for the readers here, but it
shows that `spark.sql.shuffle.partitions` also decides the output file count of a write that follows a shuffle.

## The two runs side by side

| | Last good (dt=2026-10-13) | Incident (dt=2026-10-14) |
|---|---|---|
| App duration | 5.6 min | 9.1 min |
| Events scan: input / shuffle write | 518.2 GiB / 296.8 GiB | 515.4 GiB / 295.0 GiB |
| Stage 5 tasks, duration | 2,400, 44 s | 200, 4.6 min |
| Stage 5 median task, shuffle read | 2.8 s, 129.5 MiB | 2.3 min, 1.5 GiB |
| Stage 5 total task time | 1.9 h | 7.6 h |
| Spill (memory / disk) | none | 1451.9 GiB / 609.8 GiB, every task |
| `spark.sql.shuffle.partitions` | 2400 | not set (default 200) |

## Fix

**Tonight (config only).** Put `spark.sql.shuffle.partitions=2400` back into the job's submission (in this
story the platform rolled DATAENG-2207 back on 2026-10-15, so the cron wrapper submits the job again). That is
exactly the last good run's shape: 2,400 tasks of ~130 MiB, no spill, stage 5 in ~45 s.

**Durable (size partitions from the data, let AQE do the rest).** A reduce task should read roughly 100–200
MiB of shuffle data, and its rows must fit in the execution memory one task can get (~2.35 GiB here), for every
sort or aggregation it runs. The join's shuffle is ~295 GiB: 295 GiB / 128 MiB ≈ 2,360, and **2,400 = 15 x 160
slots** fills every wave. (Even 600 partitions would have avoided the spill; 200 is 3x too few for the memory
and 12x too few for the usual task size.) On Spark 3 the robust way to say this is to start high and let AQE
merge down, instead of hard-coding the final number:

```
--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=8000
--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
```

AQE then reads 8,000 map-output partitions of ~38 MiB and merges them greedily into ~2,670 reduce tasks of ~115
MiB (it merges a neighbour only if the sum stays under 128 MiB), and keeps doing the right thing when the data
grows. Smaller shuffles in the same job (the two aggregations) get coalesced to what they need.

**Stop it reaching other teams again.**

* Job-specific tuning lives with the job (in the DAG or a conf file in its repo), and a migration is reviewed
  against the *effective* configuration: diff the Environment tab (or `GET …/environment`) of a run before and
  after, not the PR's intent.
* Alert on the job, not only on the nodes: spill per run (`diskBytesSpilled` per stage from the History Server
  REST API or the metrics sink) and duration against the last good runs. A job that never spills writing 600 GiB
  of spill files should page its owner, not Hadoop ops.
* Platform default for the DAG factory: AQE on with a high `initialPartitionNum` and an advisory size, so a job
  without explicit tuning gets sensible partitions.

## 60-second interview answer

*"Ops says your nightly job hammered the local disks of 40 nodes last night and it ran 60 % longer. The data
didn't change. How do you debug it?"*

> I line up the alert window with the Jobs timeline: it's exactly the join-and-write stage. Against the last
> good run, that stage has 200 tasks instead of 2,400, and every one of them spilled: 1.4 TB in memory, 600 GB to
> disk, which went to the YARN local dirs on every executor's node. That's the disk alert. The summary metrics
> show it's not skew: min, median and max are all around two minutes and tightly packed, and each task read 1.5 GB
> of shuffle data instead of 130 MB, with peak execution memory pinned at 2.4 GB, which is a quarter of the
> executor's unified memory with four cores. The map side wrote the same 295 GB as the night before, so only the
> number of reduce partitions changed. The Environment tab confirms it: `spark.sql.shuffle.partitions` disappeared
> in an Airflow migration, so it fell back to the default 200. AQE doesn't fix that, because coalescing only merges
> small partitions; it never splits big ones. The fix is to put the partition count back, or better, set a high
> `initialPartitionNum` with an advisory size so AQE coalesces down to ~128 MB partitions. And I'd alert on spill
> and runtime per job, so we see it before ops does.
