# Solution: a broadcast join that silently became a sort-merge join

**Root cause.** The job joins one day of web events (4.1 billion rows after the bot filter) with the 3 MB page
catalog on `page_id`. The script has no broadcast hint: it relied on Spark choosing a **broadcast hash join** by
itself, because the catalog is far below the default `spark.sql.autoBroadcastJoinThreshold` of 10 MB. On the
afternoon before the incident, the platform team's new Spark client defaults (PLAT-1184) put
**`spark.sql.autoBroadcastJoinThreshold=-1`** into the gateways' `spark-defaults.conf`, which every job submitted
from them inherits. `-1` switches automatic broadcast joins off, both when Spark plans the query and when AQE
re-plans it at runtime. So that night Spark planned the page catalog join as a **sort-merge join**: it shuffled
all 4.1 billion events by `page_id` (244 GiB written) to meet 46 thousand catalog rows, sorted them, joined them,
and shuffled them a second time by `customer_id` for the customer join. Worse, page ids are anything but even.
A few pages (home, search, cart) get a large share of the traffic, so a hash partitioning on `page_id` puts
every event of the most popular page into one reduce task: **153 million rows, 9.1 GiB**, sorted with 2.4 GiB of
execution memory, spilled 22 times and written again. That task ran for 17 minutes, while the median task of
its stage took 4 seconds. Nothing in the data changed. The page skew was always there; a broadcast join never
partitions the events by page, so it never mattered.

Every number below is what the History Server shows for the two runs, and `tests/test_scenario05_hs.py` checks
each one against its REST API (and the straggler's container log through its executor's log link).
`uv run scripts/evidence.py 05-missed-broadcast-join` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.5 min → 22 min` |
| E2 | SQL / DataFrame tab, both runs | Duration of the three queries | `3 queries: 4.7 min, 13 s, 22 s → 3 queries: 22 min, 13 s, 22 s` |
| E3 | Incident, Stages tab, stage 5 | Tasks, duration | `2,372 tasks, 17 min` |
| E4 | Stage 5, Summary Metrics, Duration | Median / 75th percentile / max | `4 s / 5 s / 17 min (REST: 3.8 s / 5.5 s / 994 s, 263x the median)` |
| E5 | Stage 5, Summary Metrics, Shuffle Read Size / Records | Median → max | `67.8 MiB / 1113158 → 9.1 GiB / 153079860` |
| E6 | Stage 5, task list sorted by Duration | The five longest tasks (shuffle read) | `17 min (9.1 GiB), 9.2 min (5 GiB), 7.9 min (3.9 GiB), 5.3 min (2.8 GiB), 4.2 min (2.4 GiB), on 5 different executors` |
| E7 | Stage 5, Spill (Memory) / Spill (Disk) | Total, and how many tasks spilled | `202.3 GiB / 85.0 GiB, from 23 of 2,372 tasks` |
| E8 | SQL tab, query 0, plan graph | The join operators, last good → incident | `BroadcastHashJoin + SortMergeJoin → SortMergeJoin + SortMergeJoin` |
| E9 | Query 0, `Scan parquet` of `web.db/page_catalog` | Files, size, rows, last good → incident | `1 file, 3.0 MiB, 46,261 rows → 1 file, 3.0 MiB, 46,261 rows` |
| E10 | Query 0, the page catalog's way into the join | Last good → incident | `BroadcastExchange: 4.4 MiB, 46,261 rows → Exchange: 2.1 MiB in 2,400 partitions` |
| E11 | Stages tab, Shuffle Write of the stages that write the events | Last good → incident | `stage 2 297.7 GiB → stage 0 243.9 GiB + stage 5 295.5 GiB` |
| E12 | Incident, query 0, the `Exchange` on the events side of the page catalog join | Records, data size, shuffle bytes written | `4,101,057,389 records, data size 580.5 GiB, shuffle bytes written 243.9 GiB` |
| E13 | Environment tab, Spark Properties, both runs | What differs | `spark.driver.maxResultSize: not set → 512m; spark.sql.autoBroadcastJoinThreshold: not set → -1` |
| E14 | Stages tab, the customer join + write (stage 5 last good, stage 10 incident) | Tasks, duration | `2,400 tasks, 44 s → 2,400 tasks, 43 s` |
| E15 | Query 0, `Scan parquet` of the events, number of output rows | Last good → incident | `4,446,998,338 → 4,409,739,121 rows (-0.8 %)` |
| E16 | Executors tab | GC share of task time, failed tasks, lost executors | `GC 1.8 % – 2.2 %, 0 failed tasks, 0 executors lost` |
| E17 | `stderr` of the executor that ran the longest task (Executors tab, Logs) | Its spill lines | `TID 6774 (Thread 60): 11 x UnsafeExternalSorter (2.2 GiB), then 11 x ShuffleExternalSorter (2.3 GiB)` |
| E18 | Incident, query 0, the page catalog's `SortMergeJoin` | Rows in from the events side, rows out | `4,101,057,389 events in → 4,101,057,389 rows out` |
| E19 | Stage 5, task list (launch time + duration) | When the stage could have finished | `2,371 tasks done 9.8 min after the first launch, then 7.4 min for the last one` |

## Click path and what each number means

**Application list and SQL tab.** 5.5 min the night before, **22 min** last night (E1). The SQL tab narrows it at
once: the two aggregation queries took exactly as long as the night before, 13 s and 22 s. All of the extra
time is in **query 0**, `events_enriched`, which went from 4.7 to 22 min (E2). Its job description still says
"page catalog (broadcast)": that is what its author expected Spark to do.

**Stages: there is a stage the last good run doesn't have.** The last good run's query 0 had four jobs: the
customer snapshot scan (stage 0), a one-task job named `$anonfun$withThreadLocalCaptured$1 at
FutureTask.java`, which is how a **broadcast** shows up in the Jobs and Stages tabs (stage 1), the events scan
(stage 2) and the customer join + write (stage 5). Last night's query 0 has five jobs, no broadcast job, and a
new stage in the middle: **stage 5, 2,372 tasks, 17 min** (E3). The customer join + write after it is unchanged:
2,400 tasks in 43 s, the same as the night before (E14).

**Stage 5 looks like skew.** Median task 4 s, 75th percentile 5 s, max **17 min**, 263x the median (E4), and this
time the longest task is the one with the most input: the median task reads 67.8 MiB and 1.1 million records,
the largest **9.1 GiB and 153 million records** (E5). It isn't one hot key like a sentinel value: the five
longest tasks read 9.1, 5, 3.9, 2.8 and 2.4 GiB and ran 17 down to 4.2 minutes, on five different executors
(E6). That is a long tail of heavy partitions, the signature of a popularity distribution (a Zipf law), not of a
bad host or a single broken value. 23 tasks spilled, 202.3 GiB in memory and 85.0 GiB on disk (E7). The stage had
finished all but one of its 2,372 tasks 9.8 min after it started, then waited **7.4 min** for the last one (E19).
In the straggler's `stderr` you can watch it: its thread spills the join's sort 11 times at 2.2 GiB each, and then
the shuffle writer spills 11 times more, because every one of its 153 million joined rows has to be written to
the next shuffle as well (E17). GC is 1.8–2.2 % of task time everywhere, nothing failed, no executor was lost
(E16).

**What stage 5 does.** Its DAG reads two shuffles. In the SQL tab, query 0's plan graph shows which join runs
there: the stage ids in the metrics of the `Sort` and `SortMergeJoin` nodes say stage 5, and the join's keys
(the **Details** text) are `page_id`. One input of that join is an `Exchange` holding **4,101,057,389 events,
580.5 GiB** of rows (243.9 GiB compressed) (E12). The other one holds **the whole page catalog: 2.1 MiB** (E10).
A sort-merge join with a 2 MiB side is the tell: it should have been a broadcast.

**Two plans.** Put query 0 of the last good run next to it (E8). The night before, the page catalog went into a
`BroadcastExchange` (4.4 MiB, 46,261 rows: every executor got its own copy) and a **BroadcastHashJoin** probed
it inside the events scan stage, with no shuffle on `page_id` at all. Last night the same join is a
**SortMergeJoin**, and the plan has two `Exchange hashpartitioning(..., 2400)` more: one for the catalog and
one for the events. The events are now written to a shuffle twice: 243.9 GiB by the scan for the page join, and
295.5 GiB again by stage 5 for the customer join. The night before they were written once, 297.7 GiB, by the scan
(E11).

**Did the data change?** No. The events scan read 0.8 % *fewer* rows than the night before (E15). The page catalog
after CMS-418 is the same single file of 3.0 MiB with **46,261 rows**, both nights (E9). The migration happened
on Monday morning, before the last good run, which already read the new export. And the join returns exactly as
many rows as it gets: **4,101,057,389 in, 4,101,057,389 out** (E18). Duplicate page ids in the catalog would
multiply rows (Scenario 07's exploding join); nothing did here.

**Why Spark changed the plan.** Without a hint, Spark's planner (`JoinSelection`) broadcasts a join side whose
estimated size is under `spark.sql.autoBroadcastJoinThreshold`. A Parquet table read by path is estimated from
its file sizes, 3 MB here, so it had always been broadcast under the 10 MB default. The Environment tabs of the
two runs differ in two properties: `spark.driver.maxResultSize` (not set → 512m, harmless for a job that
collects nothing) and **`spark.sql.autoBroadcastJoinThreshold` (not set → -1)** (E13). Neither is in the job's
`spark-submit`: they come from the gateways' `spark-defaults.conf`, which PLAT-1184 changed at 14:00 the
afternoon before ("no action needed for most jobs"). `-1` disables size-based broadcasts entirely. AQE's runtime
threshold, `spark.sql.adaptive.autoBroadcastJoinThreshold`, falls back to the same value when it isn't set, so
AQE couldn't turn the join back into a broadcast even after it saw that the catalog's shuffle held 2.1 MiB.
With broadcasts off and `spark.sql.join.preferSortMergeJoin=true`, an equi-join becomes a sort-merge join.

**Why the skew appears only now.** Page ids in clickstream data have always followed a popularity law: the
heaviest partition holds 3.7 % of all events (153 M of 4.1 B, E5), most likely the home page. A broadcast hash
join doesn't care: each events scan task probes its own copy of the catalog in whatever order the rows come.
A sort-merge join hash-partitions both sides on the join key, so every row with the same `page_id` goes to the
same reduce task. Joining a fact table to a small dimension through a shuffle on a low-cardinality key is how a
missed broadcast usually shows up in production: as skew. The red herrings point the same way. Skew says
"handle the hot keys", the migration says "the dimension changed". The question that finds the cause is
different: *why is this join shuffling at all?*

## The two runs side by side

| | Last good (dt=2026-09-21) | Incident (dt=2026-09-22) |
|---|---|---|
| App duration | 5.5 min | 22 min |
| Query 0 (`events_enriched`) | 4.7 min, 4 jobs | 22 min, 5 jobs |
| Page catalog join | BroadcastHashJoin (BroadcastExchange 4.4 MiB) | SortMergeJoin on `page_id` (both sides shuffled) |
| Events written to a shuffle | once: 297.7 GiB | twice: 243.9 GiB + 295.5 GiB |
| Longest task of the run | 10 s | 17 min (stage 5, 9.1 GiB of one page range) |
| Spill | none | 202.3 GiB in memory, 85.0 GiB on disk (23 tasks) |
| `spark.sql.autoBroadcastJoinThreshold` | not set (10 MB) | -1 (from the gateways' defaults) |
| Page catalog | 1 file, 3.0 MiB, 46,261 rows | the same |

## Fix

**Tonight.** Give the job its broadcast back on its own command line. A `--conf` passed to `spark-submit`
overrides `spark-defaults.conf`:

```
--conf spark.sql.autoBroadcastJoinThreshold=10m
```

In the simulation the incident run with this setting plans the BroadcastHashJoin again and takes **5.5 min**,
like the last good run.

**Durable.** Make the intent explicit in the code instead of relying on a size estimate and a cluster default:

```python
.join(F.broadcast(pages.select("page_id", "page_type", "funnel_step")), "page_id")
```

A broadcast hint is honoured whatever `autoBroadcastJoinThreshold` says (only Spark's 8 GB hard limit for a
broadcast table applies), so the next change of a default can't break it. The simulated incident run with the
hint and the threshold still at -1 also takes **5.5 min**, and with the old threshold it produces exactly the
plan the night before had (the same event log, byte for byte). This is the line the script has had since
2026-09-24. Hint only what is small by design and will stay small: a dimension that grows into gigabytes
behind a hint becomes a driver OOM (the very incidents PLAT-1184 was meant to prevent).

**AQE skew join would not have been the fix.** `spark.sql.adaptive.skewJoin.enabled=true` (this job switched it
off at its Spark 3.0 migration) would split stage 5's heavy partitions into pieces and join each piece with a
copy of the matching catalog partition. That cures the 17-minute task. The job would still shuffle 4.1
billion rows by `page_id`, sort them and shuffle them again, roughly 244 GiB of shuffle write and a full stage
that a broadcast join doesn't need. It treats the symptom of a plan that shouldn't exist. (This simulator
doesn't model AQE's skew-join split, so there is no simulated run of it.) Salting `page_id` is the same kind of
answer, and more code.

**For the platform.** A cluster-wide `-1` trades rare driver OOMs for silent slowdowns in every job that relied
on automatic broadcasts (two other teams were hit the same night). If misestimated sizes are the worry, there
are narrower tools. Leave the static threshold at its default and fix the estimates (statistics on catalog
tables, `ANALYZE TABLE`, no broadcast of aggregated or filtered results whose size the planner can't know). Or
keep `spark.sql.autoBroadcastJoinThreshold=-1` for planning but set `spark.sql.adaptive.autoBroadcastJoinThreshold=10m`:
AQE then decides on the real, materialized size of the small side. Here it would have converted the join to a
broadcast once the catalog's 2.1 MiB shuffle finished. By then the events were already being shuffled, so it
would have saved the sort, the skewed reduce stage and the second read, but not the first shuffle write. (This
runtime conversion is not simulated here either.) PLAT-1184's broadcast default was reverted on 2026-09-24.

**Catch it before the BI team does.**

* Watch plans, not just durations: a broadcast that turns into a sort-merge join is visible in the SQL tab
  (and in the REST API's `/sql` endpoint) the first night. A daily check that the job's join operators are the
  expected ones costs one API call.
* Treat changes to cluster-wide Spark defaults like code changes of every job on the cluster: announce the
  affected properties, and run the big recurring jobs against the new defaults before the rollout.
* "Nothing changed on our side" includes the defaults you inherit. Diff the Environment tab of a slow run against
  a good one early in the investigation, not last.

## 60-second interview answer

*"A Spark job that joins a big fact table with a small dimension got four times slower overnight. No code change.
How do you debug it?"*

> I compare the slow run with the last good one in the History Server. The SQL tab shows that all of the extra
> time is in one query, and in the Stages tab that query has a new stage. Its max task takes 17 minutes against a
> 4-second median, and the longest tasks read the most shuffle data, on different executors, so it looks like
> data skew. But I want to know why there's a shuffle there at all, so I open the plan. The night before, the
> dimension, a 3 MB page catalog, was joined with a broadcast hash join. Last night it's a sort-merge join on
> `page_id`, with a 2 MB exchange on one side and 580 GB of events on the other. The data didn't change: same
> catalog size and row count, and the join returns exactly as many rows as it gets. So Spark chose a different
> strategy. In the Environment tab `spark.sql.autoBroadcastJoinThreshold` is `-1`: a new cluster default had
> disabled automatic broadcasts. The skew is real, but harmless with a broadcast: page popularity only hurts once
> you hash-partition the events by page. Tonight I'd pass the threshold back on the job's spark-submit. The
> durable fix is an explicit `broadcast()` hint on the small table, plus a check that alerts when a job's
> expected broadcast join turns into a sort-merge join. Turning on AQE skew join would only hide it: the extra
> shuffle of the whole fact table stays.
