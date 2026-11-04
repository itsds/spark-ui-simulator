# Solution: one customer id held 13 % of the events (join-key skew)

**Root cause.** From 2026-11-03 the web events carry `customer_id = -1` on every logged-out event: the
tracking SDK's 5.0 rollout started sending logged-out traffic to `web.db/events` (+15 % events) and
stamps it with the CRM's guest placeholder. That id exists in `customers_snapshot`, so those events
survive the inner join instead of being dropped. A sort-merge join hash-partitions both sides on
`customer_id`, so all ~616 million `-1` events land in one of the 2,400 shuffle partitions
(`pmod(hash(-1L), 2400) = 1993`). One task had to read, sort, join, sort again and write 44.5 GiB of
shuffle data while every other task read ~130 MiB. It spilled 280.7 GiB and ran for 1.5 h, while the
other 2,399 tasks were done in 44 seconds. AQE could have split that partition, but the job's
spark-submit sets `spark.sql.adaptive.skewJoin.enabled=false`. Nothing in the code or the config
changed: the data did. `wn007` is a victim of the spill, not the cause.

Every number below is what the History Server shows for the two runs, and
`tests/test_scenario01_hs.py` checks each one against its REST API (and the container log through the
executor's log link). `uv run scripts/evidence.py 01-join-skew` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.5 min → 1.6 h` |
| E2 | Incident, SQL / DataFrame tab | Queries and their durations | `3 queries: 1.6 h, 15 s, 24 s` |
| E3 | Incident, Stages tab, stage 5 (join + write) | Tasks, duration | `2,400 tasks, 1.5 h` |
| E4 | Stage 5, Summary Metrics, Duration | Median / 75th percentile / max | `3 s / 3 s / 1.5 h (REST: 2.8 s / 2.9 s / 5,324 s, 1,900x the median)` |
| E5 | Stage 5, Summary Metrics, Shuffle Read Size / Records | Median → max | `129.5 MiB / 1806990 → 44.5 GiB / 617724921` |
| E6 | Stage 5, Tasks sorted by Duration | The straggler | `index 1993, TID 8310, executor 40 (wn007.hadoop.prod.internal), PROCESS_LOCAL` |
| E7 | Stage 5, task list / Event Timeline | How the stage ended | `2,399 tasks done 44 s after the first launch, then 1.5 h for the last one` |
| E8 | Stage 5 header | Spill (Memory) / Spill (Disk) | `280.7 GiB / 117.9 GiB, all from one task` |
| E9 | Stage 5, Aggregated Metrics by Executor | Task time of the busiest executors | `executor 40: 1.5 h; next, executor 35: 2.9 min` |
| E10 | SQL query 0, `Sort` under `SortMergeJoin` (events side) | spill size, total (min, med, max) | `123.2 GiB (0.0 B, 0.0 B, 123.2 GiB (stage 5.0: task 8310))` |
| E11 | SQL query 0, events `Exchange` | remote bytes read, total (min, med, max) | `332.6 GiB (119.6 MiB, 123.5 MiB, 43.4 GiB (stage 5.0: task 8310))` |
| E12 | SQL query 0, events `Scan parquet`, last good → incident | number of output rows | `4,426,997,388 → 5,094,208,027 rows (+15.1 %)` |
| E13 | Last good run, stage 5, Summary Metrics | Duration and shuffle read of the same stage | `median 2.8 s, max 3.7 s, max shuffle read 134.2 MiB` |
| E14 | Environment tab, both runs | `spark.sql.adaptive.skewJoin.enabled` | `false` |
| E15 | Incident, Executors tab | GC, failed tasks, lost executors | `GC 1.7 % – 2.1 %, 0 failed tasks, 0 executors lost` |
| E16 | Executor 40 `stderr` (Executors tab → stderr), TID 8310 | `UnsafeExternalSorter … spilling sort data` lines | `120 lines (53 + 67 spills, 2 sorters)` |
| E17 | Stage 5, task list filtered to executor 40 | The same executor's other tasks | `executor 40's other 57 tasks: median 2.8 s (all other executors: 2.8 s)` |

## Click path and what each number means

**Application list → Jobs.** The same job with the same code took **5.5 min** the night before and
**1.6 h** last night (E1). On the incident's Jobs tab, expand the Event Timeline: job 3 (`events_enriched
dt=2026-11-03 …`) is a bar from 02:15 to 03:43, and every other job is a sliver. The SQL tab says the
same: query 0 (the enrichment) took 1.6 h, while the two aggregations that read its output took 15 s
and 24 s (E2), close to their usual 13 s and 21 s.

**Stage 5, the join + write.** 2,400 tasks, 1.5 h (E3). The **Summary Metrics** table is the first thing
to read on a slow stage, and here it says everything: median and 75th percentile 3 s, max **1.5 h**
(E4). In the last good run the same stage had a max of 3.7 s (E13). A stage that is slow everywhere has
its median move; this stage's median didn't move at all. Its max is ~1,900x the median. The same shape
is in **Shuffle Read Size / Records**: 129.5 MiB / 1.8 M records at the median, **44.5 GiB / 618 M
records** at the max (E5), ~340x more input than a normal task. So the problem is one partition's
*input*, not the speed of the machine it ran on.

**The straggler.** Sort the task table by Duration: task **index 1993** (TID 8310) on executor 40, host
`wn007` (E6). The Locality Level is PROCESS_LOCAL, as for every reduce task with no preferred host. It
started at 02:15:08 with the others; **2,399 tasks were finished 44 seconds after the stage started
and the stage then waited 1.5 h for this one** (E7). In the driver log (the driver's `stderr` link on the
Executors tab) this is the classic sight of `Finished task … (2399/2400)` at 02:15:19 followed by
nothing until `(2400/2400)` at 03:43:52. Remember the event timeline above the task table only shows
the current page: sort first, then expand it, and you see one bar going off the right edge.

**Spill.** The stage spilled **280.7 GiB in memory / 117.9 GiB on disk, all of it from that one task**
(E8: the Max column equals the stage total). Spill is decided per task: the partition's rows against the
execution memory one task can get, ~2.4 GiB here (16 GiB heap, `spark.memory.fraction` 0.6, 4 cores).
130 MiB partitions sort in memory; a 44.5 GiB one can't. Aggregated Metrics by Executor shows executor 40
with **1.5 h** of task time against 2.9 min for the next busiest executor (E9).

**SQL tab.** Open query 0. The plan is the usual one: events are shuffled and sorted, customers are
shuffled and sorted, `SortMergeJoin` on `customer_id`, then the `Sort` on `dt` for the partitioned
write. Read the metrics as *total (min, med, max (stageId: taskId))*:

* the `Sort` under the join on the events side reports `spill size 123.2 GiB (0.0 B, 0.0 B, 123.2 GiB
  (stage 5.0: task 8310))` (E10): the total *is* the max, one task; the `Sort` for the write spilled
  another 157.5 GiB in the same task;
* the events `Exchange` reports `remote bytes read 332.6 GiB (119.6 MiB, 123.5 MiB, 43.4 GiB (stage
  5.0: task 8310))` (E11): one reducer fetched 360x the median.

There is no `AQEShuffleRead` above the exchanges and the join is not marked as a skew join: AQE did
nothing to this stage. The **Environment** tab of both runs says why: `spark.sql.adaptive.skewJoin.enabled
false` (E14), set in the job's spark-submit since its Spark 3.0 migration. With Spark's default (`true`),
`OptimizeSkewedJoin` treats a partition as skewed when it is larger than both
`skewedPartitionThresholdInBytes` (256 MiB) and `skewedPartitionFactor` (5) x the median (here 44.5 GiB
against ~650 MiB) and splits it by ranges of map outputs into pieces of about the average non-skewed
partition (~130 MiB here, more than the 64 MiB advisory size), each joined with a copy of the matching
customers partition.

**Ruling out the usual suspects.** GC is 1.7–2.1 % of task time on every executor, no task failed, no
executor was lost (E15). And `wn007` is not a bad node: executor 40 ran 57 other tasks in the same
stage with a median of **2.8 s**, exactly like everybody else (E17), and its scan tasks were normal too.
The ~125 GB its YARN local dirs grew by is the 117.9 GiB of spill files that task wrote under
`/data/*/yarn/nm/usercache/…`, deleted when the task finished at 03:43:52. A slow node makes *every*
task on it slow; a skewed partition makes *one* task slow wherever it lands.

**The executor log.** Executor 40's `stderr` shows `Running task 1993.0 in stage 5.0 (TID 8310)` at
02:15:08, then **120** `UnsafeExternalSorter: Thread 66 spilling sort data of 2.3 GiB to disk (n times so
far)` lines in two sequences (E16): 53 spills from the sort on the join's input while the task read its
shuffle blocks (02:15–02:53), then 67 from the sort for the write (02:55–03:35), then ~8 minutes of
merging the spill files and writing a 19 GiB Parquet file. Two counters because there are two sorters
in the task, one per `Sort` node of the plan.

**What changed, if not the code?** Compare query 0 of both runs. The events scan produced **15.1 % more
rows** (E12): 5.09 billion instead of 4.43 billion. 15 % more data explains 15 % longer scans (stage 2
took 4.2 min instead of 3.7 min, the aggregations a few seconds more), not 17x the runtime. The extra
rows are not spread over the customers: they share one key. To find it you leave the UI and ask the
data:

```python
(spark.read.parquet(".../web.db/events/dt=2026-11-03")
      .groupBy("customer_id").count().orderBy(F.desc("count")).show(3))
# customer_id = -1 comes first with ~616 million rows; the next one is orders of magnitude smaller
```

You can also go from the key to the task: `spark.range(1).select(F.expr("pmod(hash(CAST(-1 AS BIGINT)),
2400)"))` returns **1993**, the straggler's index (with 2,400 reduce tasks and no AQE coalescing, task
index = shuffle partition id). In `customers_snapshot`, `-1` is the "Guest" placeholder row, so the
guest events join successfully (the SortMergeJoin produced 4.67 billion rows instead of 4.06 billion)
instead of falling out of the inner join. The tracking team's release notes for web SDK 5.0 (rolled out
2026-11-03) confirm it: logged-out sessions are now tracked, with `customer_id = -1`.

## The two runs side by side

| | Last good (dt=2026-11-02) | Incident (dt=2026-11-03) |
|---|---|---|
| App duration | 5.5 min | 1.6 h |
| Events read | 4.43 B rows | 5.09 B rows (+15 %) |
| Stage 5 median / max task | 2.8 s / 3.7 s | 2.8 s / 1.5 h |
| Stage 5 max shuffle read | 134.2 MiB | 44.5 GiB (one task) |
| Spill | none | 280.7 GiB memory / 117.9 GiB disk, one task |
| `spark.sql.adaptive.skewJoin.enabled` | false | false |
| Code, cluster, executors | same | same |

## Fix

**Tonight (no code change).** Remove `--conf spark.sql.adaptive.skewJoin.enabled=false` (or set it to
`true`, Spark 3.5's default). AQE then splits partition 1993 by map-output ranges into roughly 350
pieces of ~130 MiB and joins each with a copy of customers' partition 1993 (~90,000 rows, read once
per piece). The stage becomes ~2,750 tasks of a few seconds each, and the job should land near its
usual 6–7 minutes (15 % more data than before). Expect ~350 more output files: each piece writes its
own.

**Durable (the data model).** A sentinel key never belongs in a shuffle join. Split the guests out
and join only the rest:

```python
cust = customers.where(~F.col("is_deleted")).select("customer_id", "segment", "loyalty_tier", "home_country")
guests = events_with_pages.where(F.col("customer_id") == -1)        # ~616 M rows, one key
known = events_with_pages.where(F.col("customer_id") != -1)
guest_row = cust.where(F.col("customer_id") == -1).drop("customer_id")  # 1 row
enriched = (known.join(cust, "customer_id")                           # sort-merge join, evenly spread
                 .unionByName(guests.crossJoin(F.broadcast(guest_row))))  # no shuffle for the guests
```

or agree with the tracking team on `NULL` for logged-out events (and decide explicitly whether
`events_enriched` should contain them). For skew you can't predict, salting spreads a hot key over N
partitions (`concat(key, rand_int(N))` on the big side, the small side exploded N times), but with
AQE's skew join on, Spark 3.x does most of that for you.

**Stop it reaching the dashboards again.**

* A data-quality check before the join: share of the top `customer_id` (and of nulls/sentinels) per day;
  alert when one key holds more than ~1 % of the rows.
* An upstream data contract: SDK changes that alter key semantics are announced to the consumers.
* Run-time alerting on the job, not just its end: a stage whose max task is > 10x its median, or a job
  running 2x its usual duration, pages before the 03:00 refresh does.
* Review inherited `--conf`s. A flag disabled years ago "to work around something" was harmless until
  the data changed.

## 60-second interview answer

*"A nightly job went from 6 minutes to an hour and a half with no code change. How do you debug it?"*

> I compare the slow run with the last good one, starting from the Jobs timeline: one job took almost
> all of the time, so I open its longest stage. The Summary Metrics tell me straight away whether it's
> uniformly slow or skewed: here the median task was 3 seconds and the max 1.5 hours, and the max
> shuffle read was 44 GB against a 130 MB median, so one partition was huge. That one task also spilled
> 280 GB, which explains the disk alert on its node. The node itself was fine: the same executor's
> other tasks took the normal 3 seconds. In the SQL tab I can see it's a sort-merge join on
> customer_id, and the Environment tab shows AQE skew join was turned off in the job's config, so
> Spark didn't split the partition. Since the code didn't change, the data did: the input grew 15 %, and
> a quick group-by on the join key showed customer_id -1, the guest placeholder, with 600 million rows
> after an SDK change. Short term I'd turn AQE skew join back on; long term I'd keep sentinel keys out of
> the shuffle join, by splitting them off and broadcasting their one dimension row, and add a data check
> on top-key share plus an alert on task-duration skew.
