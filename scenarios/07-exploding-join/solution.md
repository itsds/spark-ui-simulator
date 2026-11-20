# Solution: the customer table now has one row per version, and the join meets all of them

**Root cause.** On 2026-11-06 the CRM data team turned `crm.db/customers_snapshot` into an **SCD type 2 history**
(CRM-2231, for the churn model). It used to hold the current state, one row per customer. Now it holds one row
per *version* of a customer since 2025-07-01, backfilled from the CRM's audit log, plus three new columns
`valid_from`, `valid_to` and `is_current`. The path and every old column stayed, so nothing failed. The
enrichment selects five old columns, so even its plan is the same. But the table's **grain** changed, and the
job joins events to customers on `customer_id` as if that key were unique:

```python
.join(customers.where(~F.col("is_deleted"))
               .select("customer_id", "segment", "loyalty_tier", "home_country"), "customer_id")
```

An inner join outputs, for each event, one row per customer row with the same `customer_id`. The table now has
2.6 rows per customer on average, and **6.2 for the customers in the day's events**: the customers who browse
every day are the ones whose segment (recomputed monthly) and loyalty tier (quarterly) change most often. So the
sort-merge join turned 4.1 billion events into **25.1 billion rows**, 6.1 per event. Everything after the join
grew with it. The write's sort spilled 1.6 TiB to disk, `events_enriched` got an 809 GiB partition instead of
131 GiB, the two daily aggregates read six times the rows, and they count every event about six times. The
job is 2.7x slower, and the data is wrong.

Every number below is what the History Server shows for the two runs, and `tests/test_scenario07_hs.py`
checks each one against its REST API (and an executor's container log through its link).
`uv run scripts/evidence.py 07-exploding-join` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.6 min → 15 min` |
| E2 | Jobs tab, both runs | The join + write job (stages 3–5) and the events scan's job (stage 2) | `job 3: 44 s → 8.8 min; job 2: 3.8 min → 3.7 min` |
| E3 | Stages tab, stage 2, Input | The day's events, last good → incident | `519.3 GiB / 4,440,527,620 records → 516.7 GiB / 4,418,563,591 records` |
| E4 | Stages tab, stage 5, Shuffle Read and Output | Rows into the join stage → rows it writes | `last good 4,342,113,776 read → 4,067,745,339 written (131.0 GiB); incident 4,666,767,032 read → 25,095,283,432 written (809.0 GiB)` |
| E5 | Stage 5, Summary Metrics, median, last good; incident | Duration, shuffle read records → output records per task | `3 s, 1,809,379 → 1,695,088 records; 34 s, 1,944,363 → 10,457,932 records` |
| E6 | SQL tab, query 0, `SortMergeJoin` and the two `Exchange` nodes under it | Rows in (records read of each side) → number of output rows | `events 4,109,264,122 + customers 557,502,910 in → 25,095,283,432 out (last good: 4,129,690,696 + 212,423,080 → 4,067,745,339)` |
| E7 | SQL tab, query 0, the customers `Scan parquet` | files read / size of files read / output rows, last good → incident | `520 files / 58.6 GiB / 218,992,871 rows → 1,395 files / 158.7 GiB / 574,745,263 rows` |
| E8 | SQL tab, query 0, Details: the customers scan's block, both runs | What it reads | `crm.db/customers_snapshot, ReadSchema struct<customer_id:bigint,home_country:string,segment:string,loyalty_tier:string,is_deleted:boolean> in both runs` |
| E9 | SQL tab, query 2 (`customer_daily_activity`): Scan rows → final `HashAggregate` rows, last good; incident | Events in → customers out | `4,067,745,339 → 60,957,603; 25,095,283,432 → 60,962,343` |
| E10 | SQL tab, query 1 (`daily_funnel`): the same | Events in → funnel cells out | `4,067,745,339 → 57,594; 25,095,283,432 → 57,594` |
| E11 | Stages tab, stage 5: Spill columns, Summary Metrics max | Spill and peak execution memory, last good → incident | `no spill, max peak execution memory 501.3 MiB → 3.7 TiB (memory) / 1591.4 GiB (disk), max peak execution memory 2.4 GiB` |
| E12 | SQL tab, query 0, incident | `spill size` of the `Sort` above the join (the write's sort) and of the `SortMergeJoin` | `Sort above the join 3.7 TiB (median 1608.9 MiB per task); SortMergeJoin 0.0 B` |
| E13 | Environment tab, Spark Properties, last good vs incident | What differs | `same submit options` |
| E14 | Stages tab, stage 5: Output size / Output records | Bytes per written row, last good → incident | `34.6 B → 34.6 B per written row` |
| E15 | Executor 1's `stderr` (Executors tab, Logs) | The write sorter's spill lines | `executor 1: 60 "spilling sort data of … to disk" lines, 1536.0–1664.0 MiB each` |

## Click path and what each number means

**Application list and Jobs.** 5.6 min on Thursday, **15 min** on Friday (E1). The events scan's job didn't
change (job 2, 3.8 → 3.7 min). The job that grew is **job 3**, the sort-merge join and the write of
`events_enriched`: 44 s → **8.8 min** (E2). The customers scan (job 0) and the two daily aggregates' scans
(jobs 4 and 6) grew too, by 10 s to a little over a minute each.

**Is it traffic?** No. Stage 2 read the same day's worth of events as on Thursday: 516.7 GiB and 4.42 billion
records against 519.3 GiB and 4.44 billion (E3). Black Friday early access didn't add events to the input. Whatever
multiplied the rows happened after the scan.

**Stage 5: rows in, rows out.** Stage 5 reads the two sides of the join from the shuffle and writes the result.
On Thursday it read 4.34 billion records (events and customers together) and wrote 4.07 billion, a few % fewer:
events of unknown customers drop out of an inner join. On Friday it read 4.67 billion and **wrote 25.1 billion**
(E4): 809 GiB, the HDFS team's partition. The median task shows the same thing (E5): it reads 1.9 million
records and writes **10.5 million**, and it takes 34 s instead of 3 s. A stage that writes more rows than it
reads either explodes an array or joins something that has several rows per key. This job has no `explode`.

**SQL tab: follow the row counts.** In query 0 (`events_enriched dt=2026-11-06: …`), read the graph bottom up.
The events scan (4.42 billion rows), the bot filter and the page catalog's broadcast hash join (4.11 billion each)
look as they did on Thursday. The `SortMergeJoin` receives **4,109,264,122 events and 557,502,910 customer rows**
and outputs **25,095,283,432 rows** (E6). On Thursday it received 4.13 billion events and 212 million customers and
output 4.07 billion: at most one row per event. An inner equi-join outputs one row per *pair* of rows with the same
key, so 6.1 output rows per event mean the average event now finds about 6 customer rows with its `customer_id`.

**The customer side.** The customers `Scan parquet` read **1,395 files, 158.7 GiB and 574.7 million rows**,
against 520 files, 58.6 GiB and 219 million rows on Thursday (E7). The company didn't gain 356 million customers
overnight. The CRM team's "the table got a lot bigger" is this: 2.6 rows per customer. The query's **Details**
show the same plan in both runs (E8). The customers scan reads the same `Location` and the same five columns
(`ReadSchema`), and the plan text differs only in expression ids. The new columns aren't read, so nothing in the
plan hints at them. The plan can't show a table's grain, only the row counts can.

**The extra rows are copies.** `customer_daily_activity` (query 2) groups the enriched events by customer. Its scan
read 25.1 billion rows instead of 4.07 billion, but its final aggregation output **60,962,343 customers**, the
same ~61 million as on Thursday (E9). `daily_funnel` (query 1) output 57,594 funnel cells from 6x the rows, as
on Thursday (E10). Six times the rows, the same set of keys: the extra rows are repeats of the same events
(that's BI's 6.2x page views and revenue). Each copy carries a different version's `segment`, `loyalty_tier` and
`home_country`. They are not new data.

**The spill is a consequence.** Stage 5 spilled **3.7 TiB from memory, 1.6 TiB to disk** (E11): the disk alerts.
Its tasks reach the 2.4 GiB of execution memory a task gets on a 16 GiB, 4-core executor, against 501 MiB on
Thursday. In query 0 the spill belongs to the **`Sort` above the join**, the sort `partitionBy("dt")` needs before
the write (E12). It sorts each task's 10.5 million output rows instead of 1.7 million. The `SortMergeJoin`
itself spills nothing. It buffers the customer rows of one key at a time and joins each event with all of them.
A few dozen versions are nothing to hold. The buffer is an on-heap array until
`spark.sql.sortMergeJoinExec.buffer.in.memory.threshold` rows, 2,147,483,632 by default, so in practice it never
spills: on Spark 3.5.9 a key with 200,000 buffered rows still showed `spill size` 0
(`reference/local/exploding_join.txt`). A key with millions of rows on both sides would hurt the heap instead
(GC, `OutOfMemoryError`), not the spill columns. An executor's `stderr` shows the write sorter spilling
1.5–1.6 GiB at a time, 60 times on executor 1 alone (E15).

**Nothing else changed.** The Spark properties are the same in both runs (E13). The Parquet output takes
**34.6 bytes per row** in both runs (E14), so compression is fine: there are just 6.2x the rows.

## The two runs side by side

| | Last good (dt=2026-11-05) | Incident (dt=2026-11-06) |
|---|---|---|
| `customers_snapshot` | current state, one row per customer | SCD2 history, 2.6 rows per customer (6.2 per event) |
| Customers scan | 520 files / 58.6 GiB / 219 M rows | 1,395 files / 158.7 GiB / 575 M rows |
| Events into the join | 4.13 B | 4.11 B |
| `SortMergeJoin` output | 4.07 B rows (0.98 per event) | 25.1 B rows (6.1 per event) |
| Stage 5 | 3 s median task, no spill, 131.0 GiB written | 34 s median task, 1.6 TiB disk spill, 809.0 GiB written |
| `customer_daily_activity` | 4.07 B rows in → 61.0 M customers | 25.1 B rows in → 61.0 M customers |
| Plan | the same (apart from ids) | the same (apart from ids) |
| App duration | 5.6 min | 15 min |

## Fix

**Join the current version of each customer.** The history has a column for it:

```python
.join(customers.where(F.col("is_current") & ~F.col("is_deleted"))
               .select("customer_id", "segment", "loyalty_tier", "home_country"), "customer_id")
```

The customers scan then reads `is_current` too and pushes it to Parquet: `PushedFilters: [IsNotNull(is_current),
IsNotNull(is_deleted), EqualTo(is_current,true), Not(EqualTo(is_deleted,true)), IsNotNull(customer_id)]` (the plan
real Spark 3.5.9 prints for this query: `reference/local/exploding_join.txt`). Parquet can't skip row groups with
it: current and old versions are mixed in every row group. So the scan still reads the history's 1,395 files: 13 s
instead of 5 s, in a stage that runs next to the 3.7-minute events scan. The filter keeps one row per customer and
the join is N:1 again. In the simulation the incident night then takes **5.5 min** and writes 4.05 billion rows, as
many per event as on Thursday (`tests/test_scenario07.py`).

If analytics wants the segment **as it was when the event happened**, the history makes that possible: join on
`customer_id` *and* `event_ts >= valid_from AND event_ts < valid_to`. That is still one row per event, but with
point-in-time attributes, which changes the meaning of the output. It's a product decision, not the fix for
tonight.

**Repair the data.** Three partitions are wrong, and the dashboards read them at 02:30: `events_enriched`,
`daily_funnel` and `customer_daily_activity` for **dt=2026-11-06**. Re-run the job for that day with the fix
(`partitionOverwriteMode=dynamic` replaces just those partitions), refresh the dashboards, and tell BI which
numbers to discard. The 809 GiB partition frees the space quota when it's overwritten. Check every other job
that reads `customers_snapshot`: each one that joins on `customer_id` without `is_current` has the same bug since
Friday.

**The channel's suggestions.**

* *Black Friday traffic.* The input is the same as on Thursday (E3). The rows appear inside the join (E6).
* *More memory or more shuffle partitions.* They treat the spill, which is a symptom. With
  `spark.sql.shuffle.partitions=9600` each task sorts a quarter of the rows and nothing spills, but the job
  still takes **10 min** and still writes **25.1 billion rows** (verified in the simulation). More memory would
  do the same. Neither makes the output right.
* *Broadcast the customer table, or salt the key.* The customer side is 158.7 GiB of files, far past what a
  broadcast can hold. A broadcast hash join would output the same 25.1 billion rows anyway: the multiplication
  is in the data, not in the join strategy. Salting splits a hot key over several tasks. Here no key is hot,
  every key just has several rows, and salting doesn't remove a single one.
* *Parquet compression.* 34.6 bytes per written row in both runs (E14).
* *`dropDuplicates(["customer_id"])` on the customers.* (Someone will suggest it.) It would bring the row count
  back, at the price of an extra shuffle and aggregation of 575 million rows. But it keeps an *arbitrary*
  version of each customer, possibly a different one each run, with an old segment or tier. Filter on what
  defines the version you want (`is_current`), don't deduplicate blindly.

**Catch it next time.**

* *A contract for the table's grain.* `customers_snapshot`'s primary key (`customer_id`) is part of its interface,
  just like its columns. The CRM team's note checked the schema ("jobs that select their columns explicitly are
  not affected") and missed the key. A uniqueness test on the producer side (one row per `customer_id` where
  `is_current`) would have stopped this before any consumer. A history should arguably have been a new table
  (`customers_history`), with `customers_snapshot` kept as the current view.
* *A cardinality check in the job.* An enrichment of N:1 joins can't output more rows than it reads. Compare the
  written rows with the events read (the write's `number of output rows` and the scan's, from the SQL REST API or
  the listener) and fail the Airflow task before the dashboards read the partition. It would have failed at
  02:25 instead of BI finding it at 08:12.
* *Alert on the job's numbers.* Output rows and bytes per run, and the ratio of a join's output rows to its
  streamed input. A 6x jump is a data problem even when the job "succeeded".

## 60-second interview answer

*"A Spark job's output is suddenly six times bigger and it got slower. Nothing changed in the code. How do you
debug it in the UI?"*

> First I check whether the input grew: the scan stage's Input was the same as the night before, so the rows
> appear later in the job. The stage after the shuffle read 1.9 million records per task and wrote 10.5 million,
> so something inside it multiplies rows. In the SQL tab I follow `number of output rows` up the plan. The
> sort-merge join on `customer_id` received 4.1 billion events and output 25 billion rows, while the night before
> it output one row per event. An inner join outputs one row per matching pair, so the customer side must have
> several rows per key now, and its scan shows 575 million rows instead of 219 million for the same customers.
> The plan itself was identical, which is why nothing looked changed. Upstream had turned the snapshot into an
> SCD2 history. The spill everyone noticed was only the write's sort handling six times the rows, so more memory
> or partitions would just make a wrong result faster. The fix is to filter `is_current` before the join and to
> re-run the corrupted partitions. Then I'd add a uniqueness test on the producer side and a check that the job
> doesn't write more rows than it reads.
