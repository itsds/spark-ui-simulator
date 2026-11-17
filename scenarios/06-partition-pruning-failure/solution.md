# Solution: the filter says `dt`, but it isn't the partition column any more

**Root cause.** DATA-4398 changed how the script reads the day's events. Before, it read the day's partition
directory (`…/web.db/events/dt=2026-10-19`, with `basePath`). Now it reads the table root and derives the date
from the event's timestamp:

```python
events = (spark.read.parquet(f"{WH}/web.db/events")
          .withColumn("dt", F.to_date("event_ts"))      # the day the event happened
          .where(F.col("dt") == DT))
```

`withColumn("dt", …)` *replaces* the partition column `dt` (the day the event landed) with a new column of the same
name. The `where` that follows compares that new column, which is `cast(event_ts as date)`, not the partition
column. Spark only prunes partitions with predicates on partition columns. Here it has none, so it lists and
reads **every partition of the table**: the 7 days the table keeps, plus the next day's first two hours. That is
8 directories, 31,640 files and 8.6 TiB, where the job used to read one day. The optimizer does push the predicate
down, rewritten as a range on `event_ts` (`event_ts >= 2026-10-21 00:00:00 AND event_ts < 2026-10-22 00:00:00`).
That is a data filter, though: Parquet would need column statistics to use it, and the events files store
timestamps as INT96 (Spark's default when it writes Parquet), which has none. So every row of the 8 partitions
is decoded, and the Filter then throws away 87.5 % of them. The job reads 7x the data and spends 13 minutes
instead of 3.7 on its events scan. Code review missed it because the line still says `F.col("dt") == DT`.

Every number below is what the History Server shows for the two runs, and `tests/test_scenario06_hs.py`
checks each one against its REST API (and the driver's and an executor's container logs through their links).
`uv run scripts/evidence.py 06-partition-pruning-failure` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.6 min → 15 min` |
| E2 | Jobs tab, both runs | The events scan's job, and all the other jobs together | `job 2: 3.7 min → 13 min; the other jobs 1.3 min → 1.4 min` |
| E3 | Stages tab, stage 2 (Tasks, Input) | The events scan stage | `5,211 tasks, 519.4 GiB / 4,441,774,318 records in → 38,285 tasks, 3.7 TiB / 32,571,278,036 records in` |
| E4 | Stage 2, Summary Metrics, median | Per task: input, and shuffle write records | `median input 112 MiB → 111.1 MiB, median shuffle write records 869,644 → 0` |
| E5 | SQL tab, query 0, the events `Scan parquet` node | number of partitions read / files read / size of files read | `1 partition / 4,300 files / 1199.7 GiB → 8 partitions / 31,640 files / 8.6 TiB` |
| E6 | SQL tab, query 0: the events Scan → the `Filter` above it, incident | Rows scanned → rows kept | `32,571,278,036 → 4,079,271,537 rows (12.5% kept)` |
| E7 | The same, last good run | Rows scanned → rows kept | `4,441,774,318 → 4,130,850,110 rows (93.0% kept)` |
| E8 | SQL tab, query 0, Details: `(1) Scan parquet`, incident | What the scan lists and what it pushes | `InMemoryFileIndex [hdfs://nn1.hadoop.prod.internal:8020/warehouse/web.db/events]; PartitionFilters: none; PushedFilters include GreaterThanOrEqual(event_ts,2026-10-21 00:00:00.0), LessThan(event_ts,2026-10-22 00:00:00.0)` |
| E9 | The same block, last good run | What the scan lists | `InMemoryFileIndex [hdfs://nn1.hadoop.prod.internal:8020/warehouse/web.db/events/dt=2026-10-19]; PartitionFilters: none` |
| E10 | SQL tab, query 0, Details: `(4) Project` above the events Filter, incident | The `dt` the job filters on and writes | `cast(event_ts#1 as date) AS dt#50` |
| E11 | Environment tab, Spark Properties, last good vs incident | What differs | `same submit options` |
| E12 | The driver's `stderr` (Executors tab, `driver`, Logs) | Listing and partition pruning | `"It took 1898 ms to list leaf files for 1 paths." (last good: 258 ms); "Pruning directories with:" with nothing after it; no "Selected … partitions" line` |
| E13 | Executor 1's `stderr`, `Reading File path` lines | Which directories one executor read | `executor 1 read files of 8 dt partitions (2026-10-15 … 2026-10-22)` |
| E14 | SQL tab, query 0, `Execute InsertIntoHadoopFsRelationCommand`, both runs | Rows written to `events_enriched` | `4,068,887,351 → 4,018,082,474 rows` |

## Click path and what each number means

**Application list and Jobs.** 5.6 min the night before, **15 min** last night (E1). The whole difference is in one
job: job 2, the map stage of the events scan, went from 3.7 to **13 minutes**. All the other jobs together (the
customers scan, the page catalog broadcast, the join and write, the two daily aggregates) took 1.4 minutes as
before (E2). So it isn't the cluster, the shuffle or the write.

**Stages: seven times the input.** Stage 2 ran **38,285 tasks instead of 5,211** and read **3.7 TiB instead of 519
GiB**: 32.6 billion records instead of 4.4 billion (E3). The HDFS team's "about 4 TB" is this Input column (the
bytes of the column chunks the scan selects; the files are bigger). Per task nothing changed: the median task
reads 111 MiB, as before. But the **median task writes nothing to the shuffle** (E4): most of the 38,285 tasks
read 111 MiB of events, kept none of them, and passed nothing to the join. So the stage reads data the job
doesn't need. The question is what data.

**SQL tab: the scan reads the whole table.** In the SQL / DataFrame tab, query 0 (`events_enriched dt=2026-10-21:
…`) has three `Scan parquet` nodes. The events scan is the one with a "number of partitions read" metric (the
other two tables aren't partitioned). It read **8 partitions, 31,640 files, 8.6 TiB** of files, against 1
partition, 4,300 files and 1199.7 GiB the night before (E5). The `Filter` right above it kept **12.5 %** of the
scanned rows (E6), where the night before it kept 93 % (only the bot traffic was dropped, E7). Spark read seven
days of events and threw six of them away.

**Details: no partition filter.** Open **Details** under the plan graph and read the events scan's block (E8):
`Location: InMemoryFileIndex [hdfs://…/warehouse/web.db/events]`, the **table root**. There is **no
`PartitionFilters` line**, so Spark has no predicate to prune directories with, and lists every `dt=…`
directory under the root. `PushedFilters` does contain a date range,
`GreaterThanOrEqual(event_ts,2026-10-21 00:00:00.0), LessThan(event_ts,2026-10-22 00:00:00.0)`. That is the
channel's "the filter is pushed down", and it's true, but it's on `event_ts`, a data column. A pushed filter goes
to the Parquet reader, which can only use it to skip row groups inside files it has already opened, and here not
even that (see below). The last good run had no `PartitionFilters` either (E9): it didn't need one, because its
`Location` was the partition directory itself (`…/events/dt=2026-10-19`), so it could only read that day.

**Which `dt`?** In the same Details, the `Project` above the events Filter (block 4) outputs
**`cast(event_ts#1 as date) AS dt#50`** (E10). That is DATA-4398's `withColumn("dt", F.to_date("event_ts"))`: a
new column, with a new id (`dt#50`), that shadows the partition column (`dt#24`, still listed in the scan's
output). The `where(F.col("dt") == DT)` after it resolves `dt` to the new column. The optimizer pushes the
predicate below the Project by substituting the column's definition, `cast(event_ts as date) = 2026-10-21`. It
then rewrites that comparison of a timestamp cast to a date into the `event_ts` range above
(UnwrapCastInBinaryComparison). At no point does the predicate reference `dt#24`, so `FileSourceStrategy` finds
no partition filter. The submit options are the same in both runs (E11): the code alone did it.

**The driver says so.** The driver's `stderr` (E12) logs `It took 1898 ms to list leaf files for 1 paths.` for the
events read (258 ms the night before: one directory instead of eight), then `FileSourceStrategy`'s planning
lines. **`Pruning directories with:` has nothing after it.** A run that prunes logs its partition predicates
there, and logs `InMemoryFileIndex: Selected N partitions out of M, pruned X% partitions.` when the scan starts.
The incident has no such line. On the executors, every `Reading File path:` line names a file with its
`dt=` directory: executor 1 alone read files of all **8** directories, 2026-10-15 to 2026-10-22 (E13).

**Why Parquet skipped nothing.** Parquet can skip a row group when its min/max statistics rule out a pushed
predicate. The events table is written by the ingestion team's Spark job with Spark's default
`spark.sql.parquet.outputTimestampType=INT96`. INT96 timestamps carry no usable statistics, and Spark builds
no Parquet predicate for them, so every row group of every file was decoded: the scan's output rows equal the
records read (E3, E6). Even with INT64 timestamps (`TIMESTAMP_MICROS`), statistics would only have skipped
row groups of the older days. They could not have skipped the next day's files, where late events of the 21st
are mixed in. And Spark would still have listed, opened and scheduled every file of the table (checked on
Spark 3.5.9: `reference/local/partition_pruning.txt`).

**The output.** `events_enriched` got 4.02 billion rows, against 4.07 billion the night before (E14). That is what
DATA-4398 asked for: the 21st's events *by event time*. They are ~1.3 % fewer than the events that *landed* on a
day, because the late events of the days before now belong to those days. The slowness didn't corrupt anything.
But see "late events" below: the change has a data problem of its own.

## The two runs side by side

| | Last good (dt=2026-10-19) | Incident (dt=2026-10-21) |
|---|---|---|
| Events read | `spark.read.option("basePath", …).parquet(".../events/dt=2026-10-19")` | `spark.read.parquet(".../events").withColumn("dt", to_date(event_ts)).where(dt == …)` |
| Scan `Location` | `…/events/dt=2026-10-19` | `…/events` (the root) |
| `PartitionFilters` | none (not needed) | none (nothing on the partition column) |
| Partitions / files / size read | 1 / 4,300 / 1199.7 GiB | 8 / 31,640 / 8.6 TiB |
| Stage 2 | 5,211 tasks, 519 GiB input, 3.7 min | 38,285 tasks, 3.7 TiB input, 13 min |
| Filter keeps | 93.0 % | 12.5 % |
| App duration | 5.6 min | 15 min |

## Fix

**Filter on the partition column, then on the event date.** Late events land up to a couple of days after they
happen. By 02:10 the table holds the day itself and the next day's first two hours, so those are the only two
partitions that can hold events of the 21st. Say so with a predicate on the partition column, *before* the
`withColumn` replaces it:

```python
NEXT_DT = (date.fromisoformat(DT) + timedelta(days=1)).isoformat()
events = (spark.read.parquet(f"{WH}/web.db/events")
          .where(F.col("dt").between(DT, NEXT_DT))        # prunes: dt is still the partition column here
          .withColumn("dt", F.to_date("event_ts"))        # then the day the event happened
          .where(F.col("dt") == DT))
```

The scan then has `PartitionFilters: [isnotnull(dt#24), (dt#24 >= 2026-10-21), (dt#24 <= 2026-10-22)]`, the
driver logs `Pruning directories with: isnotnull(dt#24),(dt#24 >= 2026-10-21),(dt#24 <= 2026-10-22)` and
`Selected 2 partitions out of 8, pruned 75.0% partitions.`, and stage 2 runs 5,560 tasks. In the simulation the
incident night then takes **5.6 min** and keeps the same events of the day as the slow run
(`tests/test_scenario06.py`). Better
still, don't reuse the partition column's name for something else: call the derived column `event_date` and keep
`dt` meaning one thing. Check any change to a table read with `df.explain()` (look for `PartitionFilters` and the
`Location`) or with the scan's "number of partitions read" in the SQL tab.

**Late events: a design question for DATA-4398.** Attributing events to the day they happened means a day isn't
complete when the job runs. Events of the 21st that land after 02:10 on the 22nd, or on the 23rd, won't be in
`dt=2026-10-21` of `events_enriched`. The next night's run takes only the 22nd's events, so they are lost, about
1.3 % of a day. Before DATA-4398 every event was in exactly one partition, the day it landed. If analytics needs
event time, the job needs a reprocessing window (each night, rewrite the last two or three event days from
`dt BETWEEN day AND day + 3`, pruned the same way; dynamic partition overwrite is already on), or an
`event_date` column next to the landing `dt`, so that the dashboards can group by either.

**The channel's suggestions.**

* *"The filter is pushed down."* It is, to the Parquet reader, as a range on `event_ts`. Pushed filters
  (`PushedFilters`) can only skip row groups inside files the scan has already opened. Only `PartitionFilters`
  decide which directories are listed and read. Here the pushed range skipped nothing (INT96 timestamps).
* *`spark.sql.files.maxPartitionBytes=1g`.* Fewer, bigger tasks: 10,564 instead of 38,285. But the same 8.6 TiB
  is read and decoded, so in the simulation the run still takes **15 min**. The task count is a symptom.
* *Dynamic partition pruning.* It is on by default (`spark.sql.optimizer.dynamicPartitionPruning.enabled`), and it
  prunes a partitioned table at run time from a *join* with a filtered dimension on the partition column. Here
  the filter is a literal date on a data column, and no join key is a partition column. Nothing for it to do.
* *"Never cast a partition column."* A myth, at least for Spark 3.x file tables. Any deterministic predicate
  that references only partition columns is a partition filter, however many functions wrap the column:
  `F.col("dt").cast("string") == DT` gives `PartitionFilters: [isnotnull(dt), (cast(dt as string) =
  2026-10-21)]` and prunes, and so does `F.to_date(F.col("dt").cast("string"), "yyyy-MM-dd") == DT` (checked on
  Spark 3.5.9). The cast that hurt here is of `event_ts`, and the real problem is that the predicate never
  mentions the partition column at all. (With Hive metastore tables, a predicate Spark can't translate for the
  metastore makes it fetch every partition's *metadata*, which is slower planning, but it still prunes the read.)

**Catch it next time.** Alert on the events scan's input bytes or task count per run, or on "number of partitions
read" from the SQL REST API. A 7x jump is a pruning or a data problem, both worth a page. In code review, treat
any `withColumn` that reuses a partition column's name as a bug.

## 60-second interview answer

*"A daily job suddenly reads the whole table. How do you find out why, in the Spark UI?"*

> First the Stages tab: the scan stage had seven times the tasks and input, while each task read the same amount,
> so it was reading more files, not bigger ones. Then the SQL tab: the scan node's metrics say how many partitions
> and files it read. Here it was 8 partitions and 8.6 TiB instead of one day, and the filter above it kept only
> 12 % of the rows. In the plan details the scan had no `PartitionFilters` at all, and the `Location` was the
> table root. The code still filtered on `dt`, but a `withColumn` had replaced `dt` with `to_date(event_ts)`, so
> the filter was on a data column. Spark pushed it to Parquet as a timestamp range, which can only skip row
> groups, and with INT96 timestamps not even that. Only predicates on partition columns prune directories,
> and casting the partition column itself is fine. The fix is to filter on the partition column first, the day
> plus the next one for late events, and then on the event date. That brings it back to one day's read. I'd
> also raise the late-event semantics, because events landing after the run are now dropped.
