# Hints (open one at a time)

<details><summary>Hint 1: where the nine minutes went</summary>

Compare the two runs' **Jobs** tabs: which job grew, and did the others? Then the **Stages** tab: for the stage
that grew, compare its number of tasks and its **Input** column with the last good run's.
</details>

<details><summary>Hint 2: what the tasks of that stage do</summary>

Open stage 2 in both runs and read the **Summary Metrics**. Per task, is the input bigger, or are there just more
tasks? Now look at the shuffle write per task: what do most tasks of the incident write to the next stage? A task
that reads ~110 MiB and writes nothing: what did it read?
</details>

<details><summary>Hint 3: the SQL tab</summary>

Open the **SQL / DataFrame** tab, query 0 (`events_enriched …`), in both runs. Find the `Scan parquet` node of
the events (the one with "number of partitions read") and read its metrics: partitions read, files read, size of
files read. Then the `Filter` right above it: how many of the scanned rows does it keep?
</details>

<details><summary>Hint 4: the scan's details</summary>

At the bottom of the query page, open **Details** (the physical plan). Read the events scan's block:
`Location`, `PartitionFilters` (is there one?), `PushedFilters`. The pushed filters have a date range: on which
column? Does a filter on that column tell Spark which *directories* to skip?
</details>

<details><summary>Hint 5: how the last good run read one day</summary>

Do the same in the last good run. It has no `PartitionFilters` either. So how did it read only one day? Look at
its `Location`.
</details>

<details><summary>Hint 6: which dt?</summary>

DATA-4398 changed the read to

```python
events = (spark.read.parquet(f"{WH}/web.db/events")
          .withColumn("dt", F.to_date("event_ts"))
          .where(F.col("dt") == DT))
```

Which `dt` does the `where` compare: the partition column, or something else? Find the `Project` above the
events scan's `Filter` in **Details**: what does it output as `dt`?
</details>

<details><summary>Hint 7: the driver says so too</summary>

Open the driver's `stderr` (Executors tab, the `driver` row). Search for `list leaf files`, `Pruning directories
with` and `Selected`. Compare with what an executor's `stderr` says in its `Reading File path` lines: which `dt=`
directories does a single executor read?
</details>

<details><summary>Hint 8: the fix, and the rule about casts</summary>

What must the code filter on so that Spark can skip directories, and how many days must it keep to still catch
late events? Then test the channel's rule: would `F.col("dt").cast("string") == DT` (a cast *of the partition
column*) prune? What makes a predicate a partition filter in Spark? Last, the pushed `event_ts` range: why did
Parquet skip nothing with it?
</details>
