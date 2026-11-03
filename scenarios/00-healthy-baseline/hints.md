# Hints (open one at a time)

<details><summary>Hint 1: start from the top</summary>

The **Jobs** tab header has Total Uptime and the job count. Expand **Event
Timeline**: the executor lane shows when executors registered, the job lane
shows which jobs overlapped. Each job's description names the query that
started it.
</details>

<details><summary>Hint 2: why so many jobs?</summary>

With adaptive query execution (on by default in Spark 3.5) every shuffle is
materialized by its own job. When the last job of a query runs, it lists the
already-finished map stages again under new stage ids. Spark finds their
output and doesn't run them: those are the **skipped** stages (Stages tab,
bottom; grey boxes in a job's DAG).
</details>

<details><summary>Hint 3: the biggest stage</summary>

On the **Stages** tab, sort by Duration and open the top stage. Read the
**Summary Metrics** (min / 25th / median / 75th / max), the **Locality Level
Summary** in the header, and **Aggregated Metrics by Executor**. Tick
*Show Additional Metrics → Select All* to see scheduler delay, deserialization
and peak memory.
</details>

<details><summary>Hint 4: where to see GC, spill and balance</summary>

**Executors** tab: *Task Time (GC Time)* turns red above 10 % GC. Compare
*Complete Tasks* and *Input* across executors. Spill appears as
*Spill (Memory) / Spill (Disk)* columns on a stage page, and only when a stage
spilled.
</details>

<details><summary>Hint 5: what AQE did</summary>

Compare the number of tasks of the stage that reads a shuffle with
`spark.sql.shuffle.partitions` on the **Environment** tab. Open that stage's
DAG: is there an `AQEShuffleRead` box?
</details>

<details><summary>Hint 6: the SQL tab</summary>

Open **SQL / DataFrame** and click a query. Each box is a physical operator
with its metrics; the blue `WholeStageCodegen (n)` boxes group operators that
run as one generated function. Follow `number of output rows` from the scans
down through `Filter`, `BroadcastHashJoin` and `SortMergeJoin`. *Details* under
the graph has the plan as text, including `PushedFilters` and `ReadSchema`.
</details>

<details><summary>Hint 7: the small activity shuffle</summary>

In query 2, compare `number of output rows` of the `HashAggregate` before the
`Exchange` (partial) with the one after it (final). Then look at how query 0
produced `events_enriched`: what was its data hash-partitioned on just before
the write?
</details>

<details><summary>Hint 8: the container logs</summary>

On the **Executors** tab, open one executor's `stderr` (and the driver's). Find one task from start to
finish: `Running task … (TID n)` to `Finished task …`. What does a scan task log, what does a reduce task
log, what does a write task log? Is there any WARN or ERROR line? Remember what a quiet executor log looks
like: an incident's log is easier to read when you know what is normally there.
</details>
