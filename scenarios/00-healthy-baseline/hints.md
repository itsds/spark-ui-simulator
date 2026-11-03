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

<details><summary>Hint 6: the 2x tasks</summary>

Open the aggregation scan stage, sort the task table by Duration, and compare
the slowest tasks' *Input Size / Records* with the median in Summary Metrics.
Then look at `spark.sql.files.maxPartitionBytes` on the Environment tab.
</details>
