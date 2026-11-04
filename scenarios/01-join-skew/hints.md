# Hints (open one at a time)

<details><summary>Hint 1: where did the time go?</summary>

Open the incident run's **Jobs** tab and expand **Event Timeline**. Which job takes up almost the whole
run? Its description names the query. Compare with the same job in the last good run.
</details>

<details><summary>Hint 2: is the whole stage slow, or a part of it?</summary>

On the **Stages** tab, open the longest stage. Before reading anything else, look at the
**Summary Metrics**: compare the **Max** column with the **75th percentile** for Duration and for
Shuffle Read Size / Records. A stage where everything is a bit slower looks very different from a
stage where one task is far away from the others.
</details>

<details><summary>Hint 3: find the task</summary>

In the stage's **Tasks** table, sort by Duration (descending). Note its index, executor, host, shuffle
read and spill. The Event Timeline above the table shows the tasks of the current page only, so sort
first, then expand the timeline. What were the other 2,399 tasks doing while this one ran?
</details>

<details><summary>Hint 4: slow node or big input?</summary>

A slow node makes *every* task on it slow; a big partition makes *one* task slow wherever it runs.
Look at the other tasks the same executor ran (Aggregated Metrics by Executor, or filter the task
table). And what would make a NodeManager's local dirs grow by ~120 GB during one task?
</details>

<details><summary>Hint 5: the SQL tab</summary>

Open the query that wrote `events_enriched` on the **SQL / DataFrame** tab. On the `Exchange` and
`Sort` nodes, read the metrics as *total (min, med, max (stageId: taskId))*: which task holds the
max, and how far is it from the median? Which join strategy did Spark use, and is there anything
in the plan that splits a skewed partition?
</details>

<details><summary>Hint 6: why didn't Spark fix it by itself?</summary>

Spark 3.5's adaptive execution can split a skewed sort-merge join partition at runtime. Check the
**Environment** tab of both runs for `spark.sql.adaptive.skewJoin.*`.
</details>

<details><summary>Hint 7: what changed, if not the code?</summary>

Compare the two runs' SQL plans for query 0: the rows the events scan produced and the rows the join
produced. Then think about which value of the join key could suddenly hold hundreds of millions of
rows. The partition a key goes to is `pmod(hash(key), spark.sql.shuffle.partitions)`, and in a
2,400-task reduce stage without AQE coalescing, task index = partition id.
</details>

<details><summary>Hint 8: the executor log</summary>

Open the straggler's executor `stderr` and find its `Running task … (TID n)` line. What does
`UnsafeExternalSorter` say, how often, and why are there two separate counts?
</details>
