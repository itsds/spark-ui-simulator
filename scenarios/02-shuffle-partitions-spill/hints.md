# Hints (open one at a time)

<details><summary>Hint 1: what ran between 02:14 and 02:19?</summary>

Open the incident run's **Jobs** tab and expand **Event Timeline**. Which job was running while the
disks alerted? Its description names the query. Open the same job in the last good run and compare
the durations. (The UI shows times in your browser's time zone; the ticket's are UTC.)
</details>

<details><summary>Hint 2: the stage behind that job</summary>

Open the job, then its stage that did the work. Read the stage header before anything else: number
of tasks, **Total Time Across All Tasks**, **Spill (Memory)**, **Spill (Disk)**. Then open the same
stage in the last good run. What changed, and what didn't?
</details>

<details><summary>Hint 3: one task, or all of them?</summary>

Look at the **Summary Metrics** for Duration, Spill (memory) and Shuffle Read Size / Records. Compare
**Min**, **Median** and **Max**. In a skewed stage the max is far from the median. What does it mean
when all three are close to each other, and far above last night's?
</details>

<details><summary>Hint 4: what spill is, and where it goes</summary>

A task spills when the rows it has to sort (or aggregate) don't fit in the execution memory it can get.
Tick **Peak Execution Memory** under **Show Additional Metrics**: is there a ceiling? Work it out from the
Environment tab (executor memory, `spark.memory.fraction` default 0.6, 300 MiB reserved, cores per
executor). Where on a YARN node does Spark write its spill files?
</details>

<details><summary>Hint 5: same data?</summary>

Compare the stage that scans the events (stage 2) in both runs: input size and **Shuffle Write**. If the
map side wrote the same amount of shuffle data, why does each reduce task read so much more?
</details>

<details><summary>Hint 6: the SQL tab, and what AQE can and can't do</summary>

Open query 0 on the **SQL / DataFrame** tab of both runs. On the `Exchange` nodes, read
**number of partitions**; in the plan's Details, find `hashpartitioning(customer_id#…, N)`. Is there an
`AQEShuffleRead` above the exchanges? What does AQE's `CoalesceShufflePartitions` do with partitions that
are too *small*, and what does it do with partitions that are too *big*?
</details>

<details><summary>Hint 7: what changed, if not the data or the code?</summary>

Compare the **Spark Properties** on the **Environment** tab of both runs (ignore the values every run has
its own copy of: app id, start time, driver host and port). A property that isn't listed at all has its
default value. What is the default of the one that disappeared?
</details>

<details><summary>Hint 8: the executor log and the idle cores</summary>

Open any executor's `stderr` (Executors tab) and search for `UnsafeExternalSorter`. Why does every task
log two different spill sizes, each with its own count? Then go back to the stage: how many tasks could
run at once on 40 executors x 4 cores, and what were most cores doing after 02:16?
</details>
