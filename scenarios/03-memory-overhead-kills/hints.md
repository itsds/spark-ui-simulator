# Hints (open one at a time)

<details><summary>Hint 1: the failed job</summary>

The application list shows the failed application twice, once per YARN attempt. Open attempt 1's **Jobs**
tab. How many jobs ran, compared with the last good run? Which one failed, and how many of its tasks
succeeded, failed or were killed? Click its stage and read the **Failure Reason** to the end.
</details>

<details><summary>Hint 2: "failed 4 times"</summary>

In the failed stage's task list, sort by **Status** (or look at the **Errors** column). What do the failed
tasks have in common? What about the killed ones? The failure reason names one task index: find its four
attempts. Do the same in attempt 2.
</details>

<details><summary>Hint 3: the executors</summary>

Open the **Executors** tab. The job asks for 40 executors: what is the highest executor ID? How many are
active at the end and how many dead ones are listed (the History Server keeps the latest 100 dead
executors)? Read a few of the dead executors' remove reasons, and compare their numbers with the size of
the container.
</details>

<details><summary>Hint 4: is it the heap?</summary>

Still on the **Executors** tab, look at **Peak JVM Memory OnHeap / OffHeap** against the 16 GiB heap, in both
runs. Then work out the container: `spark.executor.memory` plus `spark.executor.memoryOverhead` (default
`max(384 MiB, 10 %)` of it), rounded up to YARN's 1 GiB allocation step. What is YARN measuring when it says
"physical memory"?
</details>

<details><summary>Hint 5: what is different</summary>

Compare the **Environment** tabs (Spark Properties) of the last good run and attempt 1. Then open the **SQL /
DataFrame** tab, query 0, and look at the events-side `Project` box in the plan graph (its description lists
the expressions) and at the events scan's `ReadSchema` under **Details**.
</details>

<details><summary>Hint 6: what a task does on an executor</summary>

Open the `stderr` of the executor named in the failure reason (Executors tab, **Logs**; a dead one is in the
list if it died recently). Look at what each task logs right after it starts reading its input, how many
times, and how the log ends.
</details>

<details><summary>Hint 7: the driver's side</summary>

The driver runs in the ApplicationMaster container: its `stderr` and `stdout` links are on the driver row of
the Executors tab. Search `stderr` for `aborting job`, `Stage cancelled` and `Final app status`; read
`stdout` from the top. Compare the end of attempt 1's `stderr` with attempt 2's.
</details>

<details><summary>Hint 8: do the arithmetic</summary>

Each of a container's 4 cores runs one task at a time. How much does each task map, how much room does the
container leave outside the heap, and how much does the JVM itself need there? Then redo the sum for
`--executor-memory 24g`, for a bigger `spark.executor.memoryOverhead`, and for a UDF that opens the database
once per executor instead of once per task.
</details>
