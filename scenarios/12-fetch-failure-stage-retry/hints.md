# Hints (open one at a time)

<details><summary>Hint 1: what ran more than once?</summary>

Open the incident's **Stages** tab and read every table on it: *Completed*, *Skipped* and *Failed Stages*.
Which stage failed, after how many of its tasks? Which stage ids appear with an `(retry 1)` or `(retry 2)`
attempt, and how many tasks did each retry run? Compare with the last good run's Stages tab.
</details>

<details><summary>Hint 2: read the failure</summary>

Open the failed stage attempt and look at the tasks with errors (sort by **Status**, or read the *Failure
Reason* on the Stages tab). A `FetchFailed` names a `BlockManagerId(<executor id>, <host>, <port>, None)` and a
`shuffleId`. Whose block manager is that, and what does the `Caused by:` line at the end of the message say
about it?
</details>

<details><summary>Hint 3: the executor behind it</summary>

Find that executor on the **Executors** tab (it is listed among the dead ones). When was it removed, and why?
Then look at every dead executor: what do their **Remove Reason**s and remove times have in common? Did the job
get replacements?
</details>

<details><summary>Hint 4: before the failed stage</summary>

Go back to the events scan, stage 2. Its summary says more tasks *succeeded* than the stage has partitions,
and it has hundreds of *failed* tasks, but its task list shows only a few dozen failures. Open the driver's
`stderr` (Executors tab, the `driver` row, Logs) and search for `Resubmitted` and `Shuffle files lost`. What
happened to work that had already finished?
</details>

<details><summary>Hint 5: why losing an executor lost its work</summary>

An executor writes its map output (shuffle files) to its node's local disks. Who serves those files to the next
stage's tasks when the executor is gone? Look at the **Environment** tab: Spark Properties, then Hadoop
Properties (search for `aux-services`). What did the job decide on 2026-10-13?
</details>

<details><summary>Hint 6: why some nights</summary>

The remove reasons tell you who took the executors. Which entry of the ticket's calendar puts something else on
the cluster at 02:13, and why would YARN take containers from *this* job? Then check the last good run's
Executors tab.
</details>

<details><summary>Hint 7: the channel's suggestions</summary>

`spark.shuffle.io.maxRetries` and `spark.network.timeout` decide how long a reduce task keeps asking for blocks.
Read the `Caused by:` again: could any number of retries fetch them? And with 60 executors instead of 40,
whose containers would the preemption take?
</details>
