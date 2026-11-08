# Hints (open one at a time)

<details><summary>Hint 1: which stages got longer?</summary>

Put the **Stages** tabs of the two runs side by side. Which completed stages took longer last night, and
by how much? Is it one stage, or all of the big ones? (The UI shows times in your browser's time zone;
the ticket's are UTC.)
</details>

<details><summary>Hint 2: skew, or something else?</summary>

Open stage 2 of the incident run, the events scan. In **Summary Metrics**, compare **Max** with
**Median** for Duration, then for **Input Size / Records**. In a skewed stage the longest task also
reads the most data. Does it here?
</details>

<details><summary>Hint 3: where did the slow tasks run?</summary>

In the stage's task table, sort by **Duration** (descending). Read the **Executor ID** and **Host**
columns of the first rows, then of the first row that looks normal. Do the same in stage 5 (the join
and write).
</details>

<details><summary>Hint 4: one executor's view</summary>

Open **Aggregated Metrics by Executor** on stage 2 and sort by **Succeeded Tasks**. How many tasks did
the suspect executor finish compared with the others, and how much **Task Time** did it spend on them?
Then open the **Executors** tab: **Complete Tasks**, **Task Time (GC Time)**, **Input**, and the
**Add Time** column.
</details>

<details><summary>Hint 5: data, GC, memory or the machine?</summary>

Rule things out with numbers: the suspect's tasks read normal-sized inputs, GC is a normal share of its
task time, nothing spills, nothing fails. What is left when the same work simply runs slower on one
host? Open that executor's `stderr` (Executors tab) and compare the time from
`Started daemon` to `Successfully registered with driver`, and its `CodeGenerator` lines, with a healthy
executor's log.
</details>

<details><summary>Hint 6: the host, not the rack</summary>

The ticket mentions switch work on `/dc1/rack04` (`wn061`–`wn080`). How did the other executors on those
hosts do on the Executors tab? Is anything in the change calendar about the suspect host itself?
</details>

<details><summary>Hint 7: why only some nights?</summary>

Open the **Executors** tab of the last good run. Which hosts did YARN give the job that night? Is the
suspect host among them? Who decides where the 40 executor containers go, and is it the same every
night?
</details>

<details><summary>Hint 8: what Spark could have done about it</summary>

Look for `spark.speculation` in the **Environment** tab of either run. What does speculative execution
do with a task that runs much longer than the median of its stage, and why would it help here but not
with a skewed partition (Scenario 01)? What does `spark.excludeOnFailure` react to, and did anything on
this host fail?
</details>
