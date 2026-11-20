# Hints (open one at a time)

<details><summary>Hint 1: where the ten minutes went</summary>

Compare the two runs' **Jobs** tabs: which job grew, and which didn't? Then the **Stages** tab: the stage that
grew most is not the one that reads the events. Put its **Shuffle Read** and **Output** columns next to the last
good run's.
</details>

<details><summary>Hint 2: is it traffic?</summary>

Look at stage 2, the events scan, in both runs: its **Input** and its **Shuffle Write**. Did Friday have more
events than Thursday? So where do the extra rows come from, if not from the input?
</details>

<details><summary>Hint 3: rows in, rows out</summary>

Open stage 5 in both runs and read the **Summary Metrics**: per task, how many records does it read from the
shuffle, and how many does it write? In the last good run a task writes a few % fewer rows than it reads. What
does a task of the incident do?
</details>

<details><summary>Hint 4: follow the row counts</summary>

Open the **SQL / DataFrame** tab, query 0 (`events_enriched …`). Walk the graph from the scans up to the write and
read every `number of output rows` (and the Exchanges' records). Which node outputs more rows than it receives?
Compare that node in the last good run.
</details>

<details><summary>Hint 5: what an inner join can output</summary>

An inner equi-join outputs, for each row of one side, one row per row of the other side *with the same key*. The
events side is the same as on Thursday. What must be true of the other side for the join to output six rows per
event? Now look at that side's `Scan parquet` node: files, size, rows, compared with the last good run. Did the
company gain 350 million customers overnight?
</details>

<details><summary>Hint 6: the plan didn't change</summary>

Open **Details** at the bottom of query 0 in both runs and compare the customers scan's block (`Location`,
`ReadSchema`, `PushedFilters`). Is anything different apart from the expression ids? Then the two daily
aggregates (queries 1 and 2): their scans read 6x the rows, but how many groups (customers, funnel cells) do they
output? What does that say about the extra rows?
</details>

<details><summary>Hint 7: the spill</summary>

Stage 5 spills 1.6 TiB to disk. In query 0's graph, which node's `spill size` is that, and which node's is 0?
Why does the sort before the write spill in the incident and not on Thursday (rows per task, peak execution
memory)? Would more memory or more shuffle partitions change the number of rows written? An executor's `stderr`
(a stage-5 task) shows the sorter spilling.
</details>

<details><summary>Hint 8: what changed upstream, and the fix</summary>

The CRM team announced on 2026-11-05: "`customers_snapshot` keeps the history of every customer from tomorrow:
one row per version, with `valid_from`, `valid_to` and `is_current`. Jobs that select their columns explicitly
are not affected by the new columns. Current state: `is_current = true`." What did that change about the table,
apart from its columns? Write the filter the join needs, and say what it will look like in the plan. Then: which
tables and partitions are wrong now, what must run again, and what check would have stopped the bad data before
the dashboards read it?
</details>
