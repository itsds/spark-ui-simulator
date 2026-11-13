# Hints (open one at a time)

<details><summary>Hint 1: which query, which stage?</summary>

Open the **SQL / DataFrame** tab of both runs. Which of the three queries got slower, and by how much? Then
put the **Stages** tabs side by side: does the incident run have the same stages as the night before? (The UI
shows times in your browser's time zone; the ticket's are UTC.)
</details>

<details><summary>Hint 2: the long stage</summary>

Open stage 5 of the incident run. In **Summary Metrics**, compare **Max** with **Median** for Duration, then
for **Shuffle Read Size / Records**. Does the longest task also read the most? Is it one task, or a few?
</details>

<details><summary>Hint 3: a host, or the data?</summary>

Sort stage 5's task list by **Duration** (descending). Do the longest tasks run on the same executor or
host? Is GC, a failure or a lost executor anywhere in the run (**Executors** tab)?
</details>

<details><summary>Hint 4: what is stage 5 doing?</summary>

Stage 5's **DAG Visualization** reads two shuffles. Open query 0 in the **SQL / DataFrame** tab and find the
operators of stage 5 (hover the boxes, or match the stage ids in their metrics). Which join runs there, on
which key, and what is on its other side?
</details>

<details><summary>Hint 5: two plans</summary>

Open query 0 of the last good run next to it. How does the page catalog reach its join each night: which
join operator, which exchange, how much data goes through the exchanges? The **Details** at the bottom show
the plan as text (`Final Plan` and `Initial Plan`). The job description of the query also says how its author
expected the catalog to be joined.
</details>

<details><summary>Hint 6: the CMS migration</summary>

Did the page catalog change? Compare its **Scan parquet** node in both runs (number of files read, size of
files read, number of output rows). Would a duplicate page id show up somewhere in the join's metrics? Then
do the same for the events scan.
</details>

<details><summary>Hint 7: who decides the join strategy?</summary>

Without a hint in the code, Spark broadcasts the smaller side of a join when its estimated size is under a
threshold. Which one? Put the **Environment** tabs of the two runs side by side and look at the Spark
Properties. Where could a property come from that is not in the job's own `spark-submit`? Check the change
calendar again.
</details>

<details><summary>Hint 8: why skew now, and only now?</summary>

The events' page ids are what they always were: some pages (home, search, cart) get far more traffic than
others. Why did that never matter before? What does a sort-merge join on `page_id` do with every event of
the most popular page? Then think about what AQE skew join would change here, and what it would not.
</details>
