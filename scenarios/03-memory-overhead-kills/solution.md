# Solution: every task maps a 2.0 GiB file, and YARN counts it

**Root cause.** Release 2.4 added `geo_city = geoip_city(ip_address)`, a Java UDF from the shared `dp-udfs`
jar. Its implementation opens its own reader on the vendor's IP database in **every task** (the reader is a
transient field of the UDF instance, and Spark deserializes one instance per task) and memory-maps the whole
**2.0 GiB** file (`--files .../ip-intel-2026q4.mmdb`). Mapped file pages live outside the JVM heap, and the
NodeManager's physical-memory check adds up the resident set (RSS) of the container's processes, which counts
the pages each mapping has touched, once per mapping. An executor with 4 cores runs 4 such tasks at once: up
to **8 GiB outside the heap**, in a container that leaves **2 GiB** for everything outside the 16 GiB heap
(the default overhead, 10 % = 1,638 MiB, rounded up to YARN's 1 GiB step: an 18 GiB container), and the JVM
itself needs about 1 GiB of that (metaspace, code cache, G1's own structures, thread stacks, netty buffers)
once its heap has grown. YARN kills such a container as soon as its RSS passes 18 GiB: exit code -104,
"Container killed by YARN for exceeding physical memory limits".

Every running task of a killed executor fails with `ExecutorLostFailure (... exited caused by one of the
running tasks)`, and a loss caused by the app **counts towards `spark.task.maxFailures` (4)**. The retries
land on executors that are just as full of mappings, so they die again, and the first task index to lose its
4th attempt aborts the stage, the job and the query. The PySpark driver gets a `Py4JJavaError` nobody
catches, exits with status 1, the ApplicationMaster reports `FAILED`, and YARN starts the application a second
time (`yarn.resourcemanager.am.max-attempts` = 2): same code, same data, same containers, same ending. The heap
was never the problem: executors used under 4 GiB of their 16 GiB heap.

Every number below is what the History Server shows for the last good run and for both attempts of the
incident, and `tests/test_scenario03_hs.py` checks each one against its REST API (and the container logs
through the executors' log links). `uv run scripts/evidence.py 03-memory-overhead-kills` prints them for a
running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → each attempt of the incident | `5.5 min → attempt 1: 4.0 min, attempt 2: 4.4 min` |
| E2 | Jobs tab, each attempt | Jobs that ran, the failed one and its tasks | `attempt 1: 3 jobs, job 2 FAILED (2,400/5,225 tasks succeeded, 477 failed, 135 killed); attempt 2: 3 jobs, job 2 FAILED (2,714/5,225 tasks succeeded, 558 failed, 122 killed)` |
| E3 | Attempt 1, Stages tab, stage 2, Failure Reason (first line) | Why the stage was aborted | `Job aborted due to stage failure: Task 386 in stage 2.0 failed 4 times, most recent failure: Lost task 386.3 in stage 2.0 (TID 3226) (wn099.hadoop.prod.internal executor 155): ExecutorLostFailure (executor 155 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding physical memory limits. 18.6 GB of 18 GB physical memory used. Consider boosting spark.executor.memoryOverhead.` |
| E4 | Stage 2's task list, each attempt | Failed tasks (all with the pmem reason) and killed ones | `attempt 1: 477 FAILED, 477 of them pmem kills; 135 KILLED by 'Stage cancelled' (135); attempt 2: 558 FAILED, 558 of them pmem kills; 122 KILLED by 'Stage cancelled' (122)` |
| E5 | Attempt 1, stage 2's task list | Failures per task index | `360 indices failed 1x, 46 indices failed 2x, 7 indices failed 3x, 1 index failed 4x` |
| E6 | Executors tab, each attempt | Executor IDs used, active and dead executors listed (the latest 100 dead are kept) | `attempt 1: executors 1–182, 37 active at the end, 95 dead listed; attempt 2: executors 1–207, 33 active at the end, 93 dead listed` |
| E7 | Executors tab, dead executors, both attempts | Remove reasons | `every dead executor: pmem kill, 18.0–19.7 GB of 18 GB` |
| E8 | Executors tab, Peak JVM Memory OnHeap (REST `peakMemoryMetrics.JVMHeapMemory`), highest executor | Heap actually used | `last good 4.7 GiB, incident 3.8 GiB / 3.8 GiB (heap 16 GiB)` |
| E9 | Environment tab, Spark Properties, last good vs attempt 1 | What differs (and the overhead) | `spark.yarn.dist.files: ip-intel-2026q4.mmdb; spark.yarn.dist.jars: dp-udfs-2.3.1.jar; spark.executor.memoryOverhead: not set` |
| E10 | SQL / DataFrame tab, query 0: the events-side Project's description and the events scan's ReadSchema | The new expression and its input | `incident: geoip_city(ip_address#10) AS geo_city#107; last good: none; ip_address in the events scan's ReadSchema: incident yes, last good no` |
| E11 | Attempt 1, `stderr` of the executor in E3 (Executors tab, Logs) | What its tasks logged, and its end | `executor 155: 12 tasks, 12 'Opened IP database ... (2.0 GiB) in MEMORY_MAPPED mode', ends with RECEIVED SIGNAL TERM` |
| E12 | Driver `stderr` (Executors tab, driver row), each attempt | How the ApplicationMaster ended | `attempt 1: Final app status: FAILED, exitCode: 1, no unregister; attempt 2: Final app status: FAILED, exitCode: 1, Unregistering ApplicationMaster with FAILED` |
| E13 | Attempt 1, driver `stdout` | The Python driver's last words | `daily_enrichment.py line 61: Py4JJavaError: An error occurred while calling o120.parquet.` |

## Click path and what each number means

**Two attempts, both short.** The last good run took 5.5 min. The failed application is listed twice in the
History Server, once per YARN attempt: **4.0 min and 4.4 min** (E1), both shorter than a good night because
they stop half way. In each attempt only **3 jobs** ran instead of 8: the page catalog's broadcast, the
customer scan's map stage and the events map stage, **job 2, which FAILED** with 2,400 of its 5,225 tasks done
in attempt 1 (2,714 in attempt 2), **477 failed and 135 killed** (558 and 122) (E2). Everything after it
(the join and write of `events_enriched`, then `daily_funnel` and `customer_daily_activity`) never started.

**The failure reason says it all, if you read it to the end.** Stage 2 was aborted because **task 386 failed
4 times**; its last attempt was lost with executor 155, and the reason is **"Container killed by YARN for
exceeding physical memory limits. 18.6 GB of 18 GB physical memory used. Consider boosting
spark.executor.memoryOverhead."** (E3). It is not one unlucky task: **every** failed task of stage 2 failed
with that reason, 477 in attempt 1 and 558 in attempt 2, and the 135 / 122 killed ones were killed by Spark
itself when it gave up on the stage (`Stage cancelled: Job aborted due to stage failure ...`) (E4). Task 386 is
simply the first index that reached 4: 360 indices failed once, 46 twice, 7 three times (E5).

**Executors die by the dozen.** The job asks for 40 executors. Attempt 1 used executor IDs **1–182**, attempt 2
**1–207**: every killed container was replaced by YARN, and the replacements died too. 37 / 33 were alive at the
end; the Executors tab lists 95 / 93 dead ones, only the latest of them (the History Server keeps 100 dead
executors, `spark.ui.retainedDeadExecutors`) (E6). Every dead executor's remove reason is a physical-memory
kill, **18.0–19.7 GB of 18 GB** (E7): the container, not the heap.

**Not the heap.** Peak JVM heap used was **4.7 GiB** in the last good run and **3.8 GiB** in both attempts, of a
16 GiB heap (E8). No `OutOfMemoryError`, no GC trouble. YARN measures the whole process tree's resident memory:
the heap the JVM has committed (G1 grows it towards `-Xmx` as it runs, whatever is live in it), the JVM's own
native memory, and anything else the process maps or allocates. What the container leaves outside the heap is
`spark.executor.memoryOverhead`, and it is **not set** (E9): the default 10 %.

**What changed: the release, not the cluster.** The Spark properties of the two runs differ in exactly two
entries: **`spark.yarn.dist.jars` = `dp-udfs-2.3.1.jar`** and **`spark.yarn.dist.files` =
`ip-intel-2026q4.mmdb`** (E9). In the SQL tab, query 0's events-side `Project` now computes
**`geoip_city(ip_address#10) AS geo_city#107`**, and the events scan reads `ip_address` (E10). The UDF runs in
stage 2, the events scan, which is where every task failed. (The Project's expressions are in the plan graph's
node description; the formatted plan text under **Details** prints only its Output and Input.)

**What one task does to an executor.** Executor 155's `stderr` shows it: each of its **12 tasks** logs
**`GeoIpCity: Opened IP database ./ip-intel-2026q4.mmdb (2.0 GiB) in MEMORY_MAPPED mode`** right after it starts
reading its input, once per task, and the log ends with **`RECEIVED SIGNAL TERM`**: the NodeManager killed the
container (E11). One reader per task means one 2.0 GiB mapping per running task; with 4 cores that's up to
8 GiB, and the lookups touch most of the file within the first part of a task. A fresh executor survives its
first tasks while its heap is small; once G1 has grown the heap, the next round of tasks pushes it over 18 GiB.

**Why Spark gave up, and why YARN didn't help.** An executor lost because of what its tasks did ("exited caused
by one of the running tasks") counts as a failure of each of its running tasks, so the same index can fail on
4 different executors in a row; then `TaskSetManager` aborts the task set (the driver's `stderr`: `Task 386 in
stage 2.0 failed 4 times; aborting job`, then `Cancelling stage 2`, `Killing all running tasks in stage 2:
Stage cancelled: ...`). The AM's own limit on executor failures (`spark.executor.maxNumFailures`, 80 here) does
not count physical-memory kills in Spark 3.5, so the task limit is what ends it. The query's error reaches the
PySpark driver as a `Py4JJavaError` from line 61, the `parquet()` write (E13), the script doesn't catch it, the
driver exits with status 1, and the AM logs **`Final app status: FAILED, exitCode: 1`** (E12). Attempt 1 does
not unregister: YARN treats it as a failed attempt and starts attempt 2 a few seconds later, in a new AM
container (`..._02_000001`) with new executors. Same code, same data: attempt 2 fails the same way and, being
the last attempt, **unregisters the application as FAILED** (E12). That's the "User application exited with
status 1" Airflow reported. An Airflow retry would have failed a third and fourth time.

## The two runs side by side

| | Last good (release 2.3) | Incident (release 2.4), attempt 1 / attempt 2 |
|---|---|---|
| Outcome | SUCCEEDED, 5.5 min, 8 jobs | FAILED, 4.0 min / FAILED, 4.4 min, 3 jobs each |
| Stage 2 (events scan) | complete | FAILED: task 386 / 2598 lost 4 times |
| Executors | 40, none lost | ids up to 182 / 207, 145 / 173 containers killed (pmem) |
| Peak JVM heap used | 4.7 GiB of 16 GiB | 3.8 GiB / 3.8 GiB of 16 GiB |
| Container (heap + overhead) | 18 GiB (16 + 1.6 rounded) | 18 GiB, the same |
| Spark properties | — | + `spark.yarn.dist.jars` (dp-udfs), `spark.yarn.dist.files` (2.0 GiB `.mmdb`) |
| Events-side Project | `…, campaign, dt` | `…, campaign, geoip_city(ip_address) AS geo_city, dt` |

## The fix

The memory has to fit, or there has to be less of it. All of these were verified in the simulation
(`tests/test_scenario03.py`):

* **Tonight: give the container room for four mappings.** `--conf spark.executor.memoryOverhead=10g`
  (16 + 10 = 26 GiB containers: 4 x 2.0 GiB of mappings plus the JVM's own native memory). The rerun succeeds
  with no executor lost, in **7.3 min**: the UDF itself costs about 1.8 min of CPU over the 5.5-minute
  release 2.3. It costs 8 GiB more of the `etl` queue per executor (320 GiB in all) for as long as it stays.
* **For good: one reader per JVM.** The UDF should open the database once per executor (a lazily initialized
  static holder) instead of once per task: one mapping per container, shared by its 4 tasks. The JVM still
  needs room for it: with that fix and `spark.executor.memoryOverhead=4g` (20 GiB containers) the job runs in
  **7.2 min** with no kills. The fixed UDF alone, without more overhead, still loses dozens of executors: one
  2.0 GiB mapping next to a fully grown 16 GiB heap doesn't fit in 2 GiB either.
* **Better still, no file at all:** an IP-range table of the vendor's data, joined with a broadcast range join or
  pre-computed per IP prefix, keeps the lookup inside Spark's memory management (and in the plan).

What doesn't fix it:

* **`--executor-memory 24g`**, the channel's suggestion: the default overhead grows by 10 % of the extra heap
  (to 2.4 GiB, a 27 GiB container), not by the 6+ GiB that's missing, and the bigger heap gives G1 more to grow
  into. In the simulation attempt 1 still failed, and attempt 2 scraped through in 8.2 min after 140 executor
  kills: a coin flip, not a fix.
* **Fewer cores per executor** reduces the mappings per container, but with 2 cores the two mappings (4 GiB)
  still don't fit in 2 GiB: the job limps through with a hundred executor kills and half the slots.
* **Retries** (`spark.task.maxFailures`, Airflow retries, a third YARN attempt) only repeat the kills.

**Why it worked locally.** In local mode there is no YARN container and no NodeManager measuring RSS: the
mappings cost nothing but page cache. The first place this can show is a YARN cluster with a physical-memory
check, at production parallelism.

**Platform note.** The NodeManager counts a file mapped four times as four times its touched pages because it
reads each process's RSS (`/proc/<pid>/stat`). Hadoop can account with `smaps` instead
(`yarn.nodemanager.container-monitor.procfs-tree.smaps-based-rss.enabled`), which treats clean, read-only
file pages differently; that is a cluster-wide decision for the platform team, not a fix for one job.

## 60-second interview answer

*"A Spark job on YARN fails with 'Container killed by YARN for exceeding physical memory limits'. How do you debug
and fix it?"*

> First I read the whole failure: the stage was aborted because one task failed four times, and every failure was
> its executor being killed by YARN for physical memory, so retries just moved the problem around, and YARN's
> second attempt failed the same way. YARN measures the container's whole process tree, not the heap, so I check
> the heap: peak use was under 4 of 16 GiB, so more executor memory isn't the answer. The container is heap plus
> memoryOverhead, 10 % by default, about 2 GiB here, and the JVM needs half of that for itself. So I look for what
> uses memory outside the heap and what changed: the release added a Java UDF, and the executor logs show every
> task memory-mapping a 2 GiB IP database. Four cores means four mappings, 8 GiB in a 2 GiB budget. Tonight I'd
> raise spark.executor.memoryOverhead to 10g so four mappings fit; for good I'd make the UDF share one reader per
> executor and give it 4g, or replace the file lookup with a broadcast join. Fewer cores or a bigger heap alone
> don't fix it, and retries only repeat the kills.
