# Solution: preempted executors took their shuffle files with them

**Root cause.** Two things, neither enough on its own. Since the NodeManager upgrade of 2026-10-13 the job runs
**without the external shuffle service**: that night the `spark_shuffle` aux service didn't start on six nodes,
executors failed with `Unable to register with external shuffle server`, and the on-call added
`--conf spark.shuffle.service.enabled=false` to the Airflow task (DATA-4127). It worked, and nobody took it out.
Without the shuffle service an executor serves its own shuffle files, and **when the executor dies, its map
output dies with it**. The second thing came on 2026-11-10: the ML platform moved its nightly retraining in queue
`ml` to 02:13 (ML-2290). At night the `etl` queue runs far above its guaranteed share; when `ml` claims its
guarantee, YARN's CapacityScheduler **preempts** the youngest containers of the queue over its share, which are
this job's executors. Last night YARN took **10 of the 40 executors** in two waves, at 02:13:42–54 and
02:15:12–21, and gave no replacements (the queue had no room). Six went while the events scan (stage 2) was ~90 %
done: their finished map tasks had to run again. Four went while the join and write (stage 5) ran: its tasks
could not fetch the blocks those executors had held, failed with **FetchFailed**, and the DAGScheduler failed the
stage attempt, re-ran the lost map partitions (the final job's map stages, in three more attempts), and then ran
the write again for the partitions that hadn't finished. Nothing is corrupt: every output partition was written
once, and the job succeeded. It just did ~2 minutes of work twice. On nights when the retraining starts later,
nothing is preempted and the job takes its usual 5.5 minutes.

Every number below is what the History Server shows for the two runs, and `tests/test_scenario12_hs.py`
checks each one against its REST API (and the driver's and an executor's container logs through their links).
`uv run scripts/evidence.py 12-fetch-failure-stage-retry` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.5 min → 7.6 min` |
| E2 | Jobs tab, both runs | The events_enriched write job (`spark.job.description` starts with `events_enriched`) | `job 3: 44 s → 1.9 min, 1 failed stage, 1 skipped` |
| E3 | Stages tab, Failed Stages; the attempt's task list | The failed stage attempt and its failed tasks | `stage 5 attempt 0 FAILED: 1,180 of 2,400 tasks succeeded, 43 FetchFailed, 4 ExecutorLostFailure; org.apache.spark.shuffle.FetchFailedException` |
| E4 | Stages tab, every stage id with more than one attempt or a skipped entry (`stage: attempt:tasks`) | Retried and re-run stages | `3: 1:153 + 2:473; 4: 0:51 + 1:12 + 2:25; 5: 0:2,400 + 1:1,220` |
| E5 | Stage 2 (events scan), summary and task list; the driver's `stderr` | Map tasks run twice | `stage 2: 5,907 succeeded for 5,200 partitions, 730 failed: 23 ExecutorLostFailure in the task table, 707 "Resubmitted ShuffleMapTask(2, …)" in the AM log` |
| E6 | Stage 5 attempt 0, the first task with a `FetchFailed` error | What failed | `task 1174.0 (TID 7478) on executor 14: FetchFailed(BlockManagerId(34, wn098.hadoop.prod.internal, 45514, None), shuffleId=0, mapIndex=17, mapId=6269, reduceId=1174` |
| E7 | Executors tab, the executor the `BlockManagerId` names | Why it is gone | `executor 34 (wn098.hadoop.prod.internal), removed 02:15:12: Container container_e23_1791902144377_50187_01_000035 on host: wn098.hadoop.prod.internal was preempted.` |
| E8 | Executors tab (Status: Dead) | Every lost executor | `40 executors, 10 dead, 10 "was preempted." (02:13:42–02:15:21), no replacement` |
| E9 | Environment tab, Spark and Hadoop Properties | Shuffle service and preemption | `spark.shuffle.service.enabled=false; yarn.nodemanager.aux-services=mapreduce_shuffle,spark_shuffle; yarn.resourcemanager.scheduler.monitor.enable=true` |
| E10 | Environment tab, Spark Properties, last good vs incident | What differs | `same submit options` |
| E11 | Last good run, Executors and Stages tabs | The night before | `40 executors, 0 dead, 0 failed tasks, 0 stage retries` |
| E12 | The driver's `stderr` (Executors tab, `driver`, Logs) | What YARN and the DAGScheduler said | `10 containers with exit status -102; Shuffle files lost for executor: 34 (epoch 9); Resubmitting ShuffleMapStage 4 (parquet at NativeMethodAccessorImpl.java:0) and ResultStage 5 (parquet at NativeMethodAccessorImpl.java:0) due to fetch failure` |
| E13 | `stderr` of the executor that ran E6's task | The fetch, from the reduce side | `org.apache.spark.ExecutorDeadException: [INTERNAL_ERROR_NETWORK] The relative remote executor(Id: 34), which maintains the block data to fetch is dead. (12 outstanding blocks; 24 "Failed to get block(s)" lines in this log)` |

## Click path and what each number means

**Application list and Jobs.** 5.5 min the night before, **7.6 min** last night (E1). The Jobs tab puts most of the
difference in job 3, the write of `events_enriched`: 44 s the night before, **1.9 min** last night, with **one
failed stage** (E2). The events scan's map job (job 2) is also 40 s longer (3.7 → 4.4 min), and the two daily
aggregates at the end a few seconds each.

**Stages: a failed attempt and retries.** The Stages tab has a *Failed Stages* table now: **stage 5, attempt 0**,
the join and write, failed after 1,180 of its 2,400 tasks had succeeded, with 43 `FetchFailed` and 4
`ExecutorLostFailure` tasks; the Failure Reason is `org.apache.spark.shuffle.FetchFailedException` (E3). Below
it, stage ids you have never seen run: **stage 3** (the events map output, which job 3 normally lists as
*skipped*) ran twice, as `(retry 1)` with 153 tasks and `(retry 2)` with 473; **stage 4** (the customers map output)
three times, 51, 12 and 25 tasks; and stage 5 again as `(retry 1)` with **1,220 tasks** (E4). 1,180 + 1,220 =
2,400: the retry ran exactly the partitions attempt 0 didn't finish. Stage 3 still appears under *Skipped
Stages* with its attempt 0. That is how Spark numbers it: when job 3 started, the stage's output was available, so
the DAGScheduler skipped it and moved its next attempt number to 1 (`increaseAttemptIdOnFirstSkip`); its first
real run is attempt 1. Stage 4 had already lost outputs when job 3 started, so it ran as attempt 0 right away
(the driver log: `Missing parents: List(ShuffleMapStage 4)`).

**Stage 2: work done twice, hidden in the counts.** The events scan's summary says **5,907 succeeded tasks for
5,200 partitions** and **730 failed** (E5), but its task table only shows 23 failed tasks, all
`ExecutorLostFailure` (their executor disappeared under them, "unrelated to the running tasks"). The other 707
are tasks that had *finished*: without the shuffle service their output was on the executors YARN took, so the
TaskSetManager put them back in the queue with a second TaskEnd, `Resubmitted`. The History Server counts that
TaskEnd as a failed task of the stage but leaves the task's row as SUCCESS; the driver's log names every one:
`Resubmitted ShuffleMapTask(2, 2773), so marking it as still running.` 707 map tasks of ~7 s each, on 34
executors instead of 40: the 40 s of job 2.

**The FetchFailed.** Open stage 5 attempt 0 and find the tasks with errors (E6):
`FetchFailed(BlockManagerId(34, wn098.hadoop.prod.internal, 45514, None), shuffleId=0, mapIndex=17, mapId=6269,
reduceId=1174, …)`. A reduce task asked executor 34 for the block of map partition 17 of shuffle 0 (the customers
side of the join), and the message ends with the reason:
`Caused by: org.apache.spark.ExecutorDeadException: … The relative remote executor(Id: 34), which maintains the
block data to fetch is dead.` Not a timeout, not a corrupt block: the executor that holds the file is gone.

**Where executor 34 went.** On the Executors tab, executor 34 is dead: removed at 02:15:12, **`Container … on host:
wn098.hadoop.prod.internal was preempted.`** (E7). So are nine others, all with the same reason, in two bursts
(02:13:42–02:13:54, then 02:15:12–02:15:21), and nobody came to replace them: the job finished on 30 executors
(E8). Preemption is YARN taking containers back for another queue (`yarn.resourcemanager.scheduler.monitor.enable
= true` on this cluster), and YARN takes the youngest containers first: executors 31–40, the last ones granted.
Spark doesn't count it against the job (the tasks lost with them are "not counting this failure towards the
maximum number of failures"), but it can't stop it either.

**Why a few lost executors cost two minutes.** On a normal YARN cluster an executor's shuffle files are served by
the **external shuffle service**, the `spark_shuffle` aux service inside every NodeManager: the files stay on the
node's disks and stay fetchable after the executor is gone. The Environment tab shows the service is installed on
the NodeManagers (`yarn.nodemanager.aux-services=mapreduce_shuffle,spark_shuffle`) but the job turned it off:
**`spark.shuffle.service.enabled=false`** (E9). Then each executor serves its own files, and losing it loses them.
The driver's log tells the story in order (E12): YARN reports 10 containers with **exit status -102**
(PREEMPTED); for each one the DAGScheduler logs `Executor lost: …` and **`Shuffle files lost for executor: 34
(epoch 9)`**; the first FetchFailed fails stage 5 (`Marking ResultStage 5 (…) as failed due to a fetch failure from
ShuffleMapStage 4`), and **`Resubmitting ShuffleMapStage 4 (…) and ResultStage 5 (…) due to fetch failure`**; 200
ms later `Resubmitting failed stages` submits the missing map partitions. While stage 3 (retry 1) ran, YARN took
three more executors, so their fresh map output was lost too and stage 3 needed a second retry (`Resubmitting
ShuffleMapStage 3 (…) because some of its tasks had failed: …`). On the reduce side (E13), the executor that ran
E6's task logged `ERROR RetryingBlockTransferor: Exception while beginning fetch of 12 outstanding blocks` with
the ExecutorDeadException and one `Failed to get block(s) from wn098.hadoop.prod.internal:45514` per block.

**Why some nights.** Same code, same submit options in both runs (E10); the shuffle service was just as off on
2026-11-09. That night the retraining still ran at 04:00: no preemption, 40 executors all night, no failed tasks,
no retries (E11). Since 2026-11-10 it starts at 02:13, three minutes into this job. On nights its features are
late, it starts after 02:16 and the job is done before YARN needs the containers back.

**The data question.** The outputs are complete and correct. A FetchFailed means *a block couldn't be fetched*, not
that a block was bad. Spark recomputed the lost map partitions from the input (these stages are deterministic:
the same rows land in the same partitions), and the write commits each output partition once through the commit
protocol: 1,180 partitions by attempt 0, 1,220 by attempt 1, 2,400 in all, and the job succeeded.

**The channel's suggestions.** More fetch retries and a longer network timeout would not have helped: the reduce
tasks didn't time out, they asked the driver whether executor 34 was alive and got a no; `ExecutorDeadException`
is not retried, and no retry can bring back a file on a dead executor's disk. More executors would not help
either: the job would sit even further above its queue's share, and the containers YARN takes back would still
hold shuffle files nobody else can serve.

## The two runs side by side

| | Last good (dt=2026-11-08) | Incident (dt=2026-11-13) |
|---|---|---|
| App duration | 5.5 min | 7.6 min |
| Job 2 (events scan) | 3.7 min | 4.4 min (707 map tasks run twice) |
| Job 3 (join + write) | 44 s | 1.9 min (1 failed attempt, 5 extra stage attempts) |
| Executors | 40, none lost | 40, 10 preempted at 02:13–02:15, no replacement |
| `spark.shuffle.service.enabled` | false | false |
| Queue `ml` at 02:13 | idle (retraining at 04:00) | the ctr-model retraining |

## Fix

**Turn the shuffle service back on.** Remove `--conf spark.shuffle.service.enabled=false` from the Airflow task
(the cluster's `spark-defaults.conf` enables it). The NodeManagers' `spark_shuffle` service has been healthy since
the 2026-10-13 upgrade; the workaround outlived its reason. In the simulation the incident run with the same 10
preemptions then takes **5.8 min**, against 7.6 min without the service and 5.5 min on a quiet night: preempted
executors still cost their running tasks and their cores, but no finished work is lost, no stage fails, nothing
is resubmitted. When you remove a workaround like DATA-4127's, put a date or a ticket on it in the task
definition, so it gets reviewed.

**Talk to the platform team about the queues.** The job depends on capacity it isn't guaranteed. Either give `etl`
a guarantee that covers its nightly run (40 executors x 18 GiB), or move the retraining after 02:20, or protect
the queue from preemption (`yarn.scheduler.capacity.root.etl.disable_preemption=true`, at the price of slower
starts for `ml`). With the shuffle service on, preemption becomes a performance question for the queue owners,
not a correctness and retry problem for the job.

**What doesn't fix it.** `spark.shuffle.io.maxRetries` / `retryWait`, `spark.network.timeout` (the executor is
dead, not slow); more executors (more of the job above its share); `spark.task.maxFailures` (preempted tasks
don't count anyway). Spark's executor decommissioning (`spark.decommission.enabled` with shuffle block migration)
needs a decommission signal; YARN preemption sends none that Spark acts on (the AM ignores the RM's preemption
messages), so it doesn't help here. Beyond the shuffle service, a remote shuffle service (Apache Celeborn,
Uniffle) keeps shuffle data off the executors entirely, which is how clusters with heavy preemption or spot nodes
live with lost executors.

**Catch it before the analysts do.** Alert on failed stage attempts and on `Resubmitted` / `FetchFailed` counts
from the History Server REST API, not only on job failures: this job "succeeded" every night. And watch for
`was preempted.` in the driver's log: a job that is regularly preempted needs a queue decision.

## 60-second interview answer

*"A Spark job succeeds but is sometimes much slower, and the UI shows FetchFailedException and stage retries. What
is going on?"*

> FetchFailed means a reduce task couldn't get a shuffle block from the executor that wrote it. The first thing I
> read is the cause at the end of the message, and the BlockManagerId it names. Here it said the executor was dead,
> so it's not the network or corrupt data. The Executors tab showed ten dead executors, all "Container was
> preempted": YARN took them back for another queue, whose retraining had just moved into our time window. Losing
> executors is normal, but this job ran with the external shuffle service disabled, a workaround from an old
> incident, so each executor served its own shuffle files and they died with it. Spark recovered by itself: it
> resubmitted the finished map tasks, failed the reduce stage attempt, re-ran the lost map partitions and then the
> unfinished reduce partitions, so the output is complete, just two minutes late. The fix is to turn the shuffle
> service back on, so shuffle files outlive their executors, and to sort out the queue capacity with the platform
> team. More fetch retries or timeouts wouldn't help: no retry brings back a file from a dead executor.
