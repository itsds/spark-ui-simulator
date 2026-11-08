# Solution: one degraded node, and no speculation to work around it

**Root cause.** One NodeManager, `wn063.hadoop.prod.internal`, has been running at a small fraction of its
CPU speed since one of its two power supplies failed on Saturday (HW-6612): with a single supply left, the
server's BMC caps its power, and its CPUs crawl. The node stayed in service because "redundancy lost" looked
harmless. Last night YARN placed one of the job's 40 executors, **executor 19**, on it. Every task executor 19
ran took **13–24x** as long as the same kind of task elsewhere, with normal input sizes, normal GC share and no
spill. Spark's scheduler hands tasks to whichever core is free, so the slow executor simply finished fewer of
them (28 against 225–259). But in each stage its last few tasks started near the end and kept the stage open
for minutes after everything else was done: the scan, the join, both aggregations. **Speculative execution**
is what Spark has for exactly this, and it is off by default (`spark.speculation` is not set in the job).
On other nights YARN put the 40 containers on other hosts, and the job ran its usual 5.5 minutes. That is the
"randomness".

Every number below is what the History Server shows for the two runs, and `tests/test_scenario09_hs.py`
checks each one against its REST API (and executor 19's container log through its log link).
`uv run scripts/evidence.py 09-bad-node-straggler` prints them for a running server.

## Evidence

| ID | Where in the UI | What | Value |
|---|---|---|---|
| E1 | History Server, application list | Duration, last good → incident | `5.5 min → 9.3 min` |
| E2 | Stages tab, the big stages, last good → incident | Duration | `stage 2: 3.7 min → 5.1 min, stage 5: 43 s → 1.2 min, stage 6: 11 s → 51 s, stage 9: 16 s → 1.2 min` |
| E3 | Incident, stage 2 (events scan), Summary Metrics, Duration | Median / max | `median 7 s, max 2.6 min (REST: 7.0 s / 157.6 s, 22x the median)` |
| E4 | Stage 2, Summary Metrics, Input Size / Records, and the task list | Input of the median, the largest and the longest task | `median 110.4 MiB, max 141.2 MiB; the longest task (index 62) read 116.5 MiB` |
| E5 | Stage 2, task list sorted by Duration | Where the longest tasks ran | `the 5 longest tasks all ran on executor 19 (wn063.hadoop.prod.internal), 95.9 s to 157.6 s; the next one: 10.5 s (executor 38)` |
| E6 | Stage 2, Aggregated Metrics by Executor | Succeeded tasks and task time per executor | `executor 19: 5 tasks, 11 min; the other 39: 122–144 tasks, 14 min–15 min` |
| E7 | Every stage executor 19 ran tasks in, task list | Its tasks (all the longest of the stage) and their median against everyone else's | `stage 2: 5, median 143.0 s vs 7.0 s; stage 5: 7, median 36.7 s vs 2.8 s; stage 6: 4, median 50.0 s vs 2.1 s; stage 8: 4, median 8.0 s vs 0.4 s; stage 9: 4, median 65.2 s vs 3.8 s; stage 11: 4, median 11.3 s vs 0.7 s` |
| E8 | Executors tab | Complete Tasks, Task Time, Input | `executor 19: 28 tasks, 24 min task time, 1033.9 MiB input; the other 39: 225–259 tasks, 19 min–20 min` |
| E9 | Executors tab, Add Time (or the Jobs tab's Event Timeline) | When the executors registered | `executor 19 at 02:11:41 UTC, the other 39 between 02:10:11 and 02:10:14 (88 s later)` |
| E10 | Executors tab, Task Time (GC Time) | GC share of task time | `executor 19: 2.5 %; the others 1.8 % – 2.1 %` |
| E11 | Executors tab, hosts `wn061`–`wn080` (the ticket's rack04) | The other executors in that rack | `8 executors on wn061–wn080 besides executor 19: 234–256 tasks each` |
| E12 | Last good run, Executors tab | Executor layout | `40 executors on 40 hosts, none on wn063` |
| E13 | Environment tab, Spark Properties, both runs | What differs, and speculation | `no property differs; spark.speculation: not set` |
| E14 | `stderr` of executor 19 against executor 1's (Executors tab, Logs) | JVM start-up and code generation | `executor 19: 53 s from 'Started daemon' to 'Successfully registered with driver', slowest 'Code generated in' 3,232 ms; executor 1: 1 s, 195 ms` |

## Click path and what each number means

**Application list and Stages.** 5.5 min the night before, **9.3 min** last night (E1). Line the Stages tabs up:
it is not one stage. The events scan went from 3.7 to 5.1 min, the join and write from 43 s to 1.2 min, and the
two aggregations' map stages from 11 s to 51 s and from 16 s to 1.2 min (E2). Every big stage lost between 29 s and
1.4 min, and the small ones grew too. Something slowed the whole job down, not one piece of it.

**Stage 2: a straggler, but not skew.** In the incident's events scan the median task takes **7 s** and the longest
one **2.6 min**, 22x the median (E3). That looks like Scenario 01 at first. Then read the next row: Input Size has a
median of 110.4 MiB and a max of 141.2 MiB, and the longest task read **116.5 MiB**, a perfectly normal split (E4).
In a skewed stage the longest task is the one with the most data. Here the slow tasks had ordinary inputs and still
took 14–22x as long.

**Where they ran.** Sort the task list by Duration: the **five longest tasks all ran on executor 19, on
`wn063.hadoop.prod.internal`**, from 95.9 s to 157.6 s; the first task from anywhere else took 10.5 s (E5). Open
*Aggregated Metrics by Executor* and sort by Succeeded Tasks: executor 19 finished **5 tasks** of stage 2 while each
of the other 39 finished 122–144, yet its Task Time (11 min) is not far below theirs (14–15 min) (E6). It was
busy the whole stage, doing a twentieth of the work. That is the signature of a slow executor in a dynamically
scheduled stage: Spark gives the next task to whichever core is free, so a slow executor takes fewer tasks instead
of slowing every task down. The damage is at the end of the stage: its last tasks started a minute or two before
the others ran out of work and held the stage open until they finished.

**Every stage, the same executor.** Do the same in each stage executor 19 ran tasks in: its tasks are always the
longest ones, and their median is 13–24x the others' (E7). In the join stage its 7 tasks took a median 36.7 s
against 2.8 s, in stage 9 65.2 s against 3.8 s. The data differs from stage to stage, the
operators differ, the executor doesn't. On the **Executors** tab, executor 19 completed **28 tasks** in the whole
run, the others 225–259, while its Task Time (24 min) is like everyone else's (19–20 min) (E8).

**Ruling out the usual suspects.**

* *Data:* normal input per task (E4), and the job's input is like the night before.
* *GC / memory:* executor 19's GC is 2.5 % of its task time, like the others' 1.8–2.1 % (E10). Nothing spilled,
  nothing failed, no executor was lost. GC time per task is high on executor 19, but in proportion: everything
  on it is slow, GC included.
* *Network / the rack04 switch work:* the 8 other executors on `wn061`–`wn080` ran 234–256 tasks each, like the rest
  of the cluster (E11). A rack problem would hit all of them.
* *The cluster being busy:* the other 39 executors kept a normal pace all night (they also absorbed most of executor
  19's share of the work).

What is left is the machine. Two more clues say it is its CPU. Executor 19 registered **88 s after** the others:
YARN granted its container as fast as the rest, but its JVM took that long to start (E9; the Jobs tab's Event
Timeline shows the late "Executor 19 added"). In its `stderr` it took **53 s from `Started daemon` to
`Successfully registered with driver`**, executor 1 took 1 s, and its `CodeGenerator` lines take up to 3.2 s
where a healthy executor needs at most ~200 ms (E14). JVM start-up and Janino compilation are pure CPU work. The
change calendar has an entry for exactly that host: **HW-6612, `wn063`: PSU 2 failed, power supply redundancy
lost**. With one power supply left, the server's BMC caps the power draw, and the CPUs run at their lowest
frequency with clock modulation: a node that is "up" but delivers ~1/20 of its usual compute.

(One side effect: stage 0 took 12 s instead of 5 s although executor 19 ran none of its tasks. It ran while
executor 19 was still starting, on 39 executors: the scan's first wave took the cores that stage 0's last rack-local
tasks needed, and they waited ~7 s for them.)

**Why only some nights.** The last good run's executors sat on 40 other hosts, **none on `wn063`** (E12). The node was
just as broken that night, but the job never used it. The ApplicationMaster asks YARN for 40 containers without a
locality preference, and the CapacityScheduler hands them out wherever free memory turns up at the next NodeManager
heartbeats. Which 40 of the 120 nodes the job gets changes every night with the other tenants' load. With one bad
node out of 120 and 40 executors, roughly one night in three draws it. Sunday and last night did; Monday and Tuesday
didn't; the 09:00 re-run didn't. Nothing about the job changed (E13).

**Why Spark didn't route around it.** Speculative execution exists for this case: once 75 % of a stage's tasks
have finished (`spark.speculation.quantile`), the driver checks every 100 ms for tasks that have run longer than
1.5x the median duration (`spark.speculation.multiplier`), launches a copy of each on another host, keeps
whichever attempt finishes first and kills the other ("another attempt succeeded"). It is **off by default**, and
the job doesn't set it (E13). `spark.excludeOnFailure.enabled` would not have helped: it excludes executors and
nodes after *task failures*, and nothing on `wn063` failed. Its tasks were slow and correct.

## The two runs side by side

| | Last good (dt=2026-10-26) | Incident (dt=2026-10-27) |
|---|---|---|
| App duration | 5.5 min | 9.3 min |
| Stage 2 (events scan) | 3.7 min | 5.1 min |
| Stage 5 (join + write) | 43 s | 1.2 min |
| Stage 2 median / max task | — | 7 s / 2.6 min (5 longest: executor 19) |
| Executor on `wn063` | none | executor 19: 28 tasks (others 225–259) |
| Executor registration | all within ~3 s | executor 19 88 s after the others |
| GC, spill, failures | normal, none, none | normal, none, none |
| `spark.speculation` | not set (false) | not set (false) |

## Fix

**Tonight.** Take the node out of the job's way:

```
--conf spark.yarn.exclude.nodes=wn063.hadoop.prod.internal
```

The AM sends it to the ResourceManager as the application's excluded node list when the allocator starts
(`YarnAllocatorNodeHealthTracker: adding nodes to YARN application master's excluded node list: List(...)` in
the driver log), so YARN never places an executor there. In the simulation the incident run with this setting
takes 5.5 min, like the last good run. Better still, and for every tenant: ask Hadoop ops to drain the node now
(YARN graceful decommission, or mark it unhealthy) instead of waiting for Friday's PSU swap.

**Durable.** Turn speculation on for this job, which writes through Spark's commit protocol and is safe to run
twice:

```
--conf spark.speculation=true
```

With Spark's defaults (quantile 0.75, multiplier 1.5, interval 100 ms) the incident run takes **~5.7 min** in the
simulation, about 10 s more than a healthy night: the driver marks executor 19's long tasks speculatable, runs
21 copies on healthy hosts, 18 of them win, and the stage no longer waits for `wn063`. The price is the work the
killed attempts did (about 8.5 min of task time, on top of the job's ~13 h) and a stage tail that still waits for the 75 %
mark. Stage pages then show the copies in the task list (`Speculative`), the killed originals
(`TaskKilled (another attempt succeeded)`, `Stage finished` for a result stage), and a *Speculation Summary*.

Keep in mind what speculation can't do: a copy of a task that is slow because of its *data* (Scenario 01's skewed
partition) is just as slow on another host, and only wastes a core.

**Catch it before the BI team does.**

* Hardware alerts that degrade performance (PSU / power capping, thermal throttling, a disk in a degraded RAID)
  should drain the node, not wait for a maintenance window. A NodeManager health-check script
  (`yarn.nodemanager.health-checker.script.path`) that checks CPU frequency and throttling counters marks such a
  node UNHEALTHY automatically, and YARN stops placing containers on it.
* Per job: alert on duration against the last good runs, and on per-host outliers, e.g. from the History Server
  REST API, an executor whose median task time is many times the stage's.
* "Random" slowness of an unchanged job almost always means something outside the job: placement, neighbours,
  hardware. Compare the executor hosts of a slow and a fast night first.

## 60-second interview answer

*"A daily Spark job is randomly twice as slow on some nights. Same code, same data. How do you debug it?"*

> I compare a slow run with a normal one in the History Server. Every big stage got longer, so I open the biggest
> one and look at the summary metrics: max duration is 22x the median, but the longest task's input is a normal
> split, so it's not skew. I sort the tasks by duration: the five slowest all ran on the same executor, on one host,
> and that executor is the slowest in every stage, finishing 28 tasks against 230-odd for every other executor.
> GC, spill and failures are all normal, its rack neighbours are fine, and its JVM took almost a minute to start,
> so the machine itself is slow. That matched a hardware alert, a failed power supply with the CPUs power-capped.
> It's random because YARN places the executors differently every night, and only the nights that land on that
> node are slow. Short term, exclude the node with `spark.yarn.exclude.nodes` and get it drained. Longer term,
> turn on speculation, so a copy of a straggling task runs on a healthy node, and have the NodeManager health check
> take throttled nodes out automatically. `excludeOnFailure` wouldn't help: nothing failed, it was just slow.
