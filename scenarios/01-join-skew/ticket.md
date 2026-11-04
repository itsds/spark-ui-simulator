# DATAOPS-1194: clickstream-daily-enrichment ran 1h35m, the 03:00 dashboards showed stale data

| | |
|---|---|
| **Priority** | P2 (SLA breach, data late) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_48391`, started 2026-11-04 02:10 UTC, finished 03:44 UTC |
| **Last good run** | `application_1791902144377_48213`, 2026-11-03 02:10 UTC, 5.5 min (Scenario 00's run) |
| **Reported by** | Growth analytics (dashboard refresh at 03:00 UTC read the previous day) |
| **Owner** | data-platform on-call (you) |

> The nightly enrichment usually finishes around 02:16. Last night it was still running when the 03:00
> dashboard refresh kicked off, so Growth's "yesterday" numbers were two days old until someone
> re-ran the refresh at 08:30. The run did finish successfully at 03:44 and nothing was retried.
> Nobody touched the job: the last change to `daily_enrichment.py` or its spark-submit options was
> three weeks ago, and the cluster had no maintenance last night.

**From the #data-platform channel (03:46):** "fyi the NodeManager on `wn007.hadoop.prod.internal`
had its YARN local dirs grow by ~125 GB between 02:16 and 03:36, freed again at 03:44. Bad node?"

Both runs are in the History Server: `http://localhost:18080` (the executor `stdout`/`stderr` links
need `services/nm-logs/run.sh`).

Your job:

1. Find out why this run took 1h35m instead of ~6 minutes. Point at the stage, the task(s) and the
   numbers that prove it, and compare them with the last good run.
2. Decide whether `wn007` is the problem.
3. Say what changed, given that the code and config did not.
4. Propose an immediate fix for tonight's run and a durable one, and say how you would stop this from
   reaching the dashboards again.
