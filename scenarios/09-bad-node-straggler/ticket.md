# DATAENG-2291: clickstream-daily-enrichment "randomly" slow since the weekend

| | |
|---|---|
| **Priority** | P2 (the 02:30 dashboard refresh missed its data twice this week) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_47131`, started 2026-10-28 02:10 UTC, finished 02:19 UTC |
| **Last good run** | `application_1791902144377_46958`, started 2026-10-27 02:10 UTC, finished 02:15 UTC |
| **Reported by** | BI team (dashboard SLA) |
| **Owner** | data-platform on-call (you) |

> **bi-oncall (08:12):** The enrichment finished at 02:19 last night, so the 02:30 refresh of the
> marketing dashboards ran into its 10-minute wait limit and skipped the day. Same thing happened on
> Sunday. On Monday and Tuesday it was done by 02:16 as usual. We can't plan around a job that is
> sometimes twice as slow. Can you find out what makes it slow and make it stop?

**From the #data-platform channel (08:40):** "I looked at Sunday's run on Monday: nothing failed, no
retries, no spill. I re-ran it by hand at 09:00 and it was as fast as ever. Bad luck with the cluster
being busy?"

**What we know:**

* No change to `daily_enrichment.py` since 2026-09-22, and none to its `spark-submit` options since the
  Airflow rollback of 2026-10-15.
* Input volume for `dt=2026-10-27` is in line with the previous days.
* The YARN queue `etl` had headroom all night: all 40 executor containers were granted within seconds.

**Change and incident calendar (last 7 days):**

* 2026-10-24 21:47: HW-6612, `wn063.hadoop.prod.internal`: "PSU 2 failed, power supply redundancy
  lost" (BMC alert). Node stays in service, replacement PSU in the 2026-10-30 maintenance window.
* 2026-10-26 08:00–16:10: rolling kernel security patches on `/dc1/rack02` (`wn021`–`wn040`).
* 2026-10-27 13:00–13:40: firmware upgrade of the top-of-rack switches of `/dc1/rack04`
  (`wn061`–`wn080`).
* 2026-10-27 15:30: NameNode JVM upgrade on the standby `nn2`.

Both runs are in the History Server: `http://localhost:18080` (`uv run sparksim up` or
`docker compose up` starts it, and the log server behind the executors' `stdout`/`stderr` links).

Your job:

1. Find out where the extra four minutes went. Point at the stages, the tasks and the numbers, and
   compare them with the last good run.
2. Decide whether it is the data. Prove it either way.
3. Find what the slow tasks have in common, and what is behind it.
4. Explain why it happens on some nights and not on others, with the same code, data and options.
5. Propose a fix for tonight and a durable one. A colleague suggests `spark.excludeOnFailure.enabled=true`:
   would it have helped?
