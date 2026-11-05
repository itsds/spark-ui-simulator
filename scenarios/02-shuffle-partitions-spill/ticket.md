# DATAOPS-1131: disk alerts on 40 worker nodes at 02:14 UTC, traced to clickstream-daily-enrichment

| | |
|---|---|
| **Priority** | P3 (other tenants degraded; the job itself succeeded) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_44836`, started 2026-10-15 02:10 UTC, finished 02:19 UTC |
| **Last good run** | `application_1791902144377_44657`, started 2026-10-14 02:10 UTC, finished 02:15 UTC |
| **Reported by** | Hadoop operations on-call |
| **Owner** | data-platform on-call (you) |

> **hadoop-ops (02:41):** Between 02:14 and 02:19 UTC our local-disk alerts (`await` > 500 ms,
> `%util` ≥ 95 % on `/data/*`) fired on 40 NodeManagers. On every one of them the top reader and writer
> was a container of `application_1791902144377_44836` (user `svc_analytics`), under
> `/data/*/yarn/nm/usercache/svc_analytics/appcache/…/blockmgr-*`. The HBase region servers on those
> nodes logged slow WAL syncs and two ad-hoc Hive queries timed out. The same job ran at the same time
> the night before without a single alert. Please find out what it did differently and make sure it
> doesn't happen again tonight.

**From the #data-platform channel (08:05):** "fwiw the enrichment also finished late-ish, 02:19 instead
of the usual ~02:16. Input volume for dt=2026-10-14 looks normal."

**Changes in the last 48 hours** (change calendar):

* 2026-10-14 09:00–17:40: HDFS balancer on rack `/r03` (finished hours before the job started).
* 2026-10-14 14:20: DATAENG-2207 merged. `clickstream-daily-enrichment` moved from the team's cron +
  `spark-submit` wrapper to the platform's Airflow DAG factory. Review comment: "no functional change".
* No change to `daily_enrichment.py` since 2026-09-22.

Both runs are in the History Server: `http://localhost:18080` (`uv run sparksim up` or
`docker compose up` starts it, and the log server behind the executors' `stdout`/`stderr` links).

Your job:

1. Find out what the job did to the local disks between 02:14 and 02:19. Point at the stage, the
   tasks and the numbers that prove it, and compare them with the last good run.
2. Explain why it happened on this run and not the night before, when the data and the code were
   the same.
3. Spark 3.5 runs with adaptive query execution on. Explain why it didn't prevent this.
4. Propose the fix for tonight and a durable one, and say how you would catch this before another
   team does.
