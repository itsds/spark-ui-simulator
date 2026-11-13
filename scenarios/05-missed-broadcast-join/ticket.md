# DATAENG-2054: clickstream-daily-enrichment four times slower overnight, "nothing changed"

| | |
|---|---|
| **Priority** | P2 (the 02:20 dashboard refresh gave up waiting and published the previous day's data) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1787640083508_40919`, started 2026-09-23 02:10 UTC, finished 02:32 UTC |
| **Last good run** | `application_1787640083508_40741`, started 2026-09-22 02:10 UTC, finished 02:15 UTC |
| **Reported by** | BI team (dashboard SLA) |
| **Owner** | data-platform on-call (you) |

> **bi-oncall (07:55):** The enrichment finished at 02:32 last night. Our 02:20 refresh waits up to ten
> minutes for `events_enriched` and then gives up, so the marketing dashboards show Monday's numbers. It's
> normally done by 02:16. Will it happen again tonight?

**From the #data-platform channel (08:20):** "Had a quick look: nothing failed, nothing was retried. One task
in the middle of the run took a quarter of an hour, so it's skew. My guess is the page catalog: CMS moved
it to their new export on Monday, maybe it has duplicate page ids now? Or we just turn on AQE skew join
and move on."

**What we know:**

* No commit to `daily_enrichment.py` since 2026-09-10, and the cron wrapper's `spark-submit` options haven't
  changed since August.
* The input volume for `dt=2026-09-22` is in line with the previous days.
* Nothing failed: no task failures or retries, no lost executors, all 40 executors granted within seconds.

**Change and incident calendar (2026-09-21 to 2026-09-22):**

* 2026-09-21 10:00: CMS-418: `web.db/page_catalog` is now written by the new CMS export job (same path, same
  schema, still one Parquet file).
* 2026-09-22 09:30–11:10: rolling restart of the DataNodes on `/dc1/rack01`–`/dc1/rack03` (log4j
  configuration change).
* 2026-09-22 14:00: PLAT-1184: "Spark client defaults 2026.09" rolled out to the gateway hosts and the
  Airflow workers (driver memory-safety settings after INC-3317 and INC-3342). "No action needed for most
  jobs."
* 2026-09-22 16:30: YARN queue `etl`: maximum capacity 40 % → 45 %.

Both runs are in the History Server: `http://localhost:18080` (`uv run sparksim up` or
`docker compose up` starts it, and the log server behind the executors' `stdout`/`stderr` links).

Your job:

1. Find out where the extra 17 minutes went. Point at the query, the stages, the tasks and the numbers,
   and compare them with the last good run.
2. Decide whether the data changed: the events, and the page catalog after its migration. Prove it either
   way.
3. Compare the physical plans of the slow query in both runs. What changed, and what made Spark change it?
4. Explain why one task ran for 17 minutes in a stage whose median task takes 4 seconds, when no task of
   the last good run took longer than 11 seconds.
5. Propose a fix for tonight and a durable one. Would turning on AQE skew join
   (`spark.sql.adaptive.skewJoin.enabled=true`) have been the right fix?
