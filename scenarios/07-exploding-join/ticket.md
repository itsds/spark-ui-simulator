# DATAENG-2241: events_enriched 6x too big, the enrichment 2.7x slower, funnel numbers far too high

| | |
|---|---|
| **Priority** | P1 (the BI dashboards show wrong revenue; the analytics space quota is nearly full) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_48925`, started 2026-11-07 02:10 UTC, finished 02:25:35 UTC (succeeded) |
| **Last good run** | `application_1791902144377_48748`, started 2026-11-06 02:10 UTC, finished 02:15:45 UTC |
| **Reported by** | HDFS operations, then BI |
| **Owner** | data-platform on-call (you) |

> **hdfs-ops (02:31):** Space quota alert on `/warehouse/analytics.db`: 91 % of its quota at 02:24. The new
> partition `events_enriched/dt=2026-11-06` is **809 GiB** (2.4 TiB with replication); the partitions before it
> are 130–132 GiB each. Also: the YARN local-dirs disk alert fired between 02:14 and 02:23 on 37 of the 40 nodes
> the job's executors ran on. Nothing else is writing there.

> **bi-analytics (08:12):** Friday on the funnel dashboard: **6.2x** the usual page views and **€ 43.1 M**
> revenue. Finance booked € 7.0 M for Friday, in line with the rest of the week. The customer activity report
> has the same problem: the number of active customers looks normal (61 M), but events and spend per customer
> are six times higher than on any other day.

**From the #data-platform channel (08:30):**

* "Black Friday early access started Friday morning (marketing's email went out at 06:00). More traffic, more
  rows, more time. Wait until the weekend is over."
* "Stage 5 spilled 1.6 TB to disk, that's what triggered the disk alerts. Give the executors more memory, or raise
  `spark.sql.shuffle.partitions` so each task sorts less."
* "The CRM people said in their channel that `customers_snapshot` got a lot bigger this week. Bigger table,
  slower join: broadcast it, or salt the join key."
* "Could a Parquet setting have changed? 809 GiB smells like the compression went away."

**What we know:**

* Nobody touched the job: the last deploy was on 2026-10-27, the Airflow task's `spark-submit` options are the
  same, same cluster, executors and queue. The run didn't fail or retry.
* The job reads three tables written by other teams: one day of `web.db/events` (the ingestion team), the
  `crm.db/customers_snapshot` written by the CRM export every night at 23:30, and `web.db/page_catalog`.
* `events_enriched` should have one row per event of the day that isn't a bot and has a known page and customer;
  `daily_funnel` and `customer_daily_activity` aggregate it.
* The job's output for dt=2026-11-06 is still in place (Airflow's downstream tasks ran on it at 02:30). Only the
  Thursday and Friday runs are in the History Server.

Both runs are in the History Server: `http://localhost:18080` (`uv run sparksim up` or
`docker compose up` starts it, and the log server behind the executors' and the driver's `stdout`/`stderr`
links).

Your job:

1. Find the operator that produces the extra rows, and show where the UI says so.
2. Explain why it does: what about its input changed, and what in the UI proves it without querying the data.
3. Answer the channel's suggestions: traffic, memory or more partitions, broadcast or salting, compression.
4. Propose the fix, say which data is wrong and what has to be re-run, and how this would be caught next time.
