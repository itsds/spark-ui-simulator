# DATAENG-2187: clickstream-daily-enrichment 2.6x slower since Wednesday, heavy HDFS reads on web.db/events

| | |
|---|---|
| **Priority** | P2 (the 02:30 dashboard refresh has a 10-minute wait limit; one more slow night and it fails) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_46071`, started 2026-10-22 02:10 UTC, finished 02:24:44 UTC (succeeded) |
| **Last good run** | `application_1791902144377_45702`, started 2026-10-20 02:10 UTC, finished 02:15:39 UTC |
| **Reported by** | HDFS operations, then analytics engineering |
| **Owner** | data-platform on-call (you) |

> **hdfs-ops (07:40):** Since Wednesday's 02:10 run `svc_analytics` reads about **4 TB per night** from
> `/warehouse/web.db/events` between 02:10 and 02:23, roughly 7x what it read on the nights before. NameNode
> RPC load and DataNode disk utilization on the `etl` racks peak at the same time. The table didn't change
> size. Please check what that job is doing.

> **analytics-eng (08:05):** Same job: `events_enriched` lands at 02:25 instead of 02:16, two nights in a row.
> The funnel dashboard refresh starts at 02:30 and waits 10 minutes at most. The output looks fine, row counts
> are within 1-2 % of last week.

**From the #data-platform channel (08:20):**

* "The only code change is DATA-4398, and it's tiny. The review checked that the filter on `dt` is still there,
  and the Spark plan says `PushedFilters` with the date range, so the filter *is* pushed down. Something else is
  slow."
* "38,000 tasks in one stage is a lot. Set `spark.sql.files.maxPartitionBytes` to `1g` and you'll have a quarter
  of the tasks."
* "Is dynamic partition pruning on? It should prune the table for us."
* "Rule of thumb: never cast a partition column, it kills pruning."

**What we know:**

* DATA-4398 "events_enriched by event time" was merged on 2026-10-20 at 16:05. Mobile SDKs upload events they
  buffered while offline, so they land in the events table (partitioned by `dt`, the day an event landed) a day
  or two after they happened. Analytics wants `events_enriched` for a day to hold the events that *happened* that
  day. The change touched the few lines that read the day's events; nothing else in the script, no
  `spark-submit` options.
* The first run with it was 2026-10-21 02:10 (dt=2026-10-20); it was just as late. Only the Tuesday and
  Thursday runs are in the History Server.
* `web.db/events` keeps 7 days (a retention job drops older partitions at 01:30). The ingestion lands hourly
  batches, so at 02:10 the next day's partition already holds the first two hours.
* Same cluster, executors and queue as always; nothing else changed in the `etl` queue's schedule.

Both runs are in the History Server: `http://localhost:18080` (`uv run sparksim up` or
`docker compose up` starts it, and the log server behind the executors' and the driver's `stdout`/`stderr`
links).

Your job:

1. Find out what the job reads now that it didn't read before, and where in the UI that shows.
2. Explain why: the filter on `dt` is still in the code, and the plan shows pushed filters.
3. Answer the channel's suggestions: more or bigger tasks, dynamic partition pruning, the rule about casts.
4. Propose the change DATA-4398 should have been, and say what it means for the late events analytics asked about.
