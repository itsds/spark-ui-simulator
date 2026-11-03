# DATAOPS-1180: Before your first on-call shift, learn the healthy baseline

| | |
|---|---|
| **Priority** | P4 (onboarding, no incident) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_48213`, started 2026-11-03 02:10 UTC |
| **Owner** | data-platform on-call |

You join the data-platform on-call rotation next week. The nightly clickstream
enrichment pages the rotation more often than any other job. Last night's run
(above) was **fine**. Open it in the Spark History Server and write down its
baseline, so that the next time it pages you, you can tell what changed.

What the job does: it reads one day of web events (`web.db/events/dt=…`),
joins them with a small page catalog and the customer snapshot, writes
`analytics.db/events_enriched`, and builds two daily aggregates from it
(`daily_funnel`, `customer_daily_activity`).

Write down:

1. How long the app ran, and how many jobs, stages and tasks it had. Why are
   there 8 jobs for 3 writes, and what are the skipped stages?
2. The executor layout (count, cores, hosts).
3. The biggest stage: its task count, duration, input and shuffle-write
   volume, the spread of task durations, and its data locality.
4. The join-and-write stage: how much it shuffled in and how much it wrote.
5. What AQE changed at runtime.
6. GC time, spill, and how evenly work was spread over executors.

Something to think about: in the two aggregation scans the slowest task takes
about twice as long as the median. Is that a problem?

History Server: `http://localhost:18080` (`services/history-server/run.sh`).
