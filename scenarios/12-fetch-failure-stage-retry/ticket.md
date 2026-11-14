# DATAENG-2342: clickstream-daily-enrichment slow on some nights, "FetchFailedException" in the Spark UI

| | |
|---|---|
| **Priority** | P3 (no SLA miss yet; the 02:30 dashboard refresh has a 10-minute wait limit) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_50187`, started 2026-11-14 02:10 UTC, finished 02:17:39 UTC (succeeded) |
| **Last good run** | `application_1791902144377_49279`, started 2026-11-09 02:10 UTC, finished 02:15:34 UTC |
| **Reported by** | analytics engineering |
| **Owner** | data-platform on-call (you) |

> **analytics-eng (07:55):** The enrichment succeeded again, but it took 7.6 minutes instead of the usual 5.5.
> Same on Wednesday and Friday; Thursday was normal. Nothing failed, the outputs look complete. But in the
> Spark UI the job has a **failed stage** with `org.apache.spark.shuffle.FetchFailedException`, and some stages
> show up two or three times. Is our data corrupt? Should we be worried about the outputs of those nights?

**From the #data-platform channel (08:20):**

* "FetchFailedException is a network timeout. Raise `spark.shuffle.io.maxRetries` to 10 and
  `spark.network.timeout` to 600s, that usually makes it go away."
* "Or give it more executors, 60 instead of 40, so the retries go faster?"

**What we know:**

* No change to `daily_enrichment.py` or to the Airflow task's `spark-submit` options since 2026-10-13.
* Input volume for `dt=2026-11-13` is in line with the previous days; the `events_enriched` partition was
  written completely (row counts match the source).
* The job runs in the `etl` queue. At night `etl` usually runs well above its guaranteed share of the cluster,
  since the other queues are mostly idle.

**Change and incident calendar (last 4 weeks):**

* 2026-10-13: NodeManager upgrade on all workers and RM restart (DATA-4127: this job failed at 02:10 with
  `Unable to register with external shuffle server`; the on-call worked around it, re-run OK at 02:45).
* 2026-11-02: HDFS balancer run on `/warehouse/web.db`.
* 2026-11-10: ML-2290, the ML platform's nightly `ctr-model` retraining (queue `ml`) moved from 04:00 to
  02:13 UTC, after its feature tables are ready.
* 2026-11-12: Kerberos keytab rotation for the `svc_*` accounts.

Both runs are in the History Server: `http://localhost:18080` (`uv run sparksim up` or
`docker compose up` starts it, and the log server behind the executors' and the driver's `stdout`/`stderr`
links).

Your job:

1. Find out where the two extra minutes went: which stages failed or ran more than once, and why.
2. Answer the data question: is anything corrupt, and are those nights' outputs complete?
3. Find what happened to the executors involved, and why that cost so much more than losing a few executors
   should.
4. Explain why it happens on some nights only.
5. Propose a fix. Would the channel's suggestions (more retries and timeouts, more executors) help?
