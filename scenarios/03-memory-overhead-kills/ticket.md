# DATAENG-2307: clickstream-daily-enrichment failed last night, both YARN attempts

| | |
|---|---|
| **Priority** | P1 (`events_enriched`, `daily_funnel` and `customer_daily_activity` have nothing for `dt=2026-11-11`) |
| **Job** | `clickstream-daily-enrichment` (`daily_enrichment.py`), PySpark 3.5.9 on YARN, cluster mode, queue `etl` |
| **Run** | `application_1791902144377_49822`, 2026-11-12: attempt 1 02:10–02:14 UTC, attempt 2 02:14–02:18 UTC, both FAILED |
| **Last good run** | `application_1791902144377_49641`, 2026-11-11 02:10–02:15 UTC |
| **Reported by** | Airflow (task failure alert), then analytics engineering |
| **Owner** | data-platform on-call (you) |

> **Airflow (02:18):** Task `clickstream_daily.enrich` failed. `SparkSubmitOperator`: `Application
> application_1791902144377_49822 finished with failed status`. YARN diagnostics: `User application exited
> with status 1`. Retries: 0 (the DAG leaves retries to YARN's own application attempts).

> **analytics-eng (08:05):** Nothing landed for yesterday. We shipped release 2.4 yesterday afternoon,
> the `geo_city` column marketing asked for (PR #412). We ran it locally on a 1 % sample of a day before
> merging and it worked. Is the cluster OK?

**From the #data-platform channel (08:20):** "Exit status 1 is the Python driver dying, so it's a bug in
2.4's code. Or the executors ran out of memory: I'd go to `--executor-memory 24g` and rerun."

**What we know:**

* Release 2.4 (deployed 2026-11-11 15:20 UTC) adds `geo_city` with `geoip_city(ip_address)`, a UDF from the
  data platform's shared UDF jar (`dp-udfs`), and ships the jar and the IP database it reads with
  `--jars` / `--files`. Nothing else in `spark-submit` changed: 40 executors x 4 cores x 16g, the same
  `--conf`s as for months.
* Input volume for `dt=2026-11-11` is in line with the previous days.
* YARN shows two application attempts, both FAILED. The queue `etl` had headroom all night.

**Change and incident calendar (last 48 hours):**

* 2026-11-11 11:00–11:30: rolling NodeManager restarts on `/dc1/rack03` (`wn041`–`wn060`): log aggregation
  retention raised from 7 to 14 days.
* 2026-11-11 15:20: release 2.4 of `clickstream-daily-enrichment` (PR #412, `geo_city`).
* 2026-11-11 21:00–23:40: HDFS balancer run (threshold 10 %).
* 2026-11-12 01:30: `customers_snapshot` compaction job moved from 01:00 to 01:30 (finished 01:52).

The last good run and both attempts of the failed one are in the History Server: `http://localhost:18080`
(`uv run sparksim up` or `docker compose up` starts it, and the log server behind the `stdout`/`stderr`
links). The failed application lists its two attempts separately.

Your job:

1. Find what failed: which job and stage, with which error, and how each YARN attempt ended. Use both
   attempts.
2. Decide whether the executors ran out of JVM heap. Prove it either way, with numbers.
3. Find what is using the memory, and why only since last night.
4. Explain why Spark gave up instead of re-running the lost tasks until they succeeded, and why YARN's
   second attempt didn't help.
5. Propose what to change for tonight's run and what to change for good. Would `--executor-memory 24g`
   have saved the night?
