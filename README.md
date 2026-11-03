# spark-ui-simulator

**Practice debugging production Spark jobs in the real Spark UI, without a production cluster.**

Reading about data skew, spill or executor OOMs is easy. Recognizing them in the
Spark UI under pressure is a different skill, and small local jobs can't produce
production-scale symptoms: 1000× skew, terabytes of spill, YARN killing containers.

This project creates **synthetic event logs for production-scale Spark
applications** (tens of executors, tens of thousands of tasks, TB of shuffle),
each with a realistic problem planted in it. It loads them into the **real
Apache Spark History Server**. You get an incident ticket, open the Spark UI,
and investigate the way you would at work: Jobs → Stages → Stage detail →
Executors → SQL → Environment. Then you check your diagnosis against a
written solution.

## How it works

```
scenario.yaml  →  planner  →  cost model  →  scheduler simulation  →  Spark event log  →  real Spark History Server
(workload +       (physical    (bytes, rows,   (executors, cores,       (Spark 3.5           (the UI you debug in)
 injected cause)   plan,        CPU, spill,     task timing, failures,   JsonProtocol)
                   stages)      GC)             YARN containers)
```

- **The real UI.** Nothing is mocked: every page, DAG, timeline, sort and REST
  endpoint is Spark's own.
- **Numbers that make sense.** Every metric comes from a simulation of the
  workload on a YARN cluster. Nothing is hand-typed, and totals agree across
  every page.
- **Causes, not symptoms.** Skew is a hot key in the data; spill is partitions
  outgrowing execution memory. The UI symptoms follow from those causes, as in
  production.
- **Last good run included.** Each scenario also ships the healthy previous run,
  so you can compare "yesterday vs today".

Target: **Apache Spark 3.5.9 on YARN** (cluster mode, Hadoop 3.x).

## Scenarios (planned)

| # | Scenario |
|---|---|
| 00 | Healthy baseline (learn what "normal" looks like) |
| 01 | Join-key skew |
| 02 | Too few shuffle partitions → spill |
| 03 | YARN memoryOverhead container kills |
| 04 | Small files / tiny tasks |
| 05 | Missed broadcast join |
| 06 | Partition pruning failure |
| 07 | Exploding join (duplicate keys) |
| 08 | GC pressure |
| 09 | Bad-node straggler |
| 10 | Python UDF bottleneck |
| 11 | Driver bottleneck |
| 12 | Fetch failure / stage retry |
| 13 | Missing cache / recomputation |
| 14 | YARN queue starvation |
| 15 | Skewed / small-file output |
| ★ | Multi-cause capstones |

Each scenario has an incident ticket, progressive hints and a solution with the
click path, the evidence numbers, the fix and a 60-second interview answer.
Details: [`docs/SCENARIOS.md`](docs/SCENARIOS.md).

## Status

🚧 Early development. See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the current phase.

### Try it now: browse the real reference apps

Phase 1 captured nine **real** Spark 3.5.9 applications on a real YARN cluster
(cluster mode). Each one exercises a UI feature: AQE, skew, spill, cache,
failures and stage retries, Python UDFs, driver-bound jobs, RDD API. You can
open them in the real History Server.

Prerequisites: [uv](https://docs.astral.sh/uv/), **JDK 17** (Spark 3.5's
bundled Arrow does not work on JDK 21).

```bash
uv sync
services/history-server/run.sh reference/event-logs
# open http://localhost:18080
```

Or with Docker (no local Java needed):

```bash
EVENT_LOG_DIR=./reference/event-logs docker compose up
```

### Re-capture the reference logs (optional, Linux)

```bash
reference/yarn/cluster.sh start      # single-node HDFS + YARN (needs Hadoop 3.3.x in /opt/hadoop)
reference/capture.sh                 # runs reference/jobs/*.py with --master yarn --deploy-mode cluster
uv run scripts/event_inventory.py    # regenerates docs/event-log-schema.md
```

Planned one-command quickstart (Phase 7):

```bash
docker compose up
# Scenario portal:        http://localhost:8000
# Spark History Server:   http://localhost:18080
```

## Docs

- [Architecture](docs/ARCHITECTURE.md): pipeline and realism rules
- [Roadmap](docs/ROADMAP.md): phases and status
- [Decisions](docs/DECISIONS.md): why it's built this way
- [Scenarios](docs/SCENARIOS.md): backlog and design notes

## Disclaimer

Not affiliated with or endorsed by the Apache Software Foundation. Apache Spark,
Spark and the Spark logo are trademarks of the ASF. This project generates data
for the unmodified Apache Spark History Server and doesn't redistribute Spark UI code.
