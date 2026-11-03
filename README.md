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

## Scenarios

| # | Scenario |
|---|---|
| 00 | Healthy baseline (learn what "normal" looks like) ✅ [`scenarios/00-healthy-baseline`](scenarios/00-healthy-baseline/ticket.md) |
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

### Run it on your machine

The "web app" is the **real Apache Spark History Server 3.5.9**, started from
the Spark distribution that comes inside the project's PySpark install. You
don't need a Spark or Hadoop install.

**Prerequisites:** macOS or Linux (on Windows, use WSL2 Ubuntu), `git`,
[uv](https://docs.astral.sh/uv/getting-started/installation/), and **JDK 17**.
Don't use JDK 21: Spark 3.5's bundled Arrow breaks on it. `scripts/env.sh` finds
JDK 17 in the usual places.

| OS | Install JDK 17 |
|---|---|
| macOS (Homebrew) | `brew install openjdk@17` (or Temurin 17: `brew install --cask temurin@17`) |
| Ubuntu / Debian / WSL2 | `sudo apt-get install -y openjdk-17-jdk-headless` |

If JDK 17 is somewhere else, set `JAVA_HOME` to it before running the commands
below. On macOS with Homebrew's `openjdk@17`, that's
`export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home`.

```bash
git clone https://github.com/itsds/spark-ui-simulator.git && cd spark-ui-simulator
uv sync                                              # Python 3.11 + PySpark 3.5.9 (contains the History Server)
uv run sparksim generate 00-healthy-baseline         # Scenario 00: event log + YARN container logs, ~10 s
services/nm-logs/run.sh &                            # mock NodeManager log server on :8042 (executor stdout/stderr)
services/history-server/run.sh event-logs            # foreground; Ctrl+C to stop
# open http://localhost:18080, then read scenarios/00-healthy-baseline/ticket.md
```

`uv run sparksim generate tiny` writes the 8-task Phase 2 smoke app instead.

To browse the nine **real** reference apps captured on YARN in Phase 1 (AQE,
skew, spill, cache, failures and stage retries, Python UDFs, driver-bound jobs,
RDD API), point the server at that directory, optionally on another port:

```bash
services/history-server/run.sh reference/event-logs 18081   # http://localhost:18081
```

The server rescans its directory every 5 s, so newly generated logs appear
without a restart. The Executors tab's `stdout`/`stderr` links (and the driver's,
which is the YARN ApplicationMaster container in cluster mode) open the generated
container logs through the mock NodeManager log server
(`spark.history.custom.executor.log.url` in `services/history-server/spark-defaults.conf`).
The logs match the run: every task an executor ran, the files it read, the shuffle
blocks it fetched, the broadcasts it pulled, and how its container ended. The
reference apps' links point at the single-node capture cluster and don't open.

Or with Docker (no local Java needed; not yet verified):

```bash
EVENT_LOG_DIR=./event-logs docker compose up
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
