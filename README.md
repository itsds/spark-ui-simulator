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
| 01 | Join-key skew ✅ [`scenarios/01-join-skew`](scenarios/01-join-skew/ticket.md) |
| 02 | Too few shuffle partitions → spill ✅ [`scenarios/02-shuffle-partitions-spill`](scenarios/02-shuffle-partitions-spill/ticket.md) |
| 03 | YARN memoryOverhead container kills |
| 04 | Small files / tiny tasks |
| 05 | Missed broadcast join ✅ [`scenarios/05-missed-broadcast-join`](scenarios/05-missed-broadcast-join/ticket.md) |
| 06 | Partition pruning failure |
| 07 | Exploding join (duplicate keys) |
| 08 | GC pressure |
| 09 | Bad-node straggler ✅ [`scenarios/09-bad-node-straggler`](scenarios/09-bad-node-straggler/ticket.md) |
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

## Quickstart

One command after cloning, with Docker or without it. Either way you get three local services:

| What | URL |
|---|---|
| **Scenario portal**: scenario cards, the incident ticket, hints one at a time, your notes, the solution, and links into the History Server for every run | http://localhost:8000 |
| **Spark History Server 3.5.9**, the real one: the UI you debug in | http://localhost:18080 |
| Container logs (a mock YARN NodeManager), behind the Executors tab's `stdout`/`stderr` links | http://localhost:8042 |

The first start generates every scenario's event logs and container logs (about a minute); later starts
reuse them and only regenerate what changed (a manifest of fingerprints, `event-logs/.sparksim-manifest.json`).

### With Docker (nothing else to install)

```bash
git clone https://github.com/itsds/spark-ui-simulator.git && cd spark-ui-simulator
docker compose up
```

Then open http://localhost:8000. `Ctrl+C` (or `docker compose down`) stops it; the generated logs stay in
Docker volumes until `docker compose down -v`. Every `up` rebuilds the simulator image from cache (seconds), so
a `git pull` takes effect. Ports taken? `PORTAL_PORT=9000 HS_PORT=19080 NM_PORT=9042 docker compose up`.

### Without Docker (uv + JDK 17)

**Prerequisites:** macOS or Linux (on Windows, use WSL2 Ubuntu), `git`,
[uv](https://docs.astral.sh/uv/getting-started/installation/), and **JDK 17** for the History Server.
Don't use JDK 21: Spark 3.5's bundled Arrow breaks on it. `scripts/env.sh` finds JDK 17 in the usual places.

| OS | Install JDK 17 |
|---|---|
| macOS (Homebrew) | `brew install openjdk@17` (or Temurin 17: `brew install --cask temurin@17`) |
| Ubuntu / Debian / WSL2 | `sudo apt-get install -y openjdk-17-jdk-headless` |

If JDK 17 is somewhere else, set `JAVA_HOME` to it. On macOS with Homebrew's `openjdk@17`, that's
`export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home`.

```bash
git clone https://github.com/itsds/spark-ui-simulator.git && cd spark-ui-simulator
uv run sparksim up --open
```

`uv run` installs Python 3.11 and the dependencies on first use (PySpark 3.5.9 is ~320 MB: the History Server
comes from its bundled Spark, so you don't need a Spark or Hadoop install). `sparksim up` starts the
container log server, the History Server and the portal, generates what is missing, waits until the History
Server lists every run, and prints `Ready.` with a link per scenario. `Ctrl+C` stops everything. Server output
goes to `build/logs/`. `uv run sparksim up 01-join-skew` prepares one scenario only; `--help` lists the ports.

### How to practise

1. Open a scenario in the portal and read its ticket. Hide the topics: they are spoilers.
2. Investigate in the History Server (the portal's links open it in a tab of its own: Jobs, Stages, SQL /
   DataFrame, Executors, Environment, for the incident run and the last good run).
3. Stuck? Open the next hint, one at a time.
4. Write your diagnosis in the portal (root cause, evidence, fix; kept in your browser), then reveal the
   solution: the click path, the evidence numbers (checked by tests against the History Server's REST API),
   the fix and a 60-second interview answer.

`uv run scripts/stack_check.py` checks a running stack (either way of starting it) the way a learner uses it.

### Piece by piece

`sparksim up` is these steps; you can also run them yourself:

```bash
uv run sparksim prepare                    # every scenario's event logs + container logs, if missing or stale
uv run sparksim generate 01-join-skew      # or regenerate one scenario's runs unconditionally (~30 s)
services/nm-logs/run.sh &                  # mock NodeManager log server on :8042 (executor stdout/stderr)
uv run sparksim portal &                   # scenario portal on :8000
services/history-server/run.sh event-logs  # foreground; Ctrl+C to stop
```

`uv run sparksim scenarios` lists the scenarios and their runs. Each scenario is a
`scenarios/<id>/scenario.yaml`: the job (a workload: PySpark-like DataFrame code over
table schemas), the cluster, the spark-submit options, and for incidents a last good
run and an incident run with the injected faults (hot keys, config, a slow node,
memory pressure, a killed executor). `uv run sparksim validate` checks them.
`uv run sparksim generate tiny` writes the 8-task Phase 2 smoke app.

The History Server rescans its directory every 5 s, so newly generated logs appear
without a restart. The Executors tab's `stdout`/`stderr` links (and the driver's,
which is the YARN ApplicationMaster container in cluster mode) open the generated
container logs through the mock NodeManager log server
(`spark.history.custom.executor.log.url` in `services/history-server/spark-defaults.conf`).
The logs match the run: every task an executor ran, the files it read, the shuffle
blocks it fetched, the broadcasts it pulled, and how its container ended.

### The real reference apps

To browse the nine **real** reference apps captured on YARN in Phase 1 (AQE,
skew, spill, cache, failures and stage retries, Python UDFs, driver-bound jobs,
RDD API), point a History Server at that directory on another port:

```bash
services/history-server/run.sh reference/event-logs 18081   # http://localhost:18081
docker compose --profile reference up                       # the same, with Docker
```

Their executor log links point at the single-node capture cluster and don't open.

### Re-capture the reference logs (optional, Linux)

```bash
reference/yarn/cluster.sh start      # single-node HDFS + YARN (needs Hadoop 3.3.x in /opt/hadoop)
reference/capture.sh                 # runs reference/jobs/*.py with --master yarn --deploy-mode cluster
uv run scripts/event_inventory.py    # regenerates docs/event-log-schema.md
```

## Status

Phases 0–9 are done (see [`docs/ROADMAP.md`](docs/ROADMAP.md)): the simulator, YARN realism, the
scenario DSL with fault injection, Scenarios 00 and 01, the portal and the one-command start, Scenario 02,
and speculative execution with Scenario 09. Phase 10+, the rest of the scenario library, is under way (one or
two scenarios per session): Scenario 05 is done.

## Docs

- [Architecture](docs/ARCHITECTURE.md): pipeline and realism rules
- [Roadmap](docs/ROADMAP.md): phases and status
- [Decisions](docs/DECISIONS.md): why it's built this way
- [Scenarios](docs/SCENARIOS.md): backlog and design notes

## Disclaimer

Not affiliated with or endorsed by the Apache Software Foundation. Apache Spark,
Spark and the Spark logo are trademarks of the ASF. This project generates data
for the unmodified Apache Spark History Server and doesn't redistribute Spark UI code.
