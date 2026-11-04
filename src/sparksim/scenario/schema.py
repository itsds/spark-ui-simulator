"""`scenarios/<id>/scenario.yaml`: the schema, as pydantic models.

    id: 01-join-skew                       # = the directory name
    title: ...                             # portal card
    difficulty: medium                     # intro | easy | medium | hard
    topics: [data skew, sort-merge join, AQE]
    summary: ...                           # one paragraph, no spoilers (portal)
    job:                                   # the same in every run: what is submitted, where, how
      workload: clickstream_enrichment     # sparksim.apps.WORKLOAD_MODULES
      app_name: clickstream-daily-enrichment
      user: svc_analytics
      queue: etl
      cluster: {cluster_timestamp: 1791902144377, rm_epoch: 23, ...}   # model.cluster.ClusterSpec fields
      executors: {instances: 40, cores: 4, memory: 16g, driver_memory: 8g, driver_cores: 2}
      conf: {spark.sql.shuffle.partitions: 2400, ...}                  # spark-submit --conf, in order
    runs:                                  # baseline (onboarding) | last_good + incident (ADR-006)
      last_good: {label: ..., app_seq: 48213, seed: 20261102, started_at: ..., params: {dt: ...}}
      incident:
        ...
        data: {events: {files: 4945}}      # how this run's input differs from the workload's usual data
        faults:                            # the injected causes (sparksim.scenario.faults)
          - {type: key_skew, dataset: events, column: customer_id, hot_keys: [{value: -1, share: 0.13}]}

Fault types: `key_skew` (hot keys, Zipf), `config` (Spark properties set/unset), `slow_node` (a degraded
NodeManager host: CPU, disk, network), `memory_pressure` (native memory per running task, e.g. Arrow/Python
buffers), `executor_killed` (an executor's container is SIGKILLed at a given time, exit 137).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------- units

_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*$", re.I)
_DURATION = re.compile(r"(\d+(?:\.\d+)?)(ms|h|m|s)")


def size_mb(v: str | int) -> int:
    """'16g' / '384m' / 2048 (MiB) -> MiB, as spark-submit reads memory sizes."""
    if isinstance(v, int):
        return v
    m = _SIZE.match(str(v))
    if not m:
        raise ValueError(f"not a memory size: {v!r} (use e.g. 16g, 384m)")
    n, unit = float(m.group(1)), m.group(2).lower()
    mult = {"": 1 / (1024 * 1024), "k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}[unit]
    return int(round(n * mult))


def duration_ms(v: str | int) -> int:
    """'6m30s' / '+390s' / '2h' / 1500 (ms) -> ms."""
    if isinstance(v, int):
        return v
    s = str(v).strip().lstrip("+")
    parts = _DURATION.findall(s)
    if not parts or "".join(a + b for a, b in parts) != s:
        raise ValueError(f"not a duration: {v!r} (use e.g. 6m30s, 90s, 1h)")
    unit = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}
    return int(round(sum(float(a) * unit[b] for a, b in parts)))


def spark_value(v) -> str:
    """A YAML scalar as spark-submit would pass it (true/false lower-case)."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------- job


class ClusterModel(_Model):
    """The YARN cluster (`model.cluster.ClusterSpec`)."""

    cluster_timestamp: int
    racks: int = 6
    hosts_per_rack: int = 20
    nm_vcores: int = 48
    nm_memory: str | int = "240g"
    min_allocation: str | int = "1g"
    max_allocation: str | int = "64g"
    domain: str = "hadoop.prod.internal"
    host_prefix: str = "wn"
    rm_host: str = "rm1.hadoop.prod.internal"
    rm_standby_hosts: tuple[str, ...] = ()
    rm_cluster_id: str = ""
    rm_epoch: int = 0
    occupancy: tuple[float, float] = (0.55, 0.97)

    @field_validator("nm_memory", "min_allocation", "max_allocation")
    @classmethod
    def _size(cls, v):
        size_mb(v)
        return v

    def host_names(self) -> list[str]:
        n = self.racks * self.hosts_per_rack
        return [f"{self.host_prefix}{i:03d}.{self.domain}" for i in range(1, n + 1)]


class ExecutorsModel(_Model):
    """spark-submit sizing: --num-executors, --executor-cores, --executor-memory, --driver-*."""

    instances: int = Field(40, ge=0)
    cores: int = Field(4, ge=1)
    memory: str | int = "16g"
    memory_overhead: str | int | None = None  # spark.executor.memoryOverhead (default max(384m, 10 %))
    driver_memory: str | int = "8g"
    driver_cores: int = Field(2, ge=1)

    @field_validator("memory", "memory_overhead", "driver_memory")
    @classmethod
    def _size(cls, v):
        if v is not None:
            size_mb(v)
        return v


class JobModel(_Model):
    workload: str
    app_name: str
    user: str
    queue: str = "default"
    cluster: ClusterModel
    executors: ExecutorsModel = ExecutorsModel()
    conf: dict[str, str | int | float | bool] = {}

    @field_validator("conf")
    @classmethod
    def _keys(cls, v):
        for k in v:
            if not k.startswith("spark."):
                raise ValueError(f"conf key {k!r} is not a Spark property")
        return v


# ---------------------------------------------------------------------------- data


class DataModel(_Model):
    """How one input dataset of this run differs from the workload's usual data."""

    files: int | None = Field(None, ge=1)
    mean_file_size: str | int | None = None
    file_size_sigma: float | None = Field(None, ge=0)

    @field_validator("mean_file_size")
    @classmethod
    def _size(cls, v):
        if v is not None:
            size_mb(v)
        return v


# ---------------------------------------------------------------------------- faults


class HotKeyModel(_Model):
    value: int | str | list[int | str]  # one value per key column
    share: float = Field(gt=0, lt=1)  # share of the dataset's rows that carry this key


class KeySkew(_Model):
    """Skewed values in a key column of a dataset: explicit hot keys and/or a Zipf law over the rest."""

    type: Literal["key_skew"]
    dataset: str
    column: str
    hot_keys: tuple[HotKeyModel, ...] = ()
    zipf: float | None = Field(None, gt=0, le=3)
    note: str = ""

    @model_validator(mode="after")
    def _something(self):
        if not self.hot_keys and self.zipf is None:
            raise ValueError("key_skew needs hot_keys or zipf")
        if sum(h.share for h in self.hot_keys) >= 1:
            raise ValueError("hot key shares must add up to less than 1")
        return self


class ConfigFault(_Model):
    """Spark properties this run was submitted with on top of (or instead of) the job's."""

    type: Literal["config"]
    set: dict[str, str | int | float | bool] = {}
    unset: tuple[str, ...] = ()
    note: str = ""


class SlowNode(_Model):
    """A degraded NodeManager host: throughput factors (1 = healthy) for CPU, local disks and its NIC."""

    type: Literal["slow_node"]
    host: str
    cpu: float = Field(1.0, gt=0, le=1)
    disk: float = Field(1.0, gt=0, le=1)
    network: float = Field(1.0, gt=0, le=1)
    note: str = ""


class MemoryPressure(_Model):
    """Native (off-heap, outside the JVM's own footprint) memory each running task holds in the container:
    Arrow/Python buffers, a JNI library, a leak. It counts towards the container's physical memory."""

    type: Literal["memory_pressure"]
    native_per_task: str | int
    note: str = ""

    @field_validator("native_per_task")
    @classmethod
    def _size(cls, v):
        size_mb(v)
        return v


class ExecutorKilled(_Model):
    """An executor's container is killed with SIGKILL (kernel OOM killer, an operator): exit code 137."""

    type: Literal["executor_killed"]
    executor: str  # Spark executor id ("7"), in registration order of this run
    at: str | int  # time after the application started, e.g. 4m30s
    note: str = ""

    @field_validator("at")
    @classmethod
    def _dur(cls, v):
        duration_ms(v)
        return v


Fault = Annotated[KeySkew | ConfigFault | SlowNode | MemoryPressure | ExecutorKilled, Field(discriminator="type")]


# ---------------------------------------------------------------------------- runs and the scenario


class RunModel(_Model):
    label: str
    app_seq: int = Field(ge=1)
    seed: int
    started_at: datetime
    params: dict[str, str | int | float | bool] = {}
    data: dict[str, DataModel] = {}
    faults: tuple[Fault, ...] = ()

    @field_validator("started_at")
    @classmethod
    def _tz(cls, v: datetime):
        if v.tzinfo is None:
            raise ValueError("started_at needs a time zone (e.g. 2026-11-03T02:10:07.318Z)")
        return v


RUN_ROLES = ("baseline", "last_good", "incident")


class ScenarioModel(_Model):
    id: str = Field(pattern=r"^\d\d-[a-z0-9-]+$")
    title: str
    difficulty: Literal["intro", "easy", "medium", "hard"]
    topics: tuple[str, ...] = ()
    summary: str = ""
    job: JobModel
    runs: dict[str, RunModel]

    @model_validator(mode="after")
    def _runs(self):
        roles = set(self.runs)
        bad = roles - set(RUN_ROLES)
        if bad:
            raise ValueError(f"unknown run roles {sorted(bad)} (use {', '.join(RUN_ROLES)})")
        if roles not in ({"baseline"}, {"last_good", "incident"}):
            raise ValueError("runs must be either {baseline} (no incident) or {last_good, incident} (ADR-006)")
        if "incident" in roles:
            good, bad_run = self.runs["last_good"], self.runs["incident"]
            if good.faults:
                raise ValueError("the last good run has no faults")
            if not bad_run.faults:
                raise ValueError("the incident run needs at least one fault: the injected cause")
            if good.started_at >= bad_run.started_at:
                raise ValueError("the last good run starts before the incident run")
        elif self.runs["baseline"].faults:
            raise ValueError("a baseline run has no faults")
        seqs = [r.app_seq for r in self.runs.values()]
        if len(set(seqs)) != len(seqs):
            raise ValueError("every run needs its own app_seq (application id)")
        hosts = set(self.job.cluster.host_names())
        for name, r in self.runs.items():
            for f in r.faults:
                if isinstance(f, SlowNode) and f.host not in hosts:
                    raise ValueError(f"run {name}: slow_node host {f.host!r} is not in the cluster")
        return self
