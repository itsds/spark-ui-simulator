"""Fault mechanisms: each fault in `scenario.yaml` changes an *input* of the simulation, never an output.

| fault             | what it changes                                   | what follows in the simulation              |
|-------------------|---------------------------------------------------|---------------------------------------------|
| `key_skew`        | a dataset's key distribution (`KeySpace.hot`,     | shuffle partition sizes (Spark's Murmur3    |
|                   | `KeySpace.zipf`)                                  | partition of each hot value), task sizes,   |
|                   |                                                   | spill, stragglers                           |
| `duplicate_keys`  | a dimension's rows per key (`KeySpace.rows_per_   | its files and rows; a join's output rows    |
|                   | key`, `rows_per_match`), its files, its schema    | per matched row, everything downstream of   |
|                   | (appended columns)                                | the join (sorts, writes, later reads)       |
| `config`          | the Spark properties the run is submitted with    | whatever the engine derives from them:      |
|                   | (executor sizing keys change the containers)      | partitions, join strategy, AQE, memory ...  |
| `slow_node`       | one NodeManager host's CPU, disk and NIC speed    | every task on executors of that host        |
| `memory_pressure` | native memory each running task holds             | container RSS, NodeManager pmem kills       |
| `executor_killed` | a SIGKILL to an executor's JVM at a given time    | exit 137, ExecutorLostFailure, re-runs,     |
|                   |                                                   | a replacement container                     |
| `yarn_preemption` | another queue reclaims its share: the RM takes    | exit -102, ExecutorLostFailure (not the     |
|                   | N of the app's youngest containers, no room for   | app's fault); without the shuffle service:  |
|                   | replacements until a given time                   | lost map output, Resubmitted, FetchFailed,  |
|                   |                                                   | stage retries                               |

Faults are applied in the order listed, after the run's `data` overrides.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from ..engine.runtime import ExecutorKill, InputDataset, Preemption
from ..model.cluster import ClusterSpec, ExecutorSizing, HostFault
from ..model.workload import HotKey
from .schema import (
    ConfigFault,
    DuplicateKeys,
    ExecutorKilled,
    KeySkew,
    MemoryPressure,
    SlowNode,
    YarnPreemption,
    duration_ms,
    size_mb,
    spark_value,
)

MiB = 1024 * 1024

# --conf keys that spark-submit turns into the executor containers' sizing
SIZING_KEYS = {
    "spark.executor.memory": "memory_mb",
    "spark.executor.cores": "cores",
    "spark.executor.instances": "instances",
    "spark.executor.memoryOverhead": "overhead_mb",
}


@dataclass
class RunSetup:
    """The simulation inputs of one run, while its data overrides and faults are applied."""

    datasets: dict[str, InputDataset]
    conf: list[tuple[str, str]]
    sizing: ExecutorSizing
    cluster: ClusterSpec
    native_per_task: int = 0
    executor_kills: list[ExecutorKill] = field(default_factory=list)
    preemptions: list[Preemption] = field(default_factory=list)

    def set_conf(self, key: str, value: str) -> None:
        for i, (k, _) in enumerate(self.conf):
            if k == key:
                self.conf[i] = (k, value)
                return
        self.conf.append((key, value))


def apply_fault(setup: RunSetup, f) -> None:
    if isinstance(f, KeySkew):
        _key_skew(setup, f)
    elif isinstance(f, DuplicateKeys):
        _duplicate_keys(setup, f)
    elif isinstance(f, ConfigFault):
        _config(setup, f)
    elif isinstance(f, SlowNode):
        setup.cluster = replace(setup.cluster, host_faults=setup.cluster.host_faults + (
            HostFault(f.host, cpu=f.cpu, disk=f.disk, network=f.network),))
    elif isinstance(f, MemoryPressure):
        setup.native_per_task += size_mb(f.native_per_task) * MiB
    elif isinstance(f, ExecutorKilled):
        setup.executor_kills.append(ExecutorKill(f.executor, duration_ms(f.at)))
    elif isinstance(f, YarnPreemption):
        setup.preemptions.append(Preemption(duration_ms(f.at), f.containers, duration_ms(f.until)))
    else:  # pragma: no cover - the schema's discriminator lists every type
        raise TypeError(f)


def _key_skew(setup: RunSetup, f: KeySkew) -> None:
    ds = setup.datasets.get(f.dataset)
    if ds is None:
        raise ValueError(f"key_skew: no dataset {f.dataset!r} (the workload has {sorted(setup.datasets)})")
    ks = ds.keys.get(f.column)
    if ks is None:
        raise ValueError(f"key_skew: {f.dataset}.{f.column} is not a key column of the workload "
                         f"(its queries shuffle on {sorted(ds.keys) or 'no column'} of {f.dataset})")
    hot = []
    for h in f.hot_keys:
        value = tuple(h.value) if isinstance(h.value, list) else (h.value,)
        if len(value) != len(ks.dtypes):
            raise ValueError(f"key_skew: {f.column} has {len(ks.dtypes)} column(s) ({', '.join(ks.dtypes)}), "
                             f"hot key {h.value!r} has {len(value)}")
        hot.append(HotKey(value, h.share))
    skewed = replace(ks, hot=ks.hot + tuple(hot), zipf=f.zipf if f.zipf is not None else ks.zipf)
    setup.datasets[f.dataset] = replace(ds, keys={**ds.keys, f.column: skewed})


def _duplicate_keys(setup: RunSetup, f: DuplicateKeys) -> None:
    ds = setup.datasets.get(f.dataset)
    if ds is None:
        raise ValueError(f"duplicate_keys: no dataset {f.dataset!r} (the workload has {sorted(setup.datasets)})")
    ks = ds.keys.get(f.column)
    if ks is None:
        raise ValueError(f"duplicate_keys: {f.dataset}.{f.column} is not a key column of the workload "
                         f"(its queries join on {sorted(ds.keys) or 'no column'} of {f.dataset})")
    if not ks.unique:
        raise ValueError(f"duplicate_keys: {f.dataset}.{f.column} already has several rows per key")
    if ds.partitions:
        raise ValueError("duplicate_keys: a partitioned dataset is not supported")
    added = tuple((c.name, c.type, c.avg_length, c.parquet_bytes) for c in f.added_columns)
    bpr = ds.bytes_per_row + sum(c.parquet_bytes for c in f.added_columns)
    # the same writer: files of the same size, `rows_per_key` times the rows, each row a little wider
    files = round(ds.files * f.rows_per_key * bpr / ds.bytes_per_row)
    dup = replace(ks, rows_per_key=f.rows_per_key, rows_per_match=f.rows_per_match, freq_cv=f.spread)
    setup.datasets[f.dataset] = replace(ds, files=files, bytes_per_row=bpr, keys={**ds.keys, f.column: dup},
                                        added_columns=ds.added_columns + added)


def _config(setup: RunSetup, f: ConfigFault) -> None:
    for key in f.unset:
        setup.conf = [(k, v) for k, v in setup.conf if k != key]
        if key == "spark.executor.memoryOverhead":
            setup.sizing = replace(setup.sizing, overhead_mb=None)
    for key, raw in f.set.items():
        value = spark_value(raw)
        attr = SIZING_KEYS.get(key)
        if attr is None:
            setup.set_conf(key, value)
            continue
        n = size_mb(value) if attr in ("memory_mb", "overhead_mb") else int(value)
        setup.sizing = replace(setup.sizing, **{attr: n})
        if attr == "overhead_mb":  # spark-submit has no flag for it: it stays a --conf
            setup.set_conf(key, value)
