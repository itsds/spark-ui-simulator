"""What a stage computes: its input, the operators it runs, and where its output goes.

A stage is a pipeline of physical operators between two shuffle boundaries,
as Spark's DAGScheduler cuts it:

    source(s) ──► [side operators] ──► [join] ──► operators ──► sink

* sources: a Parquet scan of a table's files, or the read side of one or more
  shuffle exchanges (two for a sort-merge join);
* operators: per-row CPU cost, selectivity, row width, and the memory they hold
  (sort buffers, hash-aggregation maps);
* sink: a shuffle write (hash partitioning on a key), a Parquet write, a
  broadcast, or a result sent to the driver.

Rows and bytes flow through the operators task by task (`cost.model`), so the
numbers in the UI follow from the data: a partial aggregation's output is the
number of distinct groups the task saw, a join's output is its matched rows,
a shuffle's partition sizes come from the key distribution.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from ..cost import calibration as cal

# --------------------------------------------------------------------------- keys


@dataclass(frozen=True)
class KeySpace:
    """The values of a key column that data is hash-partitioned or grouped on.

    `distinct`: distinct key values in the data being shuffled.
    `freq_cv`: coefficient of variation of rows per key (0 = every key has the
    same number of rows). It widens the spread of hash-partition sizes:
    relative std ≈ sqrt(R / distinct * (1 + cv²)).
    """

    name: str
    distinct: int
    freq_cv: float = 1.0


def expected_distinct(groups: int, rows: int) -> int:
    """Distinct groups among `rows` rows drawn uniformly from `groups` groups: G (1 - e^(-n/G))."""
    if groups <= 0 or rows <= 0:
        return 0
    return min(rows, round(groups * -math.expm1(-rows / groups)))


# --------------------------------------------------------------------------- operators


@dataclass(frozen=True)
class Op:
    """One physical operator inside a stage.

    kind: filter | project | bhj (broadcast hash join probe) | partial_agg | final_agg | sort | smj
    """

    kind: str
    ns_per_row: float
    selectivity: float = 1.0  # filter / bhj: output rows per input row
    out_row_bytes: int | None = None  # UnsafeRow bytes per output row (None: unchanged)
    groups: KeySpace | None = None  # aggregations: the grouping keys
    buffer_bytes: int = 0  # aggregation buffer bytes per group


def Filter(selectivity: float, ns: float = cal.FILTER_PROJECT_NS_PER_ROW) -> Op:  # noqa: N802
    return Op("filter", ns, selectivity=selectivity)


def Project(out_row_bytes: int, ns: float = cal.FILTER_PROJECT_NS_PER_ROW) -> Op:  # noqa: N802
    return Op("project", ns, out_row_bytes=out_row_bytes)


def BroadcastHashJoin(out_row_bytes: int, match: float = 1.0, ns: float = cal.BROADCAST_PROBE_NS_PER_ROW) -> Op:  # noqa: N802
    return Op("bhj", ns, selectivity=match, out_row_bytes=out_row_bytes)


def PartialAggregate(groups: KeySpace, out_row_bytes: int, buffer_bytes: int,  # noqa: N802
                     ns: float = cal.HASH_AGG_NS_PER_ROW) -> Op:
    return Op("partial_agg", ns, out_row_bytes=out_row_bytes, groups=groups, buffer_bytes=buffer_bytes)


def FinalAggregate(groups: KeySpace, out_row_bytes: int, buffer_bytes: int,  # noqa: N802
                   ns: float = cal.HASH_AGG_NS_PER_ROW) -> Op:
    return Op("final_agg", ns, out_row_bytes=out_row_bytes, groups=groups, buffer_bytes=buffer_bytes)


def Sort(ns: float = cal.SORT_NS_PER_ROW) -> Op:  # noqa: N802
    return Op("sort", ns)


def SortMergeJoin(out_row_bytes: int, match: float = 1.0, ns: float = cal.SMJ_NS_PER_ROW) -> Op:  # noqa: N802
    """Inner join of a fact side (first source) with a dimension side unique on the key."""
    return Op("smj", ns, selectivity=match, out_row_bytes=out_row_bytes)


# --------------------------------------------------------------------------- sources and sinks


@dataclass(frozen=True)
class Dataset:
    """A directory of Parquet files on HDFS (one table partition, or a whole unpartitioned table)."""

    path: str  # hdfs://nn/warehouse/web.db/events/dt=2026-11-02
    read_fraction: float  # share of file bytes the scan's selected columns occupy (column pruning)
    row_bytes: int  # UnsafeRow bytes of the selected columns, per row


@dataclass(frozen=True)
class ScanSource:
    dataset: Dataset


@dataclass(frozen=True)
class ShuffleSource:
    exchange: str  # name of the Exchange whose output this stage reads
    row_bytes: int  # UnsafeRow bytes per shuffled row


Source = ScanSource | ShuffleSource


@dataclass(frozen=True)
class ShuffleSink:
    """`Exchange hashpartitioning(key, num_partitions)`.

    `unique_keys`: the shuffled rows have one row per key (a dimension table), so
    partition sizes follow the key counts, not the row counts per key. Partial
    aggregation output is detected automatically.
    """

    exchange: str
    key: KeySpace
    num_partitions: int
    unique_keys: bool = False


@dataclass(frozen=True)
class WriteSink:
    """`df.write.partitionBy(...).parquet(path)`: one file per task per partition value."""

    path: str  # target table directory (the partition directory the job writes)
    bytes_per_row: float  # Parquet (snappy) bytes per output row
    file_prefix: str = "part"
    # Compressed row-group size: parquet.block.size (128 MiB of buffered rows) after encoding + snappy.
    row_group_bytes: int = 48 * 1024 * 1024


@dataclass(frozen=True)
class BroadcastSink:
    name: str


@dataclass(frozen=True)
class ResultSink:
    bytes_per_row: float = 0.0


Sink = ShuffleSink | WriteSink | BroadcastSink | ResultSink


@dataclass(frozen=True)
class StageSpec:
    """One stage of a query plan. `side_ops[i]` run on source i before the join."""

    name: str  # identifier inside the query (e.g. "events_exchange")
    sources: tuple[Source, ...]
    ops: tuple[Op, ...] = ()
    sink: Sink = field(default_factory=ResultSink)
    side_ops: tuple[tuple[Op, ...], ...] = ()
    depends_on: tuple[str, ...] = ()  # broadcast stages this stage's operators need first

    @property
    def is_result(self) -> bool:
        return isinstance(self.sink, (WriteSink, ResultSink))

    @property
    def is_broadcast(self) -> bool:
        return isinstance(self.sink, BroadcastSink)

    @property
    def shuffle_inputs(self) -> list[str]:
        return [s.exchange for s in self.sources if isinstance(s, ShuffleSource)]


@dataclass(frozen=True)
class Query:
    """One DataFrame action (a SQL execution): its stages, last one is the result stage."""

    description: str
    stages: tuple[StageSpec, ...]
    action: str = "parquet"  # call site: "<action> at NativeMethodAccessorImpl.java:0"
    planning_ms: tuple[int, int] = (350, 900)  # analysis + optimization + file listing on the driver
    think_ms: tuple[int, int] = (150, 1_200)  # Python driver code before the action

    def stage(self, name: str) -> StageSpec:
        for s in self.stages:
            if s.name == name:
                return s
        raise KeyError(name)

    @property
    def result(self) -> StageSpec:
        return self.stages[-1]
