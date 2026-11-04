"""What a stage computes: its input, the operators it runs, and where its output goes.

A stage is a pipeline of physical operators between two shuffle boundaries,
as Spark's DAGScheduler cuts it. Apps no longer write these by hand: `sql.aqe`
derives one `StageSpec` per AQE query stage from the physical plan, and each
operator, source and sink carries its plan node (`node`) so tasks can report
the node's SQL metrics.

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
class HotKey:
    """One key value that holds a fixed share of the rows (a sentinel like `customer_id = -1`, a bot)."""

    value: tuple  # one value per key column, e.g. (-1,) or ("2026-11-03", -1)
    share: float  # share of the shuffled rows that carry this key


@dataclass(frozen=True)
class KeySpace:
    """The values of a key column that data is hash-partitioned or grouped on.

    `distinct`: distinct key values in the data being shuffled.
    `freq_cv`: coefficient of variation of rows per key (0 = every key has the
    same number of rows). It widens the spread of hash-partition sizes:
    relative std ≈ sqrt(R / distinct * (1 + cv²)).

    Skewed keys (fault injection, `sparksim.scenario.faults`):

    * `hot`: explicit heavy hitters. Each lands in the partition Spark's
      `pmod(Murmur3Hash(key, 42), n)` gives its value (`dtypes` are the key
      columns' SQL types), with its share of the rows.
    * `zipf`: rows per key follow a Zipf law with this exponent over the other
      keys (replaces `freq_cv`): the top ranks are placed one by one, the tail's
      spread follows from the law's second moment.
    """

    name: str
    distinct: int
    freq_cv: float = 1.0
    dtypes: tuple[str, ...] = ("bigint",)
    hot: tuple[HotKey, ...] = ()
    zipf: float = 0.0

    @property
    def skewed(self) -> bool:
        return bool(self.hot) or self.zipf > 0

    @property
    def hot_share(self) -> float:
        return sum(h.share for h in self.hot)


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
    node: object = field(default=None, compare=False)  # the physical plan node (SQL metrics)


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
    node: object = field(default=None, compare=False)  # FileSourceScan


@dataclass(frozen=True)
class ShuffleSource:
    exchange: str  # name of the Exchange whose output this stage reads
    row_bytes: int  # UnsafeRow bytes per shuffled row
    node: object = field(default=None, compare=False)  # the upstream ShuffleExchange (read metrics)
    read: object = field(default=None, compare=False)  # AQEShuffleRead over it, if any


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
    node: object = field(default=None, compare=False)  # ShuffleExchange


@dataclass(frozen=True)
class WriteSink:
    """`df.write.partitionBy(...).parquet(path)`: one file per task per partition value."""

    path: str  # target table directory (the partition directory the job writes)
    bytes_per_row: float  # Parquet (snappy) bytes per output row
    file_prefix: str = "part"
    # Compressed row-group size: parquet.block.size (128 MiB of buffered rows) after encoding + snappy.
    row_group_bytes: int = 48 * 1024 * 1024
    node: object = field(default=None, compare=False)  # DataWritingCommand


@dataclass(frozen=True)
class BroadcastSink:
    name: str
    node: object = field(default=None, compare=False)  # BroadcastExchange


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

