"""Spark 3.5 physical operators: what the SQL tab and `explain("formatted")` show for each.

Every class mirrors one `SparkPlan` subclass:

* `node_name` / `simple_string()`: `SparkPlanInfo.nodeName` / `simpleString` (the plan graph's
  node labels and tooltips);
* `verbose_fields()`: the lines of the operator's block in the formatted explain
  (`verboseStringWithOperatorId`);
* `metric_defs()`: its SQL metrics as (key, display name, type), **in definition order**, which is
  the order Spark allocates their accumulator ids in. The JSON lists them in the iteration order of
  the Scala `Map` (`metric_json_order`).

Node identity matters. A node keeps its metric accumulator ids (allocated lazily, the first time a
`SparkPlanInfo` is built from it) for as long as the same object stays in the plan. Rules that
change a child copy the node (`with_new_children`, like `TreeNode.makeCopy`), and the copy gets
fresh metrics, exactly as AQE's re-planning does in Spark.

Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/**` at v3.5.9 (metric maps, string
args, `verboseStringWithOperatorId` overrides), checked against `ref01-daily-etl-aqe`.
"""

from __future__ import annotations

import copy as _copy
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from ..eventlog.ordering import scala_map_order
from .expr import AggExpr, Attr, Expr, bracket, catalog_string, field_string, sort_order

SUM, SIZE, TIMING, NS_TIMING, AVERAGE = "sum", "size", "timing", "nsTiming", "average"


@dataclass(frozen=True)
class MetricDef:
    key: str  # key in the operator's `metrics` Map
    name: str  # display name (accumulator name)
    type: str  # sum | size | timing | nsTiming | average


def _m(*defs: tuple[str, str, str]) -> list[MetricDef]:
    return [MetricDef(*d) for d in defs]


SHUFFLE_READ_METRICS = _m(  # SQLShuffleReadMetricsReporter.createShuffleReadMetrics
    ("remoteBlocksFetched", "remote blocks read", SUM),
    ("localBlocksFetched", "local blocks read", SUM),
    ("remoteBytesRead", "remote bytes read", SIZE),
    ("remoteBytesReadToDisk", "remote bytes read to disk", SIZE),
    ("localBytesRead", "local bytes read", SIZE),
    ("fetchWaitTime", "fetch wait time", TIMING),
    ("recordsRead", "records read", SUM),
    ("corruptMergedBlockChunks", "corrupt merged block chunks", SUM),
    ("mergedFetchFallbackCount", "merged fetch fallback count", SUM),
    ("remoteMergedBlocksFetched", "remote merged blocks fetched", SUM),
    ("localMergedBlocksFetched", "local merged blocks fetched", SUM),
    ("remoteMergedChunksFetched", "remote merged chunks fetched", SUM),
    ("localMergedChunksFetched", "local merged chunks fetched", SUM),
    ("remoteMergedBytesRead", "remote merged bytes read", SIZE),
    ("localMergedBytesRead", "local merged bytes read", SIZE),
    ("remoteReqsDuration", "remote reqs duration", TIMING),
    ("remoteMergedReqsDuration", "remote merged reqs duration", TIMING),
)
SHUFFLE_WRITE_METRICS = _m(  # SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics
    ("shuffleBytesWritten", "shuffle bytes written", SIZE),
    ("shuffleRecordsWritten", "shuffle records written", SUM),
    ("shuffleWriteTime", "shuffle write time", NS_TIMING),
)


def bytes_to_string(n: int | float) -> str:
    """`Utils.bytesToString(Long)`: binary units, a unit is used from 2x its size, one decimal."""
    for unit, size in (("EiB", 1 << 60), ("PiB", 1 << 50), ("TiB", 1 << 40), ("GiB", 1 << 30),
                       ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if n >= 2 * size:
            return f"{Decimal(int(n)) / Decimal(size):.1f} {unit}"
    return f"{float(n):.1f} B"


def big_decimal_3(n: int) -> str:
    """`BigDecimal(n, MathContext(3, HALF_UP)).toString()` (Statistics rowCount)."""
    d = Decimal(int(n))
    if d == 0:
        return "0"
    exp = d.adjusted()
    if exp < 3:
        return str(int(n))
    q = d.scaleb(-(exp - 2)).quantize(Decimal(1), rounding=ROUND_HALF_UP)
    if q >= 1000:  # rounding carried into a new digit
        q, exp = (q / 10).quantize(Decimal(1), rounding=ROUND_HALF_UP), exp + 1
    digits = str(int(q))
    return f"{digits[0]}.{digits[1:]}E+{exp}"


def abbreviate(s: str, width: int = 100) -> str:
    """commons-lang3 `StringUtils.abbreviate(s, width)`."""
    return s if len(s) <= width else s[: width - 3] + "..."


# --------------------------------------------------------------------------- base


class SparkPlan:
    NODE = ""
    codegen = False  # CodegenSupport with supportCodegen = true
    columnar = False  # supportsColumnar (vectorized Parquet scan)
    leaf = False  # LeafExecNode (verbose block shows Output instead of Input)

    def __init__(self, children: list[SparkPlan] | tuple = ()):
        self.children: list[SparkPlan] = list(children)
        self.metric_ids: dict[str, int] | None = None
        self.plan_id: int | None = None
        self.logical = None  # the logical node it was planned from (AQE re-planning)

    # ------------------------------------------------------------- identity and copies

    def copy(self, children: list[SparkPlan], plan_ids=None) -> SparkPlan:
        """`makeCopy`: a new node (new SparkPlan id, fresh lazy metrics) with these children."""
        new = _copy.copy(self)
        new.children = list(children)
        new.metric_ids = None
        if self.plan_id is not None and plan_ids is not None:
            new.plan_id = plan_ids.next()
        new._post_copy()
        return new

    def _post_copy(self) -> None:
        pass

    def with_new_children(self, children: list[SparkPlan], plan_ids=None) -> SparkPlan:
        if len(children) == len(self.children) and all(a is b for a, b in zip(children, self.children, strict=True)):
            return self
        return self.copy(children, plan_ids)

    def walk(self):
        """Pre-order over the plan tree (not into query stages)."""
        yield self
        for c in self.children:
            yield from c.walk()

    # ------------------------------------------------------------- strings

    @property
    def node_name(self) -> str:
        return self.NODE

    def output(self) -> list[Attr]:
        return self.children[0].output() if self.children else []

    def simple_string(self) -> str:
        args = self.arg_string()
        return f"{self.node_name} {args}" if args else self.node_name

    def arg_string(self) -> str:
        return ""

    def node_id_string(self, op_id: int) -> str:
        """`simpleStringWithNodeId`."""
        return f"{self.node_name} ({op_id})".strip()

    def verbose_fields(self) -> list[str]:
        """UnaryExecNode / LeafExecNode `verboseStringWithOperatorId` (after the header line)."""
        if self.leaf:
            out = [field_string("Output", self.output())]
        else:
            out = [field_string("Input", self.children[0].output())] if self.children else []
        a = self.arg_string()
        if a:
            out.append(f"Arguments: {a}")
        return out

    # ------------------------------------------------------------- metrics and graph

    def metric_defs(self) -> list[MetricDef]:
        return []

    def info_children(self) -> list[SparkPlan]:
        """Children in `SparkPlanInfo` (query stages and AdaptiveSparkPlan expose their plan)."""
        return self.children

    def metadata(self) -> dict[str, str]:
        return {}

    def metric_id(self, key: str) -> int:
        assert self.metric_ids is not None, f"{self.node_name}: metrics not allocated yet"
        return self.metric_ids[key]

    def has_metric(self, key: str) -> bool:
        return any(d.key == key for d in self.metric_defs())

    def __repr__(self) -> str:
        return f"<{self.node_name} {hex(id(self))[-5:]}>"


def metric_json_order(defs: list[MetricDef]) -> list[MetricDef]:
    """`plan.metrics.toSeq`: Map1..Map4 in insertion order, a HashMap trie above 4 entries."""
    by_key = {d.key: d for d in defs}
    return [by_key[k] for k in scala_map_order(by_key)]


# --------------------------------------------------------------------------- scans


@dataclass
class Relation:
    """A `HadoopFsRelation` over Parquet files: the paths given to `spark.read.parquet`."""

    root_paths: list[str]
    data_schema: list[Attr]  # every column of the files, in file order
    partition_schema: list[Attr]  # partition columns discovered from the paths (basePath)


class FileSourceScan(SparkPlan):
    """`FileSourceScanExec` with the vectorized Parquet reader (`Batched: true`)."""

    NODE = "Scan parquet "
    columnar = True
    leaf = True

    def __init__(self, relation: Relation, out: list[Attr], partition_filters: list[Expr],
                 data_filters: list[Expr], pushed_filters: list[str], dataset=None):
        super().__init__()
        self.relation = relation
        self.out = out
        self.partition_filters = partition_filters
        self.data_filters = data_filters
        self.pushed_filters = pushed_filters
        self.dataset = dataset  # workload description used by the engine (model.workload.Dataset)

    def output(self) -> list[Attr]:
        return self.out

    def read_schema(self) -> list[Attr]:
        return [a for a in self.out if a not in self.relation.partition_schema]

    @property
    def partitioned(self) -> bool:
        return bool(self.relation.partition_schema)

    def metric_defs(self) -> list[MetricDef]:
        defs = _m(("numOutputRows", "number of output rows", SUM), ("scanTime", "scan time", TIMING),
                  ("numFiles", "number of files read", SUM), ("metadataTime", "metadata time", TIMING),
                  ("filesSize", "size of files read", SIZE))
        if self.partitioned:
            defs += _m(("numPartitions", "number of partitions read", SUM),
                       ("pruningTime", "dynamic partition pruning time", TIMING))
        return defs

    def driver_metric_keys(self) -> list[str]:
        """`driverMetrics` Map (posted when the scan's RDD is created), in its iteration order."""
        keys = ["numFiles", "metadataTime", "filesSize"]
        if self.partitioned:
            keys += ["numPartitions", "pruningTime"]
        return scala_map_order(keys)

    def _location(self) -> str:
        paths = self.relation.root_paths
        return f"InMemoryFileIndex({len(paths)} paths)[{', '.join(paths)}]"

    def metadata(self) -> dict[str, str]:
        md = {
            "Format": "Parquet",
            "ReadSchema": catalog_string(self.read_schema()),
            "Batched": "true",
            "PartitionFilters": bracket(self.partition_filters),
            "PushedFilters": bracket(self.pushed_filters),
            "DataFilters": bracket(self.data_filters),
            "Location": self._location(),
        }
        return {k: md[k] for k in scala_map_order(md)}

    def simple_string(self) -> str:
        entries = [f"{k}: {abbreviate(v)}" for k, v in sorted(self.metadata().items())]
        return f"FileScan parquet [{','.join(str(a) for a in self.out)}] " + ", ".join(entries)

    def verbose_fields(self) -> list[str]:
        out = [field_string("Output", self.out)]
        for k, v in sorted(self.metadata().items()):
            if not v or v == "[]" or k in ("DataFilters", "Format"):
                continue
            if k == "Location":
                paths = self.relation.root_paths
                loc = bracket(paths) if len(paths) <= 1 else f"[{paths[0]}, ... {len(paths) - 1} entries]"
                out.append(f"Location: InMemoryFileIndex {loc}")
            else:
                out.append(f"{k}: {v}")
        return out


class ColumnarToRow(SparkPlan):
    NODE = "ColumnarToRow"
    codegen = True

    def metric_defs(self):
        return _m(("numOutputRows", "number of output rows", SUM), ("numInputBatches", "number of input batches", SUM))


class InputAdapter(SparkPlan):
    NODE = "InputAdapter"
    codegen = True


class WholeStageCodegen(SparkPlan):
    def __init__(self, child: SparkPlan, codegen_id: int):
        super().__init__([child])
        self.codegen_id = codegen_id

    @property
    def node_name(self) -> str:
        return f"WholeStageCodegen ({self.codegen_id})"

    def simple_string(self) -> str:
        return self.node_name

    def metric_defs(self):
        return _m(("pipelineTime", "duration", TIMING))


# --------------------------------------------------------------------------- row operators


class Filter(SparkPlan):
    NODE = "Filter"
    codegen = True

    def __init__(self, condition: Expr, child: SparkPlan, selectivity: float = 1.0, ns_per_row: float | None = None):
        super().__init__([child])
        self.condition = condition
        self.selectivity = selectivity
        self.ns_per_row = ns_per_row

    def output(self) -> list[Attr]:
        from .expr import IsNotNull

        non_null = {c.child.expr_id for c in _conj(self.condition) if isinstance(c, IsNotNull) and
                    isinstance(c.child, Attr)}
        return [a.with_nullable(False) if a.expr_id in non_null else a for a in self.children[0].output()]

    def arg_string(self) -> str:
        return str(self.condition)

    def verbose_fields(self):
        return [field_string("Input", self.children[0].output()), f"Condition : {self.condition}"]

    def metric_defs(self):
        return _m(("numOutputRows", "number of output rows", SUM))


def _conj(e):
    from .expr import conjuncts

    return conjuncts(e)


class Project(SparkPlan):
    NODE = "Project"
    codegen = True

    def __init__(self, exprs: list[Expr], child: SparkPlan, ns_per_row: float | None = None):
        super().__init__([child])
        self.exprs = exprs
        self.ns_per_row = ns_per_row

    def output(self) -> list[Attr]:
        from .expr import Alias

        child_out = {a.expr_id: a for a in self.children[0].output()}
        out = []
        for e in self.exprs:
            if isinstance(e, Alias):
                out.append(e.to_attr())
            elif isinstance(e, Attr):
                out.append(child_out.get(e.expr_id, e))
        return out

    def arg_string(self) -> str:
        return bracket(self.exprs)

    def verbose_fields(self):
        return [field_string("Output", self.output()), field_string("Input", self.children[0].output())]


class HashAggregate(SparkPlan):
    """`HashAggregateExec`. mode: partial | merge (PartialMerge) | final."""

    NODE = "HashAggregate"
    codegen = True

    def __init__(self, keys: list[Attr], aggs: list[tuple[AggExpr, str]], results: list[Expr], child: SparkPlan,
                 groups=None, ns_per_row: float | None = None, final: bool = False):
        super().__init__([child])
        self.keys = keys
        self.aggs = aggs  # (aggregate expression, mode)
        self.results = results
        self.groups = groups  # KeySpace of the grouping keys (cost model)
        self.ns_per_row = ns_per_row
        self.final = final  # the aggregate after the exchange (its groups come from the reducer)

    def output(self) -> list[Attr]:
        from .expr import Alias

        return [e.to_attr() if isinstance(e, Alias) else e for e in self.results]

    def functions(self) -> list[str]:
        return [a.func.agg_string(mode) for a, mode in self.aggs]

    def aggregate_attributes(self) -> list[Attr]:
        return [a.result_attr for a, _ in self.aggs]

    def simple_string(self) -> str:
        return f"HashAggregate(keys={bracket(self.keys)}, functions={bracket(self.functions())})"

    def verbose_fields(self):
        return [
            field_string("Input", self.children[0].output()),
            field_string("Keys", self.keys),
            field_string("Functions", self.functions()),
            field_string("Aggregate Attributes", self.aggregate_attributes()),
            field_string("Results", self.results),
        ]

    def metric_defs(self):
        return _m(("numOutputRows", "number of output rows", SUM), ("peakMemory", "peak memory", SIZE),
                  ("spillSize", "spill size", SIZE), ("aggTime", "time in aggregation build", TIMING),
                  ("avgHashProbe", "avg hash probes per key", AVERAGE),
                  ("numTasksFallBacked", "number of sort fallback tasks", SUM))

    @property
    def buffer_bytes(self) -> int:
        from .expr import unsafe_row_bytes

        buf = [b for a, _ in self.aggs for b in a.buffer]
        return unsafe_row_bytes(buf) if buf else 16


class Sort(SparkPlan):
    NODE = "Sort"
    codegen = True

    def __init__(self, order: list[Attr], child: SparkPlan, global_: bool = False, ns_per_row: float | None = None):
        super().__init__([child])
        self.order = order
        self.global_ = global_
        self.ns_per_row = ns_per_row

    def arg_string(self) -> str:
        return f"{bracket(sort_order(a) for a in self.order)}, {'true' if self.global_ else 'false'}, 0"

    def metric_defs(self):
        return _m(("sortTime", "sort time", TIMING), ("peakMemory", "peak memory", SIZE),
                  ("spillSize", "spill size", SIZE))


class _Join(SparkPlan):
    codegen = True

    def __init__(self, left_keys: list[Attr], right_keys: list[Attr], join_type: str, left: SparkPlan,
                 right: SparkPlan, match: float = 1.0, ns_per_row: float | None = None):
        super().__init__([left, right])
        self.left_keys = left_keys
        self.right_keys = right_keys
        self.join_type = join_type
        self.match = match  # output rows per streamed-side row
        self.ns_per_row = ns_per_row

    def output(self) -> list[Attr]:
        return self.children[0].output() + self.children[1].output()

    def verbose_fields(self):
        return [field_string("Left keys", self.left_keys), field_string("Right keys", self.right_keys),
                f"Join type: {self.join_type}", "Join condition: None"]


class SortMergeJoin(_Join):
    NODE = "SortMergeJoin"

    def simple_string(self) -> str:
        return f"SortMergeJoin {bracket(self.left_keys)}, {bracket(self.right_keys)}, {self.join_type}"

    def node_id_string(self, op_id: int) -> str:
        return f"{self.node_name} {self.join_type} ({op_id})"

    def metric_defs(self):
        return _m(("numOutputRows", "number of output rows", SUM), ("spillSize", "spill size", SIZE))


class BroadcastHashJoin(_Join):
    NODE = "BroadcastHashJoin"
    build_side = "BuildRight"

    def simple_string(self) -> str:
        return (f"BroadcastHashJoin {bracket(self.left_keys)}, {bracket(self.right_keys)}, {self.join_type}, "
                f"{self.build_side}, false")

    def node_id_string(self, op_id: int) -> str:
        return f"{self.node_name} {self.join_type} {self.build_side} ({op_id})"

    def metric_defs(self):
        return _m(("numOutputRows", "number of output rows", SUM))


# --------------------------------------------------------------------------- exchanges


class ShuffleExchange(SparkPlan):
    """`ShuffleExchangeExec(hashpartitioning(keys, n), child, ENSURE_REQUIREMENTS)`."""

    NODE = "Exchange"

    def __init__(self, keys: list[Attr], num_partitions: int, child: SparkPlan, plan_id: int,
                 key_space=None, unique_keys: bool = False):
        super().__init__([child])
        self.keys = keys
        self.num_partitions = num_partitions
        self.plan_id = plan_id
        self.key_space = key_space  # KeySpace (cost model: partition sizes)
        self.unique_keys = unique_keys

    def partitioning(self) -> str:
        return f"hashpartitioning({', '.join(str(k) for k in self.keys)}, {self.num_partitions})"

    def arg_string(self) -> str:
        return f"{self.partitioning()}, ENSURE_REQUIREMENTS, [plan_id={self.plan_id}]"

    def metric_defs(self):
        return _m(("dataSize", "data size", SIZE), ("numPartitions", "number of partitions", SUM)) \
            + SHUFFLE_READ_METRICS + SHUFFLE_WRITE_METRICS


class BroadcastExchange(SparkPlan):
    NODE = "BroadcastExchange"

    def __init__(self, keys: list[Attr], child: SparkPlan, plan_id: int):
        super().__init__([child])
        self.keys = keys
        self.plan_id = plan_id

    def mode(self) -> str:
        out = self.children[0].output()
        bound = []
        for k in self.keys:
            i = next(i for i, a in enumerate(out) if a.expr_id == k.expr_id)
            bound.append(f"input[{i}, {out[i].dtype}, {'true' if out[i].nullable else 'false'}]")
        return f"HashedRelationBroadcastMode(List({','.join(bound)}),false)"

    def arg_string(self) -> str:
        return f"{self.mode()}, [plan_id={self.plan_id}]"

    def metric_defs(self):
        return _m(("dataSize", "data size", SIZE), ("numOutputRows", "number of output rows", SUM),
                  ("collectTime", "time to collect", TIMING), ("buildTime", "time to build", TIMING),
                  ("broadcastTime", "time to broadcast", TIMING))


# --------------------------------------------------------------------------- AQE nodes


class QueryStage(SparkPlan):
    """`ShuffleQueryStageExec` / `BroadcastQueryStageExec`: a leaf wrapping a materializable exchange."""

    leaf = True

    def __init__(self, stage_id: int, plan: SparkPlan, broadcast: bool):
        super().__init__()
        self.stage_id = stage_id
        self.plan = plan
        self.broadcast = broadcast
        self.stats: tuple[int, int] | None = None  # (sizeInBytes, rowCount) once materialized
        self.engine_stage = None  # the engine's StageSpec name

    @property
    def node_name(self) -> str:
        return "BroadcastQueryStage" if self.broadcast else "ShuffleQueryStage"

    @property
    def exchange(self) -> SparkPlan:
        return self.plan

    @property
    def materialized(self) -> bool:
        return self.stats is not None

    def output(self) -> list[Attr]:
        return self.plan.output()

    def arg_string(self) -> str:
        return str(self.stage_id)

    def node_id_string(self, op_id: int) -> str:
        s = f"{self.node_name} ({op_id})"
        if self.stats is not None:
            size, rows = self.stats
            s += f", Statistics(sizeInBytes={bytes_to_string(size)}, rowCount={big_decimal_3(rows)})"
        return s

    def info_children(self) -> list[SparkPlan]:
        return [self.plan]


class AQEShuffleRead(SparkPlan):
    """`AQEShuffleReadExec` over a shuffle query stage with coalesced partition specs."""

    NODE = "AQEShuffleRead"

    def __init__(self, child: QueryStage, ranges: list[tuple[int, int]]):
        super().__init__([child])
        self.ranges = ranges  # CoalescedPartitionSpec(start, end) per task

    @property
    def coalesced(self) -> bool:
        return any(b - a > 1 for a, b in self.ranges)

    def arg_string(self) -> str:
        return "coalesced" if self.coalesced else ""

    def metric_defs(self):
        defs = _m(("numPartitions", "number of partitions", SUM), ("partitionDataSize", "partition data size", SIZE))
        if self.coalesced:
            defs += _m(("numCoalescedPartitions", "number of coalesced partitions", SUM))
        return defs


# --------------------------------------------------------------------------- writes and the root


class WriteFiles(SparkPlan):
    NODE = "WriteFiles"

    def output(self) -> list[Attr]:
        return []


@dataclass(eq=False)
class WriteCommand:
    """`InsertIntoHadoopFsRelationCommand`: owns the write metrics (shared by copies of its exec node)."""

    output_path: str
    path_option: str
    partition_columns: list[Attr]
    output_columns: list[str]
    mode: str = "Overwrite"
    sink: object = None  # model.workload.WriteSink
    metric_ids: dict[str, int] | None = None


class DataWritingCommand(SparkPlan):
    NODE = "Execute InsertIntoHadoopFsRelationCommand"

    def __init__(self, cmd: WriteCommand, child: SparkPlan):
        super().__init__([child])
        self.cmd = cmd

    def output(self) -> list[Attr]:
        return []

    def arg_string(self) -> str:
        c = self.cmd
        parts = ", ".join(f'"{a.name}"' for a in c.partition_columns)
        return (f"{c.output_path}, false, {bracket(c.partition_columns)}, Parquet, "
                f"[__partition_columns=[{parts}], path={c.path_option}], {c.mode}, {bracket(c.output_columns)}")

    def verbose_fields(self):
        return [field_string("Input", self.children[0].output()), f"Arguments: {self.arg_string()}"]

    def metric_defs(self):
        return _m(("numFiles", "number of written files", SUM), ("numOutputBytes", "written output", SIZE),
                  ("numOutputRows", "number of output rows", SUM), ("numParts", "number of dynamic part", SUM),
                  ("taskCommitTime", "task commit time", TIMING), ("jobCommitTime", "job commit time", TIMING))

    # metrics live on the command: copies of this node share them
    @property
    def metric_ids(self):  # type: ignore[override]
        return self.cmd.metric_ids if hasattr(self, "cmd") else None

    @metric_ids.setter
    def metric_ids(self, v):
        if hasattr(self, "cmd"):
            self.cmd.metric_ids = v

    def copy(self, children, plan_ids=None):
        new = _copy.copy(self)
        new.children = list(children)
        return new

    @staticmethod
    def driver_metric_keys() -> list[str]:
        """`driverSideMetrics` = metrics - taskCommitTime, posted after the job commit."""
        return scala_map_order(["numFiles", "numOutputBytes", "numOutputRows", "numParts", "jobCommitTime"])


class AdaptiveSparkPlan(SparkPlan):
    NODE = "AdaptiveSparkPlan"
    leaf = True

    def __init__(self, initial: SparkPlan, out: list[Attr]):
        super().__init__()
        self.initial = initial
        self.current = initial
        self.final = False
        self.out = out

    def output(self) -> list[Attr]:
        return self.out

    def arg_string(self) -> str:
        return f"isFinalPlan={'true' if self.final else 'false'}"

    def info_children(self) -> list[SparkPlan]:
        return [self.current]
