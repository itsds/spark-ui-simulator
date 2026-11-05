"""A small PySpark-like DataFrame API and the physical planner behind it.

Apps describe their queries the way the PySpark job is written:

    events = spark.read(EVENTS_TABLE, f"{root}/dt={DT}")                  # basePath read: dt is a column
    pages = spark.read(PAGES_TABLE)
    q = (events.where(events["is_bot"].eq(False), selectivity=0.93)
               .select(events["event_id"], ..., Func("parse_url", ...).alias("host"))
               .join(pages, "page_id", broadcast=True)
               .write_parquet(path, partition_by=["dt"]))

and `plan_physical()` turns that into the physical plan Spark 3.5 would build, applying the
optimizer and planner rules that shape what the UI shows:

* filters pushed to the scan; `isnotnull` inferred for null-intolerant predicates (first) and for
  inner equi-join keys (appended, bottom-up), partition predicates moved to `PartitionFilters`;
  data predicates become `DataFilters`, the translatable ones `PushedFilters`;
* column pruning: scans read only the columns needed above, a `Project` drops columns a `Filter`
  or a join no longer needs (in the child's output order), USING joins output key, left, right;
* join strategy: `BroadcastHashJoin` (hint, or the build side's estimated size under
  `spark.sql.autoBroadcastJoinThreshold`) with a `BroadcastExchange`, else `SortMergeJoin` with an
  `Exchange hashpartitioning(key, spark.sql.shuffle.partitions)` and a `Sort` on each side
  (EnsureRequirements);
* aggregation: partial `HashAggregate` → `Exchange` → final `HashAggregate`
  (`AggUtils.planAggregateWithoutDistinct`), or the four-aggregate plan for one distinct aggregate;
* partitioned writes: `Sort [partition columns]` (V1Writes) → `WriteFiles` →
  `Execute InsertIntoHadoopFsRelationCommand`.

The planner is re-entrant: AQE re-plans the query after every completed query stage, passing the
stages created so far; their subtrees are not re-planned (`aqe.py`).

Cost annotations (selectivity, ns per row, key spaces) ride along on the nodes for the engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..model.workload import Dataset, KeySpace, WriteSink
from .expr import (
    AggExpr,
    AggFunc,
    Alias,
    Attr,
    Expr,
    ExprIds,
    IsNotNull,
    and_all,
    conjuncts,
    null_intolerant_refs,
    source_filter,
    unsafe_row_bytes,
)
from .nodes import (
    BroadcastExchange,
    BroadcastHashJoin,
    DataWritingCommand,
    FileSourceScan,
    Filter,
    HashAggregate,
    Project,
    Relation,
    ShuffleExchange,
    Sort,
    SortMergeJoin,
    SparkPlan,
    WriteCommand,
    WriteFiles,
)

# --------------------------------------------------------------------------- tables


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str
    avg_bytes: float = 0.0  # strings: average UTF-8 length (UnsafeRow)
    pq_bytes: float = 8.0  # Parquet (encoded + compressed) bytes per row of this column chunk


@dataclass(frozen=True)
class Table:
    """A Parquet table directory. `partition_columns` are discovered from `col=value` directories."""

    root: str
    columns: tuple[Column, ...]
    partition_columns: tuple[Column, ...] = ()

    @property
    def file_bytes_per_row(self) -> float:
        return sum(c.pq_bytes for c in self.columns)


@dataclass
class PlanIds:
    """`SparkPlan.newPlanId`: one counter per JVM; every node instance Catalyst creates takes one."""

    rng: object
    next_id: int = 60

    def next(self) -> int:
        self.next_id += self.rng.randint(2, 7)
        return self.next_id

    def skip(self, lo: int, hi: int) -> None:
        self.next_id += self.rng.randint(lo, hi)


# --------------------------------------------------------------------------- logical plan


class LNode:
    children: tuple[LNode, ...] = ()

    def output(self) -> list[Attr]:
        raise NotImplementedError


@dataclass(eq=False)
class LRelation(LNode):
    table: Table
    paths: list[str]
    attrs: list[Attr]  # data columns then partition columns
    partition_attrs: list[Attr]
    filters: list[Expr] = field(default_factory=list)  # user predicates, in where() order
    inferred: list[Expr] = field(default_factory=list)  # isnotnull from joins above, bottom-up
    selectivity: float = 1.0
    filter_ns: float | None = None

    def output(self):
        return list(self.attrs)


@dataclass(eq=False)
class LProject(LNode):
    exprs: list[Expr]
    child: LNode
    ns: float | None = None

    @property
    def children(self):
        return (self.child,)

    def output(self):
        return [e.to_attr() if isinstance(e, Alias) else e for e in self.exprs]


@dataclass(eq=False)
class LJoin(LNode):
    left: LNode
    right: LNode
    left_keys: list[Attr]
    right_keys: list[Attr]
    broadcast: bool
    match: float
    ns: float | None
    unique_right: bool = True  # the right side has one row per key (a dimension)

    @property
    def children(self):
        return (self.left, self.right)

    def output(self):
        """USING join: the keys (left side), left's other columns, right's other columns."""
        lo = [a for a in self.left.output() if a not in self.left_keys]
        ro = [a for a in self.right.output() if a not in self.right_keys]
        return list(self.left_keys) + lo + ro


@dataclass(eq=False)
class AggSpec:
    func: AggFunc
    name: str
    result_id: int = -1
    alias_id: int = -1
    buffer_ids: list[int] = field(default_factory=list)


@dataclass(eq=False)
class LAggregate(LNode):
    keys: list[Attr]
    aggs: list[AggSpec]
    child: LNode
    groups: KeySpace
    ns: float | None = None

    @property
    def children(self):
        return (self.child,)

    def output(self):
        # Count is never null (Count.nullable = false); sum/min/max/avg are
        return list(self.keys) + [Attr(a.name, a.func.dtype, a.alias_id, a.func.name != "count") for a in self.aggs]


@dataclass(eq=False)
class LWrite(LNode):
    child: LNode
    path: str
    path_option: str
    partition_by: list[str]
    sink: WriteSink
    mode: str = "Overwrite"

    @property
    def children(self):
        return (self.child,)

    def output(self):
        return []


# --------------------------------------------------------------------------- the DataFrame API


def _true(v: str) -> bool:
    return v.strip().lower() == "true"


class SparkSession:
    def __init__(self, ids: ExprIds, conf: dict[str, str] | None = None):
        self.ids = ids
        self.conf = conf or {}

    @property
    def shuffle_partitions(self) -> int:
        """`SQLConf.numShufflePartitions`: with AQE's partition coalescing on, its initial partition number
        (if set) replaces `spark.sql.shuffle.partitions` for every shuffle the planner inserts."""
        initial = self.conf.get("spark.sql.adaptive.coalescePartitions.initialPartitionNum")
        if initial is not None and _true(self.conf.get("spark.sql.adaptive.enabled", "true")) \
                and _true(self.conf.get("spark.sql.adaptive.coalescePartitions.enabled", "true")):
            return int(initial)
        return int(self.conf.get("spark.sql.shuffle.partitions", "200"))

    def read(self, table: Table, path: str | None = None) -> DataFrame:
        """`spark.read.option("basePath", table.root).parquet(path or table.root)`."""
        attrs = [self.ids.attr(c.name, c.dtype, c.avg_bytes) for c in table.columns]
        parts = [self.ids.attr(c.name, c.dtype, c.avg_bytes) for c in table.partition_columns]
        # DataSource.resolveRelation builds the relation's attributes twice; the DataFrame keeps the first set
        # (ref01: orders #0-7, customers #16-23, products #32-34).
        self.ids.skip(len(attrs) + len(parts))
        rel = LRelation(table, [path or table.root], attrs + parts, parts)
        return DataFrame(self, rel)


class DataFrame:
    def __init__(self, spark: SparkSession, plan: LNode):
        self.spark = spark
        self.plan = plan

    def __getitem__(self, name: str) -> Attr:
        for a in self.plan.output():
            if a.name == name:
                return a
        raise KeyError(name)

    def _relation_of(self, attrs: list[Attr]) -> LRelation:
        def find(n: LNode) -> LRelation | None:
            if isinstance(n, LRelation):
                return n if all(a in n.attrs for a in attrs) else None
            for c in n.children:
                r = find(c)
                if r is not None:
                    return r
            return None

        r = find(self.plan)
        if r is None:
            raise ValueError("where(): predicates must reference the columns of one scanned table")
        return r

    def where(self, cond: Expr, selectivity: float = 1.0, ns: float | None = None) -> DataFrame:
        """A predicate on scanned columns; pushed down to the scan, as PushDownPredicates does."""
        rel = self._relation_of(cond.refs())
        rel.filters.extend(conjuncts(cond))
        rel.selectivity *= selectivity
        if ns is not None:
            rel.filter_ns = ns
        return self

    def select(self, *exprs: Expr, ns: float | None = None) -> DataFrame:
        out = []
        for e in exprs:
            if isinstance(e, Alias) and e.expr_id < 0:
                e = Alias(e.child, e.name, self.spark.ids.next())
            out.append(e)
        return DataFrame(self.spark, LProject(out, self.plan, ns))

    def join(self, other: DataFrame, on: str | list[str], broadcast: bool = False, match: float = 1.0,
             ns: float | None = None, unique_right: bool = True, key_space: KeySpace | None = None) -> DataFrame:
        """Inner equi-join USING `on`. `key_space`: the join key's values (shuffle partition sizes)."""
        keys = [on] if isinstance(on, str) else list(on)
        lk = [self[k] for k in keys]
        rk = [other[k] for k in keys]
        j = LJoin(self.plan, other.plan, lk, rk, broadcast, match, ns, unique_right)
        j.key_space = key_space
        return DataFrame(self.spark, j)

    def group_by(self, *keys: str) -> GroupedData:
        return GroupedData(self, [self[k] for k in keys])

    def write_parquet(self, path: str, partition_by: list[str], sink: WriteSink, path_option: str | None = None,
                      mode: str = "Overwrite") -> LWrite:
        return LWrite(self.plan, path, path_option or path, partition_by, sink, mode)


class GroupedData:
    def __init__(self, df: DataFrame, keys: list[Attr]):
        self.df = df
        self.keys = keys

    def agg(self, *specs: AggSpec, groups: KeySpace, ns: float | None = None) -> DataFrame:
        ids = self.df.spark.ids
        for s in specs:
            if s.result_id < 0:
                s.result_id = ids.next()
            if s.alias_id < 0:
                s.alias_id = ids.next()
        return DataFrame(self.df.spark, LAggregate(self.keys, list(specs), self.df.plan, groups, ns))


def agg(name: str, child: Expr | None, alias: str, distinct: bool = False) -> AggSpec:
    """F.sum(col).alias(name) / F.count("*") / F.countDistinct(col) …"""
    return AggSpec(AggFunc(name, child, distinct), alias)


# --------------------------------------------------------------------------- optimizer: filter inference


def infer_filters(root: LNode) -> None:
    """InferFiltersFromConstraints for inner equi-joins: isnotnull(key) on each side, bottom-up."""

    def visit(n: LNode) -> None:
        for c in n.children:
            visit(c)
        if isinstance(n, LJoin):
            for side, keys in ((n.left, n.left_keys), (n.right, n.right_keys)):
                for k in keys:
                    rel = _producer(side, k)
                    if rel is not None and not any(_is_notnull_of(e, k) for e in rel.filters + rel.inferred):
                        rel.inferred.append(IsNotNull(k))

    visit(root)


def _producer(n: LNode, a: Attr) -> LRelation | None:
    if isinstance(n, LRelation):
        return n if a in n.attrs else None
    for c in n.children:
        r = _producer(c, a)
        if r is not None:
            return r
    return None


def _is_notnull_of(e: Expr, a: Attr) -> bool:
    return isinstance(e, IsNotNull) and isinstance(e.child, Attr) and e.child == a


def relation_predicates(rel: LRelation) -> tuple[list[Expr], list[Expr]]:
    """(partition filters, data filters) of a scan, as FileSourceStrategy splits them."""
    preds: list[Expr] = []
    for e in rel.filters:
        for a in null_intolerant_refs(e):
            if not any(_is_notnull_of(p, a) for p in preds + rel.filters):
                preds.append(IsNotNull(a))
    preds += rel.filters + [e for e in rel.inferred if not any(_is_notnull_of(p, e.child) for p in preds)]
    part = [p for p in preds if p.refs() and all(a in rel.partition_attrs for a in p.refs())]
    data = [p for p in preds if p not in part]
    return part, data


# --------------------------------------------------------------------------- physical planning


@dataclass
class PlanContext:
    spark: SparkSession
    plan_ids: PlanIds
    stages: dict = field(default_factory=dict)  # exchange key -> QueryStage (AQE re-planning)
    broadcast_threshold: int = 10 * 1024 * 1024
    dataset_stats: dict = field(default_factory=dict)  # table root -> estimated bytes (join strategy)


def exchange_key(n: LNode, kind: str, keys: list[Attr]) -> tuple:
    return (id(n), kind, tuple(k.expr_id for k in keys))


def _ordered(req: list[Attr], out: list[Attr]) -> list[Attr]:
    s = set(req)
    return [a for a in out if a in s]


def plan_physical(root: LNode, ctx: PlanContext) -> SparkPlan:
    """Plan the whole query (the plan AdaptiveSparkPlanExec wraps)."""
    return _plan(root, ctx, None)


def _plan(n: LNode, ctx: PlanContext, required: list[Attr] | None) -> SparkPlan:
    if isinstance(n, LWrite):
        child = _plan(n.child, ctx, None)
        out = child.output()
        parts = [next(a for a in out if a.name == p) for p in n.partition_by]
        sorted_child = Sort(parts, child) if parts else child
        cmd = WriteCommand(n.path, n.path_option, parts, [a.name for a in out], n.mode, n.sink)
        return DataWritingCommand(cmd, WriteFiles([sorted_child]))

    if isinstance(n, LRelation):
        part, data = relation_predicates(n)
        need = set(required if required is not None else n.attrs)
        for p in data:
            need.update(p.refs())
        scan_out = [a for a in n.attrs if a in need]
        pushed = [f for f in (source_filter(p) for p in data) if f]
        rel = Relation(n.paths, n.attrs[: len(n.attrs) - len(n.partition_attrs)], n.partition_attrs)
        scan = FileSourceScan(rel, scan_out, part, data, pushed)
        read = [a for a in scan_out if a not in n.partition_attrs]
        tb = n.table.file_bytes_per_row
        frac = sum(c.pq_bytes for c in n.table.columns if any(a.name == c.name for a in read)) / tb if tb else 1.0
        scan.dataset = Dataset(n.paths[0], read_fraction=round(frac, 4), row_bytes=unsafe_row_bytes(scan_out))
        scan.logical = n
        top: SparkPlan = scan
        if data:
            top = Filter(and_all(data), scan, selectivity=n.selectivity, ns_per_row=n.filter_ns)
        if required is not None and len(required) < len(top.output()):
            top = Project(_ordered(required, top.output()), top)
        return top

    if isinstance(n, LProject):
        out_attrs = n.output()
        keep = [e for e, a in zip(n.exprs, out_attrs, strict=True) if required is None or a in required]
        child_req: list[Attr] = []
        for e in keep:
            for a in e.refs():
                if a not in child_req:
                    child_req.append(a)
        child = _plan(n.child, ctx, _ordered(child_req, n.child.output()))
        if isinstance(child, Project) and all(isinstance(e, Attr) for e in child.exprs):
            child = child.children[0]  # CollapseProject: a column-pruning Project folds into this one
        if all(isinstance(e, Attr) for e in keep) and [a.expr_id for a in keep] == [a.expr_id for a in child.output()]:
            return child
        return Project(keep, child, ns_per_row=n.ns)

    if isinstance(n, LJoin):
        req = required if required is not None else n.output()
        lreq = _ordered(list(req) + n.left_keys, n.left.output())
        rreq = _ordered(list(req) + n.right_keys, n.right.output())
        left = _plan(n.left, ctx, lreq)
        if n.broadcast or _estimated_size(n.right, ctx) <= ctx.broadcast_threshold:
            key = exchange_key(n.right, "broadcast", n.right_keys)
            build = ctx.stages.get(key)
            if build is None:
                right = _plan(n.right, ctx, rreq)
                build = BroadcastExchange(n.right_keys, right, ctx.plan_ids.next())
                build.key = key
            join: SparkPlan = BroadcastHashJoin(n.left_keys, n.right_keys, "Inner", left, build, match=n.match,
                                                ns_per_row=n.ns)
        else:
            sides = []
            for side, keys, sreq, phys in ((n.left, n.left_keys, lreq, left), (n.right, n.right_keys, rreq, None)):
                key = exchange_key(side, "shuffle", keys)
                ex = ctx.stages.get(key)
                if ex is None:
                    child = phys if phys is not None else _plan(side, ctx, sreq)
                    ks = _key_space(n, side is n.right)
                    ex = ShuffleExchange(keys, ctx.spark.shuffle_partitions, child, ctx.plan_ids.next(),
                                         key_space=ks, unique_keys=side is n.right and n.unique_right)
                    ex.key = key
                sides.append(Sort(keys, ex))
            join = SortMergeJoin(n.left_keys, n.right_keys, "Inner", sides[0], sides[1], match=n.match,
                                 ns_per_row=n.ns)
        join.logical = n
        if list(req) != join.output():
            return Project(_using_order(req, n), join)
        return join

    if isinstance(n, LAggregate):
        return _plan_aggregate(n, ctx)

    raise TypeError(n)


def _using_order(req: list[Attr], n: LJoin) -> list[Attr]:
    s = set(req)
    return [a for a in n.output() if a in s]


def _estimated_size(n: LNode, ctx: PlanContext) -> float:
    """Logical plan stats: file size of the scanned table scaled by the columns read (sizeInBytes)."""
    if isinstance(n, LRelation):
        return ctx.dataset_stats.get(n.paths[0], float("inf"))
    sizes = [_estimated_size(c, ctx) for c in n.children]
    return max(sizes) if sizes else float("inf")


def _key_space(n: LJoin, right: bool) -> KeySpace | None:
    return getattr(n, "key_space", None)


def _plan_aggregate(n: LAggregate, ctx: PlanContext) -> SparkPlan:
    ids = ctx.spark.ids
    distinct = [a for a in n.aggs if a.func.distinct]
    child_req: list[Attr] = list(n.keys)
    for a in n.aggs:
        if a.func.child is not None:
            for r in a.func.child.refs():
                if r not in child_req:
                    child_req.append(r)
    child = _plan(n.child, ctx, _ordered(child_req, n.child.output()))

    def agg_expr(spec: AggSpec) -> AggExpr:
        if not spec.buffer_ids:
            spec.buffer_ids = [ids.next() for _ in spec.func.buffer_names()]
        buf = [Attr(nm, t, i) for (nm, t), i in zip(spec.func.buffer_names(), spec.buffer_ids, strict=True)]
        return AggExpr(spec.func, spec.result_id, buf)

    results = list(n.keys) + [Alias(agg_expr(a).result_attr, a.name, a.alias_id) for a in n.aggs]
    key = exchange_key(n, "shuffle", n.keys)
    if not distinct:
        aes = [agg_expr(a) for a in n.aggs]
        stage = ctx.stages.get(key)
        if stage is None:
            partial = HashAggregate(n.keys, [(e, "partial") for e in aes],
                                    list(n.keys) + [b for e in aes for b in e.buffer], child,
                                    groups=n.groups, ns_per_row=n.ns)
            ex = ShuffleExchange(n.keys, ctx.spark.shuffle_partitions, partial, ctx.plan_ids.next(),
                                 key_space=n.groups)
            ex.key = key
            stage = ex
        final = HashAggregate(n.keys, [(e, "final") for e in aes], results, stage, groups=n.groups, ns_per_row=n.ns,
                              final=True)
        final.logical = n
        return final

    # AggUtils.planAggregateWithOneDistinct
    (d,) = distinct
    dcols = d.func.child.refs()
    regular = [a for a in n.aggs if not a.func.distinct]
    raes = [agg_expr(a) for a in regular]
    dae = agg_expr(d)
    stage = ctx.stages.get(key)
    if stage is None:
        keys2 = list(n.keys) + [c for c in dcols if c not in n.keys]
        buf = [b for e in raes for b in e.buffer]
        p1 = HashAggregate(keys2, [(e, "partial") for e in raes], keys2 + buf, child, groups=n.groups, ns_per_row=n.ns)
        p2 = HashAggregate(keys2, [(e, "merge") for e in raes], keys2 + buf, p1, groups=n.groups, ns_per_row=n.ns)
        p3 = HashAggregate(list(n.keys), [(e, "merge") for e in raes] + [(dae, "partial")],
                           list(n.keys) + buf + dae.buffer, p2, groups=n.groups, ns_per_row=n.ns)
        ex = ShuffleExchange(n.keys, ctx.spark.shuffle_partitions, p3, ctx.plan_ids.next(), key_space=n.groups)
        ex.key = key
        stage = ex
    final = HashAggregate(list(n.keys), [(e, "final") for e in raes] + [(dae, "final")], results, stage,
                          groups=n.groups, ns_per_row=n.ns, final=True)
    final.logical = n
    return final


# --------------------------------------------------------------------------- an app's queries


@dataclass(frozen=True)
class SqlQuery:
    """One DataFrame action (one SQL execution). `build` writes the query with the app's session, so
    expression ids are allocated in the order the PySpark script creates its DataFrames."""

    description: str  # sc.setJobDescription(...)
    build: object  # Callable[[SparkSession], LWrite]
    action: str = "parquet"  # call site: "<action> at NativeMethodAccessorImpl.java:0"
    planning_ms: tuple[int, int] = (350, 900)  # analysis + optimization + physical planning on the driver
    think_ms: tuple[int, int] = (150, 1_200)  # Python driver code before the action
