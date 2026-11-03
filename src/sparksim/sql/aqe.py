"""`AdaptiveSparkPlanExec` (Spark 3.5.9): how one SQL execution's plan evolves while it runs.

    initial plan ──createQueryStages──► stages whose children are all materialized become query stages
         ▲                              (SQLAdaptiveExecutionUpdate, then their jobs start)
         │ a stage completes
         └── re-plan the query around the finished stages ──► createQueryStages ...
    no exchange left ──► final plan (coalesced reads + codegen for the result stage, isFinalPlan=true)

Ported pieces, each with the node-identity behaviour the event log exposes:

* `createQueryStages`: depth first, left to right; an exchange becomes a stage only when every query
  stage below it is materialized; nodes whose children change are copied (`withNewChildren`).
* `newQueryStage`: `CoalesceShufflePartitions` on the stage's child (an `AQEShuffleRead` over the
  shuffle stages it reads, if coalescing reduces the partition count), then
  `ApplyColumnarRulesAndInsertTransitions` (ColumnarToRow) and `CollapseCodegenStages`
  (WholeStageCodegen + InputAdapter; ids from one counter per query, children first).
* re-planning after each completed stage: the query is planned again around the existing stages,
  so every node outside a stage is new (fresh metric ids, new operator ids in the explain). In
  `ref01-daily-etl-aqe` every re-plan was adopted (the write command's metrics change each time),
  so we always adopt.

`stage_spec()` turns a query stage (or the final plan) into the engine's `StageSpec`: sources, the
operators in pipeline order (each carrying its plan node, for SQL metrics) and the sink.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace

from ..model.workload import (
    BroadcastHashJoin as BHJOp,
)
from ..model.workload import (
    BroadcastSink,
    FinalAggregate,
    Op,
    PartialAggregate,
    ScanSource,
    ShuffleSink,
    ShuffleSource,
    StageSpec,
)
from ..model.workload import (
    Filter as FilterOp,
)
from ..model.workload import (
    Project as ProjectOp,
)
from ..model.workload import (
    Sort as SortOp,
)
from ..model.workload import (
    SortMergeJoin as SMJOp,
)
from .dataframe import LNode, PlanContext, infer_filters, plan_physical
from .expr import unsafe_row_bytes
from .nodes import (
    AdaptiveSparkPlan,
    AQEShuffleRead,
    BroadcastExchange,
    BroadcastHashJoin,
    ColumnarToRow,
    DataWritingCommand,
    FileSourceScan,
    Filter,
    HashAggregate,
    InputAdapter,
    Project,
    QueryStage,
    ShuffleExchange,
    Sort,
    SortMergeJoin,
    SparkPlan,
    WholeStageCodegen,
    WriteFiles,
)

# coalesce(shuffle stages read together) -> coalesced (start, end) reducer ranges, or None to keep them
Coalescer = Callable[[list[QueryStage]], list[tuple[int, int]] | None]


def stage_name(stage: QueryStage) -> str:
    return getattr(stage, "name", None) or f"qs{stage.stage_id}"


class AdaptiveExecution:
    def __init__(self, query: LNode, ctx: PlanContext, coalescer: Coalescer, name_prefix: str = ""):
        self.name_prefix = name_prefix  # makes stage names unique across the app's executions
        infer_filters(query)
        self.logical = query
        self.ctx = ctx
        self.coalescer = coalescer
        initial = plan_physical(query, ctx)
        self.root = AdaptiveSparkPlan(initial, initial.output())
        self.stage_counter = 0
        self.codegen_counter = 0
        self.all_materialized = False
        self.stages: list[QueryStage] = []

    # ------------------------------------------------------------------ the loop's steps

    def create_stages(self) -> list[QueryStage]:
        plan, mat, new = self._create(self.root.current)
        self.root.current = plan
        self.all_materialized = mat
        self.stages.extend(new)
        # SPARK-33933: broadcast stages are materialized first
        return sorted(new, key=lambda s: not s.broadcast)

    def replan(self) -> None:
        """reOptimize: plan the query again; finished and running stages are reused as they are."""
        self.ctx.plan_ids.skip(20, 60)  # optimizer and planner create many intermediate nodes
        self.root.current = plan_physical(self.logical, self.ctx)

    def finalize(self) -> SparkPlan:
        """The result stage: optimizeQueryStage(isFinalStage = true) + postStageCreationRules."""
        plan = self._optimize_stage(self.root.current)
        plan = apply_columnar(plan, self.ctx)
        plan = self._collapse(plan)
        self.root.current = plan
        self.root.final = True
        return plan

    # ------------------------------------------------------------------ createQueryStages

    def _create(self, p: SparkPlan) -> tuple[SparkPlan, bool, list[QueryStage]]:
        if isinstance(p, (ShuffleExchange, BroadcastExchange)):
            child, mat, new = self._create(p.children[0])
            np_ = p.with_new_children([child], self.ctx.plan_ids)
            if mat:
                st = self._new_stage(np_)
                return st, False, [st]
            return np_, False, new
        if isinstance(p, QueryStage):
            return p, p.materialized, []
        if not p.children:
            return p, True, []
        rs = [self._create(c) for c in p.children]
        return (p.with_new_children([r[0] for r in rs], self.ctx.plan_ids), all(r[1] for r in rs),
                [s for r in rs for s in r[2]])

    def _new_stage(self, e: SparkPlan) -> QueryStage:
        child = self._optimize_stage(e.children[0])
        e2 = e.with_new_children([child], self.ctx.plan_ids)
        e2 = apply_columnar(e2, self.ctx)
        e2 = self._collapse(e2)
        st = QueryStage(self.stage_counter, e2, broadcast=isinstance(e, BroadcastExchange))
        st.name = f"{self.name_prefix}qs{st.stage_id}"
        self.stage_counter += 1
        self.ctx.stages[e.key] = st
        return st

    # ------------------------------------------------------------------ queryStageOptimizerRules

    def _optimize_stage(self, plan: SparkPlan) -> SparkPlan:
        """CoalesceShufflePartitions: one coalesce group (no Union in our plans)."""
        leaves = [n for n in plan.walk() if isinstance(n, QueryStage) and not n.broadcast]
        if not leaves:
            return plan
        ranges = self.coalescer(leaves)
        if ranges is None:
            return plan

        def rewrite(n: SparkPlan) -> SparkPlan:
            if isinstance(n, QueryStage) and not n.broadcast:
                return AQEShuffleRead(n, ranges)
            return n.with_new_children([rewrite(c) for c in n.children], self.ctx.plan_ids)

        return rewrite(plan)

    # ------------------------------------------------------------------ CollapseCodegenStages

    def _collapse(self, plan: SparkPlan) -> SparkPlan:
        return self._insert_wsc(plan)

    def _insert_wsc(self, p: SparkPlan) -> SparkPlan:
        if p.codegen:
            inner = self._insert_input_adapter(p)
            self.codegen_counter += 1
            return WholeStageCodegen(inner, self.codegen_counter)
        return p.with_new_children([self._insert_wsc(c) for c in p.children], self.ctx.plan_ids)

    def _insert_input_adapter(self, p: SparkPlan) -> SparkPlan:
        if not p.codegen:
            return InputAdapter([self._insert_wsc(p)])
        if isinstance(p, SortMergeJoin):
            return p.with_new_children([InputAdapter([self._insert_wsc(c)]) for c in p.children], self.ctx.plan_ids)
        return p.with_new_children([self._insert_input_adapter(c) for c in p.children], self.ctx.plan_ids)

    # ------------------------------------------------------------------ engine stage specs

    def stage_spec(self, stage: QueryStage) -> StageSpec:
        ex = stage.plan
        b = _chain(ex.children[0])
        if isinstance(ex, BroadcastExchange):
            sink = BroadcastSink(stage_name(stage), node=ex)
        else:
            sink = ShuffleSink(stage_name(stage), ex.key_space, ex.num_partitions, ex.unique_keys, node=ex)
        stage.engine_stage = stage_name(stage)
        return b.spec(stage_name(stage), sink)

    def final_spec(self) -> StageSpec:
        root = self.root.current
        assert isinstance(root, DataWritingCommand), "only write queries are modelled so far"
        writes = root.children[0]
        assert isinstance(writes, WriteFiles)
        b = _chain(writes.children[0])
        sink = replace(root.cmd.sink, node=root)
        return b.spec("result", sink)


# --------------------------------------------------------------------------- columnar transitions


def apply_columnar(plan: SparkPlan, ctx: PlanContext) -> SparkPlan:
    """ApplyColumnarRulesAndInsertTransitions: ColumnarToRow under row-based parents of columnar nodes."""

    def visit(n: SparkPlan) -> SparkPlan:
        kids = [visit(c) for c in n.children]
        if not n.columnar:
            kids = [ColumnarToRow([k]) if k.columnar else k for k in kids]
        return n.with_new_children(kids, ctx.plan_ids)

    return visit(plan)


# --------------------------------------------------------------------------- plan -> StageSpec


@dataclass
class _Branch:
    sources: list = field(default_factory=list)
    side_ops: list[list[Op]] = field(default_factory=list)
    ops: list[Op] = field(default_factory=list)
    deps: list[str] = field(default_factory=list)
    joined: bool = False

    def spec(self, name: str, sink) -> StageSpec:
        side = tuple(tuple(s) for s in self.side_ops) if self.joined else ()
        return StageSpec(name, tuple(self.sources), tuple(self.ops), sink, side, tuple(self.deps))


def _op(n: SparkPlan) -> Op:
    out_bytes = unsafe_row_bytes(n.output())
    ns = getattr(n, "ns_per_row", None)
    kw = {"ns": ns} if ns is not None else {}
    if isinstance(n, Filter):
        op = FilterOp(n.selectivity, **kw)
    elif isinstance(n, Project):
        op = ProjectOp(out_bytes, **kw)
    elif isinstance(n, BroadcastHashJoin):
        op = BHJOp(out_bytes, n.match, **kw)
    elif isinstance(n, SortMergeJoin):
        op = SMJOp(out_bytes, n.match, **kw)
    elif isinstance(n, Sort):
        op = SortOp(**kw)
    elif isinstance(n, HashAggregate):
        mk = FinalAggregate if n.final else PartialAggregate
        op = mk(n.groups, out_row_bytes=out_bytes, buffer_bytes=n.buffer_bytes, **kw)
    else:
        raise TypeError(f"no cost model for {n.node_name}")
    return replace(op, node=n)


def _chain(n: SparkPlan) -> _Branch:
    if isinstance(n, (WholeStageCodegen, InputAdapter, ColumnarToRow)):
        return _chain(n.children[0])
    if isinstance(n, FileSourceScan):
        return _Branch([ScanSource(n.dataset, node=n)], [[]])
    if isinstance(n, AQEShuffleRead):
        st = n.children[0]
        return _Branch([ShuffleSource(stage_name(st), unsafe_row_bytes(st.output()), node=st.plan, read=n)], [[]])
    if isinstance(n, QueryStage):
        assert not n.broadcast
        return _Branch([ShuffleSource(stage_name(n), unsafe_row_bytes(n.output()), node=n.plan)], [[]])
    if isinstance(n, SortMergeJoin):
        left, right = _chain(n.children[0]), _chain(n.children[1])
        return _Branch(left.sources + right.sources, [left.ops, right.ops], [_op(n)], left.deps + right.deps, True)
    if isinstance(n, BroadcastHashJoin):
        b = _chain(n.children[0])
        build = n.children[1]
        while isinstance(build, InputAdapter):
            build = build.children[0]
        assert isinstance(build, QueryStage) and build.broadcast
        b.deps.append(stage_name(build))
        b.ops.append(_op(n))
        return b
    if isinstance(n, (Filter, Project, HashAggregate, Sort)):
        b = _chain(n.children[0])
        b.ops.append(_op(n))
        return b
    raise TypeError(f"unexpected node in a query stage: {n.node_name}")
