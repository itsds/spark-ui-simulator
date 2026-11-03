"""RDD operation scopes and RDD lineage of AQE query stages, from the execute()/produce() calls Spark makes.

`SparkPlan.executeQuery` (behind `execute`, `executeColumnar`, `executeBroadcast`, `executeWrite`) and
`CodegenSupport.produce` each open a new `RDDOperationScope` named after the node, with an id from one
counter per JVM. RDDs take the scope that is open when they are created. So the stage DAG's boxes
(`WholeStageCodegen (3)`, `Exchange`, `Scan parquet `, `AQEShuffleRead`) and their ids follow from
the order in which a stage's plan is executed:

    Exchange.submitShuffleJob         scope Exchange ── child.execute() ── MapPartitionsRDD [Exchange]
    WholeStageCodegen.execute         scope WSC ── produce() down the pipeline (one scope per node) ──
                                      (BHJ: prepareBroadcast, then needCopyResult at the top: 2 scopes each)
                                      ── inputRDDs() ── MapPartitionsRDD [WSC] (ZippedPartitionsRDD2 for 2 inputs)
    ColumnarToRow.inputRDDs           InputAdapter.executeColumnar ── Scan.executeColumnar ──
                                      FileScanRDD + MapPartitionsRDD [Scan parquet ]
    InputAdapter.inputRDD             child.execute() (no InputAdapter scope)
    SortMergeJoin.inputRDDs           left.execute() / right.execute() (InputAdapter scope, then the child WSC)
    AQEShuffleRead.execute            ShuffledRowRDD [AQEShuffleRead]
    ShuffleQueryStage.execute         scope ShuffleQueryStage ── Exchange.execute: ShuffledRowRDD [Exchange] (a new id)
    BroadcastExchange.submitBroadcastJob  scope on the caller's thread; the child runs on a broadcast thread
    WriteFiles.executeWrite           scope WriteFiles ── child.execute() ── MapPartitionsRDD [WriteFiles]

Checked against every RDD scope of `ref01-daily-etl-aqe`. Stages materialized in the same AQE round run
on two threads (the broadcast's child on its own), so their scope and RDD ids interleave; we interleave
the two call sequences with a seeded coin.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from ..eventlog import IdAllocator, RDDInfo, rdd_scope
from .nodes import (
    AQEShuffleRead,
    BroadcastHashJoin,
    ColumnarToRow,
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


@dataclass(eq=False)
class RddNode:
    name: str
    scope: tuple[int, str] | None
    parents: list[RddNode] = field(default_factory=list)
    shuffle_read: bool = False  # ShuffledRowRDD: its parent belongs to the map stage
    rdd_id: int = -1


@dataclass
class StageLineage:
    final: RddNode
    top_scope: tuple[int, str]

    def rdd_infos(self, callsite: str, num_partitions: int, rng: random.Random) -> list[RDDInfo]:
        """`Stage Info."RDD Info"`: the stage's RDD, then its narrow ancestors in HashSet order (seeded)."""
        nodes: list[RddNode] = []

        def visit(n: RddNode) -> None:
            if n in nodes:
                return
            nodes.append(n)
            if not n.shuffle_read:
                for p in n.parents:
                    visit(p)

        visit(self.final)
        unordered = any(n.shuffle_read for n in nodes)
        level = "UNORDERED" if unordered else "DETERMINATE"
        infos = {n.rdd_id: RDDInfo(n.rdd_id, n.name, rdd_scope(*n.scope) if n.scope else None, callsite,
                                   [p.rdd_id for p in n.parents], deterministic_level=level,
                                   num_partitions=num_partitions) for n in nodes}
        rest = sorted(i for i in infos if i != self.final.rdd_id)
        rng.shuffle(rest)
        return [infos[self.final.rdd_id]] + [infos[i] for i in rest]


class ScopeSimulator:
    """Runs the execute()/produce() call sequences of one AQE round, allocating scope and RDD ids."""

    def __init__(self, scope_ids: IdAllocator, rdd_ids: IdAllocator, rng: random.Random):
        self.scope_ids = scope_ids
        self.rdd_ids = rdd_ids
        self.rng = rng
        self.map_rdds: dict[int, RddNode] = {}  # id(query stage) -> the map side's last RDD

    # ------------------------------------------------------------------ driver loop

    def _run(self, programs: list) -> None:
        threads = list(programs)
        pending: dict[int, object] = {}
        while threads:
            t = threads[0] if len(threads) == 1 else self.rng.choice(threads)
            try:
                ev = t.send(pending.pop(id(t), None))
            except StopIteration:
                threads.remove(t)
                continue
            kind, arg = ev
            if kind == "scope":
                pending[id(t)] = self.scope_ids.next()
            elif kind == "rdd":
                arg.rdd_id = self.rdd_ids.next()
                pending[id(t)] = arg
            elif kind == "spawn":
                threads.append(arg)
            else:  # pragma: no cover
                raise ValueError(kind)

    def skip(self, n: int) -> None:
        """Scopes the driver opens outside stage materialization (planning, commands, listing)."""
        self.scope_ids.skip(n)

    def materialize(self, stages: list[QueryStage]) -> dict[int, StageLineage]:
        """One createQueryStages round: broadcast stages first (SPARK-33933), each on its own thread."""
        out: dict[int, StageLineage] = {}

        def main():
            for st in stages:
                if st.broadcast:
                    s = yield ("scope", "BroadcastExchange")
                    yield ("spawn", self._broadcast_thread(st, s, out))
                else:
                    yield from self._shuffle_stage(st, out)

        self._run([main()])
        return out

    def materialize_write(self, plan: SparkPlan) -> StageLineage:
        """The result stage of a write: `WriteFilesExec.executeWrite`."""
        out: dict[str, StageLineage] = {}
        writes = next(n for n in plan.walk() if isinstance(n, WriteFiles))

        def main():
            s = yield ("scope", "WriteFiles")
            child = yield from self._execute(writes.children[0])
            r = yield ("rdd", RddNode("MapPartitionsRDD", (s, "WriteFiles"), [child]))
            out["w"] = StageLineage(r, (s, "WriteFiles"))

        self._run([main()])
        return out["w"]

    # ------------------------------------------------------------------ stage roots

    def _shuffle_stage(self, st: QueryStage, out):
        s = yield ("scope", "Exchange")
        child = yield from self._execute(st.plan.children[0])
        r = yield ("rdd", RddNode("MapPartitionsRDD", (s, "Exchange"), [child]))
        self.map_rdds[id(st)] = r
        out[id(st)] = StageLineage(r, (s, "Exchange"))

    def _broadcast_thread(self, st: QueryStage, s: int, out):
        child = yield from self._execute(st.plan.children[0])
        r = yield ("rdd", RddNode("MapPartitionsRDD", (s, "BroadcastExchange"), [child]))
        out[id(st)] = StageLineage(r, (s, "BroadcastExchange"))

    # ------------------------------------------------------------------ SparkPlan.execute

    def _execute(self, n: SparkPlan):
        s = yield ("scope", n.node_name)
        scope = (s, n.node_name)
        if isinstance(n, WholeStageCodegen):
            pending: list[SparkPlan] = []
            yield from self._produce(n.children[0], pending)
            for b in pending:  # BroadcastHashJoin.prepareBroadcast, bottom-up during consume()
                yield from self._execute_broadcast(b)
            for b in pending:  # WholeStageCodegen.doConsume -> needCopyResult -> multipleOutputForOneInput
                yield from self._execute_broadcast(b)
            rdds = yield from self._input_rdds(n.children[0])
            if len(rdds) == 1:
                return (yield ("rdd", RddNode("MapPartitionsRDD", scope, rdds)))
            z = yield ("rdd", RddNode("ZippedPartitionsRDD2", scope, rdds))
            return (yield ("rdd", RddNode("MapPartitionsRDD", scope, [z])))
        if isinstance(n, InputAdapter):
            return (yield from self._execute(n.children[0]))
        if isinstance(n, AQEShuffleRead):
            return (yield ("rdd", RddNode("ShuffledRowRDD", scope, [self.map_rdds[id(n.children[0])]], True)))
        if isinstance(n, QueryStage):
            ex = n.plan
            s2 = yield ("scope", ex.node_name)
            return (yield ("rdd", RddNode("ShuffledRowRDD", (s2, ex.node_name), [self.map_rdds[id(n)]], True)))
        if isinstance(n, FileSourceScan):
            f = yield ("rdd", RddNode("FileScanRDD", scope))
            return (yield ("rdd", RddNode("MapPartitionsRDD", scope, [f])))
        raise TypeError(f"execute() of {n.node_name} is not modelled")

    def _execute_broadcast(self, build: SparkPlan):
        """InputAdapter.executeBroadcast -> BroadcastQueryStage -> BroadcastExchange.executeBroadcast."""
        yield ("scope", "InputAdapter")
        yield ("scope", "BroadcastExchange")

    # ------------------------------------------------------------------ CodegenSupport

    def _produce(self, n: SparkPlan, pending: list[SparkPlan]):
        yield ("scope", n.node_name)
        if isinstance(n, BroadcastHashJoin):
            yield from self._produce(n.children[0], pending)
            pending.append(n.children[1])
        elif isinstance(n, (Filter, Project, HashAggregate, Sort)):
            yield from self._produce(n.children[0], pending)
        # ColumnarToRow, InputAdapter (InputRDDCodegen) and SortMergeJoin read their inputs directly

    def _input_rdds(self, n: SparkPlan):
        if isinstance(n, (Filter, Project, HashAggregate, Sort)):
            return (yield from self._input_rdds(n.children[0]))
        if isinstance(n, BroadcastHashJoin):
            return (yield from self._input_rdds(n.children[0]))
        if isinstance(n, ColumnarToRow):
            ia = n.children[0]
            assert isinstance(ia, InputAdapter)
            yield ("scope", "InputAdapter")  # InputAdapter.executeColumnar
            return [(yield from self._execute(ia.children[0]))]
        if isinstance(n, InputAdapter):
            return [(yield from self._execute(n.children[0]))]
        if isinstance(n, SortMergeJoin):
            return [(yield from self._execute(n.children[0])), (yield from self._execute(n.children[1]))]
        raise TypeError(f"inputRDDs() of {n.node_name} is not modelled")


def exchange_of(stage: QueryStage) -> ShuffleExchange:
    return stage.plan
