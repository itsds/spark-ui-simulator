"""RDD lineage and operation scopes of a DataFrame stage, as the stage DAG in the UI draws them.

The shapes are taken from the real AQE app `ref01-daily-etl-aqe`:

    scan stage      FileScanRDD [Scan parquet ] → MapPartitionsRDD [Scan parquet ]
                    → MapPartitionsRDD [WholeStageCodegen (n)] → MapPartitionsRDD [Exchange]
    reduce stage    ShuffledRowRDD [AQEShuffleRead | Exchange] → MapPartitionsRDD [WholeStageCodegen (n)] → …
    sort-merge join ShuffledRowRDD → MapPartitionsRDD [WSC (a)]   (sort, left)
                    ShuffledRowRDD → MapPartitionsRDD [WSC (b)]   (sort, right)
                    ZippedPartitionsRDD2 [WSC (c)] → MapPartitionsRDD [WSC (c)] → …
    sinks           MapPartitionsRDD [Exchange | WriteFiles | BroadcastExchange | mapPartitionsInternal]

Scope ids are allocated top-down per stage (sink first), RDD ids bottom-up
(sources first), as Spark creates them. Phase 4 derives the same from the
physical plan and adds the SQL plan graph.

`Stage Info."RDD Info"` lists the stage's own RDD first, then its narrow
ancestors in `mutable.HashSet[RDD]` order. RDD hash codes are identity hashes,
so that order differs run to run; we use a seeded shuffle.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from ..eventlog import IdAllocator, RDDInfo, rdd_scope
from ..model.workload import BroadcastSink, ResultSink, ScanSource, ShuffleSink, ShuffleSource, StageSpec, WriteSink


@dataclass
class QueryScopes:
    """Per-query counters: whole-stage-codegen ids restart at 1 for every query."""

    scope_ids: IdAllocator
    codegen_ids: IdAllocator = field(default_factory=lambda: IdAllocator(1))
    exchange_scope: dict[str, int] = field(default_factory=dict)  # exchange name -> scope id of its Exchange node


@dataclass
class _Node:
    name: str
    scope: tuple[int, str] | None
    parents: list[_Node]
    rdd_id: int = -1


def build_lineage(spec: StageSpec, num_partitions: int, callsite: str, scopes: QueryScopes, rdd_ids: IdAllocator,
                  coalesced: dict[str, bool], rng: random.Random) -> tuple[list[RDDInfo], int, str]:
    """Return (RDD infos in StageInfo order, scope id of the stage's top node, its scope name)."""
    sid = scopes.scope_ids

    # ---- top-down scope allocation
    sink = spec.sink
    if isinstance(sink, ShuffleSink):
        top = (sid.next(), "Exchange")
        scopes.exchange_scope[sink.exchange] = top[0]
    elif isinstance(sink, WriteSink):
        top = (sid.next(), "WriteFiles")
    elif isinstance(sink, BroadcastSink):
        top = (sid.next(), "BroadcastExchange")
    else:
        top = (sid.next(), "mapPartitionsInternal")
    join = any(op.kind == "smj" for op in spec.ops)
    # Codegen stage ids are numbered children first (CollapseCodegenStages builds the inner
    # WholeStageCodegenExec before taking the counter for the outer one): join sides, then the join.
    side_ids = [scopes.codegen_ids.next() if (join and i < len(spec.side_ops) and spec.side_ops[i]) else None
                for i in range(len(spec.sources))]
    main_wsc = (sid.next(), f"WholeStageCodegen ({scopes.codegen_ids.next()})") if (spec.ops or not join) else None
    sid.skip(rng.randint(2, 5))  # Project / Filter / ColumnarToRow nodes take scope ids without RDDs

    source_nodes: list[_Node] = []
    side_wscs: list[tuple[int, str] | None] = []
    for i, src in enumerate(spec.sources):
        side_wscs.append((sid.next(), f"WholeStageCodegen ({side_ids[i]})") if side_ids[i] else None)
        sid.skip(rng.randint(1, 4))
        if isinstance(src, ScanSource):
            scan = (sid.next(), "Scan parquet ")
            f = _Node("FileScanRDD", scan, [])
            source_nodes.append(_Node("MapPartitionsRDD", scan, [f]))
        elif isinstance(src, ShuffleSource):
            if coalesced.get(src.exchange):
                read = (sid.next(), "AQEShuffleRead")
            else:
                read = (scopes.exchange_scope.get(src.exchange, sid.next()), "Exchange")
            source_nodes.append(_Node("ShuffledRowRDD", read, []))
        sid.skip(rng.randint(0, 2))

    # ---- bottom-up RDD chain
    if join:
        sides = []
        for node, wsc in zip(source_nodes, side_wscs, strict=True):
            sides.append(_Node("MapPartitionsRDD", wsc, [node]) if wsc else node)
        zipped = _Node("ZippedPartitionsRDD2", main_wsc, sides[:2])
        body = _Node("MapPartitionsRDD", main_wsc, [zipped])
    else:
        body = _Node("MapPartitionsRDD", main_wsc, [source_nodes[0]]) if main_wsc else source_nodes[0]
    final = _Node("MapPartitionsRDD", top, [body])

    nodes: list[_Node] = []

    def assign(n: _Node):
        for p in n.parents:
            assign(p)
        if n.rdd_id < 0:
            n.rdd_id = rdd_ids.next()
            nodes.append(n)

    assign(final)
    unordered = any(isinstance(s, ShuffleSource) for s in spec.sources)
    level = "UNORDERED" if unordered else "DETERMINATE"
    infos = {n.rdd_id: RDDInfo(n.rdd_id, n.name, rdd_scope(*n.scope) if n.scope else None, callsite,
                               [p.rdd_id for p in n.parents], deterministic_level=level,
                               num_partitions=num_partitions) for n in nodes}
    ancestors = [i for i in infos if i != final.rdd_id]
    rng.shuffle(ancestors)
    return [infos[final.rdd_id]] + [infos[i] for i in ancestors], top[0], top[1]


def is_result_sink(spec: StageSpec) -> bool:
    return isinstance(spec.sink, (WriteSink, ResultSink, BroadcastSink))
