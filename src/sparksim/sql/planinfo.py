"""`SparkPlanInfo.fromSparkPlan`: the plan graph of the SQL tab, with each metric's accumulator id.

SQL metrics are `lazy val`s, so a node's accumulator ids are allocated the first time anything
reads `plan.metrics`, which for a new node is this conversion when the SQLExecutionStart or an AQE
update is posted. The walk is pre-order (a node's metrics are read before its children are
converted), so new ids grow down the tree; nodes reused from an earlier plan keep theirs. Verified
on all 47 plan events of the reference logs (the only exceptions are Python UDF nodes, whose
metrics are eager).
"""

from __future__ import annotations

from ..eventlog import IdAllocator, SparkPlanInfo
from ..eventlog.common import SQLPlanMetric
from .nodes import SparkPlan, metric_json_order


def allocate_metrics(node: SparkPlan, ids: IdAllocator) -> dict[str, int]:
    if node.metric_ids is None:
        node.metric_ids = {d.key: ids.next() for d in node.metric_defs()}
    return node.metric_ids


def plan_info(node: SparkPlan, ids: IdAllocator) -> SparkPlanInfo:
    mids = allocate_metrics(node, ids)
    metrics = [SQLPlanMetric(d.name, mids[d.key], d.type) for d in metric_json_order(node.metric_defs())]
    children = [plan_info(c, ids) for c in node.info_children()]
    return SparkPlanInfo(node.node_name, node.simple_string(), children, node.metadata(), metrics)
