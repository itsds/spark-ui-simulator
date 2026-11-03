"""The SQL / DataFrame layer: physical plans as Spark 3.5 builds, explains and executes them.

    dataframe.py  a small PySpark-like DSL (read.parquet, where, select, join, groupBy/agg, write)
                  and the physical planner (join strategy, EnsureRequirements, partial/final
                  aggregation, planned-write sort)
    expr.py       attributes and expressions, printed exactly like Catalyst's toString
    nodes.py      physical operators: node names, simple strings, explain fields, SQL metrics
    aqe.py        AdaptiveSparkPlanExec: query stages, codegen collapse, AQEShuffleRead, re-planning,
                  the final plan; the engine's StageSpecs come from the query stages
    explain.py    `explain("formatted")` text (`physicalPlanDescription`)
    planinfo.py   `SparkPlanInfo` with metric accumulator ids allocated as Spark does (lazily, pre-order)
    scopes.py     RDD operation scopes and lineage from execute()/produce() calls
    metrics.py    per-task SQL metric updates and driver-side metric updates
"""
