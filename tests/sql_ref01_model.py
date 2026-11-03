"""`ref01-daily-etl-aqe` execution 0, written with sparksim's DataFrame API (for the golden SQL tests).

    daily = (orders.where(F.col("dt").between("2026-10-03", "2026-10-06"))
             .where(F.col("status") != "RETURNED")
             .join(products, "product_id")
             .join(customers, "customer_id")
             .groupBy("dt", "country", "segment", "category")
             .agg(F.sum(F.col("amount") * F.col("quantity")).alias("revenue"),
                  F.countDistinct("customer_id").alias("buyers"),
                  F.count("*").alias("orders")))
    daily.write.mode("overwrite").partitionBy("dt").parquet(table("out_daily_revenue"))

The analyzer's expression ids of the aggregates are pinned to the real ones; everything else
(plan shape, filters, pruning, codegen, query stages, metric ids) is derived.
"""

from __future__ import annotations

import random

from sparksim.eventlog import IdAllocator
from sparksim.model.workload import WriteSink
from sparksim.sql.aqe import AdaptiveExecution
from sparksim.sql.dataframe import AggSpec, Column, PlanContext, PlanIds, SparkSession, Table
from sparksim.sql.explain import explain_formatted
from sparksim.sql.expr import AggFunc, ExprIds, Lit
from sparksim.sql.planinfo import plan_info

REF = "hdfs://localhost:9000/data/ref"
ORDERS = Table(f"{REF}/orders", (
    Column("order_id", "bigint"), Column("customer_id", "bigint"), Column("product_id", "bigint"),
    Column("quantity", "int"), Column("amount", "double"), Column("status", "string", 7),
    Column("order_ts", "timestamp")), (Column("dt", "date"),))
CUSTOMERS = Table(f"{REF}/customers", (
    Column("customer_id", "bigint"), Column("name", "string", 15), Column("segment", "string", 6),
    Column("country", "string", 2), Column("signup_dt", "date"), Column("email", "string", 28),
    Column("address", "string", 30), Column("phone", "string", 13)))
PRODUCTS = Table(f"{REF}/products", (
    Column("product_id", "bigint"), Column("category", "string", 4), Column("list_price", "double")))

# (sizeInBytes = Exchange "data size", rowCount) of each query stage, from the real log
STATS = {0: (1088576, 5000), 1: (50_666_448, 1_000_000), 2: (96_048_048, 1_714_018), 3: (52_224, 576)}
# AQEShuffleRead partition specs the real run used (24 reducers coalesced to 6, then to 1)
COALESCED = {False: [(i, i + 4) for i in range(0, 24, 4)], True: [(0, 24)]}


def build():
    ids = ExprIds(0)
    spark = SparkSession(ids, {"spark.sql.shuffle.partitions": "24"})
    orders = spark.read(ORDERS)
    customers = spark.read(CUSTOMERS)
    products = spark.read(PRODUCTS)
    dt = orders["dt"]
    a, q = orders["amount"], orders["quantity"]
    daily = (orders.where(dt.ge(Lit("2026-10-03", "date")) & dt.le(Lit("2026-10-06", "date")))
             .where(orders["status"].ne("RETURNED"))
             .join(products, "product_id")
             .join(customers, "customer_id")
             .group_by("dt", "country", "segment", "category")
             .agg(AggSpec(AggFunc("sum", a * q.cast("double")), "revenue", 85, 86, [106]),
                  AggSpec(AggFunc("count", orders["customer_id"], distinct=True), "buyers", 90, 87, [111]),
                  AggSpec(AggFunc("count", None), "orders", 88, 89, [108]),
                  groups=None))
    write = daily.write_parquet(f"{REF}/out_daily_revenue", ["dt"], WriteSink(f"{REF}/out_daily_revenue", 1.0),
                                path_option="hdfs:///data/ref/out_daily_revenue")
    ctx = PlanContext(spark, PlanIds(random.Random(1)),
                      dataset_stats={f"{REF}/products": 1_000_000})
    return AdaptiveExecution(write, ctx, coalescer=lambda stages: None)


def plan_events():
    """The 5 plan events of execution 0 as (physicalPlanDescription, SparkPlanInfo), replaying ref01's
    sequence: accumulator ids 106.. (jobs 0-2 took 1-105), 35 task-metric ids per submitted stage."""
    aqe = build()
    acc = IdAllocator(106)
    final_round = [False]
    aqe.coalescer = lambda stages: COALESCED[final_round[0]]
    out = [(explain_formatted(aqe.root), plan_info(aqe.root, acc))]

    def update():
        out.append((explain_formatted(aqe.root), plan_info(aqe.root, acc)))

    new = aqe.create_stages()  # broadcast products (0), shuffle customers (1)
    assert [s.stage_id for s in new] == [0, 1]
    update()
    acc.skip(70)  # stages 3 and 4 submitted
    new[0].stats = STATS[0]  # the broadcast finished first
    aqe.replan()
    new = aqe.create_stages()  # shuffle orders (2)
    update()
    acc.skip(35)  # stage 5
    aqe.stages[1].stats = STATS[1]
    aqe.replan()
    assert aqe.create_stages() == []
    aqe.stages[2].stats = STATS[2]
    aqe.replan()
    new = aqe.create_stages()  # partial aggregate exchange (3)
    update()
    acc.skip(35)  # stage 8
    aqe.stages[3].stats = STATS[3]
    aqe.replan()
    assert aqe.create_stages() == [] and aqe.all_materialized
    final_round[0] = True
    aqe.finalize()
    update()
    return out


def lineage_rounds(seed: int = 0):
    """RDD scopes of execution 0's stages, replaying ref01: scope ids from 13, RDD ids from 6."""
    from sparksim.sql.scopes import ScopeSimulator

    aqe = build()
    final_round = [False]
    aqe.coalescer = lambda stages: COALESCED[final_round[0]]
    sim = ScopeSimulator(IdAllocator(13), IdAllocator(6), random.Random(seed))
    sim.skip(2)  # 13, 14: opened by the driver before the first stage
    rounds = []
    new = aqe.create_stages()
    rounds.append(sim.materialize(new))
    new[0].stats = STATS[0]
    sim.skip(4)  # 27-30: opened after the broadcast completed (observed)
    aqe.replan()
    rounds.append(sim.materialize(aqe.create_stages()))
    aqe.stages[1].stats, aqe.stages[2].stats = STATS[1], STATS[2]
    aqe.replan()
    rounds.append(sim.materialize(aqe.create_stages()))
    aqe.stages[3].stats = STATS[3]
    aqe.replan()
    aqe.create_stages()
    final_round[0] = True
    aqe.finalize()
    rounds.append({"final": sim.materialize_write(aqe.root.current)})
    return aqe, rounds
