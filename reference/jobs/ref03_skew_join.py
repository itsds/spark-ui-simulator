"""ref03 — join-key skew, with and without AQE skew-join handling.

orders_skewed has ~30% of rows on customer_id = 0. Query 1 runs with AQE skew
join disabled (one straggler reduce task). Query 2 enables it with thresholds
lowered to this data size, so OptimizeSkewedJoin splits the hot partition
(AQEShuffleRead "skewed" metrics, SparkListenerSQLAdaptiveExecutionUpdate).
"""

from common import session, table
from pyspark.sql import functions as F

spark = session(
    "ref03-skew-join",
    {
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
    },
)
sc = spark.sparkContext

orders = spark.read.parquet(table("orders_skewed"))
customers = spark.read.parquet(table("customers")).union(
    spark.createDataFrame([(0, "guest", "CONSUMER", "US", None, None, None, None)],
                          "customer_id long, name string, segment string, country string, signup_dt date, "
                          "email string, address string, phone string")
)


def revenue_by_segment():
    return (
        orders.join(customers, "customer_id")
        .groupBy("segment", "country")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
        .collect()
    )


sc.setJobDescription("revenue by segment — skew join OFF")
revenue_by_segment()

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "4m")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m")

sc.setJobDescription("revenue by segment — skew join ON")
revenue_by_segment()

spark.stop()
