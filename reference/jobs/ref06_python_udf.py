"""ref06 — Python execution: row-at-a-time UDF (BatchEvalPython) vs pandas UDF
(ArrowEvalPython) vs native functions, plus mapInPandas (MapInPandas)."""

import pandas as pd
from common import session, table
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

spark = session("ref06-python-udf")
sc = spark.sparkContext
orders = spark.read.parquet(table("orders")).where(F.col("dt") == "2026-10-02")


@F.udf(DoubleType())
def tax_row(amount, quantity):
    return round(amount * quantity * 1.18, 2)


@F.pandas_udf(DoubleType())
def tax_vec(amount: pd.Series, quantity: pd.Series) -> pd.Series:
    return (amount * quantity * 1.18).round(2)


sc.setJobDescription("python row UDF")
orders.select(tax_row("amount", "quantity").alias("t")).agg(F.sum("t")).collect()

sc.setJobDescription("pandas UDF")
orders.select(tax_vec("amount", "quantity").alias("t")).agg(F.sum("t")).collect()

sc.setJobDescription("native expression")
orders.select(F.round(F.col("amount") * F.col("quantity") * 1.18, 2).alias("t")).agg(F.sum("t")).collect()


def big_orders(batches):
    for pdf in batches:
        yield pdf[pdf["amount"] > 400][["order_id", "amount"]]


sc.setJobDescription("mapInPandas")
orders.mapInPandas(big_orders, "order_id long, amount double").count()

spark.stop()
