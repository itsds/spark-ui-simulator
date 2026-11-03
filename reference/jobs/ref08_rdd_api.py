"""ref08 — pure RDD API (no SQL tab entries): narrow/wide transformations,
groupByKey vs reduceByKey, sortByKey (range partitioning sample job), join."""

from operator import add

from common import session

spark = session("ref08-rdd-api")
sc = spark.sparkContext

pairs = sc.parallelize(range(2_000_000), 12).map(lambda x: (x % 20_000, x % 97))

sc.setJobDescription("reduceByKey")
pairs.reduceByKey(add).count()

sc.setJobDescription("groupByKey")
pairs.groupByKey().mapValues(len).count()

sc.setJobDescription("sortByKey")
pairs.reduceByKey(add).sortByKey(ascending=False).take(10)

sc.setJobDescription("rdd join")
other = sc.parallelize(range(20_000), 6).map(lambda k: (k, f"name_{k}"))
pairs.reduceByKey(add).join(other).count()

spark.stop()
