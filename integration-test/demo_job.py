"""
spark-lens integration test job.

Deliberately generates three detectable patterns:
  1. Data skew   — 90% of rows on "hot_key", triggers SkewAnalyzer
  2. SortMergeJoin with broadcast disabled — triggers JoinAnalyzer
  3. Repeated scan of the same RDD name — triggers CacheAnalyzer
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("spark-lens integration test")
    # disable AQE so skew and join issues are not auto-fixed
    .config("spark.sql.adaptive.enabled", "false")
    # force SortMergeJoin (no broadcast)
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("\n=== spark-lens integration test: generating skew + join + cache patterns ===\n")

# ── 1. Skew: 90% of rows on a single key ────────────────────────────────────
print(">> Job 1: skewed groupBy")
rows = (
    [("hot_key", i) for i in range(90_000)] +
    [("key_" + str(i % 100), i) for i in range(10_000)]
)
df_skewed = spark.createDataFrame(rows, ["key", "value"])
result1 = df_skewed.groupBy("key").agg(F.sum("value").alias("total")).count()
print(f"   distinct keys: {result1}")

# ── 2. SortMergeJoin with broadcast disabled ─────────────────────────────────
print(">> Job 2: forced SortMergeJoin (autoBroadcastJoinThreshold=-1)")
orders   = spark.range(50_000).withColumn("product_id", (F.col("id") % 20).cast("int"))
products = spark.range(20).withColumn("product_id", F.col("id").cast("int")) \
               .withColumn("name", F.concat(F.lit("product_"), F.col("id")))
result2 = orders.join(products, "product_id").agg(F.count("*")).collect()[0][0]
print(f"   joined rows: {result2}")

# ── 3. Repeated scan (same base DataFrame, two separate jobs) ─────────────────
print(">> Job 3a + 3b: repeated scan of same RDD (no cache)")
base = spark.range(100_000).withColumn("v", F.rand())
agg1 = base.agg(F.sum("v")).collect()[0][0]   # job 3a
agg2 = base.agg(F.mean("v")).collect()[0][0]  # job 3b — re-scans base
print(f"   sum={agg1:.2f}  mean={agg2:.6f}")

print("\n=== jobs complete — spark-lens report follows ===\n")
spark.stop()
