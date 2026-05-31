"""
spark-lens integration test job.

Deliberately generates three detectable patterns:
  1. Data skew   — 90% of rows on "hot_key", triggers SkewAnalyzer
  2. SortMergeJoin with broadcast disabled — triggers JoinAnalyzer
  3. Repeated scan of the same named RDD — triggers CacheAnalyzer
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
    # fewer shuffle partitions → higher bytes per partition → bytes-based skew signal fires
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("\n=== spark-lens integration test: generating skew + join + cache patterns ===\n")

# ── 1. Skew: SortMergeJoin with 90% of rows on a single key ─────────────────
# groupBy uses map-side pre-aggregation (only partial sums flow through shuffle),
# so skew appears in the join's reduce-side shuffle read bytes instead.
# With 5 shuffle partitions, one task reads 900K rows (~14 MB) vs ~400 KB median.
print(">> Job 1: skewed SortMergeJoin (1M rows, 90% on key=1)")
df_a = spark.range(1_000_000).select(
    F.when(F.col("id") < 900_000, F.lit(1))
     .otherwise((F.col("id") % 100 + 1).cast("int"))
     .alias("key"),
    F.col("id").alias("val_a"),
)
df_b = spark.range(100).select(
    (F.col("id") + 1).cast("int").alias("key"),
    (F.col("id") * 10).alias("val_b"),
)
# autoBroadcastJoinThreshold=-1 forces SortMergeJoin → full rows in shuffle
result1 = df_a.join(df_b, "key").agg(F.sum("val_a")).collect()[0][0]
print(f"   total: {result1}")

# ── 2. SortMergeJoin with broadcast disabled ─────────────────────────────────
print(">> Job 2: forced SortMergeJoin (autoBroadcastJoinThreshold=-1)")
orders   = spark.range(50_000).withColumn("product_id", (F.col("id") % 20).cast("int"))
products = spark.range(20).withColumn("product_id", F.col("id").cast("int")) \
               .withColumn("name", F.concat(F.lit("product_"), F.col("id")))
result2 = orders.join(products, "product_id").agg(F.count("*")).collect()[0][0]
print(f"   joined rows: {result2}")

# ── 3. Repeated scan — named RDD read twice across two jobs ──────────────────
# Naming the RDD exposes "user_events" in StageInfo.rddInfos, which
# CacheAnalyzer uses to track re-scanned datasets.
print(">> Job 3a + 3b: repeated scan of named RDD (no cache)")
rdd = sc.parallelize(range(100_000), 10).setName("user_events")
count1 = rdd.filter(lambda x: x % 2 == 0).count()   # job 3a
count2 = rdd.map(lambda x: x * 2).sum()              # job 3b — re-scans user_events
print(f"   even count={count1}  double_sum={count2}")

print("\n=== jobs complete — spark-lens report follows ===\n")
spark.stop()
