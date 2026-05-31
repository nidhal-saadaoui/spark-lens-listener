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
    # 20 shuffle partitions: satisfies MinTasks=10 AND gives p50 shuffle bytes > 100 KB
    # so the shuffle-bytes skew signal fires reliably on CI hardware (no timing dependency)
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("\n=== spark-lens integration test: generating skew + join + cache patterns ===\n")

# ── 1. Skew: SortMergeJoin with 90% of rows on a single key ─────────────────
# groupBy uses map-side pre-aggregation (only partial sums flow through shuffle),
# so skew appears in the join's reduce-side shuffle read bytes instead.
# With 5 shuffle partitions, one task reads 900K rows (~14 MB) vs ~400 KB median.
print(">> Job 1: skewed SortMergeJoin (2M rows, 90% on key=1)")
df_a = spark.range(2_000_000).select(
    F.when(F.col("id") < 1_800_000, F.lit(1))
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

# ── 3. Repeated scan — named RDD used in two separate jobs without caching ───
# Using .count() and .reduce() directly on the named RDD keeps "user_events"
# as the top-level RDD in each stage's rddInfos (no PythonRDD wrapper layer),
# so CacheAnalyzer reliably detects the repeated scan.
print(">> Job 3a + 3b: repeated scan of named RDD (no cache)")
rdd = sc.parallelize(range(100_000), 10).setName("user_events")
count1 = rdd.count()                        # job 3a — scans user_events
count2 = rdd.reduce(lambda a, b: a + b)    # job 3b — re-scans user_events
print(f"   count={count1}  sum={count2}")

# ── 4. Window without PARTITION BY → PlanAnalyzer fires ─────────────────────
# AQE disabled so the Exchange SinglePartition stays visible in the physical plan.
print(">> Job 4: Window.orderBy() without partitionBy (all data to one partition)")
from pyspark.sql.window import Window as W
spark.conf.set("spark.sql.adaptive.enabled", "false")
df_w = (spark.range(5_000)
        .withColumn("ts", F.col("id").cast("long"))
        .withColumn("v",  F.col("id").cast("double")))
win  = W.orderBy("ts")                       # no partitionBy → SinglePartition exchange
df_w.withColumn("cum", F.sum("v").over(win)).agg(F.sum("cum")).collect()
print("   window job done")

# ── 5. Cartesian product → PlanAnalyzer Critical ─────────────────────────────
print(">> Job 5: crossJoin() — CartesianProduct in physical plan")
df1 = spark.range(200).withColumn("id", F.col("id").cast("int"))
df2 = spark.range(100).withColumn("id", F.col("id").cast("int")).withColumn("v", F.rand(seed=1))
df1.crossJoin(df2).count()
print("   cartesian job done")

# ── 6. CollectLimit → DriverBottleneckAnalyzer (Info) ────────────────────────
# limit(N).collect() inserts a CollectLimit node in the physical plan.
print(">> Job 6: limit().collect() — CollectLimit in physical plan")
spark.range(5_000).toDF("id").limit(500).collect()
print("   collect-limit job done")

print("\n=== jobs complete — spark-lens report follows ===\n")
spark.stop()
