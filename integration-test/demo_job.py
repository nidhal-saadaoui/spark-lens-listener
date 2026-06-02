"""
spark-lens integration test job.

Deliberately generates detectable patterns for the CI verification step:
  1. Data skew (shuffle bytes) — triggers SkewAnalyzer
  2. SortMergeJoin with broadcast disabled — triggers JoinAnalyzer (Broadcast)
  3. Repeated scan of parquet file 3× — triggers CacheAnalyzer (needs minExecCount=3 conf)
  4. Window without PARTITION BY — triggers PlanAnalyzer (Window)
  5. Excessive shuffles (4 Exchange nodes) — triggers JoinAnalyzer (Shuffles)
  6. Cartesian product — triggers PlanAnalyzer (Critical)
  7. CollectLimit — triggers DriverBottleneckAnalyzer
  8. Python UDF — triggers UdfAnalyzer
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

# ── 3. Repeated scan — same parquet file read 3× without caching ─────────────
# Writes a small parquet file then reads it three times via separate SQL
# executions.  With spark.sparklens.cache.sql.minExecCount=3 (set via --conf in
# the CI spark-submit command) CacheAnalyzer fires the Repeated Scan warning.
# Using a real FileScan (not in-memory range) ensures the path appears in the
# physical plan and the FileScan regex in CacheAnalyzer matches it.
_rspath = "/tmp/spark-lens-repeated-scan.parquet"
spark.range(5_000).withColumn("v", F.rand(seed=7)).write.mode("overwrite").parquet(_rspath)
print(">> Job 3a+3b+3c: repeated scan of parquet file (no cache)")
rs1 = spark.read.parquet(_rspath).agg(F.count("*")).collect()[0][0]
rs2 = spark.read.parquet(_rspath).agg(F.sum("v")).collect()[0][0]
rs3 = spark.read.parquet(_rspath).agg(F.avg("v")).collect()[0][0]
print(f"   count={rs1}  sum={rs2:.1f}  avg={rs3:.4f}")

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

# ── 5. Excessive Shuffles → JoinAnalyzer Warning ─────────────────────────────
# Three-way join + groupBy on a DIFFERENT key forces 4 shuffle Exchange nodes:
# Exchange for a, Exchange for b, Exchange for c (second join right side),
# Exchange for groupBy k2 (different from the join key k).  AQE is already off.
print(">> Job 5: 3-way join + groupBy different key — 4 shuffle exchanges")
a = spark.range(100_000).withColumn("k",  (F.col("id") % 100).cast("int")) \
                         .withColumn("k2", (F.col("id") % 50).cast("int"))
b = spark.range(100).withColumn("k", F.col("id").cast("int")).withColumn("v", F.rand(seed=1))
c = spark.range(100).withColumn("k", F.col("id").cast("int")).withColumn("w", F.rand(seed=2))
(a.join(b, "k").join(c, "k")
  .groupBy("k2")
  .agg(F.sum("v").alias("sv"))
  .count())
print("   excessive-shuffles job done")

# ── 6. Cartesian product → PlanAnalyzer Critical ─────────────────────────────
print(">> Job 6: crossJoin() — CartesianProduct in physical plan")
df1 = spark.range(200).withColumn("id", F.col("id").cast("int"))
df2 = spark.range(100).withColumn("id", F.col("id").cast("int")).withColumn("v", F.rand(seed=1))
df1.crossJoin(df2).count()
print("   cartesian job done")

# ── 7. CollectLimit → DriverBottleneckAnalyzer (Info) ────────────────────────
# limit(N).collect() inserts a CollectLimit node in the physical plan.
print(">> Job 7: limit().collect() — CollectLimit in physical plan")
spark.range(5_000).toDF("id").limit(500).collect()
print("   collect-limit job done")

# ── 8. Python UDF → UdfAnalyzer Warning ──────────────────────────────────────
# A standard Python UDF produces BatchEvalPython in the physical plan.
# UdfAnalyzer checks for PythonUDF / BatchEvalPython / ArrowEvalPython.
print(">> Job 8: Python UDF — BatchEvalPython in physical plan")
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
double_id = udf(lambda x: x * 2, LongType())
spark.range(10_000).withColumn("doubled", double_id(F.col("id"))).agg(F.sum("doubled")).collect()
print("   udf job done")

print("\n=== jobs complete — spark-lens report follows ===\n")
spark.stop()
