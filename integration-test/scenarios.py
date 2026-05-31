"""
spark-lens benchmark scenarios.
Usage: spark-submit scenarios.py <scenario_id> <phase>

Scenario IDs: skew, cache, join_broadcast, join_shuffles,
              config_aqe, config_serializer, plan_cartesian,
              plan_window, driver_collect
Phases: before, after
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCENARIO = sys.argv[1]
PHASE    = sys.argv[2]

# ─── skew ────────────────────────────────────────────────────────────────────

def run_skew_before(spark):
    """SortMergeJoin with 90% of rows on key=1, 5 shuffle partitions, AQE off."""
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
    result = df_a.join(df_b, "key").agg(F.sum("val_a")).collect()[0][0]
    print(f"   skew-before total: {result}")


def run_skew_after(spark):
    """Same join but with AQE + skewJoin enabled so Spark auto-splits skewed partitions."""
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
    result = df_a.join(df_b, "key").agg(F.sum("val_a")).collect()[0][0]
    print(f"   skew-after total: {result}")


# ─── cache ───────────────────────────────────────────────────────────────────

def run_cache_before(spark):
    """Named RDD used in two separate jobs without caching."""
    sc = spark.sparkContext
    rdd = sc.parallelize(range(200_000), 10).setName("user_events")
    count1 = rdd.filter(lambda x: x % 2 == 0).count()
    total2 = rdd.map(lambda x: x * 2).sum()
    print(f"   cache-before: count={count1} sum={total2}")


def run_cache_after(spark):
    """Same RDD but cached before the first action and unpersisted after."""
    sc = spark.sparkContext
    rdd = sc.parallelize(range(200_000), 10).setName("user_events").cache()
    rdd.count()          # materialise
    count1 = rdd.filter(lambda x: x % 2 == 0).count()
    total2 = rdd.map(lambda x: x * 2).sum()
    rdd.unpersist()
    print(f"   cache-after: count={count1} sum={total2}")


# ─── join_broadcast ──────────────────────────────────────────────────────────

def run_join_broadcast_before(spark):
    """autoBroadcastJoinThreshold=-1 forces SortMergeJoin even for tiny table."""
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    orders   = spark.range(100_000).withColumn("product_id", (F.col("id") % 20).cast("int"))
    products = spark.range(20).withColumn("product_id", F.col("id").cast("int")) \
                   .withColumn("name", F.concat(F.lit("product_"), F.col("id")))
    result = orders.join(products, "product_id").agg(F.count("*")).collect()[0][0]
    print(f"   join_broadcast-before joined rows: {result}")


def run_join_broadcast_after(spark):
    """Re-enable broadcast threshold so Spark broadcasts the small products table."""
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
    orders   = spark.range(100_000).withColumn("product_id", (F.col("id") % 20).cast("int"))
    products = spark.range(20).withColumn("product_id", F.col("id").cast("int")) \
                   .withColumn("name", F.concat(F.lit("product_"), F.col("id")))
    result = orders.join(products, "product_id").agg(F.count("*")).collect()[0][0]
    print(f"   join_broadcast-after joined rows: {result}")


# ─── join_shuffles ───────────────────────────────────────────────────────────

def run_join_shuffles_before(spark):
    """Multiple joins + groupBys producing ≥4 shuffle exchanges, AQE off."""
    # Ensure AQE is off so plan is static and exchange count is preserved
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    a = spark.range(100_000).withColumn("k", (F.col("id") % 100).cast("int"))
    b = spark.range(100).withColumn("k", F.col("id").cast("int")).withColumn("v", F.rand(seed=42))
    c = spark.range(100).withColumn("k", F.col("id").cast("int")).withColumn("w", F.rand(seed=7))
    result = (
        a.join(b, "k")
         .join(c, "k")
         .groupBy("k")
         .agg(F.sum("v").alias("sv"), F.sum("w").alias("sw"))
         .count()
    )
    print(f"   join_shuffles-before groups: {result}")


def run_join_shuffles_after(spark):
    """Enable AQE so Spark can coalesce and optimise shuffle exchanges."""
    a = spark.range(100_000).withColumn("k", (F.col("id") % 100).cast("int"))
    b = spark.range(100).withColumn("k", F.col("id").cast("int")).withColumn("v", F.rand(seed=42))
    c = spark.range(100).withColumn("k", F.col("id").cast("int")).withColumn("w", F.rand(seed=7))
    result = (
        a.join(b, "k")
         .join(c, "k")
         .groupBy("k")
         .agg(F.sum("v").alias("sv"), F.sum("w").alias("sw"))
         .count()
    )
    print(f"   join_shuffles-after groups: {result}")


# ─── config_aqe ──────────────────────────────────────────────────────────────

def run_config_aqe_before(spark):
    """AQE is disabled at builder time — ConfigAnalyzer fires."""
    result = spark.range(50_000).groupBy((F.col("id") % 50).alias("k")).count().count()
    print(f"   config_aqe-before groups: {result}")


def run_config_aqe_after(spark):
    """AQE is enabled at builder time — ConfigAnalyzer AQE issue resolved."""
    result = spark.range(50_000).groupBy((F.col("id") % 50).alias("k")).count().count()
    print(f"   config_aqe-after groups: {result}")


# ─── config_serializer ───────────────────────────────────────────────────────

def run_config_serializer_before(spark):
    """Use default Java serializer — ConfigAnalyzer Java-serializer issue fires."""
    # Default is JavaSerializer; we do an RDD shuffle to exercise it
    sc = spark.sparkContext
    result = sc.parallelize(range(50_000), 10) \
               .map(lambda x: (x % 100, x)) \
               .groupByKey() \
               .mapValues(list) \
               .count()
    print(f"   config_serializer-before groups: {result}")


def run_config_serializer_after(spark):
    """Kryo serializer is set at builder time — Java-serializer issue resolved."""
    sc = spark.sparkContext
    result = sc.parallelize(range(50_000), 10) \
               .map(lambda x: (x % 100, x)) \
               .groupByKey() \
               .mapValues(list) \
               .count()
    print(f"   config_serializer-after groups: {result}")


# ─── plan_cartesian ──────────────────────────────────────────────────────────

def run_plan_cartesian_before(spark):
    """crossJoin() forces CartesianProduct node in physical plan."""
    df1 = spark.range(1_000).withColumn("id", F.col("id").cast("int"))
    df2 = spark.range(100).withColumn("id", F.col("id").cast("int")).withColumn("val", F.rand(seed=1))
    result = df1.crossJoin(df2).count()
    print(f"   plan_cartesian-before rows: {result}")


def run_plan_cartesian_after(spark):
    """Explicit join condition eliminates CartesianProduct."""
    df1 = spark.range(1_000).withColumn("id", F.col("id").cast("int"))
    df2 = spark.range(100).withColumn("id", F.col("id").cast("int")).withColumn("val", F.rand(seed=1))
    result = df1.join(df2, "id").count()
    print(f"   plan_cartesian-after rows: {result}")


# ─── plan_window ─────────────────────────────────────────────────────────────

def run_plan_window_before(spark):
    """Window.orderBy without partitionBy → single-partition execution."""
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    df = spark.range(10_000).withColumn(
        "user_id", (F.col("id") % 100).cast("int")
    ).withColumn(
        "ts", F.col("id").cast("long")
    )
    # Window without partitionBy — all data goes to one partition
    w = Window.orderBy("ts")
    result = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= 5).count()
    print(f"   plan_window-before rows: {result}")


def run_plan_window_after(spark):
    """Add partitionBy to distribute window computation."""
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    df = spark.range(10_000).withColumn(
        "user_id", (F.col("id") % 100).cast("int")
    ).withColumn(
        "ts", F.col("id").cast("long")
    )
    # Window WITH partitionBy — distributed across partitions
    w = Window.partitionBy("user_id").orderBy("ts")
    result = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= 5).count()
    print(f"   plan_window-after rows: {result}")


# ─── driver_collect ──────────────────────────────────────────────────────────

def run_driver_collect_before(spark):
    """limit(N).collect() adds CollectLimit to the physical plan → DriverBottleneckAnalyzer Info fires.
    Note: resultSize-based Warning requires cluster mode; CollectLimit check works locally."""
    df = spark.range(5_000_000).withColumn("val", F.rand(seed=42))
    rows = df.limit(500_000).collect()
    print(f"   driver_collect-before rows fetched: {len(rows)}")


def run_driver_collect_after(spark):
    """Write to parquet instead of take() — no CollectLimit plan node, issue resolved."""
    df = spark.range(500_000).withColumn("val", F.rand(seed=42))
    df.write.mode("overwrite").parquet("/tmp/spark-lens-output")
    print(f"   driver_collect-after: written to parquet")


# ─── dispatch ────────────────────────────────────────────────────────────────

SCENARIOS = {
    "skew":             {"before": run_skew_before,              "after": run_skew_after},
    "cache":            {"before": run_cache_before,             "after": run_cache_after},
    "join_broadcast":   {"before": run_join_broadcast_before,    "after": run_join_broadcast_after},
    "join_shuffles":    {"before": run_join_shuffles_before,     "after": run_join_shuffles_after},
    "config_aqe":       {"before": run_config_aqe_before,        "after": run_config_aqe_after},
    "config_serializer":{"before": run_config_serializer_before, "after": run_config_serializer_after},
    "plan_cartesian":   {"before": run_plan_cartesian_before,    "after": run_plan_cartesian_after},
    "plan_window":      {"before": run_plan_window_before,       "after": run_plan_window_after},
    "driver_collect":   {"before": run_driver_collect_before,    "after": run_driver_collect_after},
}

if SCENARIO not in SCENARIOS:
    print(f"Unknown scenario: {SCENARIO}. Available: {list(SCENARIOS.keys())}", file=sys.stderr)
    sys.exit(1)
if PHASE not in ("before", "after"):
    print(f"Unknown phase: {PHASE}. Use 'before' or 'after'.", file=sys.stderr)
    sys.exit(1)

# ─── spark session ────────────────────────────────────────────────────────────
# Baseline: worst-case defaults so analyzers fire.
# Static configs (spark.serializer) and configs read by SparkListenerEnvironmentUpdate
# (spark.sql.adaptive.enabled) MUST be set at builder time — spark.conf.set() after
# session start is either silently ignored or raises CANNOT_MODIFY_CONFIG.

BUILDER_OVERRIDES = {
    # skew-after: AQE + skewJoin so Spark auto-splits the hot partition
    ("skew",              "after"): {
        "spark.sql.adaptive.enabled":         "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    },
    # join_shuffles-after: AQE so ConfigAnalyzer sees it enabled
    ("join_shuffles",     "after"): {"spark.sql.adaptive.enabled": "true"},
    # config_aqe-after: AQE on so ConfigAnalyzer AQE issue is gone
    ("config_aqe",        "after"): {"spark.sql.adaptive.enabled": "true"},
    # config_serializer-after: Kryo is a static config, must be set at builder time
    ("config_serializer", "after"): {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    },
}

builder = (
    SparkSession.builder
    .appName(f"{SCENARIO}-{PHASE}")
    .config("spark.sql.adaptive.enabled",          "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions",         "5")
    .config("spark.sql.crossJoin.enabled",          "true")
)
for k, v in BUILDER_OVERRIDES.get((SCENARIO, PHASE), {}).items():
    builder = builder.config(k, v)
spark = builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

SCENARIOS[SCENARIO][PHASE](spark)
spark.stop()
