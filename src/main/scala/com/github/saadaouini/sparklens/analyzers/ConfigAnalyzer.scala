package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object ConfigAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()
    val p      = app.sparkProperties

    def get(key: String)    = p.get(key)
    def getOrElse(key: String, default: String) = p.getOrElse(key, default)

    // AQE disabled
    if (getOrElse("spark.sql.adaptive.enabled", "false").toLowerCase != "true") {
      issues += Issue(
        id             = "config-aqe-disabled",
        severity       = Warning,
        category       = "config",
        title          = "Adaptive Query Execution (AQE) Is Disabled",
        description    = "AQE automatically coalesces shuffle partitions, handles skew, and switches join strategies at runtime. Disabling it forces static planning.",
        recommendation = "Enable AQE for all production workloads on Spark 3.x.",
        configFix      = Some("spark.sql.adaptive.enabled=true"),
      )
    }

    // Java serializer
    val serializer = getOrElse("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    if (serializer.contains("JavaSerializer")) {
      issues += Issue(
        id             = "config-java-serializer",
        severity       = Warning,
        category       = "config",
        title          = "Java Serializer in Use — Switch to Kryo",
        description    = "Java serialization is 10× slower than Kryo and produces much larger objects. It is the default but should not be used in production.",
        recommendation = "Switch to Kryo. Register your domain classes for maximum performance.",
        configFix      = Some("spark.serializer=org.apache.spark.serializer.KryoSerializer"),
      )
    }

    // Default shuffle partitions (200)
    val shufflePartitions = getOrElse("spark.sql.shuffle.partitions", "200")
    if (shufflePartitions == "200" && getOrElse("spark.sql.adaptive.enabled", "false").toLowerCase != "true") {
      issues += Issue(
        id             = "config-default-shuffle-partitions",
        severity       = Info,
        category       = "config",
        title          = "Default Shuffle Partitions (200) — May Be Too Few or Too Many",
        description    = "The default of 200 shuffle partitions is rarely optimal. Too few causes large tasks and spill; too many causes overhead from small tasks.",
        recommendation = "Enable AQE to auto-tune, or set to 2–3× the number of executor cores in your cluster.",
        configFix      = Some("spark.sql.adaptive.enabled=true  # lets AQE pick the right value"),
      )
    }

    // Missing memory overhead
    val executorMemory  = getOrElse("spark.executor.memory", "1g")
    val memoryOverhead  = get("spark.executor.memoryOverhead")
    val overheadFactor  = getOrElse("spark.executor.memoryOverheadFactor", "0.1")
    val overheadFactorDbl = scala.util.Try(overheadFactor.toDouble).getOrElse(0.1)
    if (memoryOverhead.isEmpty && overheadFactorDbl < 0.15) {
      issues += Issue(
        id             = "config-low-memory-overhead",
        severity       = Info,
        category       = "config",
        title          = "Low Executor Memory Overhead — Risk of Off-Heap OOM",
        description    = s"Executor memory is $executorMemory with only ${(overheadFactorDbl * 100).toInt}% overhead. Native memory usage (Python UDFs, Arrow, native libs) can exceed this.",
        recommendation = "Set memoryOverhead to at least 10% of executor memory, or 512 MB minimum for PySpark jobs.",
        configFix      = Some("spark.executor.memoryOverheadFactor=0.2"),
      )
    }

    // AQE skewJoin sub-feature
    if (getOrElse("spark.sql.adaptive.enabled", "false").toLowerCase == "true" &&
        getOrElse("spark.sql.adaptive.skewJoin.enabled", "true").toLowerCase != "true") {
      issues += Issue(
        id             = "config-aqe-skew-disabled",
        severity       = Info,
        category       = "config",
        title          = "AQE Skew Join Optimization Disabled",
        description    = "AQE is enabled but the skew join sub-feature is disabled. Skewed join partitions will not be automatically split.",
        recommendation = "Re-enable AQE skew join handling.",
        configFix      = Some("spark.sql.adaptive.skewJoin.enabled=true"),
      )
    }

    issues.toSeq
  }
}
