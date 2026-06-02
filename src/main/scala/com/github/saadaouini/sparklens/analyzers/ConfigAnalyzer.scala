package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object ConfigAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()
    val p      = app.sparkProperties

    def get(key: String)    = p.get(key)
    def getOrElse(key: String, default: String) = p.getOrElse(key, default)

    if (getOrElse("spark.sql.adaptive.enabled", "false").toLowerCase != "true") {
      issues += Issue(
        id              = "config-aqe-disabled",
        severity        = Warning,
        category        = "config",
        title           = "Adaptive Query Execution (AQE) Is Disabled",
        description     = "AQE automatically coalesces shuffle partitions, handles skew, and switches join strategies at runtime. Disabling it forces static planning.",
        recommendation  = "Enable AQE for all production workloads on Spark 3.x.",
        configFix       = Some("spark.sql.adaptive.enabled=true"),
        estimatedImpact = Some(configRisk),
      )
    }

    val serializer = getOrElse("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    if (serializer.contains("JavaSerializer")) {
      issues += Issue(
        id              = "config-java-serializer",
        severity        = Warning,
        category        = "config",
        title           = "Java Serializer in Use — Switch to Kryo",
        description     = "Java serialization is 10× slower than Kryo and produces 2–10× larger byte arrays. Every JVM shuffle write, broadcast variable, and RDD persist pays this cost. Note: Python closures in PySpark are always serialized by cloudpickle regardless of this setting — the benefit applies to JVM-side objects (Scala/Java RDDs, broadcast variables, accumulators).",
        recommendation  = "Switch to Kryo for JVM workloads. Register your domain classes for maximum performance (unregistered classes fall back to class-name strings which waste space). For pure PySpark jobs this is a minor improvement; focus on Arrow/pandas UDFs to reduce Python serialization overhead instead.",
        codeFix         = Some("spark.conf.set(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\")\n// optionally: spark.conf.set(\"spark.kryo.registrationRequired\", \"false\")"),
        configFix       = Some("spark.serializer=org.apache.spark.serializer.KryoSerializer"),
        estimatedImpact = Some(configRisk),
      )
    }

    val shufflePartitions = getOrElse("spark.sql.shuffle.partitions", "200")
    if (shufflePartitions == "200" && getOrElse("spark.sql.adaptive.enabled", "false").toLowerCase != "true") {
      issues += Issue(
        id              = "config-default-shuffle-partitions",
        severity        = Info,
        category        = "config",
        title           = "Default Shuffle Partitions (200) — May Be Too Few or Too Many",
        description     = "The default of 200 shuffle partitions is rarely optimal. Too few causes large tasks and spill; too many causes overhead from small tasks.",
        recommendation  = "Enable AQE to auto-tune, or set to 2–3× the number of executor cores in your cluster.",
        configFix       = Some("spark.sql.adaptive.enabled=true  # lets AQE pick the right value"),
        estimatedImpact = Some(configRisk),
      )
    }

    val executorMemory  = getOrElse("spark.executor.memory", "1g")
    val memoryOverhead  = get("spark.executor.memoryOverhead")
    val overheadFactor  = getOrElse("spark.executor.memoryOverheadFactor", "0.1")
    val overheadFactorDbl = scala.util.Try(overheadFactor.toDouble).getOrElse(0.1)
    if (memoryOverhead.isEmpty && overheadFactorDbl < 0.15) {
      issues += Issue(
        id              = "config-low-memory-overhead",
        severity        = Info,
        category        = "config",
        title           = "Low Executor Memory Overhead — Risk of Off-Heap OOM",
        description     = s"Executor memory is $executorMemory with only ${(overheadFactorDbl * 100).toInt}% overhead. Native memory usage (Python UDFs, Arrow, native libs) can exceed this.",
        recommendation  = "Set memoryOverhead to at least 10% of executor memory, or 512 MB minimum for PySpark jobs.",
        configFix       = Some("spark.executor.memoryOverheadFactor=0.2"),
        estimatedImpact = Some(configRisk),
      )
    }

    if (getOrElse("spark.sql.adaptive.enabled", "false").toLowerCase == "true" &&
        getOrElse("spark.sql.adaptive.skewJoin.enabled", "true").toLowerCase != "true") {
      issues += Issue(
        id              = "config-aqe-skew-disabled",
        severity        = Info,
        category        = "config",
        title           = "AQE Skew Join Optimization Disabled",
        description     = "AQE is enabled but the skew join sub-feature is disabled. Skewed join partitions will not be automatically split.",
        recommendation  = "Re-enable AQE skew join handling.",
        configFix       = Some("spark.sql.adaptive.skewJoin.enabled=true"),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── shuffle.file.buffer ──────────────────────────────────────────────────
    // Default 32k means Spark flushes the shuffle write buffer to disk every 32 KB.
    // At 32k Spark does ~30× more I/O syscalls than at 1 MB for a 30 MB shuffle file.
    val shuffleBuf = getOrElse("spark.shuffle.file.buffer", "32k")
    val shuffleBufKb = parseKilobytes(shuffleBuf).getOrElse(32L)
    if (shuffleBufKb < 64L) {
      issues += Issue(
        id              = "config-small-shuffle-buffer",
        severity        = Info,
        category        = "config",
        title           = s"Small Shuffle Write Buffer ($shuffleBuf) — Excessive Disk Syscalls",
        description     = s"spark.shuffle.file.buffer=${shuffleBuf} causes Spark to flush the shuffle write buffer to disk too frequently, adding I/O overhead for every shuffle-heavy stage.",
        recommendation  = "Increase to at least 1 MB. This reduces the number of syscalls on each shuffle write without increasing memory use significantly (one buffer per active output partition, default 200).",
        configFix       = Some("spark.shuffle.file.buffer=1m"),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── CBO histogram statistics ─────────────────────────────────────────────
    if (getOrElse("spark.sql.statistics.histogram.enabled", "false").toLowerCase != "true") {
      issues += Issue(
        id              = "config-cbo-histogram-disabled",
        severity        = Info,
        category        = "config",
        title           = "CBO Column Histograms Disabled — Join Order May Be Suboptimal",
        description     = "Without column histograms, the cost-based optimizer uses only row count and total size to estimate selectivity. This can lead to poor join ordering and missed broadcast opportunities for skewed distributions.",
        recommendation  = "Enable histograms and rerun ANALYZE TABLE after enabling.",
        configFix       = Some(
          "spark.sql.statistics.histogram.enabled=true\n" +
          "-- then: ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS"
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── task.maxFailures ─────────────────────────────────────────────────────
    val maxFailures = scala.util.Try(getOrElse("spark.task.maxFailures", "4").toInt).getOrElse(4)
    if (maxFailures < 3) {
      issues += Issue(
        id              = "config-low-task-max-failures",
        severity        = Warning,
        category        = "config",
        title           = s"Low spark.task.maxFailures ($maxFailures) — Transient Errors Will Abort Jobs",
        description     = s"With maxFailures=$maxFailures, a task that fails due to a transient network error, GC pause, or preempted executor will abort the entire job with as few as $maxFailures attempts. Cloud environments and shared clusters typically need 4–8.",
        recommendation  = "Raise to at least 4. In cloud environments with spot/preemptible instances, consider 8.",
        configFix       = Some("spark.task.maxFailures=4"),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── locality.wait ────────────────────────────────────────────────────────
    val localityWait = getOrElse("spark.locality.wait", "3s")
    val localityWaitMs = parseDurationMs(localityWait).getOrElse(3000L)
    if (localityWaitMs > 5000L) {
      issues += Issue(
        id              = "config-high-locality-wait",
        severity        = Info,
        category        = "config",
        title           = s"High spark.locality.wait ($localityWait) — Tasks May Be Delayed",
        description     = s"Spark waits up to ${localityWait} for a data-local executor slot before relaxing locality. In dynamic or heavily loaded clusters this can stall task launch for seconds per stage.",
        recommendation  = "Reduce to 1s or less. Modern cloud storage (S3, GCS, ADLS) has uniform access time so data-locality optimisation provides no benefit and only adds scheduling delay.",
        configFix       = Some("spark.locality.wait=1s"),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── reducer.maxReqsInFlight ──────────────────────────────────────────────
    val maxReqs = getOrElse("spark.reducer.maxReqsInFlight", "Int.MaxValue")
    val maxReqsVal = if (maxReqs == "Int.MaxValue") Int.MaxValue
                     else scala.util.Try(maxReqs.toInt).getOrElse(Int.MaxValue)
    val totalCores = app.executors.values.map(_.totalCores).sum
    // Rule of thumb: if more than 1000 concurrent fetch requests possible, overwhelm shuffle service
    if (maxReqsVal == Int.MaxValue && totalCores > 200) {
      issues += Issue(
        id              = "config-max-reqs-in-flight",
        severity        = Info,
        category        = "config",
        title           = "Unlimited Shuffle Fetch Requests — Risk of OOM on Large Clusters",
        description     = s"With $totalCores cores and spark.reducer.maxReqsInFlight unlimited, a shuffle stage can open thousands of simultaneous network connections, overwhelming the shuffle service and causing reducer OOM or request timeouts.",
        recommendation  = "Cap concurrent fetch requests to prevent connection storm. A value of 500–1000 is safe for most clusters.",
        configFix       = Some("spark.reducer.maxReqsInFlight=500"),
        estimatedImpact = Some(configRisk),
      )
    }

    issues.toSeq
  }

  private def parseKilobytes(s: String): Option[Long] = {
    val lower = s.trim.toLowerCase
    scala.util.Try {
      if      (lower.endsWith("m")) lower.dropRight(1).toLong * 1024L
      else if (lower.endsWith("k")) lower.dropRight(1).toLong
      else                          lower.toLong / 1024L
    }.toOption
  }

  private def parseDurationMs(s: String): Option[Long] = {
    val lower = s.trim.toLowerCase
    scala.util.Try {
      if      (lower.endsWith("ms")) lower.dropRight(2).toLong
      else if (lower.endsWith("s"))  lower.dropRight(1).toLong * 1000L
      else if (lower.endsWith("m"))  lower.dropRight(1).toLong * 60000L
      else                           lower.toLong
    }.toOption
  }
}
