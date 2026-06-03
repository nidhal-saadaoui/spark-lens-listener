package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object YarnAnalyzer extends Analyzer {

  // ── YARN detection ────────────────────────────────────────────────────────
  // Best-effort: use master URL, YARN-specific properties, or removalReason
  // strings as evidence. Absence of all three means we skip YARN-specific checks.
  private def isYarn(app: SparkAppModel): Boolean =
    app.prop("spark.master").exists(m => m == "yarn" || m.startsWith("yarn")) ||
    app.prop("spark.yarn.queue").isDefined                                      ||
    app.prop("spark.yarn.am.memory").isDefined                                 ||
    app.executors.values.exists(_.removalReason.exists(_.toLowerCase.contains("yarn")))

  // ── Patterns ─────────────────────────────────────────────────────────────
  // Virtual memory OOM — "virtual" distinguishes this from physical OOM (exit 137).
  // YARN message: "Container killed by YARN for exceeding virtual memory limits."
  private val vmemPattern =
    """(?i)(virtual memory|vmem)""".r

  // Disk-full patterns across Linux, JVM, and Spark error messages.
  private val diskFullPattern =
    """(?i)(no space left on device|DiskSpaceException|disk.?full|ENOSPC|failed to allocate.*local|IOException.*disk|diskspace)""".r

  // Python UDF node names in physical plans (covers classic UDFs and pandas/arrow variants).
  private val pythonUdfNodes =
    Set("PythonUDF", "BatchEvalPython", "ArrowEvalPython",
        "FlatMapGroupsInPandas", "MapInPandas", "MapInArrow")

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val issues  = scala.collection.mutable.ArrayBuffer[Issue]()
    val onYarn  = isYarn(app)

    // ── 1. YARN queue wait time ───────────────────────────────────────────
    // Measures the delay between application start and first executor allocation.
    // A long gap means the YARN queue was congested before the job ran a single task.
    if (onYarn && app.executors.nonEmpty) {
      val warnMs  = propLong(app, "spark.sparklens.yarn.queueWaitWarnMs", 30000L)
      val firstMs = app.executors.values.map(_.addedTimeMs).min
      val waitMs  = firstMs - app.startTimeMs

      if (waitMs >= warnMs) {
        val severity = if (waitMs >= 120000L) Warning else Info
        issues += Issue(
          id              = "yarn-queue-wait",
          severity        = severity,
          category        = "config",
          title           = s"YARN Queue Wait — ${fmtMs(waitMs)} Before First Executor Allocated",
          description     =
            s"The application waited ${fmtMs(waitMs)} in the YARN queue before receiving " +
            s"its first container. All stage timings in this report started accumulating " +
            s"from application start, so the effective overhead is ${fmtMs(waitMs)} on top " +
            s"of the reported durations. On congested queues this wait recurs on every run.",
          recommendation  =
            "If the cluster is shared, request a dedicated YARN queue with guaranteed " +
            "capacity for production jobs. For batch jobs that run on a schedule, shifting " +
            "the submission time to off-peak hours reduces queue contention. " +
            "Also check whether spark.yarn.maxAppAttempts is causing retries that restart " +
            "the queue wait.",
          configFix       = Some(
            "spark.yarn.queue=prod-etl  # use a dedicated queue with guaranteed capacity"
          ),
          metrics         = Map(
            "queue_wait_ms"      -> waitMs.toString,
            "app_start_ms"       -> app.startTimeMs.toString,
            "first_executor_ms"  -> firstMs.toString,
          ),
          estimatedImpact = Some(EstimatedImpact(
            summary     = s"${fmtMs(waitMs)} queue wait on every run before any compute starts",
            savedTimeMs = timeOpt(waitMs),
            savedBytes  = None,
            confidence  = "high",
          )),
        )
      }
    }

    // ── 2. YARN virtual memory OOM ────────────────────────────────────────
    // Virtual memory OOM is a completely different failure mode from physical
    // memory OOM (exit 137, handled by DynamicAllocationAnalyzer).
    // The YARN NodeManager enforces vmem = physmem × yarn.nodemanager.vmem-pmem-ratio
    // (default 2.1). JVM maps large address spaces even for unused heap, so
    // this fires frequently with default settings on newer JVMs.
    // Fix: disable vmem checking cluster-side (preferred) or shrink JVM heap.
    if (onYarn) {
      val vmemKilled = app.executors.values.filter { e =>
        e.removalReason.exists(r => vmemPattern.findFirstIn(r).isDefined)
      }.toSeq

      if (vmemKilled.nonEmpty) {
        val hosts = vmemKilled.map(_.host).distinct.take(5).mkString(", ")
        issues += Issue(
          id              = "yarn-vmem-oom-kill",
          severity        = Warning,
          category        = "reliability",
          title           = s"${vmemKilled.size} Executor(s) Killed by YARN Virtual Memory Enforcer",
          description     =
            s"${vmemKilled.size} executor(s) on $hosts were killed because virtual memory " +
            s"usage exceeded the NodeManager limit (yarn.nodemanager.vmem-pmem-ratio × " +
            s"container physical memory). This is NOT the same as a physical memory OOM — " +
            s"the JVM maps large address spaces for unused heap, native libraries, and " +
            s"code cache that count against vmem even when physical usage is low. " +
            s"The fix is at the cluster level, not in Spark heap settings.",
          recommendation  =
            "Ask the cluster admin to set yarn.nodemanager.vmem-check-enabled=false in " +
            "yarn-site.xml on all NodeManagers — this is the standard fix for this class " +
            "of false-positive vmem kill. If changing cluster config is not possible, " +
            "reduce spark.executor.memory to shrink the JVM heap reservation, which " +
            "lowers the vmem footprint at the cost of a smaller execution pool.",
          configFix       = Some(
            "# In yarn-site.xml on all NodeManagers (cluster-admin change):\n" +
            "yarn.nodemanager.vmem-check-enabled=false\n" +
            "# If cluster config cannot be changed, reduce heap to lower vmem footprint:\n" +
            "# spark.executor.memory=<current_value - 20%>"
          ),
          metrics         = Map(
            "vmem_killed"     -> vmemKilled.size.toString,
            "affected_hosts"  -> vmemKilled.map(_.host).distinct.mkString(", "),
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    // ── 3. Hot-node failure pattern ───────────────────────────────────────
    // A node with degraded hardware (bad disk, overheating CPU, bad NIC) will
    // produce a disproportionate share of task failures. Spark's exclusion
    // mechanism catches this eventually, but identifying the pattern early
    // saves time spent chasing transient-looking failures.
    // Not YARN-specific — fires on any cluster manager.
    val hotNodeMinFailed = propLong(app, "spark.sparklens.yarn.hotNodeMinFailed", 5L).toInt
    val hotNodePct       = propDouble(app, "spark.sparklens.yarn.hotNodePct",      50.0)

    val failedByHost: Map[String, Seq[TaskData]] = app.stages.values.flatMap(_.tasks)
      .filter(_.failed)
      .toSeq
      .groupBy(_.host)
    val totalFailed = failedByHost.values.map(_.size).sum

    if (totalFailed >= hotNodeMinFailed && failedByHost.nonEmpty) {
      val (host, hostFailed) = failedByHost.toSeq.maxBy(_._2.size)
      val pct = hostFailed.size.toDouble / totalFailed * 100
      if (pct >= hotNodePct) {
        val affectedStages = hostFailed.flatMap { t =>
          app.stages.values.find(_.tasks.exists(_.taskId == t.taskId)).map(_.stageId)
        }.distinct.sorted
        issues += Issue(
          id              = "yarn-hot-node-failure",
          severity        = Warning,
          category        = "reliability",
          title           = s"Hot-Node Failure Pattern — ${fmtDouble(pct, 0)}% of Task Failures on $host",
          description     =
            s"$host accounts for ${hostFailed.size} of $totalFailed failed tasks " +
            s"(${fmtDouble(pct, 0)}%) across the application. This concentration suggests " +
            s"degraded hardware on that node — slow or failing disk, high CPU temperature, " +
            s"bad network card, or a corrupt JVM installation — rather than data or code issues.",
          recommendation  =
            s"Enable Spark's node exclusion mechanism so future applications avoid $host " +
            s"automatically. Report the node to your cluster operations team for hardware " +
            s"inspection. Check YARN NodeManager logs on $host for disk I/O errors, " +
            s"JVM crashes, or OOM killer activity.",
          configFix       = Some(
            "spark.excludeOnFailure.enabled=true\n" +
            "spark.excludeOnFailure.task.maxTaskAttemptsPerNode=2"
          ),
          affectedStages  = affectedStages,
          metrics         = Map(
            "hot_node"           -> host,
            "node_failed_tasks"  -> hostFailed.size.toString,
            "total_failed_tasks" -> totalFailed.toString,
            "failure_pct"        -> fmtDouble(pct, 1),
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    // ── 4. Shuffle disk full ──────────────────────────────────────────────
    // On YARN, shuffle data lives in yarn.nodemanager.local-dirs (often a
    // separate smaller mount from HDFS). When local disk fills up, tasks fail
    // with I/O exceptions rather than OOM — a distinct failure mode with a
    // distinct fix. Not YARN-specific: also occurs on standalone / K8s with
    // insufficiently large local storage.
    val diskFullTasks = app.stages.values.flatMap(_.tasks).filter { t =>
      t.failed && t.errorMessage.exists(m => diskFullPattern.findFirstIn(m).isDefined)
    }.toSeq

    if (diskFullTasks.nonEmpty) {
      val affectedStages = diskFullTasks.flatMap { t =>
        app.stages.values.find(_.tasks.exists(_.taskId == t.taskId)).map(_.stageId)
      }.toSeq.distinct.sorted
      val affectedHosts = diskFullTasks.map(_.host).distinct.take(5).mkString(", ")
      val totalSpill = app.stages.values.map(_.totalDiskSpillBytes).sum

      issues += Issue(
        id              = "yarn-shuffle-disk-full",
        severity        = Warning,
        category        = "io",
        title           = s"Shuffle Disk Full — ${diskFullTasks.size} Task(s) Failed With Disk I/O Error",
        description     =
          s"${diskFullTasks.size} task(s) failed because the local shuffle disk ran out of " +
          s"space on $affectedHosts. The application wrote ${fmtBytes(totalSpill)} of shuffle " +
          s"spill to local disk. On YARN, shuffle data lives in yarn.nodemanager.local-dirs " +
          s"which is often a small separate partition from HDFS.",
        recommendation  =
          "Short-term: point spark.local.dir at a larger volume. " +
          "Medium-term: reduce shuffle write volume by enabling AQE partition coalescing, " +
          "using broadcast joins for small tables, or increasing executor memory to reduce spill. " +
          "On YARN: verify yarn.nodemanager.local-dirs points to a partition with sufficient " +
          "free space relative to your largest shuffle.",
        configFix       = Some(
          "spark.local.dir=/data/local/spark-tmp  # point to larger volume\n" +
          "spark.sql.adaptive.enabled=true         # reduces shuffle write volume"
        ),
        affectedStages  = affectedStages,
        metrics         = Map(
          "disk_full_tasks"      -> diskFullTasks.size.toString,
          "total_disk_spill"     -> fmtBytes(totalSpill),
          "affected_hosts"       -> affectedHosts,
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── 5. PySpark + low executor memory overhead on YARN ─────────────────
    // Python executors (Python subprocess + Arrow buffers + cloudpickle) consume
    // significant off-heap memory that is charged against the YARN container.
    // With the default 10% overhead, the container limit is often breached before
    // the JVM heap fills — causing silent YARN kills (exit 137) that look like
    // network failures. ConfigAnalyzer flags low overhead generically; this check
    // escalates to Warning specifically when Python UDFs are confirmed in the plan.
    if (onYarn) {
      val hasPythonUdfs = app.sqlExecutions.values.exists { sql =>
        sql.planTree.map(_.flatten.exists(n => pythonUdfNodes.contains(n.nodeName)))
          .getOrElse(pythonUdfNodes.exists(sql.physicalPlan.contains))
      }

      val overheadFactor = app.prop("spark.executor.memoryOverheadFactor")
        .flatMap(s => scala.util.Try(s.toDouble).toOption)
        .getOrElse(0.1)
      val explicitOverhead = app.prop("spark.executor.memoryOverhead").isDefined
      val lowOverhead      = !explicitOverhead && overheadFactor < 0.2

      if (hasPythonUdfs && lowOverhead) {
        val execMem = app.prop("spark.executor.memory").getOrElse("unknown")
        issues += Issue(
          id              = "yarn-pyspark-overhead-risk",
          severity        = Warning,
          category        = "config",
          title           = s"PySpark on YARN With Low Memory Overhead — High YARN OOM Risk",
          description     =
            s"Python UDFs were detected in the query plan but executor memory overhead is " +
            s"only ${fmtDouble(overheadFactor * 100, 0)}% of ${execMem} heap. " +
            s"Each executor runs a Python subprocess that consumes off-heap memory " +
            s"(interpreter + cloudpickle + Arrow buffers for pandas UDFs). On YARN this " +
            s"off-heap usage counts against the container limit, which exceeds the physical " +
            s"JVM heap allocation — making YARN container kills likely under any sustained load.",
          recommendation  =
            "Raise memoryOverheadFactor to at least 0.4 for PySpark workloads that use " +
            "pandas or Arrow UDFs (they hold large off-heap buffers during batch execution). " +
            "For classic row-at-a-time Python UDFs 0.25 is usually sufficient. " +
            "Alternatively, replace Python UDFs with pandas UDFs (Arrow-based) which are " +
            "more memory-efficient, or with native Spark SQL functions which avoid the " +
            "Python subprocess entirely.",
          configFix       = Some(
            "spark.executor.memoryOverheadFactor=0.4  # for pandas/Arrow UDFs\n" +
            "# or explicit: spark.executor.memoryOverhead=4g"
          ),
          metrics         = Map(
            "overhead_factor" -> fmtDouble(overheadFactor, 2),
            "executor_memory" -> execMem,
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    issues.toSeq
  }
}
