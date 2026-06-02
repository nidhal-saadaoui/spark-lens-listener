package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

/**
 * Analyses whether executor and driver are sized correctly for the actual workload,
 * using the peak memory, CPU, and result-size metrics captured per stage and task.
 *
 * Three independent checks:
 *  1. Executor memory — under-provisioned (spill risk) vs over-provisioned (wasted cost)
 *  2. Driver memory   — large result transfers risk driver OOM
 *  3. Cluster cores   — max stage parallelism vs total available cores
 */
object ExecutorSizingAnalyzer extends Analyzer {

  /** Parse Spark memory strings ("4g", "2048m", "1024k", plain bytes). */
  private def parseMemory(s: String): Option[Long] = {
    val t = s.trim.toLowerCase
    scala.util.Try {
      if      (t.endsWith("g")) t.dropRight(1).toLong * GB
      else if (t.endsWith("m")) t.dropRight(1).toLong * MB
      else if (t.endsWith("k")) t.dropRight(1).toLong * 1024L
      else                      t.toLong
    }.toOption
  }

  /** Round up to the nearest "nice" memory size (multiples of 512 MB above 2 GB, else 256 MB). */
  private def roundMemory(bytes: Long): Long = {
    val step = if (bytes > 2L * GB) 512L * MB else 256L * MB
    ((bytes + step - 1) / step) * step
  }

  private def fmtGiB(bytes: Long): String =
    if (bytes >= GB) s"${fmtDouble(bytes.toDouble / GB, 1)}g"
    else s"${fmtDouble(bytes.toDouble / MB, 0)}m"

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // ── Resolve sizing config ─────────────────────────────────────────────────
    val executorMemory: Option[Long] = app.prop("spark.executor.memory").flatMap(parseMemory)
    val driverMemory:   Option[Long] = app.prop("spark.driver.memory").flatMap(parseMemory)
    // memoryFraction controls the execution+storage pool size (Spark unified memory model).
    val memFraction = app.prop("spark.memory.fraction")
      .flatMap(s => scala.util.Try(s.toDouble).toOption).getOrElse(0.6)
    // Cores per executor — use the property, fall back to actual executor core count.
    val coresPerExec: Int = app.prop("spark.executor.cores")
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .orElse(app.executors.values.headOption.map(_.totalCores))
      .getOrElse(1)
    val totalCores = app.executors.values.map(_.totalCores).sum
    val numExecutors = app.executors.size

    // ── 1. Executor memory analysis ───────────────────────────────────────────
    // Only consider stages with real duration and peak memory data.
    val longStages = app.stages.values.filter(s => s.durationMs > 5000L && s.avgPeakExecutionMemory > 0)
    val stagePeaks = longStages.map(_.avgPeakExecutionMemory).toSeq

    if (stagePeaks.nonEmpty && executorMemory.isDefined) {
      val currentMem    = executorMemory.get
      val execMemPool   = (currentMem * memFraction).toLong  // execution+storage pool
      val maxPeakPerTask = stagePeaks.max

      // Worst-case memory demand: all coresPerExec tasks simultaneously at peak.
      // In practice the unified memory model lets tasks share the pool, but when
      // all tasks are in their peak phase (e.g., HashAggregate build) this is real.
      val peakTotalDemand = maxPeakPerTask * coresPerExec

      // Required executor memory: peak demand / memFraction + standard overhead (384 MB min).
      val overheadBytes = math.max(
        app.prop("spark.executor.memoryOverhead").flatMap(parseMemory).getOrElse(0L),
        app.prop("spark.executor.memory").flatMap(parseMemory).map(_ / 10).getOrElse(384L * MB),
      )
      val requiredMem = roundMemory((peakTotalDemand / memFraction).toLong + overheadBytes)

      val hasSpill = app.stages.values.exists(_.totalDiskSpillBytes > 100L * MB)

      if (peakTotalDemand > execMemPool * 0.85 || (hasSpill && currentMem < requiredMem)) {
        // ── Under-provisioned ──────────────────────────────────────────────
        val severity  = if (hasSpill) Warning else Info
        val recommended = roundMemory(math.max(requiredMem, (currentMem * 1.5).toLong))
        val peakPct   = fmtDouble(peakTotalDemand.toDouble / execMemPool * 100, 0)
        issues += Issue(
          id              = "executor-memory-underprovisioned",
          severity        = severity,
          category        = "config",
          title           = s"Executor Memory Under-Provisioned — tasks use $peakPct% of execution pool",
          description     =
            s"With $coresPerExec core(s) per executor, concurrent tasks can demand " +
            s"${fmtBytes(peakTotalDemand)} peak memory but the execution pool is only " +
            s"${fmtBytes(execMemPool)} (${fmtDouble(memFraction * 100, 0)}% of " +
            s"${fmtBytes(currentMem)}).${if (hasSpill) " Disk spill detected — this is already causing I/O overhead." else " Risk of disk spill under peak load."}",
          recommendation  =
            s"Raise spark.executor.memory to ${fmtGiB(recommended)}. " +
            s"This provides ${fmtBytes((recommended * memFraction).toLong)} execution pool " +
            s"for $coresPerExec concurrent tasks averaging ${fmtBytes(maxPeakPerTask)} each. " +
            s"Alternatively, reduce spark.executor.cores to ${ math.max(1, (execMemPool * 0.8 / maxPeakPerTask).toInt) } " +
            s"to lower the simultaneous memory demand.",
          configFix       = Some(
            s"spark.executor.memory=${fmtGiB(recommended)}\n" +
            s"# or reduce cores to lower concurrent peak demand:\n" +
            s"# spark.executor.cores=${math.max(1, (execMemPool * 0.8 / maxPeakPerTask).toInt)}"
          ),
          metrics         = Map(
            "current_executor_memory" -> fmtBytes(currentMem),
            "execution_pool"          -> fmtBytes(execMemPool),
            "peak_task_memory"        -> fmtBytes(maxPeakPerTask),
            "peak_concurrent_demand"  -> fmtBytes(peakTotalDemand),
            "recommended_memory"      -> fmtGiB(recommended),
            "cores_per_executor"      -> coresPerExec.toString,
          ),
          estimatedImpact = Some(configRisk),
        )

      } else if (peakTotalDemand < execMemPool * 0.25 && !hasSpill && currentMem > 2L * GB) {
        // ── Over-provisioned ───────────────────────────────────────────────
        val utilizationPct = fmtDouble(peakTotalDemand.toDouble / execMemPool * 100, 0)
        val recommended    = roundMemory((peakTotalDemand / memFraction * 1.5).toLong + overheadBytes)
        val savingPct      = fmtDouble((1.0 - recommended.toDouble / currentMem) * 100, 0)
        issues += Issue(
          id              = "executor-memory-overprovisioned",
          severity        = Info,
          category        = "config",
          title           = s"Executor Memory Over-Provisioned — only $utilizationPct% of execution pool used",
          description     =
            s"Tasks averaged ${fmtBytes(maxPeakPerTask)} peak memory per task " +
            s"($coresPerExec core(s) per executor → ${fmtBytes(peakTotalDemand)} concurrent demand) " +
            s"but the execution pool is ${fmtBytes(execMemPool)}. " +
            s"${fmtDouble(100 - utilizationPct.toDouble, 0)}% of executor memory is unused.",
          recommendation  =
            s"Reduce spark.executor.memory to ${fmtGiB(recommended)} " +
            s"(includes 50%% headroom above measured peak). " +
            s"This saves ~$savingPct%% of memory cost per executor" +
            (if (numExecutors > 1) s" — ${fmtGiB((currentMem - recommended) * numExecutors)} freed across $numExecutors executors." else "."),
          configFix       = Some(s"spark.executor.memory=${fmtGiB(recommended)}"),
          metrics         = Map(
            "current_executor_memory"  -> fmtBytes(currentMem),
            "peak_task_memory"         -> fmtBytes(maxPeakPerTask),
            "peak_concurrent_demand"   -> fmtBytes(peakTotalDemand),
            "execution_pool_used_pct"  -> utilizationPct,
            "recommended_memory"       -> fmtGiB(recommended),
            "potential_saving_pct"     -> savingPct,
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    // ── 2. Driver memory analysis ─────────────────────────────────────────────
    // Large task result sizes risk driver OOM; plan compilation overhead also stresses driver heap.
    val totalResultBytes = app.stages.values.map(_.totalResultSize).sum
    if (driverMemory.isDefined && totalResultBytes > 0) {
      val driverMem = driverMemory.get
      val resultPct = totalResultBytes.toDouble / driverMem
      if (resultPct > 0.4) {
        val severity = if (resultPct > 0.8) Critical else Warning
        val recommended = roundMemory((totalResultBytes * 3).toLong)
        issues += Issue(
          id              = "driver-memory-underprovisioned",
          severity        = severity,
          category        = "config",
          title           = s"Driver Memory Risk — ${fmtBytes(totalResultBytes)} sent to driver against ${fmtBytes(driverMem)} heap",
          description     =
            s"Task results totalling ${fmtBytes(totalResultBytes)} were collected to the driver, " +
            s"consuming ${fmtDouble(resultPct * 100, 0)}% of the ${fmtBytes(driverMem)} driver heap. " +
            s"This leaves little room for plan compilation, broadcast variables, and JVM overhead.",
          recommendation  =
            s"Either raise spark.driver.memory to at least ${fmtGiB(recommended)} " +
            s"(3× the total result size), or replace collect() calls with df.write() " +
            s"to avoid transferring data to the driver at all.",
          configFix       = Some(s"spark.driver.memory=${fmtGiB(recommended)}"),
          metrics         = Map(
            "driver_memory"      -> fmtBytes(driverMem),
            "total_result_bytes" -> fmtBytes(totalResultBytes),
            "result_pct"         -> fmtDouble(resultPct * 100, 1),
            "recommended_memory" -> fmtGiB(recommended),
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    // ── 3. Cluster cores (total parallelism) analysis ─────────────────────────
    // If the widest stage never uses more than a small fraction of available cores,
    // the cluster is over-provisioned for this workload.
    if (totalCores >= 8) {
      val maxStageTasks = if (app.stages.nonEmpty)
        app.stages.values.map(_.numTasks).max else 0
      val coreUtilPct = if (totalCores > 0)
        maxStageTasks.toDouble / totalCores * 100 else 0.0

      val minPctForWarn = propDouble(app, "spark.sparklens.sizing.coreUtilWarnPct", 30.0)

      if (maxStageTasks > 0 && coreUtilPct < minPctForWarn) {
        val recommendedCores = math.max(coresPerExec, ((maxStageTasks * 1.5) / coresPerExec).toInt * coresPerExec)
        val recommendedExecs = math.max(1, (maxStageTasks * 1.5 / coresPerExec).toInt)
        issues += Issue(
          id              = "cluster-cores-overprovisioned",
          severity        = Info,
          category        = "config",
          title           = s"Cluster Over-Provisioned — widest stage uses only ${fmtDouble(coreUtilPct, 0)}% of $totalCores cores",
          description     =
            s"The widest stage in this job has $maxStageTasks tasks but the cluster has $totalCores " +
            s"cores across $numExecutors executor(s). ${fmtDouble(100 - coreUtilPct, 0)}% of cores " +
            s"are always idle, paying for capacity that cannot be used.",
          recommendation  =
            s"This workload needs at most ~$recommendedCores total cores ($recommendedExecs executors × $coresPerExec cores). " +
            s"Consider reducing the cluster size or enabling dynamic allocation so idle executors are released.",
          configFix       = Some(
            s"spark.dynamicAllocation.enabled=true\n" +
            s"spark.dynamicAllocation.maxExecutors=$recommendedExecs\n" +
            s"# or fix the executor count:\n" +
            s"# spark.executor.instances=$recommendedExecs"
          ),
          metrics         = Map(
            "total_cores"       -> totalCores.toString,
            "num_executors"     -> numExecutors.toString,
            "max_stage_tasks"   -> maxStageTasks.toString,
            "core_util_pct"     -> fmtDouble(coreUtilPct, 1),
            "recommended_execs" -> recommendedExecs.toString,
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    issues.toSeq
  }
}
