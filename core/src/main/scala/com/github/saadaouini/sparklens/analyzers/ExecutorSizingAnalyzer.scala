package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

/**
 * Analyses whether executor and driver are sized correctly for the actual workload,
 * using the peak memory, CPU, GC, and result-size metrics captured per stage and task.
 *
 * Six independent checks:
 *  1. Executor memory — under-provisioned (spill risk) vs over-provisioned (wasted cost)
 *  2. Driver memory   — large result transfers risk driver OOM
 *  3. Cluster cores   — max stage parallelism vs total available cores
 *  4. Cores per executor — GC-driven recommendation (high GC → reduce cores)
 *  5. Storage/execution memory conflict — cache competes with execution, causing spill
 *  6. Off-heap memory — Python/Arrow workloads need off-heap sizing
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
    // Exclude single-task stages (coalesce(1), repartition(1), etc.) — they process
    // the full dataset in one task and would inflate the per-task memory recommendation
    // for all other stages. Fall back to include them only when no multi-task stages exist.
    val allLongStages  = app.stages.values.filter(s => s.durationMs > 5000L && s.avgPeakExecutionMemory > 0)
    val multiTaskStages = allLongStages.filter(_.numTasks > 1)
    val (longStages, singleTaskFallback) =
      if (multiTaskStages.nonEmpty) (multiTaskStages, false) else (allLongStages, true)
    // Use p95 of task-sample peaks when available; fall back to avg when the task
    // sample is empty (only exact aggregates recorded).
    val stagePeaks = longStages.map { s =>
      val sorted = s.tasks.map(_.metrics.peakExecutionMemory).filter(_ > 0).sorted
      if (sorted.nonEmpty) percentile(sorted, 95) else s.avgPeakExecutionMemory
    }.toSeq

    if (stagePeaks.nonEmpty && executorMemory.isDefined) {
      val currentMem    = executorMemory.get
      val execMemPool   = (currentMem * memFraction).toLong  // execution+storage pool
      // p95 task peak across all long stages — more realistic than max(avg).
      val p95PeakPerTask = stagePeaks.max

      // Concurrent memory demand: p95 task peak × cores per executor.
      // This is a realistic worst-case (not all tasks hit peak simultaneously,
      // but a few tasks at the heavy tail can).
      val peakTotalDemand = p95PeakPerTask * coresPerExec

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
            s"${fmtBytes(currentMem)}).${if (hasSpill) " Disk spill detected — this is already causing I/O overhead." else " Risk of disk spill under peak load."}${if (singleTaskFallback) " Note: peak estimate is based on single-task stages only — consider fixing coalesce(1) or repartition(1) before tuning memory." else ""}",
          recommendation  =
            s"Raise spark.executor.memory to ${fmtGiB(recommended)}. " +
            s"This provides ${fmtBytes((recommended * memFraction).toLong)} execution pool " +
            s"for $coresPerExec concurrent tasks averaging ${fmtBytes(p95PeakPerTask)} each. " +
            s"Alternatively, reduce spark.executor.cores to ${ math.max(1, (execMemPool * 0.8 / p95PeakPerTask).toInt) } " +
            s"to lower the simultaneous memory demand.",
          configFix       = Some(
            s"spark.executor.memory=${fmtGiB(recommended)}\n" +
            s"# or reduce cores to lower concurrent peak demand:\n" +
            s"# spark.executor.cores=${math.max(1, (execMemPool * 0.8 / p95PeakPerTask).toInt)}"
          ),
          metrics         = Map(
            "current_executor_memory" -> fmtBytes(currentMem),
            "execution_pool"          -> fmtBytes(execMemPool),
            "p95_task_memory"         -> fmtBytes(p95PeakPerTask),
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
            s"Tasks averaged ${fmtBytes(p95PeakPerTask)} peak memory per task " +
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
            "peak_task_memory"         -> fmtBytes(p95PeakPerTask),
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

    // ── 4. Cores per executor — GC-driven recommendation ─────────────────────
    // High app-wide GC fraction combined with multiple cores per executor is a strong
    // signal that concurrent tasks are competing for heap. Halving cores doubles the
    // heap available per task, typically cutting GC pressure substantially.
    val gcWarnCoresFraction = propDouble(app, "spark.sparklens.sizing.gcWarnCoresFraction", 0.15)
    val substageRun = app.stages.values.filter(_.totalExecutorRunTimeMs > 5000L)
    val appRunMs  = substageRun.map(_.totalExecutorRunTimeMs).sum
    val appGcMs   = substageRun.map(_.totalGcTimeMs).sum
    val appGcFraction = if (appRunMs > 0) appGcMs.toDouble / appRunMs else 0.0

    if (appGcFraction > gcWarnCoresFraction && coresPerExec > 2 && executorMemory.isDefined) {
      val currentMem      = executorMemory.get
      val recommendedCores = math.max(1, coresPerExec / 2)
      // Reducing cores means the same memory is shared by fewer tasks → more heap per task.
      // To keep the same total parallelism, executor count must increase proportionally.
      val gcPct = fmtDouble(appGcFraction * 100, 0)
      issues += Issue(
        id              = "executor-cores-gc-pressure",
        severity        = Warning,
        category        = "config",
        title           = s"$coresPerExec Cores per Executor Amplify GC Pressure — $gcPct% of executor time in GC",
        description     =
          s"Executors spend $gcPct% of their run time in garbage collection across all stages. " +
          s"With $coresPerExec concurrent tasks sharing ${fmtBytes(currentMem)} of heap, " +
          s"each task has only ${fmtBytes(currentMem / coresPerExec)} average memory. " +
          s"Fewer cores per executor increases heap per task and reduces GC stop-the-world pauses.",
        recommendation  =
          s"Reduce spark.executor.cores to $recommendedCores. " +
          s"Each task will have ${fmtBytes(currentMem / recommendedCores)} of heap instead of " +
          s"${fmtBytes(currentMem / coresPerExec)}. " +
          s"Increase spark.executor.instances proportionally to maintain the same total parallelism. " +
          s"Also switch to G1GC for shorter, more predictable pauses.",
        configFix       = Some(
          s"spark.executor.cores=$recommendedCores\n" +
          s"spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"
        ),
        metrics         = Map(
          "cores_per_executor" -> coresPerExec.toString,
          "app_gc_fraction"    -> fmtDouble(appGcFraction, 3),
          "app_gc_ms"          -> appGcMs.toString,
          "executor_memory"    -> fmtBytes(currentMem),
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── 5. Storage/execution memory conflict ──────────────────────────────────
    // Spark's unified memory model uses spark.memory.storageFraction (default 0.5) to
    // split the pool between storage (cache) and execution. When both are under pressure
    // simultaneously — cache consuming its half while execution tasks need to sort/hash
    // large datasets — storage evicts execution pages, causing disk spill even when the
    // heap would otherwise be large enough. Lowering storageFraction shifts the balance
    // towards execution at the cost of less stable cache.
    val hasCaching = app.stages.values.exists(_.rddCachedNames.nonEmpty)
    val hasSpillForConflict = app.stages.values.exists(_.totalDiskSpillBytes > 100L * MB)
    val storageFraction = app.prop("spark.memory.storageFraction")
      .flatMap(s => scala.util.Try(s.toDouble).toOption).getOrElse(0.5)

    if (hasCaching && hasSpillForConflict && storageFraction >= 0.5) {
      issues += Issue(
        id              = "executor-storage-execution-conflict",
        severity        = Warning,
        category        = "config",
        title           = "Storage and Execution Memory Competing — Cache May Be Evicting Execution Pages",
        description     =
          "This job both caches RDDs/DataFrames and has significant disk spill. " +
          s"With spark.memory.storageFraction=${fmtDouble(storageFraction, 1)}, storage memory " +
          "holds half the unified pool. When execution tasks need more memory to sort or hash, " +
          "storage pages are evicted — but the eviction itself triggers more I/O, compounding the spill.",
        recommendation  =
          "Lower spark.memory.storageFraction to 0.3 to give execution more headroom. " +
          "If cache hit rate is critical, switch cached datasets to MEMORY_AND_DISK so evicted " +
          "partitions spill to disk rather than causing execution-side spill. " +
          "Alternatively, increase executor memory so both pools are adequately sized.",
        configFix       = Some(
          "spark.memory.storageFraction=0.3\n" +
          "# and ensure cached datasets use MEMORY_AND_DISK:\n" +
          "# df.persist(StorageLevel.MEMORY_AND_DISK)"
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── 6. Off-heap memory for Python/Arrow workloads ─────────────────────────
    // Python UDFs, pandas UDFs (Arrow), and native libraries allocate memory outside the
    // JVM heap. This off-heap usage is invisible to Spark's memory manager and counts
    // against the OS container limit (YARN container, K8s pod memory limit). Without
    // spark.memory.offHeap, this memory is untracked and can cause OOM kills even when
    // the JVM heap appears healthy. Enabling off-heap allows Spark to account for and
    // limit off-heap usage via the memory manager.
    val offHeapEnabled = app.prop("spark.memory.offHeap.enabled")
      .map(_.toLowerCase == "true").getOrElse(false)

    if (!offHeapEnabled) {
      val pythonPatterns = Seq("PythonUDF", "BatchEvalPython", "ArrowEvalPython",
                               "MapInArrow", "FlatMapGroupsInPandas", "MapInPandas")
      val hasPython = app.sqlExecutions.values.exists { sql =>
        pythonPatterns.exists(sql.physicalPlan.contains)
      }

      if (hasPython) {
        val execMem      = executorMemory.getOrElse(4L * GB)
        // Recommended off-heap: 25% of executor memory, minimum 2 GB.
        // This accounts for Arrow batch buffers + Python subprocess + cloudpickle overhead.
        val offHeapSize  = fmtGiB(math.max(2L * GB, execMem / 4))
        issues += Issue(
          id              = "executor-offheap-not-configured",
          severity        = Warning,
          category        = "config",
          title           = "Python/Arrow UDFs Detected — Off-Heap Memory Not Configured",
          description     =
            "Python UDFs (including pandas UDFs with Arrow) allocate memory outside the JVM heap. " +
            "This off-heap usage — Python subprocess RSS, Arrow batch buffers, cloudpickle serialization — " +
            "is invisible to Spark's memory manager and counts against the OS container limit. " +
            "Without off-heap configuration, executors can be killed by YARN/K8s for exceeding the " +
            "container memory limit even though the JVM heap looks healthy.",
          recommendation  =
            s"Enable off-heap memory and size it to at least 25% of executor memory ($offHeapSize). " +
            "For heavy pandas UDF workloads (large Arrow batches), increase to 50% or more. " +
            "Also raise spark.executor.memoryOverheadFactor to 0.4 on YARN to ensure the container " +
            "limit covers both JVM heap and off-heap usage.",
          configFix       = Some(
            s"spark.memory.offHeap.enabled=true\n" +
            s"spark.memory.offHeap.size=$offHeapSize\n" +
            s"# on YARN also set:\n" +
            s"# spark.executor.memoryOverheadFactor=0.4"
          ),
          metrics         = Map(
            "executor_memory"      -> fmtBytes(execMem),
            "recommended_offheap"  -> offHeapSize,
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    issues.toSeq
  }
}
