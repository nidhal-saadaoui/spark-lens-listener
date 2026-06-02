package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.model._
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SparkPlanInfo

import scala.collection.mutable

/** Accumulates Spark listener events and builds a SparkAppModel at application end. */
private[sparklens] class SparkAppModelBuilder(runtimeVersion: String = "") {

  private var appId        = "unknown"
  private var appName      = "unknown"
  private var sparkVersion = runtimeVersion   // seeded from SPARK_VERSION constant
  private var startTimeMs  = 0L

  private val sparkProperties = mutable.Map[String, String]()
  private val jobs            = mutable.Map[Int, JobData]()
  private val executors       = mutable.Map[String, ExecutorData]()
  private val sqlExecutions   = mutable.Map[Long, SqlExecutionData]()

  // Internal stage tracking keyed by (stageId, attemptId) to keep attempts isolated.
  // At build() time we collapse to the latest attempt per stageId.
  private val stageTasks         = mutable.Map[(Int, Int), mutable.ArrayBuffer[TaskData]]()
  private val stageInfo          = mutable.Map[(Int, Int), StageData]()
  private val stageRddNames      = mutable.Map[(Int, Int), Seq[String]]()
  private val stageRddCachedNames= mutable.Map[(Int, Int), Set[String]]()

  // track which stages belong to which job (for cache analyzer)
  private val jobStageMap     = mutable.Map[Int, Seq[Int]]()
  private val jobSubmitTimeMs = mutable.Map[Int, Long]()

  // ── SQL plan metric collection ─────────────────────────────────────────────
  // Accumulator IDs that belong to Exchange nodes in any SQL execution's plan tree.
  // Populated at ExecutionStart so onTaskEnd can skip irrelevant accumulators.
  private val exchangeAccumIds = mutable.Set[Long]()

  // Per-task accumulator updates: accumulatorId → list of per-task delta values.
  // Only Exchange-node accumulators are collected to keep memory bounded.
  private val accumPerTask = mutable.Map[Long, mutable.ArrayBuffer[Long]]()

  // ── Exact aggregate tracking ───────────────────────────────────────────────
  // Stores precise totals for every task that arrived, regardless of whether the
  // task was admitted to the reservoir sample in stageTasks.
  private val stageExactTaskCount = mutable.Map[(Int, Int), Int]()
  private val stageAgg            = mutable.Map[(Int, Int), StageSummary]()

  // Maximum task objects kept in memory per stage (reservoir sample).
  // Percentile/distribution analysis uses the sample; totals come from StageSummary.
  private val MaxSampledTasksPerStage = 10000

  private class StageSummary {
    var taskCount:                    Int  = 0
    var failedCount:                  Int  = 0
    var killedCount:                  Int  = 0
    var speculativeCount:             Int  = 0
    var tasksWithInputBytes:          Int  = 0
    var tasksWithOutputBytes:         Int  = 0
    var totalExecutorRunTimeMs:       Long = 0L
    var totalExecutorCpuTimeNs:       Long = 0L
    var totalExecutorDeserializeTimeMs:Long= 0L
    var totalGcTimeMs:                Long = 0L
    var totalPeakExecutionMemorySum:  Long = 0L
    var totalInputBytes:              Long = 0L
    var totalInputRecords:            Long = 0L
    var totalOutputBytes:             Long = 0L
    var totalOutputRecords:           Long = 0L
    var totalResultSize:              Long = 0L
    var totalDiskSpillBytes:          Long = 0L
    var totalMemorySpillBytes:        Long = 0L
    var totalShuffleRemoteBytes:      Long = 0L
    var totalShuffleLocalBytes:       Long = 0L
    var totalShuffleBytesWritten:     Long = 0L
    var totalShuffleFetchWaitTimeMs:  Long = 0L
    var totalShuffleRecordsRead:      Long = 0L
    var totalShuffleRecordsWritten:   Long = 0L
  }

  def onApplicationStart(e: SparkListenerApplicationStart): Unit = {
    appId       = e.appId.getOrElse("unknown")
    appName     = e.appName
    startTimeMs = e.time
  }

  def onEnvironmentUpdate(e: SparkListenerEnvironmentUpdate): Unit = {
    e.environmentDetails.get("Spark Properties").foreach {
      _.foreach { case (k, v) => sparkProperties(k) = v }
    }
    // extract spark version from properties if present
    sparkProperties.get("spark.version").foreach(v => sparkVersion = v)
  }

  def onExecutorAdded(e: SparkListenerExecutorAdded): Unit = {
    val info = e.executorInfo
    executors(e.executorId) = ExecutorData(
      executorId    = e.executorId,
      host          = info.executorHost,
      totalCores    = info.totalCores,
      addedTimeMs   = e.time,
      removedTimeMs = None,
      removalReason = None,
    )
  }

  def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit = {
    executors.get(e.executorId).foreach { existing =>
      executors(e.executorId) = existing.copy(
        removedTimeMs = Some(e.time),
        removalReason = Option(e.reason),
      )
    }
  }

  def onJobStart(e: SparkListenerJobStart): Unit = {
    jobStageMap(e.jobId)     = e.stageIds
    jobSubmitTimeMs(e.jobId) = e.time
  }

  def onJobEnd(e: SparkListenerJobEnd): Unit = {
    val status = e.jobResult match {
      case JobSucceeded => "SUCCEEDED"
      case _            => "FAILED"
    }
    // name comes from the SQL description or the first stage name
    val name = sqlExecutions.values
      .find(_.jobIds.contains(e.jobId))
      .map(_.description)
      .orElse(jobStageMap.get(e.jobId)
        .flatMap(_.headOption)
        .flatMap(sid => stageInfo.find(_._1._1 == sid).map(_._2.name)))
      .getOrElse(s"Job ${e.jobId}")

    jobs(e.jobId) = JobData(
      jobId            = e.jobId,
      name             = name,
      stageIds         = jobStageMap.getOrElse(e.jobId, Nil),
      submissionTimeMs = jobSubmitTimeMs.getOrElse(e.jobId, 0L),
      completionTimeMs = Some(e.time),
      status           = status,
    )
    jobSubmitTimeMs.remove(e.jobId)
  }

  def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
    val info    = e.stageInfo
    val key     = (info.stageId, info.attemptNumber())
    val rddNames       = info.rddInfos.map(_.name).filter(_.nonEmpty)
    val rddCachedNames = info.rddInfos.filter(_.isCached).map(_.name).toSet
    val rddCacheInfos  = info.rddInfos.filter(_.name.nonEmpty).map { r =>
      RddCacheInfo(
        name             = r.name,
        numPartitions    = r.numPartitions,
        cachedPartitions = r.numCachedPartitions,
        memSizeBytes     = r.memSize,
        diskSizeBytes    = r.diskSize,
        storageLevel     = r.storageLevel.description,
      )
    }
    stageRddNames(key)       = rddNames
    stageRddCachedNames(key) = rddCachedNames
    stageInfo(key) = StageData(
      stageId          = info.stageId,
      attemptId        = info.attemptNumber(),
      name             = info.name,
      numTasks         = info.numTasks,
      submissionTimeMs = info.submissionTime,
      rddNames         = rddNames,
      rddCachedNames   = rddCachedNames,
      rddCacheInfos    = rddCacheInfos,
      parentIds        = info.parentIds.toSeq,
      details          = info.details,
    )
    stageTasks.getOrElseUpdate(key, mutable.ArrayBuffer.empty)
  }

  def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
    if (e.taskInfo == null || e.taskMetrics == null) return
    val info    = e.taskInfo
    val metrics = e.taskMetrics
    val shufR   = metrics.shuffleReadMetrics
    val shufW   = metrics.shuffleWriteMetrics
    val inp     = metrics.inputMetrics
    val out     = metrics.outputMetrics

    val errorMessage: Option[String] =
      if (info.failed) Some(e.reason.toString.take(300))
      else None

    val task = TaskData(
      taskId       = info.taskId,
      index        = info.index,
      attempt      = info.attemptNumber,
      executorId   = info.executorId,
      host         = info.host,
      status       = info.status,
      launchTimeMs = info.launchTime,
      finishTimeMs = info.finishTime,
      failed       = info.failed,
      killed       = info.killed,
      speculative  = info.speculative,
      errorMessage = errorMessage,
      metrics = TaskMetrics(
        executorRunTimeMs         = metrics.executorRunTime,
        executorCpuTimeNs         = metrics.executorCpuTime,
        executorDeserializeTimeMs = metrics.executorDeserializeTime,
        jvmGcTimeMs               = metrics.jvmGCTime,
        memoryBytesSpilled        = metrics.memoryBytesSpilled,
        diskBytesSpilled          = metrics.diskBytesSpilled,
        peakExecutionMemory       = metrics.peakExecutionMemory,
        shuffleRemoteBytesRead    = shufR.remoteBytesRead,
        shuffleLocalBytesRead     = shufR.localBytesRead,
        shuffleBytesWritten       = shufW.bytesWritten,
        shuffleFetchWaitTimeMs    = shufR.fetchWaitTime,
        shuffleRecordsRead        = shufR.recordsRead,
        shuffleRecordsWritten     = shufW.recordsWritten,
        inputBytesRead            = inp.bytesRead,
        inputRecordsRead          = inp.recordsRead,
        outputBytesWritten        = out.bytesWritten,
        outputRecordsWritten      = out.recordsWritten,
        resultSize                = metrics.resultSize,
      ),
    )

    val key = (e.stageId, e.stageAttemptId)

    // ── Always update exact aggregates ──────────────────────────────────────
    val s = stageAgg.getOrElseUpdate(key, new StageSummary)
    s.taskCount               += 1
    if (info.failed)      s.failedCount      += 1
    if (info.killed)      s.killedCount       += 1
    if (info.speculative) s.speculativeCount  += 1
    if (inp.bytesRead > 0)    s.tasksWithInputBytes  += 1
    if (out.bytesWritten > 0) s.tasksWithOutputBytes += 1
    s.totalExecutorRunTimeMs          += metrics.executorRunTime
    s.totalExecutorCpuTimeNs          += metrics.executorCpuTime
    s.totalExecutorDeserializeTimeMs  += metrics.executorDeserializeTime
    s.totalGcTimeMs                   += metrics.jvmGCTime
    s.totalPeakExecutionMemorySum     += metrics.peakExecutionMemory
    s.totalInputBytes                 += inp.bytesRead
    s.totalInputRecords               += inp.recordsRead
    s.totalOutputBytes                += out.bytesWritten
    s.totalOutputRecords              += out.recordsWritten
    s.totalResultSize                 += metrics.resultSize
    s.totalDiskSpillBytes             += metrics.diskBytesSpilled
    s.totalMemorySpillBytes           += metrics.memoryBytesSpilled
    s.totalShuffleRemoteBytes         += shufR.remoteBytesRead
    s.totalShuffleLocalBytes          += shufR.localBytesRead
    s.totalShuffleBytesWritten        += shufW.bytesWritten
    s.totalShuffleFetchWaitTimeMs     += shufR.fetchWaitTime
    s.totalShuffleRecordsRead         += shufR.recordsRead
    s.totalShuffleRecordsWritten      += shufW.recordsWritten

    // ── Collect per-task SQL metric updates for Exchange nodes ───────────────
    // AccumulableInfo.update is the per-task delta; we sum these later to get
    // per-task partition bytes for skew detection.  Only Exchange-node accumulators
    // are collected — the set is populated at SQL ExecutionStart so this filter is cheap.
    if (exchangeAccumIds.nonEmpty) {
      info.accumulables.foreach { a =>
        if (exchangeAccumIds.contains(a.id)) {
          a.update.foreach {
            case v: Long if v > 0 =>
              accumPerTask.getOrElseUpdate(a.id, mutable.ArrayBuffer.empty) += v
            case _ =>
          }
        }
      }
    }

    // ── Reservoir-sample the task object ────────────────────────────────────
    // Key by (stageId, stageAttemptId) so tasks from retried attempts don't mix.
    val n   = stageExactTaskCount.getOrElse(key, 0) + 1
    stageExactTaskCount(key) = n
    val buf = stageTasks.getOrElseUpdate(key, mutable.ArrayBuffer.empty)
    if (n <= MaxSampledTasksPerStage) {
      buf += task
    } else {
      // Vitter's Algorithm R: replace a randomly chosen existing sample slot
      val j = scala.util.Random.nextInt(n)
      if (j < MaxSampledTasksPerStage) buf(j) = task
    }
  }

  def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    val info  = e.stageInfo
    val key   = (info.stageId, info.attemptNumber())
    val tasks = stageTasks.getOrElse(key, mutable.ArrayBuffer.empty).toSeq

    // Merge submission-time and completion-time cached names.  At submission time isCached can
    // be false even for RDDs that are being cached: block-status updates are async and may not
    // have propagated yet.  By completion time all cached blocks are registered, so this union
    // captures caching that was missed at submission.
    val cachedAtCompletion = info.rddInfos.filter(_.isCached).map(_.name).toSet
    val mergedCachedNames  = stageRddCachedNames.getOrElse(key, Set.empty) ++ cachedAtCompletion

    val existing = stageInfo.getOrElse(key, StageData(info.stageId, info.attemptNumber(), info.name, info.numTasks))
    val agg = stageAgg.get(key)
    stageInfo(key) = StageData(
      stageId          = info.stageId,
      attemptId        = info.attemptNumber(),
      name             = info.name,
      numTasks         = info.numTasks,
      tasks            = tasks,
      submissionTimeMs = info.submissionTime,
      completionTimeMs = info.completionTime,
      failureReason    = info.failureReason.filter(_.nonEmpty),
      rddNames         = stageRddNames.getOrElse(key, Nil),
      rddCachedNames   = mergedCachedNames,
      rddCacheInfos    = existing.rddCacheInfos,
      parentIds        = existing.parentIds,
      details          = info.details,

      hasExactAggregates               = agg.isDefined,
      exactTaskCount                   = agg.map(_.taskCount).getOrElse(0),
      exactFailedCount                 = agg.map(_.failedCount).getOrElse(0),
      exactKilledCount                 = agg.map(_.killedCount).getOrElse(0),
      exactSpeculativeCount            = agg.map(_.speculativeCount).getOrElse(0),
      exactInputBytes                  = agg.map(_.totalInputBytes).getOrElse(0L),
      exactInputRecords                = agg.map(_.totalInputRecords).getOrElse(0L),
      exactOutputBytes                 = agg.map(_.totalOutputBytes).getOrElse(0L),
      exactOutputRecords               = agg.map(_.totalOutputRecords).getOrElse(0L),
      exactResultSize                  = agg.map(_.totalResultSize).getOrElse(0L),
      exactDiskSpillBytes              = agg.map(_.totalDiskSpillBytes).getOrElse(0L),
      exactMemorySpillBytes            = agg.map(_.totalMemorySpillBytes).getOrElse(0L),
      exactGcTimeMs                    = agg.map(_.totalGcTimeMs).getOrElse(0L),
      exactExecutorRunTimeMs           = agg.map(_.totalExecutorRunTimeMs).getOrElse(0L),
      exactExecutorCpuTimeNs           = agg.map(_.totalExecutorCpuTimeNs).getOrElse(0L),
      exactExecutorDeserializeTimeMs   = agg.map(_.totalExecutorDeserializeTimeMs).getOrElse(0L),
      exactPeakExecutionMemorySum      = agg.map(_.totalPeakExecutionMemorySum).getOrElse(0L),
      exactShuffleRemoteBytes          = agg.map(_.totalShuffleRemoteBytes).getOrElse(0L),
      exactShuffleLocalBytes           = agg.map(_.totalShuffleLocalBytes).getOrElse(0L),
      exactShuffleBytesWritten         = agg.map(_.totalShuffleBytesWritten).getOrElse(0L),
      exactShuffleFetchWaitTimeMs      = agg.map(_.totalShuffleFetchWaitTimeMs).getOrElse(0L),
      exactShuffleRecordsRead          = agg.map(_.totalShuffleRecordsRead).getOrElse(0L),
      exactShuffleRecordsWritten       = agg.map(_.totalShuffleRecordsWritten).getOrElse(0L),
      exactTasksWithInputBytes         = agg.map(_.tasksWithInputBytes).getOrElse(0),
      exactTasksWithOutputBytes        = agg.map(_.tasksWithOutputBytes).getOrElse(0),
    )

    // Release raw accumulator memory. All data is now in stageInfo — holding it in
    // both places doubles peak driver heap until build() is called.
    stageTasks.remove(key)
    stageAgg.remove(key)
    stageExactTaskCount.remove(key)
    stageRddNames.remove(key)
    stageRddCachedNames.remove(key)
  }

  def onSqlExecutionStart(
    executionId:  Long,
    description:  String,
    physicalPlan: String,
    planInfo:     SparkPlanInfo,
    startTimeMs:  Long,
  ): Unit = {
    // physicalPlanDescription from SparkListenerSQLExecutionStart is queryExecution.toString()
    // which includes Parsed/Analyzed/Optimized/Physical sections.  Extract only the Physical
    // section so plan analyzers don't match logical-plan text (e.g. "Window" in logical plan
    // combined with "SinglePartition" in physical plan produces false positives).
    val physSection = {
      val marker = "== Physical Plan =="
      val idx    = physicalPlan.indexOf(marker)
      if (idx >= 0) physicalPlan.substring(idx + marker.length).trim else physicalPlan
    }
    val treeOpt = scala.util.Try(toPlanNode(planInfo)).toOption
    treeOpt.foreach { tree =>
      tree.nodesContaining("Exchange").flatMap(_.accumulatorIds).foreach(exchangeAccumIds += _)
    }
    sqlExecutions(executionId) = SqlExecutionData(
      executionId      = executionId,
      description      = description,
      physicalPlan     = physSection,
      startTimeMs      = startTimeMs,
      completionTimeMs = None,
      jobIds           = Nil,
      planTree         = treeOpt,
    )
  }

  /** AQE rewrites the plan at runtime; replace the stored tree so analyzers see the final plan. */
  def onSqlPlanUpdate(executionId: Long, planInfo: SparkPlanInfo): Unit = {
    sqlExecutions.get(executionId).foreach { existing =>
      val treeOpt = scala.util.Try(toPlanNode(planInfo)).toOption
      treeOpt.foreach { tree =>
        tree.nodesContaining("Exchange").flatMap(_.accumulatorIds).foreach(exchangeAccumIds += _)
      }
      sqlExecutions(executionId) = existing.copy(planTree = treeOpt.orElse(existing.planTree))
    }
  }

  def onSqlExecutionEnd(executionId: Long, completionTimeMs: Long): Unit = {
    sqlExecutions.get(executionId).foreach { existing =>
      // Resolve accumulator IDs in the plan tree to their summed per-task values.
      val resolved = existing.planTree.map(resolveTree)
      sqlExecutions(executionId) = existing.copy(
        completionTimeMs = Some(completionTimeMs),
        planTree         = resolved.orElse(existing.planTree),
      )
    }
  }

  // ── Private plan helpers ──────────────────────────────────────────────────

  private def toPlanNode(info: SparkPlanInfo): PlanNode =
    PlanNode(
      nodeName       = info.nodeName,
      simpleString   = info.simpleString,
      accumulatorIds = info.metrics.map(_.accumulatorId).toSeq,
      metricNames    = info.metrics.map(m => m.accumulatorId -> m.name).toMap,
      children       = info.children.map(toPlanNode).toSeq,
    )

  private def resolveTree(node: PlanNode): PlanNode = {
    val metrics = node.accumulatorIds
      .flatMap(id => accumPerTask.get(id).map(buf => id -> buf.sum))
      .toMap
    node.copy(
      resolvedMetrics = metrics,
      children        = node.children.map(resolveTree),
    )
  }

  def linkSqlJob(executionId: Long, jobId: Int): Unit = {
    sqlExecutions.get(executionId).foreach { existing =>
      sqlExecutions(executionId) = existing.copy(jobIds = existing.jobIds :+ jobId)
    }
  }

  def build(endTimeMs: Long): SparkAppModel = {
    // Collapse multiple attempts per stage: keep only the latest attempt
    val latestStages: Map[Int, StageData] = stageInfo.toMap
      .groupBy(_._1._1)
      .map { case (stageId, entries) => stageId -> entries.maxBy(_._1._2)._2 }

    SparkAppModel(
      appId           = appId,
      appName         = appName,
      sparkVersion    = sparkVersion,
      startTimeMs     = startTimeMs,
      endTimeMs       = Some(endTimeMs),
      sparkProperties = sparkProperties.toMap,
      jobs            = jobs.toMap,
      stages          = latestStages,
      executors       = executors.toMap,
      sqlExecutions   = sqlExecutions.toMap,
    )
  }
}
