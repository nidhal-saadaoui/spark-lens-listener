package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.model._
import org.apache.spark.scheduler._

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
  private val jobStageMap = mutable.Map[Int, Seq[Int]]()

  // ── Exact aggregate tracking ───────────────────────────────────────────────
  // Stores precise totals for every task that arrived, regardless of whether the
  // task was admitted to the reservoir sample in stageTasks.
  private val stageExactTaskCount = mutable.Map[(Int, Int), Int]()
  private val stageAgg            = mutable.Map[(Int, Int), StageSummary]()

  // Maximum task objects kept in memory per stage (reservoir sample).
  // Percentile/distribution analysis uses the sample; totals come from StageSummary.
  private val MaxSampledTasksPerStage = 10000

  private class StageSummary {
    var taskCount:               Int  = 0
    var failedCount:             Int  = 0
    var killedCount:             Int  = 0
    var speculativeCount:        Int  = 0
    var tasksWithInputBytes:     Int  = 0
    var tasksWithOutputBytes:    Int  = 0
    var totalExecutorRunTimeMs:  Long = 0L
    var totalExecutorCpuTimeNs:  Long = 0L
    var totalGcTimeMs:           Long = 0L
    var totalInputBytes:         Long = 0L
    var totalOutputBytes:        Long = 0L
    var totalResultSize:         Long = 0L
    var totalDiskSpillBytes:     Long = 0L
    var totalMemorySpillBytes:   Long = 0L
    var totalShuffleRemoteBytes: Long = 0L
    var totalShuffleLocalBytes:  Long = 0L
    var totalShuffleBytesWritten:Long = 0L
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
    jobStageMap(e.jobId) = e.stageIds
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
      submissionTimeMs = 0L,
      completionTimeMs = Some(e.time),
      status           = status,
    )
  }

  def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
    val info    = e.stageInfo
    val key     = (info.stageId, info.attemptNumber())
    val rddNames       = info.rddInfos.map(_.name).filter(_.nonEmpty)
    val rddCachedNames = info.rddInfos.filter(_.isCached).map(_.name).toSet
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
        executorRunTimeMs      = metrics.executorRunTime,
        executorCpuTimeNs      = metrics.executorCpuTime,
        jvmGcTimeMs            = metrics.jvmGCTime,
        memoryBytesSpilled     = metrics.memoryBytesSpilled,
        diskBytesSpilled       = metrics.diskBytesSpilled,
        shuffleRemoteBytesRead = shufR.remoteBytesRead,
        shuffleLocalBytesRead  = shufR.localBytesRead,
        shuffleBytesWritten    = shufW.bytesWritten,
        inputBytesRead         = inp.bytesRead,
        outputBytesWritten     = out.bytesWritten,
        resultSize             = metrics.resultSize,
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
    s.totalExecutorRunTimeMs  += metrics.executorRunTime
    s.totalExecutorCpuTimeNs  += metrics.executorCpuTime
    s.totalGcTimeMs           += metrics.jvmGCTime
    s.totalInputBytes         += inp.bytesRead
    s.totalOutputBytes        += out.bytesWritten
    s.totalResultSize         += metrics.resultSize
    s.totalDiskSpillBytes     += metrics.diskBytesSpilled
    s.totalMemorySpillBytes   += metrics.memoryBytesSpilled
    s.totalShuffleRemoteBytes += shufR.remoteBytesRead
    s.totalShuffleLocalBytes  += shufR.localBytesRead
    s.totalShuffleBytesWritten+= shufW.bytesWritten

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
      details          = info.details,

      hasExactAggregates        = agg.isDefined,
      exactTaskCount            = agg.map(_.taskCount).getOrElse(0),
      exactFailedCount          = agg.map(_.failedCount).getOrElse(0),
      exactKilledCount          = agg.map(_.killedCount).getOrElse(0),
      exactSpeculativeCount     = agg.map(_.speculativeCount).getOrElse(0),
      exactInputBytes           = agg.map(_.totalInputBytes).getOrElse(0L),
      exactOutputBytes          = agg.map(_.totalOutputBytes).getOrElse(0L),
      exactResultSize           = agg.map(_.totalResultSize).getOrElse(0L),
      exactDiskSpillBytes       = agg.map(_.totalDiskSpillBytes).getOrElse(0L),
      exactMemorySpillBytes     = agg.map(_.totalMemorySpillBytes).getOrElse(0L),
      exactGcTimeMs             = agg.map(_.totalGcTimeMs).getOrElse(0L),
      exactExecutorRunTimeMs    = agg.map(_.totalExecutorRunTimeMs).getOrElse(0L),
      exactExecutorCpuTimeNs    = agg.map(_.totalExecutorCpuTimeNs).getOrElse(0L),
      exactShuffleRemoteBytes   = agg.map(_.totalShuffleRemoteBytes).getOrElse(0L),
      exactShuffleLocalBytes    = agg.map(_.totalShuffleLocalBytes).getOrElse(0L),
      exactShuffleBytesWritten  = agg.map(_.totalShuffleBytesWritten).getOrElse(0L),
      exactTasksWithInputBytes  = agg.map(_.tasksWithInputBytes).getOrElse(0),
      exactTasksWithOutputBytes = agg.map(_.tasksWithOutputBytes).getOrElse(0),
    )
  }

  def onSqlExecutionStart(executionId: Long, description: String, physicalPlan: String, startTimeMs: Long): Unit = {
    // physicalPlanDescription from SparkListenerSQLExecutionStart is queryExecution.toString()
    // which includes Parsed/Analyzed/Optimized/Physical sections.  Extract only the Physical
    // section so plan analyzers don't match logical-plan text (e.g. "Window" in logical plan
    // combined with "SinglePartition" in physical plan produces false positives).
    val physSection = {
      val marker = "== Physical Plan =="
      val idx    = physicalPlan.indexOf(marker)
      val section = if (idx >= 0) physicalPlan.substring(idx + marker.length).trim else physicalPlan
      section
    }
    sqlExecutions(executionId) = SqlExecutionData(
      executionId      = executionId,
      description      = description,
      physicalPlan     = physSection,
      startTimeMs      = startTimeMs,
      completionTimeMs = None,
      jobIds           = Nil,
    )
  }

  def onSqlExecutionEnd(executionId: Long, completionTimeMs: Long): Unit = {
    sqlExecutions.get(executionId).foreach { existing =>
      sqlExecutions(executionId) = existing.copy(completionTimeMs = Some(completionTimeMs))
    }
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
