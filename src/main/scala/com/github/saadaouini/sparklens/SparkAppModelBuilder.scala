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
    // Key by (stageId, stageAttemptId) so tasks from retried attempts don't mix
    stageTasks.getOrElseUpdate((e.stageId, e.stageAttemptId), mutable.ArrayBuffer.empty) += task
  }

  def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    val info  = e.stageInfo
    val key   = (info.stageId, info.attemptNumber())
    val tasks = stageTasks.getOrElse(key, mutable.ArrayBuffer.empty).toSeq
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
      rddCachedNames   = stageRddCachedNames.getOrElse(key, Set.empty),
      details          = info.details,
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
