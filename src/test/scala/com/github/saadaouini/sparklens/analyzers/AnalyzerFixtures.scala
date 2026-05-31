package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object AnalyzerFixtures {
  val MB: Long = 1024L * 1024L
  val GB: Long = 1024L * MB

  def task(
    durationMs:         Long = 1000L,
    gcMs:               Long = 0L,
    diskSpill:          Long = 0L,
    memSpill:           Long = 0L,
    cpuNs:              Long = 0L,
    remoteShuffleBytes: Long = 0L,
    localShuffleBytes:  Long = 0L,
    inputBytes:         Long = 0L,
    resultSize:         Long = 0L,
    failed:             Boolean = false,
    killed:             Boolean = false,
    speculative:        Boolean = false,
    executorRunTimeMs:  Long = 0L,
    id:                 Long = 0L,
  ): TaskData = TaskData(
    taskId       = id,
    index        = id.toInt,
    attempt      = 0,
    executorId   = "0",
    host         = "host1",
    status       = if (failed) "FAILED" else "SUCCESS",
    launchTimeMs = 0L,
    finishTimeMs = durationMs,
    failed       = failed,
    killed       = killed,
    speculative  = speculative,
    errorMessage = if (failed) Some("error") else None,
    metrics = TaskMetrics(
      executorRunTimeMs      = if (executorRunTimeMs > 0) executorRunTimeMs else durationMs,
      executorCpuTimeNs      = cpuNs,
      jvmGcTimeMs            = gcMs,
      memoryBytesSpilled     = memSpill,
      diskBytesSpilled       = diskSpill,
      shuffleRemoteBytesRead = remoteShuffleBytes,
      shuffleLocalBytesRead  = localShuffleBytes,
      inputBytesRead         = inputBytes,
      resultSize             = resultSize,
    ),
  )

  def stage(
    stageId:    Int          = 0,
    tasks:      Seq[TaskData] = Nil,
    attemptId:  Int          = 0,
    name:       String       = "test-stage",
    rddNames:   Seq[String]  = Nil,
    failReason: Option[String] = None,
    submitMs:   Option[Long] = Some(0L),
    completeMs: Option[Long] = Some(60000L),
  ): StageData = StageData(
    stageId          = stageId,
    attemptId        = attemptId,
    name             = name,
    numTasks         = tasks.size,
    tasks            = tasks,
    submissionTimeMs = submitMs,
    completionTimeMs = completeMs,
    failureReason    = failReason,
    rddNames         = rddNames,
  )

  def app(
    stages:     Map[Int, StageData]          = Map.empty,
    jobs:       Map[Int, JobData]            = Map.empty,
    props:      Map[String, String]          = Map.empty,
    sqlExecs:   Map[Long, SqlExecutionData]  = Map.empty,
    executors:  Map[String, ExecutorData]    = Map.empty,
  ): SparkAppModel = SparkAppModel(
    appId           = "test-001",
    appName         = "TestApp",
    sparkVersion    = "3.5.0",
    startTimeMs     = 0L,
    endTimeMs       = Some(300000L),
    sparkProperties = props,
    jobs            = jobs,
    stages          = stages,
    executors       = executors,
    sqlExecutions   = sqlExecs,
  )

  def sqlExec(
    id:          Long   = 0L,
    description: String = "query",
    plan:        String = "",
    jobIds:      Seq[Int] = Nil,
  ): SqlExecutionData = SqlExecutionData(
    executionId      = id,
    description      = description,
    physicalPlan     = plan,
    startTimeMs      = 0L,
    completionTimeMs = Some(1000L),
    jobIds           = jobIds,
  )
}
