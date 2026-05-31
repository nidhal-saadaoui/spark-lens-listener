package com.github.saadaouini.sparklens.model

case class StageData(
  stageId:           Int,
  attemptId:         Int,
  name:              String,
  numTasks:          Int,
  tasks:             Seq[TaskData]  = Nil,
  submissionTimeMs:  Option[Long]   = None,
  completionTimeMs:  Option[Long]   = None,
  failureReason:     Option[String] = None,
  rddNames:          Seq[String]    = Nil,
  rddCachedNames:    Set[String]    = Set.empty,
  details:           String         = "",
) {
  def durationMs: Long = (submissionTimeMs, completionTimeMs) match {
    case (Some(s), Some(e)) => e - s
    case _                  => 0L
  }

  def totalGcTimeMs: Long          = tasks.map(_.metrics.jvmGcTimeMs).sum
  def totalExecutorRunTimeMs: Long  = tasks.map(_.metrics.executorRunTimeMs).sum
  def totalDiskSpillBytes: Long     = tasks.map(_.metrics.diskBytesSpilled).sum
  def totalMemorySpillBytes: Long   = tasks.map(_.metrics.memoryBytesSpilled).sum
  def totalInputBytes: Long         = tasks.map(_.metrics.inputBytesRead).sum
  def totalShuffleRemoteBytes: Long = tasks.map(_.metrics.shuffleRemoteBytesRead).sum
  def totalShuffleLocalBytes: Long  = tasks.map(_.metrics.shuffleLocalBytesRead).sum
  def totalResultSize: Long         = tasks.map(_.metrics.resultSize).sum
}
