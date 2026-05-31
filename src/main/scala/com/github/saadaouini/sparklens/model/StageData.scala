package com.github.saadaouini.sparklens.model

case class StageData(
  stageId:           Int,
  attemptId:         Int,
  name:              String,
  numTasks:          Int,
  tasks:             Seq[TaskData]  = Nil,   // reservoir-sampled ≤ 10 K per stage
  submissionTimeMs:  Option[Long]   = None,
  completionTimeMs:  Option[Long]   = None,
  failureReason:     Option[String] = None,
  rddNames:          Seq[String]    = Nil,
  rddCachedNames:    Set[String]    = Set.empty,
  details:           String         = "",

  // Exact aggregate fields — populated by SparkAppModelBuilder from every task event,
  // regardless of whether the task was kept in the reservoir sample.
  // When hasExactAggregates is true, the computed methods below use these fields instead
  // of summing over the (sampled) tasks list, giving correct totals for any job size.
  hasExactAggregates:        Boolean = false,
  exactTaskCount:            Int     = 0,
  exactFailedCount:          Int     = 0,
  exactKilledCount:          Int     = 0,
  exactSpeculativeCount:     Int     = 0,
  exactInputBytes:           Long    = 0L,
  exactOutputBytes:          Long    = 0L,
  exactResultSize:           Long    = 0L,
  exactDiskSpillBytes:       Long    = 0L,
  exactMemorySpillBytes:     Long    = 0L,
  exactGcTimeMs:             Long    = 0L,
  exactExecutorRunTimeMs:    Long    = 0L,
  exactExecutorCpuTimeNs:    Long    = 0L,
  exactShuffleRemoteBytes:   Long    = 0L,
  exactShuffleLocalBytes:    Long    = 0L,
  exactShuffleBytesWritten:  Long    = 0L,
  exactTasksWithInputBytes:  Int     = 0,
  exactTasksWithOutputBytes: Int     = 0,
) {
  def durationMs: Long = (submissionTimeMs, completionTimeMs) match {
    case (Some(s), Some(e)) => e - s
    case _                  => 0L
  }

  // Prefer exact aggregates when available; fall back to summing the (possibly sampled)
  // tasks list so that unit-test fixtures that set tasks directly continue to work.
  def totalGcTimeMs: Long =
    if (hasExactAggregates) exactGcTimeMs
    else tasks.map(_.metrics.jvmGcTimeMs).sum

  def totalExecutorRunTimeMs: Long =
    if (hasExactAggregates) exactExecutorRunTimeMs
    else tasks.map(_.metrics.executorRunTimeMs).sum

  def totalDiskSpillBytes: Long =
    if (hasExactAggregates) exactDiskSpillBytes
    else tasks.map(_.metrics.diskBytesSpilled).sum

  def totalMemorySpillBytes: Long =
    if (hasExactAggregates) exactMemorySpillBytes
    else tasks.map(_.metrics.memoryBytesSpilled).sum

  def totalInputBytes: Long =
    if (hasExactAggregates) exactInputBytes
    else tasks.map(_.metrics.inputBytesRead).sum

  def totalShuffleRemoteBytes: Long =
    if (hasExactAggregates) exactShuffleRemoteBytes
    else tasks.map(_.metrics.shuffleRemoteBytesRead).sum

  def totalShuffleLocalBytes: Long =
    if (hasExactAggregates) exactShuffleLocalBytes
    else tasks.map(_.metrics.shuffleLocalBytesRead).sum

  def totalOutputBytes: Long =
    if (hasExactAggregates) exactOutputBytes
    else tasks.map(_.metrics.outputBytesWritten).sum

  def totalResultSize: Long =
    if (hasExactAggregates) exactResultSize
    else tasks.map(_.metrics.resultSize).sum
}
