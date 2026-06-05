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
  rddCacheInfos:     Seq[RddCacheInfo] = Nil,
  parentIds:         Seq[Int]       = Nil,
  details:           String         = "",

  // Exact aggregate fields — populated by SparkAppModelBuilder from every task event,
  // regardless of whether the task was kept in the reservoir sample.
  // When hasExactAggregates is true, the computed methods below use these fields instead
  // of summing over the (sampled) tasks list, giving correct totals for any job size.
  hasExactAggregates:              Boolean = false,
  exactTaskCount:                  Int     = 0,
  exactFailedCount:                Int     = 0,
  exactKilledCount:                Int     = 0,
  exactSpeculativeCount:           Int     = 0,
  exactInputBytes:                 Long    = 0L,
  exactInputRecords:               Long    = 0L,
  exactOutputBytes:                Long    = 0L,
  exactOutputRecords:              Long    = 0L,
  exactResultSize:                 Long    = 0L,
  exactDiskSpillBytes:             Long    = 0L,
  exactMemorySpillBytes:           Long    = 0L,
  exactGcTimeMs:                   Long    = 0L,
  exactExecutorRunTimeMs:          Long    = 0L,
  exactExecutorCpuTimeNs:          Long    = 0L,
  exactExecutorDeserializeTimeMs:  Long    = 0L,
  exactPeakExecutionMemorySum:     Long    = 0L,
  exactShuffleRemoteBytes:         Long    = 0L,
  exactShuffleLocalBytes:          Long    = 0L,
  exactShuffleBytesWritten:        Long    = 0L,
  exactShuffleFetchWaitTimeMs:     Long    = 0L,
  exactShuffleRecordsRead:         Long    = 0L,
  exactShuffleRecordsWritten:      Long    = 0L,
  exactTasksWithInputBytes:        Int     = 0,
  exactTasksWithOutputBytes:       Int     = 0,
) {
  def durationMs: Long = (submissionTimeMs, completionTimeMs) match {
    case (Some(s), Some(e)) => e - s
    case _                  => 0L
  }

  // ── Computed metrics ────────────────────────────────────────────────────────
  // When hasExactAggregates is true (set by SparkAppModelBuilder after real task events),
  // the exactXxx fields hold precise totals computed from every task, regardless of the
  // reservoir sample in `tasks`.  When false (unit-test fixtures that set `tasks` directly),
  // the methods fall back to summing the tasks list.
  //
  // INVARIANT: every new computed method added here MUST follow the same
  // `if (hasExactAggregates) exactXxx else tasks.map(...).sum` pattern.

  def totalGcTimeMs: Long =
    if (hasExactAggregates) exactGcTimeMs
    else tasks.map(_.metrics.jvmGcTimeMs).sum

  def totalExecutorRunTimeMs: Long =
    if (hasExactAggregates) exactExecutorRunTimeMs
    else tasks.map(_.metrics.executorRunTimeMs).sum

  def totalExecutorDeserializeTimeMs: Long =
    if (hasExactAggregates) exactExecutorDeserializeTimeMs
    else tasks.map(_.metrics.executorDeserializeTimeMs).sum

  def totalDiskSpillBytes: Long =
    if (hasExactAggregates) exactDiskSpillBytes
    else tasks.map(_.metrics.diskBytesSpilled).sum

  def totalMemorySpillBytes: Long =
    if (hasExactAggregates) exactMemorySpillBytes
    else tasks.map(_.metrics.memoryBytesSpilled).sum

  /** Sum of peakExecutionMemory across all tasks. Divide by totalTaskCount for the average. */
  def totalPeakExecutionMemory: Long =
    if (hasExactAggregates) exactPeakExecutionMemorySum
    else tasks.map(_.metrics.peakExecutionMemory).sum

  def avgPeakExecutionMemory: Long = {
    val count = if (hasExactAggregates) exactTaskCount else tasks.size
    if (count > 0) totalPeakExecutionMemory / count else 0L
  }

  def totalInputBytes: Long =
    if (hasExactAggregates) exactInputBytes
    else tasks.map(_.metrics.inputBytesRead).sum

  def totalInputRecords: Long =
    if (hasExactAggregates) exactInputRecords
    else tasks.map(_.metrics.inputRecordsRead).sum

  def totalOutputBytes: Long =
    if (hasExactAggregates) exactOutputBytes
    else tasks.map(_.metrics.outputBytesWritten).sum

  def totalOutputRecords: Long =
    if (hasExactAggregates) exactOutputRecords
    else tasks.map(_.metrics.outputRecordsWritten).sum

  def totalShuffleRemoteBytes: Long =
    if (hasExactAggregates) exactShuffleRemoteBytes
    else tasks.map(_.metrics.shuffleRemoteBytesRead).sum

  def totalShuffleLocalBytes: Long =
    if (hasExactAggregates) exactShuffleLocalBytes
    else tasks.map(_.metrics.shuffleLocalBytesRead).sum

  def totalShuffleBytesWritten: Long =
    if (hasExactAggregates) exactShuffleBytesWritten
    else tasks.map(_.metrics.shuffleBytesWritten).sum

  def totalShuffleFetchWaitTimeMs: Long =
    if (hasExactAggregates) exactShuffleFetchWaitTimeMs
    else tasks.map(_.metrics.shuffleFetchWaitTimeMs).sum

  def totalShuffleRecordsRead: Long =
    if (hasExactAggregates) exactShuffleRecordsRead
    else tasks.map(_.metrics.shuffleRecordsRead).sum

  def totalShuffleRecordsWritten: Long =
    if (hasExactAggregates) exactShuffleRecordsWritten
    else tasks.map(_.metrics.shuffleRecordsWritten).sum

  def totalResultSize: Long =
    if (hasExactAggregates) exactResultSize
    else tasks.map(_.metrics.resultSize).sum

  def totalTaskCount: Int =
    if (hasExactAggregates) exactTaskCount else tasks.size

  /** First line of user code that submitted this stage, extracted from the callsite stack trace.
   *  Returns empty string when details is absent or contains only Spark/JVM internals. */
  def callSite: String = {
    val skip = Seq("org.apache.spark.", "scala.", "java.", "sun.", "jdk.",
                   "com.sun.", "at org.apache.", "at scala.", "at java.", "at sun.")
    details.linesIterator
      .map(_.trim)
      .filter(l => l.nonEmpty && !skip.exists(p => l.startsWith(p) || l.startsWith("at " + p)))
      .take(1)
      .mkString
  }
}
