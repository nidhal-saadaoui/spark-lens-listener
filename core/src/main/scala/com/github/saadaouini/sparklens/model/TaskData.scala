package com.github.saadaouini.sparklens.model

case class TaskMetrics(
  executorRunTimeMs:        Long = 0L,
  executorCpuTimeNs:        Long = 0L,
  executorDeserializeTimeMs:Long = 0L,
  jvmGcTimeMs:              Long = 0L,
  memoryBytesSpilled:       Long = 0L,
  diskBytesSpilled:         Long = 0L,
  peakExecutionMemory:      Long = 0L,
  shuffleRemoteBytesRead:   Long = 0L,
  shuffleLocalBytesRead:    Long = 0L,
  shuffleBytesWritten:      Long = 0L,
  shuffleFetchWaitTimeMs:   Long = 0L,
  shuffleRecordsRead:       Long = 0L,
  shuffleRecordsWritten:    Long = 0L,
  inputBytesRead:           Long = 0L,
  inputRecordsRead:         Long = 0L,
  outputBytesWritten:       Long = 0L,
  outputRecordsWritten:     Long = 0L,
  resultSize:               Long = 0L,
)

case class TaskData(
  taskId:       Long,
  index:        Int,
  attempt:      Int,
  executorId:   String,
  host:         String,
  status:       String,
  launchTimeMs: Long,
  finishTimeMs: Long,
  failed:       Boolean,
  killed:       Boolean,
  speculative:  Boolean,
  errorMessage: Option[String],
  metrics:      TaskMetrics,
) {
  def durationMs: Long = finishTimeMs - launchTimeMs
}
