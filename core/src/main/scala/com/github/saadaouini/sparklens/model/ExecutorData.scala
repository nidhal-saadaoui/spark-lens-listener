package com.github.saadaouini.sparklens.model

case class ExecutorData(
  executorId:     String,
  host:           String,
  totalCores:     Int,
  addedTimeMs:    Long,
  removedTimeMs:  Option[Long],
  removalReason:  Option[String],
)
