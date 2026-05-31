package com.github.saadaouini.sparklens.model

case class SqlExecutionData(
  executionId:     Long,
  description:     String,
  physicalPlan:    String,
  startTimeMs:     Long,
  completionTimeMs: Option[Long],
  jobIds:          Seq[Int],
)
