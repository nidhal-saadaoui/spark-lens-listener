package com.github.saadaouini.sparklens.model

case class SqlExecutionData(
  executionId:     Long,
  description:     String,
  physicalPlan:    String,
  startTimeMs:     Long,
  completionTimeMs: Option[Long],
  jobIds:          Seq[Int],
  /** Structured physical plan tree with per-operator accumulator IDs and resolved metric values. */
  planTree:        Option[PlanNode] = None,
)
