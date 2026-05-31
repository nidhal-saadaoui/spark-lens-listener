package com.github.saadaouini.sparklens.model

case class JobData(
  jobId:            Int,
  name:             String,
  stageIds:         Seq[Int],
  submissionTimeMs: Long,
  completionTimeMs: Option[Long],
  status:           String,
)
