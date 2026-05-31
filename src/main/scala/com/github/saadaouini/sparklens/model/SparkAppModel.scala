package com.github.saadaouini.sparklens.model

case class SparkAppModel(
  appId:           String,
  appName:         String,
  sparkVersion:    String,
  startTimeMs:     Long,
  endTimeMs:       Option[Long],
  sparkProperties: Map[String, String],
  jobs:            Map[Int, JobData],
  stages:          Map[Int, StageData],
  executors:       Map[String, ExecutorData],
  sqlExecutions:   Map[Long, SqlExecutionData],
) {
  def prop(key: String): Option[String]            = sparkProperties.get(key)
  def propOrDefault(key: String, default: String)  = sparkProperties.getOrElse(key, default)
  def durationMs: Option[Long]                     = endTimeMs.map(_ - startTimeMs)
}
