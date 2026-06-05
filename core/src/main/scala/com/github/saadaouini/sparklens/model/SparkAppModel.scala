package com.github.saadaouini.sparklens.model

/** Overhead incurred by SparkLens itself on the driver event bus. */
case class ListenerStats(
  taskEventsProcessed: Long,
  overheadMs:          Long,
)

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
  listenerStats:   ListenerStats = ListenerStats(0L, 0L),
) {
  def prop(key: String): Option[String]            = sparkProperties.get(key)
  def propOrDefault(key: String, default: String)  = sparkProperties.getOrElse(key, default)
  def durationMs: Option[Long]                     = endTimeMs.map(_ - startTimeMs)
}
