package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.EstimatedImpact

/**
 * Shared helpers for computing EstimatedImpact values.
 *
 * Speed assumptions are configurable via spark.sparklens.impact.* properties:
 *   spark.sparklens.impact.networkSpeedMbps  (default 1024 — 1 GB/s cluster network)
 *   spark.sparklens.impact.diskSpeedMbps     (default 200  — conservative SSD/HDD spill)
 *   spark.sparklens.impact.readSpeedMbps     (default 512  — HDFS/S3 effective read)
 */
object ImpactEstimator {

  /** Converts bytes to milliseconds at the given speed in MB/s. */
  def msFromBytes(bytes: Long, speedMbps: Long): Long =
    if (speedMbps <= 0 || bytes <= 0) 0L
    else (bytes / (speedMbps.toDouble * 1048576.0 / 1000.0)).toLong

  def networkMs(bytes: Long, speedMbps: Long = 1024L): Long = msFromBytes(bytes, speedMbps)
  def diskMs   (bytes: Long, speedMbps: Long = 200L):  Long = msFromBytes(bytes, speedMbps)
  def readMs   (bytes: Long, speedMbps: Long = 512L):  Long = msFromBytes(bytes, speedMbps)

  /** Wraps Some(ms) but returns None when ms == 0 so JSON shows null rather than 0. */
  def timeOpt(ms: Long): Option[Long] = if (ms > 0) Some(ms) else None
  def bytesOpt(b: Long): Option[Long] = if (b  > 0) Some(b)  else None

  /** Impact for issues where the metric is not quantifiable (config checks, etc.). */
  val configRisk: EstimatedImpact = EstimatedImpact(
    summary     = "Configuration risk — impact depends on workload scale",
    savedTimeMs = None,
    savedBytes  = None,
    confidence  = "low",
  )
}
