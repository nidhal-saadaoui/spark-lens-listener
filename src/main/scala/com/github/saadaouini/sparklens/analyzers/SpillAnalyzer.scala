package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object SpillAnalyzer extends Analyzer {

  /** Parse Spark memory strings like "4g", "2048m", "1024k" to bytes. */
  private def parseMemory(s: String): Option[Long] = {
    val lower = s.trim.toLowerCase
    scala.util.Try {
      if      (lower.endsWith("g")) lower.dropRight(1).toLong * 1024L * 1024L * 1024L
      else if (lower.endsWith("m")) lower.dropRight(1).toLong * 1024L * 1024L
      else if (lower.endsWith("k")) lower.dropRight(1).toLong * 1024L
      else                          lower.toLong
    }.toOption
  }

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val warnDiskBytes   = propLong(app, "spark.sparklens.spill.warnDiskMb",  100L) * MB
    val critDiskBytes   = propLong(app, "spark.sparklens.spill.critDiskMb", 1024L) * MB
    val diskSpeedMbps   = propLong(app, "spark.sparklens.impact.diskSpeedMbps", 200L)
    val executorMemory  = app.prop("spark.executor.memory").flatMap(parseMemory)

    app.stages.values.toSeq.flatMap { stage =>
      val disk   = stage.totalDiskSpillBytes
      val memory = stage.totalMemorySpillBytes
      if (disk < warnDiskBytes && memory == 0) Nil
      else {
        val severity   = if (disk >= critDiskBytes) Critical else Warning
        val penaltyMs  = diskMs(disk, diskSpeedMbps)

        // Precise recommendation: compare avg task peak memory to executor memory.
        val avgPeak     = stage.avgPeakExecutionMemory
        val taskCount   = if (stage.hasExactAggregates) stage.exactTaskCount else stage.tasks.size
        val memAdvice = executorMemory match {
          case Some(execMem) if avgPeak > 0 =>
            val needed = avgPeak * 2          // 2× for safety margin
            if (avgPeak > execMem * 0.8)
              s"Tasks averaged ${fmtBytes(avgPeak)} peak memory against ${fmtBytes(execMem)} executor heap — " +
              s"set spark.executor.memory=${fmtBytes(needed)} or raise " +
              s"spark.sql.shuffle.partitions to split data across more tasks."
            else
              s"Executor has ${fmtBytes(execMem)} but tasks averaged ${fmtBytes(avgPeak)} peak — " +
              s"raise spark.sql.shuffle.partitions to reduce per-task data volume."
          case _ =>
            "Enable AQE so Spark picks the right partition count, or increase spark.executor.memory. " +
            "As a last resort, increase the number of partitions manually to shrink each task's working set."
        }

        val impact = EstimatedImpact(
          summary     = s"~${fmtBytes(disk)} spilled to disk, ~${fmtMs(penaltyMs)} I/O penalty (est. $diskSpeedMbps MB/s disk)",
          savedTimeMs = timeOpt(penaltyMs),
          savedBytes  = bytesOpt(disk),
          confidence  = "medium",
        )
        Seq(Issue(
          id              = s"spill-${stage.stageId}",
          severity        = severity,
          category        = "spill",
          title           = s"Disk Spill in Stage ${stage.stageId} (${stage.name})",
          description     = s"Stage spilled ${fmtBytes(disk)} to disk and ${fmtBytes(memory)} to memory across $taskCount tasks. Disk I/O is 10–100× slower than RAM.${if (stage.callSite.nonEmpty) s" Triggered from: ${stage.callSite}." else ""}",
          recommendation  = memAdvice,
          configFix       = Some("spark.sql.adaptive.enabled=true\n# or: spark.executor.memory=4g"),
          affectedStages  = Seq(stage.stageId),
          metrics         = Map(
            "disk_bytes"       -> disk.toString,
            "memory_bytes"     -> memory.toString,
            "avg_peak_mem"     -> avgPeak.toString,
          ) ++ executorMemory.map(m => "executor_memory" -> m.toString).toMap,
          estimatedImpact = Some(impact),
        ))
      }
    }
  }
}
