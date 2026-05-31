package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SpillAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val warnDiskBytes = propLong(app, "spark.sparklens.spill.warnDiskMb",  100L) * MB
    val critDiskBytes = propLong(app, "spark.sparklens.spill.critDiskMb", 1024L) * MB
    app.stages.values.toSeq.flatMap { stage =>
      val disk   = stage.totalDiskSpillBytes
      val memory = stage.totalMemorySpillBytes
      if (disk < warnDiskBytes && memory == 0) Nil
      else {
        val severity = if (disk >= critDiskBytes) Critical else Warning
        Seq(Issue(
          id             = s"spill-${stage.stageId}",
          severity       = severity,
          category       = "spill",
          title          = s"Disk Spill in Stage ${stage.stageId} (${stage.name})",
          description    = s"Stage spilled ${fmtBytes(disk)} to disk and ${fmtBytes(memory)} to memory. Disk I/O is 10–100× slower than RAM — spill directly lengthens task duration and, if disk space runs out, causes the stage to fail and retry.",
          recommendation = "Reduce per-task data volume: enable AQE so Spark picks the right partition count, or increase spark.executor.memory. As a last resort, increase the number of partitions manually to shrink each task's working set.",
          configFix      = Some("spark.sql.adaptive.enabled=true\n# or: spark.executor.memory=4g  (increase to fit data in RAM)"),
          affectedStages = Seq(stage.stageId),
          metrics        = Map("disk_bytes" -> disk.toString, "memory_bytes" -> memory.toString),
        ))
      }
    }
  }
}
