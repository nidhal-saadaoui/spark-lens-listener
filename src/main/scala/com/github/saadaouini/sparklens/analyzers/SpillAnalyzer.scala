package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SpillAnalyzer extends Analyzer {
  private val WarnDiskBytes = 100L * MB
  private val CritDiskBytes = GB

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      val disk   = stage.totalDiskSpillBytes
      val memory = stage.totalMemorySpillBytes
      if (disk < WarnDiskBytes && memory == 0) Nil
      else {
        val severity = if (disk >= CritDiskBytes) Critical else Warning
        Seq(Issue(
          id             = s"spill-${stage.stageId}",
          severity       = severity,
          category       = "spill",
          title          = s"Disk Spill in Stage ${stage.stageId} (${stage.name})",
          description    = s"Stage spilled ${fmtBytes(disk)} to disk and ${fmtBytes(memory)} to memory. Executors ran out of memory during shuffle.",
          recommendation = "Increase executor memory (spark.executor.memory) or reduce parallelism. Enable AQE to auto-tune partition count and reduce per-task data.",
          configFix      = Some("spark.executor.memory=<increase>  or  spark.sql.adaptive.enabled=true"),
          affectedStages = Seq(stage.stageId),
          metrics        = Map("disk_bytes" -> disk.toString, "memory_bytes" -> memory.toString),
        ))
      }
    }
}
