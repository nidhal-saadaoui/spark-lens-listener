package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object OutputSmallFilesAnalyzer extends Analyzer {
  private val MinWritingTasks = 10

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val targetBytesPerTask = propLong(app, "spark.sparklens.outputSmallFiles.targetMb", 128L) * MB
    app.stages.values.toSeq.flatMap { stage =>
      val writingTasks = stage.tasks.filter(_.metrics.outputBytesWritten > 0)
      if (writingTasks.size < MinWritingTasks) Nil
      else {
        val totalOutput = writingTasks.map(_.metrics.outputBytesWritten).sum
        val avgPerTask  = totalOutput / writingTasks.size
        if (avgPerTask >= targetBytesPerTask / 2) Nil
        else {
          val targetTasks = math.max(1, (totalOutput.toDouble / targetBytesPerTask).round.toInt)
          Seq(Issue(
            id             = s"output-small-files-${stage.stageId}",
            severity       = Warning,
            category       = "io",
            title          = s"Small Output Files in Stage ${stage.stageId} — ${writingTasks.size} tasks writing avg ${fmtBytes(avgPerTask)}",
            description    = s"Stage ${stage.stageId} (${stage.name}) writes ${fmtBytes(totalOutput)} across ${writingTasks.size} tasks (avg ${fmtBytes(avgPerTask)}/task, ideal ≥ ${fmtBytes(targetBytesPerTask / 2)}). Each task produces one output file — ${writingTasks.size} small files slow every downstream job that reads this data.",
            recommendation = s"Reduce to ~$targetTasks output partitions. Use coalesce($targetTasks) to avoid a full shuffle, or repartition($targetTasks) if the data needs rebalancing first. On Delta/Iceberg, run OPTIMIZE / rewrite_data_files periodically instead.",
            codeFix        = Some(s"df.coalesce($targetTasks)" + ".write.parquet(outputPath)"),
            affectedStages = Seq(stage.stageId),
            metrics        = Map(
              "total_output_bytes" -> totalOutput.toString,
              "task_count"         -> writingTasks.size.toString,
              "avg_bytes_per_task" -> avgPerTask.toString,
            ),
          ))
        }
      }
    }
  }
}
