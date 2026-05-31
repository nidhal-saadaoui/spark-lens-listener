package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SmallFilesAnalyzer extends Analyzer {
  private val TargetBytesPerTask = 128L * MB
  private val MinTasks           = 10
  private val SmallFileRatio     = 0.5  // majority of tasks have < target bytes

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      val inputTasks = stage.tasks.filter(_.metrics.inputBytesRead > 0)
      if (inputTasks.size < MinTasks) Nil
      else {
        val totalInput = stage.totalInputBytes
        val avgPerTask = totalInput / inputTasks.size
        val smallTasks = inputTasks.count(_.metrics.inputBytesRead < TargetBytesPerTask / 4)
        val smallRatio = smallTasks.toDouble / inputTasks.size

        if (avgPerTask > 0 && avgPerTask < TargetBytesPerTask / 2 && smallRatio >= SmallFileRatio) {
          val targetTasks = math.max(1, (totalInput.toDouble / TargetBytesPerTask).round.toInt)
          Seq(Issue(
            id             = s"small-files-${stage.stageId}",
            severity       = Warning,
            category       = "io",
            title          = s"Small Files in Stage ${stage.stageId} — ${inputTasks.size} tasks reading avg ${fmtBytes(avgPerTask)}",
            description    = s"Stage ${stage.stageId} reads ${fmtBytes(totalInput)} across ${inputTasks.size} tasks (avg ${fmtBytes(avgPerTask)}/task). Ideal task size is 128–256 MB.",
            recommendation = s"Target $targetTasks tasks for this stage (${fmtBytes(totalInput)} ÷ 128 MB). Compact source files to ~128–256 MB using Delta OPTIMIZE, Hudi compaction, or Iceberg rewrite_data_files.",
            configFix      = Some("spark.sql.files.maxPartitionBytes=134217728  # 128 MB"),
            affectedStages = Seq(stage.stageId),
            metrics        = Map(
              "total_input_bytes" -> totalInput.toString,
              "task_count"        -> inputTasks.size.toString,
              "avg_bytes_per_task" -> avgPerTask.toString,
            ),
          ))
        } else Nil
      }
    }
}
