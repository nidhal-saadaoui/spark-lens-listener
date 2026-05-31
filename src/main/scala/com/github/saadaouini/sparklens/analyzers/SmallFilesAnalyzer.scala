package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SmallFilesAnalyzer extends Analyzer {
  private val MinTasks       = 10
  private val SmallFileRatio = 0.5

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val targetBytesPerTask = propLong(app, "spark.sparklens.smallFiles.targetMb", 128L) * MB
    app.stages.values.toSeq.flatMap { stage =>
      val inputTasks = stage.tasks.filter(_.metrics.inputBytesRead > 0)
      // Skip stages that read from in-memory cache: cached RDD blocks are tracked through
      // inputBytesRead but represent memory reads, not source-file I/O.
      if (inputTasks.size < MinTasks || stage.rddCachedNames.nonEmpty) Nil
      else {
        val totalInput = stage.totalInputBytes
        val avgPerTask = totalInput / inputTasks.size
        val smallTasks = inputTasks.count(_.metrics.inputBytesRead < targetBytesPerTask / 4)
        val smallRatio = smallTasks.toDouble / inputTasks.size

        if (avgPerTask > 0 && avgPerTask < targetBytesPerTask / 2 && smallRatio >= SmallFileRatio) {
          val targetTasks = math.max(1, (totalInput.toDouble / targetBytesPerTask).round.toInt)
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
}
