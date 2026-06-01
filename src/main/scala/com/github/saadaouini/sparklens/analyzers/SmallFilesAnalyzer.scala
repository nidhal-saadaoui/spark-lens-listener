package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object SmallFilesAnalyzer extends Analyzer {
  private val MinTasks       = 10
  private val SmallFileRatio = 0.5

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val targetBytesPerTask = propLong(app, "spark.sparklens.smallFiles.targetMb", 128L) * MB
    app.stages.values.toSeq.flatMap { stage =>
      val inputTaskCount = if (stage.hasExactAggregates) stage.exactTasksWithInputBytes
                           else stage.tasks.count(_.metrics.inputBytesRead > 0)
      if (inputTaskCount < MinTasks || stage.rddCachedNames.nonEmpty) Nil
      else {
        val totalInput = stage.totalInputBytes
        val avgPerTask = totalInput / inputTaskCount
        val sampleInputTasks = stage.tasks.filter(_.metrics.inputBytesRead > 0)
        val sampledSmall = sampleInputTasks.count(_.metrics.inputBytesRead < targetBytesPerTask / 4)
        val smallTasks = if (sampleInputTasks.nonEmpty && inputTaskCount > sampleInputTasks.size)
          (sampledSmall.toDouble / sampleInputTasks.size * inputTaskCount).round.toInt
        else sampledSmall
        val smallRatio = smallTasks.toDouble / inputTaskCount

        if (avgPerTask > 0 && avgPerTask < targetBytesPerTask / 2 && smallRatio >= SmallFileRatio) {
          val targetTasks = math.max(1, (totalInput.toDouble / targetBytesPerTask).round.toInt)
          val impact      = EstimatedImpact(
            summary     = s"$inputTaskCount small file tasks (avg ${fmtBytes(avgPerTask)}) — open/metadata overhead at each reader",
            savedTimeMs = None,
            savedBytes  = bytesOpt(totalInput),
            confidence  = "low",
          )
          Seq(Issue(
            id              = s"small-files-${stage.stageId}",
            severity        = Warning,
            category        = "io",
            title           = s"Small Files in Stage ${stage.stageId} — $inputTaskCount tasks reading avg ${fmtBytes(avgPerTask)}",
            description     = s"Stage ${stage.stageId} reads ${fmtBytes(totalInput)} across $inputTaskCount tasks (avg ${fmtBytes(avgPerTask)}/task). Ideal task size is 128–256 MB.",
            recommendation  = s"Target $targetTasks tasks for this stage (${fmtBytes(totalInput)} ÷ 128 MB). Compact source files to ~128–256 MB using Delta OPTIMIZE, Hudi compaction, or Iceberg rewrite_data_files.",
            configFix       = Some("spark.sql.files.maxPartitionBytes=134217728  # 128 MB"),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "total_input_bytes"  -> totalInput.toString,
              "task_count"         -> inputTaskCount.toString,
              "avg_bytes_per_task" -> avgPerTask.toString,
            ),
            estimatedImpact = Some(impact),
          ))
        } else Nil
      }
    }
  }
}
