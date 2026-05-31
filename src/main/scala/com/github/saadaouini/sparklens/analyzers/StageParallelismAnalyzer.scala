package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object StageParallelismAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val minCores     = propLong(app,   "spark.sparklens.stageParallelism.minCores",             8L).toInt
    val utilRatio    = propDouble(app, "spark.sparklens.stageParallelism.underutilizationRatio", 0.5)
    val minStageSec  = propLong(app,   "spark.sparklens.stageParallelism.minStageSec",          10L)

    val totalCores = app.executors.values.map(_.totalCores).sum
    if (totalCores < minCores) Nil
    else {
      val threshold = math.max(1, (totalCores * utilRatio).toInt)
      app.stages.values.toSeq.flatMap { stage =>
        val dur = stage.durationMs
        if (dur < minStageSec * 1000L || stage.numTasks >= threshold) Nil
        else {
          val utilPct = fmtDouble(stage.numTasks.toDouble / totalCores * 100, 0)
          Seq(Issue(
            id             = s"low-parallelism-${stage.stageId}",
            severity       = Info,
            category       = "io",
            title          = s"Low Parallelism in Stage ${stage.stageId} — ${stage.numTasks} tasks on $totalCores cores ($utilPct% utilization)",
            description    = s"Stage ${stage.stageId} (${stage.name}) ran ${stage.numTasks} tasks on a cluster with $totalCores available cores, using only $utilPct% of available parallelism. The stage ran for ${fmtMs(dur)} with most cores idle.",
            recommendation = s"Increase the number of partitions to at least $threshold (${(utilRatio * 100).toInt}% of $totalCores cores). For shuffle stages raise spark.sql.shuffle.partitions; for scan stages lower spark.sql.files.maxPartitionBytes or call repartition() after the scan.",
            configFix      = Some(s"spark.sql.shuffle.partitions=$threshold"),
            codeFix        = Some(s"df.repartition($threshold)"),
            affectedStages = Seq(stage.stageId),
            metrics        = Map(
              "num_tasks"   -> stage.numTasks.toString,
              "total_cores" -> totalCores.toString,
              "util_pct"    -> utilPct,
              "duration_ms" -> dur.toString,
            ),
          ))
        }
      }
    }
  }
}
