package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object StageParallelismAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val singleTaskMinMs = propLong(app, "spark.sparklens.stageParallelism.singleTaskMinMs", 5000L)

    // Single-task stages run serially on one core; flag before the core-utilisation check.
    val totalCoresForSingle = app.executors.values.map(_.totalCores).sum
    val singleTaskIssues: Seq[Issue] = app.stages.values.toSeq.flatMap { stage =>
      if (stage.numTasks != 1 || stage.durationMs < singleTaskMinMs) Nil
      else {
        val dur         = stage.durationMs
        val idleCores   = math.max(0, totalCoresForSingle - 1)
        // Theoretical savings: if the work could be split across all cores, wall-clock
        // time would drop by (1 - 1/totalCores). Use totalCores=2 floor so single-core
        // local-mode jobs don't produce misleading 0ms savings.
        val parallelism = math.max(totalCoresForSingle, 2)
        val savedMs     = dur * (parallelism - 1) / parallelism
        val singleImpact = EstimatedImpact(
          summary     = s"${fmtMs(dur)} stage ran on 1 task — $idleCores cores sat idle",
          savedTimeMs = timeOpt(savedMs),
          savedBytes  = None,
          confidence  = "medium",
        )
        Seq(Issue(
          id              = s"single-task-${stage.stageId}",
          severity        = Warning,
          category        = "io",
          title           = s"Single-Task Stage ${stage.stageId} — Entire Stage Runs on One Executor",
          description     = s"Stage ${stage.stageId} (${stage.name}) ran as a single task for ${fmtMs(dur)}, leaving all other cluster resources idle.",
          recommendation  = "Check whether repartition(1) or coalesce(1) was called upstream. " +
            "A CollectLimit or TakeOrderedAndProject node in the plan also forces single-task execution. " +
            "Increase partitions to distribute work across available cores.",
          codeFix         = Some("df.repartition(spark.sparkContext.defaultParallelism)"),
          affectedStages  = Seq(stage.stageId),
          metrics         = Map(
            "num_tasks"   -> "1",
            "duration_ms" -> dur.toString,
          ),
          estimatedImpact = Some(singleImpact),
        ))
      }
    }

    val minCores     = propLong(app,   "spark.sparklens.stageParallelism.minCores",             8L).toInt
    val utilRatio    = propDouble(app, "spark.sparklens.stageParallelism.underutilizationRatio", 0.5)
    val minStageSec  = propLong(app,   "spark.sparklens.stageParallelism.minStageSec",          10L)

    val totalCores = totalCoresForSingle
    val coreIssues: Seq[Issue] = if (totalCores < minCores) Nil
    else {
      val threshold = math.max(1, (totalCores * utilRatio).toInt)
      app.stages.values.toSeq.flatMap { stage =>
        val dur = stage.durationMs
        if (dur < minStageSec * 1000L || stage.numTasks >= threshold) Nil
        else {
          val utilPct   = fmtDouble(stage.numTasks.toDouble / totalCores * 100, 0)
          val idleCores = totalCores - stage.numTasks
          val impact    = EstimatedImpact(
            summary     = s"$idleCores of $totalCores cores idle for ${fmtMs(dur)} — stage under-parallelised at $utilPct%",
            savedTimeMs = timeOpt((dur * (1.0 - stage.numTasks.toDouble / threshold)).toLong),
            savedBytes  = None,
            confidence  = "medium",
          )
          Seq(Issue(
            id              = s"low-parallelism-${stage.stageId}",
            severity        = Info,
            category        = "io",
            title           = s"Low Parallelism in Stage ${stage.stageId} — ${stage.numTasks} tasks on $totalCores cores ($utilPct% utilization)",
            description     = s"Stage ${stage.stageId} (${stage.name}) ran ${stage.numTasks} tasks on a cluster with $totalCores available cores, using only $utilPct% of available parallelism. The stage ran for ${fmtMs(dur)} with most cores idle.",
            recommendation  = s"Increase the number of partitions to at least $threshold (${(utilRatio * 100).toInt}% of $totalCores cores). For shuffle stages raise spark.sql.shuffle.partitions; for scan stages lower spark.sql.files.maxPartitionBytes or call repartition() after the scan.",
            configFix       = Some(s"spark.sql.shuffle.partitions=$threshold"),
            codeFix         = Some(s"df.repartition($threshold)"),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "num_tasks"   -> stage.numTasks.toString,
              "total_cores" -> totalCores.toString,
              "util_pct"    -> utilPct,
              "duration_ms" -> dur.toString,
            ),
            estimatedImpact = Some(impact),
          ))
        }
      }
    }

    singleTaskIssues ++ coreIssues
  }
}
