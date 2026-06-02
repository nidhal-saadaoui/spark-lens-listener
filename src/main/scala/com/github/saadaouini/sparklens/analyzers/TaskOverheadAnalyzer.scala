package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object TaskOverheadAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val warnRatio   = propDouble(app, "spark.sparklens.overhead.deserializeRatioWarn", 0.3)
    val minDuration = propLong(app,   "spark.sparklens.overhead.minStageSec",          5L) * 1000L

    app.stages.values.toSeq.flatMap { stage =>
      val runTime     = stage.totalExecutorRunTimeMs
      val deserTime   = stage.totalExecutorDeserializeTimeMs
      val taskCount   = if (stage.hasExactAggregates) stage.exactTaskCount else stage.tasks.size

      if (runTime < minDuration || deserTime == 0 || taskCount == 0) Nil
      else {
        val ratio = deserTime.toDouble / runTime
        if (ratio < warnRatio) Nil
        else {
          val pct          = fmtDouble(ratio * 100, 0)
          val avgDeserMs   = deserTime / taskCount
          val avgRunMs     = runTime / taskCount
          val wastedMs     = deserTime  // overhead that would disappear with fewer, larger tasks
          Seq(Issue(
            id              = s"task-overhead-${stage.stageId}",
            severity        = Warning,
            category        = "io",
            title           = s"High Task Serialisation Overhead in Stage ${stage.stageId} — $pct% of task time on JVM setup",
            description     =
              s"Stage ${stage.stageId} ($taskCount tasks) spent $pct% of executor time deserializing tasks " +
              s"(avg ${fmtMs(avgDeserMs)} setup vs ${fmtMs(avgRunMs)} work per task). " +
              s"This is the signature of too many small tasks — JVM startup cost dominates actual computation." +
              s"${if (stage.callSite.nonEmpty) s" Triggered from: ${stage.callSite}." else ""}",
            recommendation  =
              s"Reduce the number of tasks in this stage by increasing spark.sql.files.maxPartitionBytes " +
              s"(for scan stages) or coalescing partitions after a shuffle. " +
              s"Target tasks of at least 128 MB of input data.",
            configFix       = Some(
              "spark.sql.files.maxPartitionBytes=268435456  # 256 MB\n" +
              "spark.sql.adaptive.coalescePartitions.enabled=true"
            ),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "deserialize_ms"    -> deserTime.toString,
              "executor_run_ms"   -> runTime.toString,
              "deserialize_ratio" -> fmtDouble(ratio, 3),
              "task_count"        -> taskCount.toString,
            ),
            estimatedImpact = Some(EstimatedImpact(
              summary     = s"${fmtMs(wastedMs)} of task deserialization overhead across $taskCount tasks",
              savedTimeMs = timeOpt(wastedMs),
              savedBytes  = None,
              confidence  = "medium",
            )),
          ))
        }
      }
    }
  }
}
