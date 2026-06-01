package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object StageFailureAnalyzer extends Analyzer {
  private val MinTasks = 5

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val failedTaskRateWarn = propDouble(app, "spark.sparklens.stageFailure.failedTaskRateWarn", 0.05)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    app.stages.values.foreach { stage =>
      if (stage.attemptId > 0) {
        val retryMs = stage.durationMs
        val impact  = EstimatedImpact(
          summary     = s"Stage ${stage.stageId} retried ${stage.attemptId} time(s) — ~${fmtMs(retryMs * stage.attemptId)} lost to retries",
          savedTimeMs = timeOpt(retryMs * stage.attemptId),
          savedBytes  = None,
          confidence  = "high",
        )
        issues += Issue(
          id              = s"stage-retry-${stage.stageId}",
          severity        = Warning,
          category        = "reliability",
          title           = s"Stage ${stage.stageId} Retried (Attempt ${stage.attemptId})",
          description     = stage.failureReason
            .map(r => s"Stage ${stage.stageId} (${stage.name}) failed and was retried. Reason: ${r.take(200)}")
            .getOrElse(s"Stage ${stage.stageId} (${stage.name}) was retried without a recorded reason."),
          recommendation  = "Investigate the stage failure reason. Common causes: executor OOM (increase memory), shuffle fetch failures (increase spark.reducer.maxReqsInFlight), or transient HDFS errors.",
          affectedStages  = Seq(stage.stageId),
          metrics         = Map("attempt_id" -> stage.attemptId.toString),
          estimatedImpact = Some(impact),
        )
      }

      val totalTasks = if (stage.exactTaskCount > 0) stage.exactTaskCount else stage.tasks.size
      if (totalTasks >= MinTasks) {
        val failed     = if (stage.hasExactAggregates) stage.exactFailedCount else stage.tasks.count(_.failed)
        val failedRate = failed.toDouble / totalTasks
        if (failedRate >= failedTaskRateWarn) {
          val sample = stage.tasks
            .filter(_.failed)
            .flatMap(_.errorMessage)
            .headOption
            .map(_.take(150))
            .getOrElse("no error message recorded")
          val avgTaskMs = if (totalTasks > 0) stage.totalExecutorRunTimeMs / totalTasks else 0L
          val rerunMs   = failed * avgTaskMs
          val impact    = EstimatedImpact(
            summary     = s"$failed failed task(s) × ~${fmtMs(avgTaskMs)} avg = ~${fmtMs(rerunMs)} wasted re-computation",
            savedTimeMs = timeOpt(rerunMs),
            savedBytes  = None,
            confidence  = "medium",
          )
          issues += Issue(
            id              = s"task-failure-${stage.stageId}",
            severity        = Warning,
            category        = "reliability",
            title           = s"High Task Failure Rate in Stage ${stage.stageId} — ${fmtDouble(failedRate * 100, 0)}%",
            description     = s"$failed of $totalTasks tasks failed in stage ${stage.stageId} (${stage.name}). Sample error: $sample",
            recommendation  = "Check executor logs for OOM, network errors, or application-level exceptions. Increase spark.task.maxFailures if transient failures are expected.",
            affectedStages  = Seq(stage.stageId),
            metrics         = Map("failed_tasks" -> failed.toString, "total_tasks" -> totalTasks.toString),
            estimatedImpact = Some(impact),
          )
        }
      }
    }

    issues.toSeq
  }
}
