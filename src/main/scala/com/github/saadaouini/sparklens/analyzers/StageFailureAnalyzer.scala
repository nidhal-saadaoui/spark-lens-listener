package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object StageFailureAnalyzer extends Analyzer {
  private val MinTasks = 5

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val failedTaskRateWarn = propDouble(app, "spark.sparklens.stageFailure.failedTaskRateWarn", 0.05)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // ── Job-level failures ──────────────────────────────────────────────────
    // A failed job (red marker in the Spark UI jobs timeline) means all retry
    // attempts for one of its stages were exhausted or an unhandled exception
    // reached the driver.  This is a higher-severity signal than a stage retry.
    app.jobs.values.foreach { job =>
      if (job.status == "FAILED") {
        val durationMs = job.completionTimeMs.map(_ - job.submissionTimeMs).getOrElse(0L)
        val impact = EstimatedImpact(
          summary     = s"Job ${job.jobId} (${job.name}) failed — ~${fmtMs(durationMs)} of work lost",
          savedTimeMs = timeOpt(durationMs),
          savedBytes  = None,
          confidence  = "high",
        )
        issues += Issue(
          id              = s"job-failed-${job.jobId}",
          severity        = Critical,
          category        = "reliability",
          title           = s"Job ${job.jobId} Failed — ${job.name}",
          description     = s"Job ${job.jobId} (${job.name}) did not complete successfully. All work performed by its stages was wasted.",
          recommendation  = "Check the driver logs for the root exception. Common causes: executor OOM (increase driver/executor memory), a stage that exhausted spark.task.maxFailures retries, or an unhandled application exception.",
          affectedJobs    = Seq(job.jobId),
          metrics         = Map("job_name" -> job.name, "duration_ms" -> durationMs.toString),
          estimatedImpact = Some(impact),
        )
      }
    }

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
          val rerunMs = (stage.durationMs * failed.toDouble / totalTasks).toLong
          val impact  = EstimatedImpact(
            summary     = s"$failed of $totalTasks tasks failed — ~${fmtMs(rerunMs)} wall-clock overhead",
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
