package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object SchedulerDelayAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val warnMs  = propLong(app, "spark.sparklens.schedulerDelay.warnMs",   2000L)
    val minTasks = propLong(app, "spark.sparklens.schedulerDelay.minTasks", 5L).toInt

    app.stages.values.toSeq.flatMap { s =>
      val submitMs = s.submissionTimeMs.getOrElse(0L)
      val delays   = s.tasks
        .filter(t => !t.failed && !t.killed && t.launchTimeMs >= submitMs)
        .map(t => t.launchTimeMs - submitMs)
        .sorted

      if (delays.size < minTasks) Nil
      else {
        val p50Delay = percentile(delays, 50)
        val p95Delay = percentile(delays, 95)
        if (p50Delay < warnMs) Nil
        else {
          val dur        = s.durationMs
          // Scheduler delay adds directly to stage wall time; savings ≈ median delay
          val savedMs    = timeOpt(p50Delay * math.min(delays.size, 10).toLong / 10)
          val totalCores = app.executors.values.map(_.totalCores).sum
          Seq(Issue(
            id              = s"scheduler-delay-${s.stageId}",
            severity        = if (p50Delay >= 5000L) Warning else Info,
            category        = "config",
            title           = s"High Scheduler Delay in Stage ${s.stageId} — median ${fmtMs(p50Delay)} to first task",
            description     =
              s"Tasks in stage ${s.stageId} took a median of ${fmtMs(p50Delay)} to launch " +
              s"after stage submission (p95: ${fmtMs(p95Delay)}). " +
              s"This idle time adds directly to stage wall time before any computation starts.",
            recommendation  =
              "High scheduler delay typically indicates: (1) all executors are busy with tasks " +
              "from earlier stages — increase cluster size or use dynamic allocation; " +
              "(2) data locality wait is too long — reduce spark.locality.wait; " +
              "(3) driver GC is pausing the scheduler — tune driver heap.",
            configFix       = Some(
              s"spark.locality.wait=0s  # if locality is the cause\n" +
              s"# or enable dynamic allocation:\n" +
              s"# spark.dynamicAllocation.enabled=true"
            ),
            affectedStages  = Seq(s.stageId),
            metrics         = Map(
              "median_launch_delay_ms" -> p50Delay.toString,
              "p95_launch_delay_ms"    -> p95Delay.toString,
              "stage_duration_ms"      -> dur.toString,
              "tasks_sampled"          -> delays.size.toString,
              "total_cluster_cores"    -> totalCores.toString,
            ),
            estimatedImpact = Some(EstimatedImpact(
              summary     = s"median scheduler delay ${fmtMs(p50Delay)} in stage ${s.stageId}",
              savedTimeMs = savedMs,
              savedBytes  = None,
              confidence  = "medium",
            )),
          ))
        }
      }
    }
  }
}
