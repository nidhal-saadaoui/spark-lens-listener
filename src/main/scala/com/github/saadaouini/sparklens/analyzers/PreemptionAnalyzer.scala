package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object PreemptionAnalyzer extends Analyzer {
  private val MinTasks = 10

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val killedTaskRateWarn = propDouble(app, "spark.sparklens.preemption.killedTaskRateWarn", 0.05)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    val removedMidJob = app.executors.values.filter { exc =>
      exc.removedTimeMs.isDefined &&
      exc.removalReason.exists(r => r.toLowerCase.contains("lost") || r.toLowerCase.contains("preempt") || r.toLowerCase.contains("kill"))
    }
    if (removedMidJob.nonEmpty) {
      val hosts  = removedMidJob.map(_.host).toSeq.distinct.take(3).mkString(", ")
      val impact = EstimatedImpact(
        summary     = s"${removedMidJob.size} executor(s) preempted — tasks rescheduled, cost depends on stage progress at preemption",
        savedTimeMs = None,
        savedBytes  = None,
        confidence  = "low",
      )
      issues += Issue(
        id              = "preemption-executor-lost",
        severity        = Warning,
        category        = "preemption",
        title           = s"${removedMidJob.size} Executor(s) Lost / Preempted",
        description     = s"Executors on $hosts were removed during the application (reason: ${removedMidJob.head.removalReason.getOrElse("unknown")}). Tasks had to be rescheduled.",
        recommendation  = "On YARN: increase spark.yarn.executor.memoryOverhead or reduce executor memory to avoid container kill by the Node Manager. On k8s: set resource limits. On spot/preemptible instances: enable dynamic allocation.",
        configFix       = Some("spark.yarn.executor.memoryOverheadFactor=0.2"),
        metrics         = Map("lost_executors" -> removedMidJob.size.toString),
        estimatedImpact = Some(impact),
      )
    }

    app.stages.values.foreach { stage =>
      val totalTasks = if (stage.exactTaskCount > 0) stage.exactTaskCount else stage.tasks.size
      if (totalTasks >= MinTasks) {
        val killed     = if (stage.hasExactAggregates) stage.exactKilledCount else stage.tasks.count(_.killed)
        val killedRate = killed.toDouble / totalTasks
        if (killedRate >= killedTaskRateWarn) {
          val avgTaskMs = if (totalTasks > 0) stage.totalExecutorRunTimeMs / totalTasks else 0L
          val rerunMs   = killed * avgTaskMs
          val impact    = EstimatedImpact(
            summary     = s"$killed killed task(s) × ~${fmtMs(avgTaskMs)} avg = ~${fmtMs(rerunMs)} re-computation cost",
            savedTimeMs = timeOpt(rerunMs),
            savedBytes  = None,
            confidence  = "medium",
          )
          issues += Issue(
            id              = s"preemption-killed-${stage.stageId}",
            severity        = Warning,
            category        = "preemption",
            title           = s"High Task Kill Rate in Stage ${stage.stageId} — ${fmtDouble(killedRate * 100, 0)}%",
            description     = s"$killed of $totalTasks tasks were killed in stage ${stage.stageId} (${stage.name}). Indicates resource contention or speculation killing slow tasks.",
            recommendation  = "Disable speculation (spark.speculation=false) if tasks are killed by speculative execution. Otherwise investigate cluster resource pressure.",
            affectedStages  = Seq(stage.stageId),
            metrics         = Map("killed_tasks" -> killed.toString, "total_tasks" -> totalTasks.toString),
            estimatedImpact = Some(impact),
          )
        }
      }
    }

    issues.toSeq
  }
}
