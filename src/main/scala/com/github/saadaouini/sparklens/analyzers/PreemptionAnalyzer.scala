package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object PreemptionAnalyzer extends Analyzer {
  private val MinTasks = 10

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val killedTaskRateWarn = propDouble(app, "spark.sparklens.preemption.killedTaskRateWarn", 0.05)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // Executor lost mid-job (removed while jobs were running)
    val removedMidJob = app.executors.values.filter { exc =>
      exc.removedTimeMs.isDefined &&
      exc.removalReason.exists(r => r.toLowerCase.contains("lost") || r.toLowerCase.contains("preempt") || r.toLowerCase.contains("kill"))
    }
    if (removedMidJob.nonEmpty) {
      val hosts = removedMidJob.map(_.host).toSeq.distinct.take(3).mkString(", ")
      issues += Issue(
        id             = "preemption-executor-lost",
        severity       = Warning,
        category       = "preemption",
        title          = s"${removedMidJob.size} Executor(s) Lost / Preempted",
        description    = s"Executors on $hosts were removed during the application (reason: ${removedMidJob.head.removalReason.getOrElse("unknown")}). Tasks had to be rescheduled.",
        recommendation = "On YARN: increase spark.yarn.executor.memoryOverhead or reduce executor memory to avoid container kill by the Node Manager. On k8s: set resource limits. On spot/preemptible instances: enable dynamic allocation.",
        configFix      = Some("spark.yarn.executor.memoryOverheadFactor=0.2"),
        metrics        = Map("lost_executors" -> removedMidJob.size.toString),
      )
    }

    // High task kill rate per stage
    app.stages.values.foreach { stage =>
      if (stage.tasks.size >= MinTasks) {
        val killed     = stage.tasks.count(_.killed)
        val killedRate = killed.toDouble / stage.tasks.size
        if (killedRate >= killedTaskRateWarn) {
          issues += Issue(
            id             = s"preemption-killed-${stage.stageId}",
            severity       = Warning,
            category       = "preemption",
            title          = s"High Task Kill Rate in Stage ${stage.stageId} — ${fmtDouble(killedRate * 100, 0)}%",
            description    = s"$killed of ${stage.tasks.size} tasks were killed in stage ${stage.stageId} (${stage.name}). Indicates resource contention or speculation killing slow tasks.",
            recommendation = "Disable speculation (spark.speculation=false) if tasks are killed by speculative execution. Otherwise investigate cluster resource pressure.",
            affectedStages = Seq(stage.stageId),
            metrics        = Map("killed_tasks" -> killed.toString, "total_tasks" -> stage.tasks.size.toString),
          )
        }
      }
    }

    issues.toSeq
  }
}
