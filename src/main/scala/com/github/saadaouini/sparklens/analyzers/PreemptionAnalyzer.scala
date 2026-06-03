package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object PreemptionAnalyzer extends Analyzer {
  private val MinTasks = 10

  private def adviceForReason(reason: String): (String, Option[String]) = {
    val r = reason.toLowerCase
    if (r.contains("killed by driver") || r.contains("blacklist") || r.contains("decommission"))
      (
        "The Spark driver removed this executor — typically caused by too many task failures " +
        "(executor blacklisting) or a dynamic allocation scale-down. Check the driver logs for " +
        "the root cause. If blacklisting, investigate the failing tasks or raise " +
        "spark.blacklist.task.maxTaskAttemptsPerExecutor. If dynamic allocation, this is expected.",
        Some("spark.blacklist.enabled=false\n# or investigate task failure root cause"),
      )
    else if (r.contains("lost") || r.contains("heartbeat") || r.contains("timeout"))
      (
        "Executor lost contact with the driver — usually a network partition or a GC pause " +
        "exceeding the heartbeat timeout. Increase spark.network.timeout and " +
        "spark.executor.heartbeatInterval, or investigate GC pressure on the executor.",
        Some("spark.network.timeout=300s\nspark.executor.heartbeatInterval=60s"),
      )
    else if (r.contains("container") || r.contains("overhead") || r.contains("memory limit"))
      (
        "YARN killed the container for exceeding physical memory limits. Increase " +
        "spark.yarn.executor.memoryOverhead or spark.yarn.executor.memoryOverheadFactor, " +
        "or reduce spark.executor.cores to lower concurrent memory demand.",
        Some("spark.yarn.executor.memoryOverheadFactor=0.2"),
      )
    else
      (
        "On YARN: increase spark.yarn.executor.memoryOverhead or reduce executor memory to avoid " +
        "container kill by the Node Manager. On k8s: set resource limits. " +
        "On spot/preemptible instances: enable dynamic allocation.",
        Some("spark.yarn.executor.memoryOverheadFactor=0.2"),
      )
  }

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val killedTaskRateWarn = propDouble(app, "spark.sparklens.preemption.killedTaskRateWarn", 0.05)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    val removedMidJob = app.executors.values.filter { exc =>
      exc.removedTimeMs.isDefined &&
      exc.removalReason.exists { r =>
        val rl = r.toLowerCase
        rl.contains("lost") || rl.contains("preempt") || rl.contains("kill") ||
        rl.contains("timeout") || rl.contains("heartbeat")
      }
    }
    if (removedMidJob.nonEmpty) {
      val hosts  = removedMidJob.map(_.host).toSeq.distinct.take(3).mkString(", ")
      val reason = removedMidJob.head.removalReason.getOrElse("unknown")
      val (advice, fix) = adviceForReason(reason)
      val impact = EstimatedImpact(
        summary     = s"${removedMidJob.size} executor(s) preempted — tasks rescheduled, cost depends on stage progress at preemption",
        savedTimeMs = None,
        savedBytes  = None,
        confidence  = "low",
      )
      issues += Issue(
        id              = "preemption-executor-lost-0",
        severity        = Warning,
        category        = "preemption",
        title           = s"${removedMidJob.size} Executor(s) Lost / Preempted",
        description     = s"Executors on $hosts were removed during the application (reason: $reason). Tasks had to be rescheduled.",
        recommendation  = advice,
        configFix       = fix,
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
          val rerunMs = (stage.durationMs * killed.toDouble / totalTasks).toLong
          val impact  = EstimatedImpact(
            summary     = s"$killed of $totalTasks tasks killed — ~${fmtMs(rerunMs)} wall-clock overhead",
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
