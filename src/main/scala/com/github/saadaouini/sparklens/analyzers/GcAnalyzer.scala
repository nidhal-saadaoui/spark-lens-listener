package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object GcAnalyzer extends Analyzer {
  private val MinRunTimeMs = 10000L

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val warnFraction = propDouble(app, "spark.sparklens.gc.warnFraction", 0.10)
    val critFraction = propDouble(app, "spark.sparklens.gc.critFraction", 0.20)
    app.stages.values.toSeq.flatMap { stage =>
      val runTime = stage.totalExecutorRunTimeMs
      val gcTime  = stage.totalGcTimeMs
      if (runTime < MinRunTimeMs || gcTime == 0) Nil
      else {
        val fraction = gcTime.toDouble / runTime
        if (fraction < warnFraction) Nil
        else {
          val pct      = s"${fmtDouble(fraction * 100, 0)}%"
          val severity = if (fraction >= critFraction) Critical else Warning
          val impact   = EstimatedImpact(
            summary     = s"~${fmtMs(gcTime)} spent in GC ($pct of executor time in stage ${stage.stageId})",
            savedTimeMs = timeOpt(gcTime),
            savedBytes  = None,
            confidence  = "high",
          )
          Seq(Issue(
            id              = s"gc-${stage.stageId}",
            severity        = severity,
            category        = "gc",
            title           = s"High GC Overhead in Stage ${stage.stageId} — $pct of executor time",
            description     = s"Executors spent $pct of their time in JVM garbage collection in stage ${stage.stageId} (${stage.name}). GC pauses stop all threads on the executor — tasks stall mid-execution, and Spark's heartbeat may time out, causing the executor to be marked lost.",
            recommendation  = "Increase spark.executor.memory so the heap has headroom. Switch to G1GC which pauses shorter and more predictably than the default collector. Reduce object churn: use primitive arrays over boxed collections, and avoid large intermediate DataFrames held in executor memory.",
            configFix       = Some("spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map("gc_fraction" -> fmtDouble(fraction, 3), "gc_ms" -> gcTime.toString, "run_ms" -> runTime.toString),
            estimatedImpact = Some(impact),
          ))
        }
      }
    }
  }
}
