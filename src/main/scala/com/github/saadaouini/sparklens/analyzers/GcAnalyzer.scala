package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object GcAnalyzer extends Analyzer {
  private val WarnFraction = 0.10
  private val CritFraction = 0.20
  private val MinRunTimeMs = 10000L

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      val runTime = stage.totalExecutorRunTimeMs
      val gcTime  = stage.totalGcTimeMs
      if (runTime < MinRunTimeMs || gcTime == 0) Nil
      else {
        val fraction = gcTime.toDouble / runTime
        if (fraction < WarnFraction) Nil
        else {
          val pct      = f"${fraction * 100}%.0f%%"
          val severity = if (fraction >= CritFraction) Critical else Warning
          Seq(Issue(
            id             = s"gc-${stage.stageId}",
            severity       = severity,
            category       = "gc",
            title          = s"High GC Overhead in Stage ${stage.stageId} — $pct of executor time",
            description    = s"Executors spent $pct of their time in JVM garbage collection in stage ${stage.stageId} (${stage.name}). This indicates memory pressure.",
            recommendation = "Increase spark.executor.memory. Switch to G1GC (spark.executor.extraJavaOptions=-XX:+UseG1GC). Use off-heap storage for RDD caching.",
            configFix      = Some("spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"),
            affectedStages = Seq(stage.stageId),
            metrics        = Map("gc_fraction" -> f"$fraction%.3f", "gc_ms" -> gcTime.toString, "run_ms" -> runTime.toString),
          ))
        }
      }
    }
}
