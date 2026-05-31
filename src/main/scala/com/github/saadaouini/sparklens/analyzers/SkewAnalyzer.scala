package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SkewAnalyzer extends Analyzer {
  private val MinTasks     = 5
  private val MinMedianMs  = 1000L
  private val RatioWarn    = 3.0
  private val RatioCrit    = 8.0

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      if (stage.tasks.size < MinTasks) Nil
      else {
        val durations = stage.tasks.map(_.durationMs).sorted
        val med       = median(durations)
        val max       = durations.last
        if (med < MinMedianMs) Nil
        else {
          val ratio = max.toDouble / med
          if (ratio >= RatioCrit)
            Seq(Issue(
              id             = s"skew-crit-${stage.stageId}",
              severity       = Critical,
              category       = "skew",
              title          = s"Severe Task Skew in Stage ${stage.stageId} (${stage.name})",
              description    = f"Max task duration ${fmtMs(max)} is ${ratio}%.1f× the median ${fmtMs(med)}. One or more tasks dominate the stage runtime.",
              recommendation = "Use AQE skewJoin (spark.sql.adaptive.skewJoin.enabled=true). For groupBy hot keys, apply key salting before aggregation.",
              configFix      = Some("spark.sql.adaptive.skewJoin.enabled=true"),
              affectedStages = Seq(stage.stageId),
              metrics        = Map("max_ms" -> max.toString, "median_ms" -> med.toString, "ratio" -> f"$ratio%.1f"),
            ))
          else if (ratio >= RatioWarn)
            Seq(Issue(
              id             = s"skew-warn-${stage.stageId}",
              severity       = Warning,
              category       = "skew",
              title          = s"Task Skew in Stage ${stage.stageId} (${stage.name})",
              description    = f"Max task duration ${fmtMs(max)} is ${ratio}%.1f× the median ${fmtMs(med)}.",
              recommendation = "Enable AQE (spark.sql.adaptive.enabled=true) to automatically detect and mitigate skew.",
              configFix      = Some("spark.sql.adaptive.enabled=true"),
              affectedStages = Seq(stage.stageId),
              metrics        = Map("max_ms" -> max.toString, "median_ms" -> med.toString, "ratio" -> f"$ratio%.1f"),
            ))
          else Nil
        }
      }
    }
}
