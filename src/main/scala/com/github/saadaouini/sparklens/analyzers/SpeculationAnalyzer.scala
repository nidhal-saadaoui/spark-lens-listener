package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SpeculationAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val speculationEnabled = app.propOrDefault("spark.speculation", "false").toLowerCase == "true"
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // Count speculative tasks that actually ran
    val speculativeTasks = app.stages.values.flatMap(_.tasks).count(_.speculative)

    if (speculationEnabled && speculativeTasks > 0) {
      issues += Issue(
        id             = "speculation-active",
        severity       = Warning,
        category       = "config",
        title          = s"Speculation Fired $speculativeTasks Speculative Task(s) — Masking Skew",
        description    = s"Spark launched $speculativeTasks speculative copies of slow tasks. Speculation treats the symptom (slow tasks) not the cause (data skew or straggler executor).",
        recommendation = "Identify the root cause of slow tasks: check SkewAnalyzer results. Fix skew with key salting or AQE instead of relying on speculation.",
        configFix      = Some("spark.sql.adaptive.skewJoin.enabled=true  # fix the root cause"),
        metrics        = Map("speculative_tasks" -> speculativeTasks.toString),
      )
    } else if (speculationEnabled && speculativeTasks == 0) {
      issues += Issue(
        id             = "speculation-configured-not-firing",
        severity       = Info,
        category       = "config",
        title          = "Speculation Enabled But Not Firing",
        description    = "spark.speculation=true is set but no speculative tasks were launched. The threshold may be too high, or tasks are uniform enough that speculation is not triggered.",
        recommendation = "If speculation is not needed, disable it to avoid the monitoring overhead. If slow tasks are expected, lower spark.speculation.multiplier.",
        metrics        = Map("speculative_tasks" -> "0"),
      )
    }

    issues.toSeq
  }
}
