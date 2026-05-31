package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SkewAnalyzer extends Analyzer {
  private val MinTasks        = 5
  private val MinMedianMs     = 1000L
  private val MinShuffleBytes = 100L * 1024L   // min median shuffle read before bytes-based check
  private val RatioWarn       = 3.0
  private val RatioCrit       = 8.0

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      if (stage.tasks.size < MinTasks) Nil
      else {
        val durations = stage.tasks.map(_.durationMs).sorted
        val medMs     = median(durations)
        val maxMs     = durations.last

        // Secondary signal: shuffle read bytes per task (more reliable in local/fast clusters)
        val shuffleBytes = stage.tasks.map(t =>
          t.metrics.shuffleRemoteBytesRead + t.metrics.shuffleLocalBytesRead).sorted
        val medBytes = median(shuffleBytes)
        val maxBytes = shuffleBytes.last

        // Use whichever signal produces the larger ratio; prefer bytes when both are valid
        val (ratio, signal) =
          if (medBytes >= MinShuffleBytes && maxBytes > 0) {
            val br = maxBytes.toDouble / medBytes
            val dr = if (medMs >= MinMedianMs) maxMs.toDouble / medMs else 0.0
            if (br >= dr) (br, "bytes") else (dr, "duration")
          } else if (medMs >= MinMedianMs) {
            (maxMs.toDouble / medMs, "duration")
          } else {
            (0.0, "none")
          }

        if (ratio < RatioWarn) Nil
        else {
          val sigDesc = if (signal == "bytes")
            f"Max shuffle read ${fmtBytes(maxBytes)} is ${ratio}%.1f× the median ${fmtBytes(medBytes)} across tasks."
          else
            f"Max task duration ${fmtMs(maxMs)} is ${ratio}%.1f× the median ${fmtMs(medMs)}."

          val severity = if (ratio >= RatioCrit) Critical else Warning
          val idPrefix = if (severity == Critical) "skew-crit" else "skew-warn"
          Seq(Issue(
            id             = s"$idPrefix-${stage.stageId}",
            severity       = severity,
            category       = "skew",
            title          = s"${if (severity == Critical) "Severe Task" else "Task"} Skew in Stage ${stage.stageId} (${stage.name}) — ${f"$ratio%.1f"}× skew",
            description    = s"$sigDesc One or more partitions hold disproportionate data.",
            recommendation = "Use AQE skewJoin (spark.sql.adaptive.skewJoin.enabled=true). For groupBy hot keys, apply key salting before aggregation.",
            configFix      = Some("spark.sql.adaptive.skewJoin.enabled=true"),
            affectedStages = Seq(stage.stageId),
            metrics        = Map(
              "skew_signal"   -> signal,
              "ratio"         -> f"$ratio%.1f",
              "max_ms"        -> maxMs.toString,
              "median_ms"     -> medMs.toString,
              "max_bytes"     -> maxBytes.toString,
              "median_bytes"  -> medBytes.toString,
            ),
          ))
        }
      }
    }
}
