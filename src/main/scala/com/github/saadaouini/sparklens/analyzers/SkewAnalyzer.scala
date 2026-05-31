package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object SkewAnalyzer extends Analyzer {
  private val MinP50Ms     = 500L
  private val ConcWarn     = 0.25   // top-5% tasks hold 25%+ of stage time → warning
  private val ConcCrit     = 0.50   // top-5% tasks hold 50%+ of stage time → critical
  private val MinShufBytes = 100L * 1024L
  private val ShufP95Warn  = 3.0
  private val ShufP95Crit  = 8.0

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val minTasks     = propLong(app,   "spark.sparklens.skew.minTasks",      10L).toInt
    val p95WarnRatio = propDouble(app, "spark.sparklens.skew.warnP95Ratio",   3.0)
    val p95CritRatio = propDouble(app, "spark.sparklens.skew.critP95Ratio",   8.0)
    app.stages.values.toSeq.flatMap { stage =>
      // Use exact task count when available (tasks list may be a reservoir sample)
      val taskCount = if (stage.exactTaskCount > 0) stage.exactTaskCount else stage.tasks.size
      if (taskCount < minTasks) Nil
      else {
        // Use executorRunTimeMs (time actually spent on the executor) rather than wall-clock
        // durationMs.  Wall-clock includes scheduling lag and shuffle-fetch stalls, so a stage
        // where all tasks wait equally for a slow shuffle partner looks skewed even though every
        // task does the same amount of work.  Filter out zero-valued entries (tasks killed before
        // execution started).
        val sampleSize = stage.tasks.size  // reservoir size BEFORE filtering zero-runTime tasks
        val durations  = stage.tasks.map(_.metrics.executorRunTimeMs).filter(_ > 0).sorted
        if (durations.size < minTasks) Nil
        else {
        val p50 = percentile(durations, 50)
        val p95 = percentile(durations, 95)
        if (p50 < MinP50Ms) Nil
        else {
          val durRatio = p95.toDouble / p50
          val conc     = concentration(durations)

          val shuffleBytes = stage.tasks.map(t =>
            t.metrics.shuffleRemoteBytesRead + t.metrics.shuffleLocalBytesRead).sorted
          val p50Shuf   = percentile(shuffleBytes, 50)
          val p95Shuf   = percentile(shuffleBytes, 95)
          val shufRatio = if (p50Shuf >= MinShufBytes) p95Shuf.toDouble / p50Shuf else 0.0

          val durWarn  = durRatio  >= p95WarnRatio
          val concWarn = conc      >= ConcWarn
          val shufWarn = shufRatio >= ShufP95Warn

          if (!(durWarn || concWarn || shufWarn)) Nil
          else {
            val durCrit  = durRatio  >= p95CritRatio
            val concCrit = conc      >= ConcCrit
            val shufCrit = shufRatio >= ShufP95Crit
            val severity = if (durCrit || concCrit || shufCrit) Critical else Warning
            val idPrefix = if (severity == Critical) "skew-crit" else "skew-warn"

            val totalShuffle = stage.totalShuffleRemoteBytes + stage.totalShuffleLocalBytes
            val totalInput   = stage.totalInputBytes
            val skewType   =
              if (totalShuffle > 0 && totalShuffle > totalInput) "shuffle"
              else if (totalInput > 0)                            "input"
              else                                                "unknown"

            // Scale straggler count only when the reservoir was actually truncated.
            // Use sampleSize (reservoir size before filtering) rather than durations.size
            // so that tasks killed before execution (executorRunTimeMs == 0, excluded by
            // the filter above) don't spuriously trigger scaling.
            val sampledStragglers = durations.count(_ > p50 * p95WarnRatio)
            val stragglers = if (stage.exactTaskCount > sampleSize && sampleSize > 0)
              (sampledStragglers.toDouble / sampleSize * taskCount).round.toInt
            else sampledStragglers
            val ratioFmt   = fmtDouble(durRatio, 1)
            val concPct    = fmtDouble(conc * 100, 0)

            val sigDesc =
              s"p95 executor run time (${fmtMs(p95)}) is ${ratioFmt}× the median (${fmtMs(p50)}). " +
              s"The top 5% slowest tasks hold ${concPct}% of total stage time " +
              s"($stragglers straggler(s) out of $taskCount)."

            val (title, rec, confFix, codFix) = skewType match {
              case "shuffle" =>
                (
                  s"Shuffle Hot-Key Skew in Stage ${stage.stageId} (${stage.name})",
                  "Enable AQE skewJoin to split oversized join partitions automatically. " +
                  "For hot-key groupBy, apply two-phase key salting: add a random integer suffix " +
                  "before aggregation and strip it after.",
                  "spark.sql.adaptive.enabled=true\nspark.sql.adaptive.skewJoin.enabled=true",
                  "// Salt the hot key to spread hot partitions across buckets:\n" +
                  "val SALT = 8\n" +
                  "df.withColumn(\"_salt\", (rand() * SALT).cast(\"int\"))\n" +
                  "  .groupBy(\"key\", \"_salt\").agg(sum(\"value\").as(\"sub\"))\n" +
                  "  .withColumn(\"key\", regexp_replace(col(\"key\"), \"_\\\\d+$\", \"\"))\n" +
                  "  .groupBy(\"key\").agg(sum(\"sub\"))",
                )
              case "input" =>
                (
                  s"Input Partition Skew in Stage ${stage.stageId} (${stage.name})",
                  "Reduce spark.sql.files.maxPartitionBytes to split oversized input partitions. " +
                  "Re-write source data with uniform partition sizes, or use repartition(N, key) " +
                  "to rebalance after the scan.",
                  "spark.sql.files.maxPartitionBytes=67108864  # 64 MB\n" +
                  "spark.sql.adaptive.enabled=true\n" +
                  "spark.sql.adaptive.coalescePartitions.enabled=true",
                  "// Force a balanced repartition after the scan:\n" +
                  "df.repartition(200, col(\"partition_key\"))",
                )
              case _ =>
                (
                  s"Task Skew in Stage ${stage.stageId} (${stage.name}) — ${ratioFmt}× p95/p50",
                  "Enable AQE skewJoin to split oversized partitions automatically. " +
                  "For hot-key groupBy, apply two-phase key salting.",
                  "spark.sql.adaptive.enabled=true\nspark.sql.adaptive.skewJoin.enabled=true",
                  "// Phase 1: salt the key to spread hot partitions\n" +
                  "df.withColumn(\"k\", concat(col(\"key\"), lit(\"_\"), (rand()*10).cast(\"int\")))\n" +
                  "  .groupBy(\"k\").agg(sum(\"value\").as(\"sub\"))\n" +
                  "// Phase 2: strip salt and final aggregate\n" +
                  "  .withColumn(\"key\", regexp_replace(col(\"k\"), \"_\\\\d+$\", \"\"))\n" +
                  "  .groupBy(\"key\").agg(sum(\"sub\"))",
                )
            }

            Seq(Issue(
              id             = s"$idPrefix-${stage.stageId}",
              severity       = severity,
              category       = "skew",
              title          = title,
              description    = s"$sigDesc One or more partitions hold disproportionate data — the slowest tasks block the entire stage.",
              recommendation = rec,
              configFix      = Some(confFix),
              codeFix        = Some(codFix),
              affectedStages = Seq(stage.stageId),
              metrics        = Map(
                "skew_type"     -> skewType,
                "p95_ratio"     -> ratioFmt,
                "concentration" -> fmtDouble(conc, 4),
                "p50_ms"        -> p50.toString,
                "p95_ms"        -> p95.toString,
                "stragglers"    -> stragglers.toString,
              ),
            ))
          }
        }
        }   // close: if (durations.size < minTasks) Nil else
      }
    }
  }
}
