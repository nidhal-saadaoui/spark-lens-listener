package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object SkewAnalyzer extends Analyzer {
  private val MinP50Ms      = 500L
  private val ConcWarn      = 0.25   // top-5% tasks hold 25%+ of stage time → warning
  private val ConcCrit      = 0.50   // top-5% tasks hold 50%+ of stage time → critical
  private val MinShufBytes  = 100L * 1024L
  private val ShufP95Warn   = 3.0
  private val ShufP95Crit   = 8.0
  // Concentration thresholds for Exchange-node byte skew (scale-independent: works even
  // when p50 shuffle bytes is near zero because most partitions are empty on a hot key).
  private val ExchConcWarn  = 0.25   // top-5% partitions hold 25%+ of Exchange bytes → warning
  private val ExchConcCrit  = 0.50   // top-5% partitions hold 50%+ of Exchange bytes → critical
  private val ExchMinBytes  = 1024L  // ignore Exchange nodes with < 1 KB total (trivial queries)

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val minTasks     = propLong(app,   "spark.sparklens.skew.minTasks",      10L).toInt
    val p95WarnRatio = propDouble(app, "spark.sparklens.skew.warnP95Ratio",   3.0)
    val p95CritRatio = propDouble(app, "spark.sparklens.skew.critP95Ratio",   8.0)

    val stageIssues = app.stages.values.toSeq.flatMap { stage =>
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
        val p50    = percentile(durations, 50)
        val p75    = percentile(durations, 75)
        val p95    = percentile(durations, 95)
        val maxDur = durations.last

        // Shuffle-bytes skew is evaluated before the MinP50Ms guard so that
        // hot-key imbalance in fast local-mode or CI stages is still detected
        // when task durations are sub-500 ms.
        val shuffleBytes = stage.tasks.map(t =>
          t.metrics.shuffleRemoteBytesRead + t.metrics.shuffleLocalBytesRead).sorted
        val p50Shuf   = percentile(shuffleBytes, 50)
        val p95Shuf   = percentile(shuffleBytes, 95)
        val shufRatio = if (p50Shuf >= MinShufBytes) p95Shuf.toDouble / p50Shuf else 0.0
        val shufWarn  = shufRatio >= ShufP95Warn

        // Databricks guide threshold: max > p75WarnRatio × p75 indicates a hidden-outlier
        // skew that the p95/p50 ratio can miss (e.g., 1 slow task in 100).
        // Gated on MinP50Ms so trivially fast stages don't fire.
        val p75WarnRatio = propDouble(app, "spark.sparklens.skew.p75WarnRatio", 1.5)
        val p75Warn = p75 >= MinP50Ms && maxDur > (p75 * p75WarnRatio).toLong

        // Duration / concentration signals only apply when stages are slow enough
        // to give meaningful timing data.
        val (durRatio, conc, durWarn, concWarn) =
          if (p50 >= MinP50Ms) {
            val dr = p95.toDouble / p50
            val cn = concentration(durations)
            (dr, cn, dr >= p95WarnRatio, cn >= ConcWarn)
          } else (0.0, 0.0, false, false)

        if (!(durWarn || concWarn || shufWarn || p75Warn)) Nil
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

            val sigDesc = if (durWarn || concWarn)
              s"p95 executor run time (${fmtMs(p95)}) is ${ratioFmt}× the median (${fmtMs(p50)}). " +
              s"The top 5% slowest tasks hold ${concPct}% of total stage time " +
              s"($stragglers straggler(s) out of $taskCount)."
            else
              s"Shuffle read bytes are highly skewed: p95 (${fmtBytes(p95Shuf)}) is " +
              s"${fmtDouble(shufRatio, 1)}× the median (${fmtBytes(p50Shuf)}) across $taskCount tasks."

            // If most of the task time is shuffle fetch-wait, the real bottleneck is the
            // upstream stage that writes the shuffle data, not skew in this stage.
            val fetchWaitRatio = if (stage.totalExecutorRunTimeMs > 0)
              stage.totalShuffleFetchWaitTimeMs.toDouble / stage.totalExecutorRunTimeMs
            else 0.0
            val fetchWaitNote = if (fetchWaitRatio >= 0.5)
              s" NOTE: ${fmtDouble(fetchWaitRatio * 100, 0)}% of task time was shuffle fetch-wait — " +
              s"the upstream stage writing this data may be the real bottleneck."
            else ""

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

            val stragglerWasteMs = (p95 - p50) * stragglers
            val stageImpact = EstimatedImpact(
              summary     = s"~$stragglers straggler(s) wasted ~${fmtMs(p95 - p50)} each (p95 ${fmtMs(p95)} vs median ${fmtMs(p50)})",
              savedTimeMs = timeOpt(stragglerWasteMs),
              savedBytes  = None,
              confidence  = "high",
            )
            Seq(Issue(
              id              = s"$idPrefix-${stage.stageId}",
              severity        = severity,
              category        = "skew",
              title           = title,
              description     = s"$sigDesc One or more partitions hold disproportionate data — the slowest tasks block the entire stage.$fetchWaitNote${if (stage.callSite.nonEmpty) s" Triggered from: ${stage.callSite}." else ""}",
              recommendation  = rec,
              configFix       = Some(confFix),
              codeFix         = Some(codFix),
              affectedStages  = Seq(stage.stageId),
              metrics         = Map(
                "skew_type"        -> skewType,
                "p95_ratio"        -> ratioFmt,
                "concentration"    -> fmtDouble(conc, 4),
                "p50_ms"           -> p50.toString,
                "p95_ms"           -> p95.toString,
                "stragglers"       -> stragglers.toString,
                "max_ms"           -> maxDur.toString,
                "p75_ms"           -> p75.toString,
                "fetch_wait_ratio" -> fmtDouble(fetchWaitRatio, 3),
              ),
              estimatedImpact = Some(stageImpact),
            ))
          }
        }   // close: if (durations.size < minTasks) Nil else
      }
    }

    // ── Exchange-node byte skew (SQL plan metric signal) ─────────────────────
    // Uses per-task accumulator values collected at task-end time and resolved into
    // the plan tree at SQL execution-end.  Concentration is computed over the distribution
    // of bytes written per task across the Exchange node (write-side partition bytes).
    // This signal is scale-independent: it fires even when p50 task bytes is near zero
    // (hot-key case where 90%+ of data lands on one partition, leaving 90%+ of tasks empty).
    val networkSpeedMbps = propLong(app, "spark.sparklens.impact.networkSpeedMbps", 1024L)
    val sqlSkewIssues: Seq[Issue] = app.sqlExecutions.values.toSeq.flatMap { sql =>
      val sqlDurationMs = sql.completionTimeMs.map(_ - sql.startTimeMs).getOrElse(Long.MaxValue)
      sql.planTree.toSeq.flatMap { tree =>
        tree.nodesContaining("Exchange").flatMap { node =>
          // resolvedMetrics: accumulatorId → sum-of-per-task-updates.
          // Each entry is the total bytes for ONE task (one write-side partition).
          val taskBytes = node.resolvedMetrics.values.toSeq.sorted
          val total     = taskBytes.sum
          if (total < ExchMinBytes || taskBytes.size < minTasks) Nil
          else {
            val conc = concentration(taskBytes)
            if (conc < ExchConcWarn) Nil
            else {
              val severity  = if (conc >= ExchConcCrit) Critical else Warning
              val idPrefix  = if (severity == Critical) "skew-crit" else "skew-warn"
              val concPct   = fmtDouble(conc * 100, 0)
              val p50       = percentile(taskBytes, 50)
              val p95       = percentile(taskBytes, 95)
              val hotBytes  = (total * conc).toLong
              val penaltyMs = math.min(networkMs(hotBytes, networkSpeedMbps), sqlDurationMs)
              val exchImpact = EstimatedImpact(
                summary     = s"~${fmtBytes(hotBytes)} in top 5% of partitions (${concPct}%), ~${fmtMs(penaltyMs)} network penalty",
                savedTimeMs = timeOpt(penaltyMs),
                savedBytes  = bytesOpt(hotBytes),
                confidence  = "medium",
              )
              Seq(Issue(
                id              = s"$idPrefix-exchange-${node.nodeName.hashCode.abs}-${sql.executionId}",
                severity        = severity,
                category        = "skew",
                title           = s"""Exchange Byte Skew in "${sql.description.take(80)}" — ${concPct}% of bytes in top 5% of partitions""",
                description     =
                  s"The ${node.nodeName} node wrote ${fmtBytes(total)} total. " +
                  s"The top 5% of write-side partitions hold ${concPct}% of data " +
                  s"(p50 = ${fmtBytes(p50)}, p95 = ${fmtBytes(p95)}). " +
                  s"The corresponding reduce-side tasks will be severely imbalanced.",
                recommendation  =
                  "Enable AQE skewJoin to split oversized join partitions automatically. " +
                  "For hot-key groupBy, apply two-phase key salting: add a random integer " +
                  "suffix before aggregation and strip it after.",
                configFix       = Some("spark.sql.adaptive.enabled=true\nspark.sql.adaptive.skewJoin.enabled=true"),
                codeFix         = Some(
                  "// Salt the hot key to spread hot partitions across buckets:\n" +
                  "val SALT = 8\n" +
                  "df.withColumn(\"_salt\", (rand() * SALT).cast(\"int\"))\n" +
                  "  .groupBy(\"key\", \"_salt\").agg(sum(\"value\").as(\"sub\"))\n" +
                  "  .withColumn(\"key\", regexp_replace(col(\"key\"), \"_\\\\d+$\", \"\"))\n" +
                  "  .groupBy(\"key\").agg(sum(\"sub\"))"),
                affectedJobs    = sql.jobIds,
                metrics         = Map(
                  "skew_type"     -> "exchange",
                  "concentration" -> fmtDouble(conc, 4),
                  "total_bytes"   -> total.toString,
                  "p50_bytes"     -> p50.toString,
                  "p95_bytes"     -> p95.toString,
                  "node_name"     -> node.nodeName,
                ),
                estimatedImpact = Some(exchImpact),
              ))
            }
          }
        }
      }
    }

    stageIssues ++ sqlSkewIssues
  }
}
