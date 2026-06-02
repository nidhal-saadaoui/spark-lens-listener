package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object PartitionImbalanceAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val minBytes  = propLong  (app, "spark.sparklens.partition.imbalance.minInputMb",   100L) * MB
    val ratioWarn = propDouble(app, "spark.sparklens.partition.imbalance.p95p50Ratio",   3.0)

    app.stages.values.toSeq.flatMap { s =>
      val inputTasks = s.tasks.filter(_.metrics.inputBytesRead > 0)
      val sorted  = inputTasks.map(_.metrics.inputBytesRead).sorted
      val p50     = if (sorted.nonEmpty) percentile(sorted, 50) else 0L
      val p95     = if (sorted.nonEmpty) percentile(sorted, 95) else 0L
      val pMax    = if (sorted.nonEmpty) sorted.last             else 0L
      val total   = s.totalInputBytes
      if (inputTasks.size < 10 || p50 == 0 || total < minBytes) Nil
      else {
        val ratio = p95.toDouble / p50
        if (ratio < ratioWarn) Nil
        else {
          val dur      = s.durationMs
          // Bottleneck tasks (> 2× p50) slow down the whole stage; save ~ratio-reduction of dur
          val savedMs  = timeOpt((dur * (1.0 - 2.0 / ratio)).toLong)
          Seq(Issue(
            id              = s"partition-imbalance-${s.stageId}",
            severity        = if (ratio >= 5.0) Warning else Info,
            category        = "io",
            title           = s"Input Partition Imbalance in Stage ${s.stageId} — p95/p50 ratio ${fmtDouble(ratio, 1)}×",
            description     =
              s"Stage ${s.stageId} reads input partitions that vary widely in size: " +
              s"median ${fmtBytes(p50)}, p95 ${fmtBytes(p95)}, max ${fmtBytes(pMax)}. " +
              s"The largest partitions take ${fmtDouble(ratio, 1)}× longer than the median, " +
              s"stalling the stage until they finish.",
            recommendation  =
              "Repartition the input data to equalise partition sizes. " +
              "For file-based sources increase the number of splits by lowering " +
              "spark.sql.files.maxPartitionBytes or adding a repartition() call upstream. " +
              "For Hive tables with heavily skewed partition columns, use DISTRIBUTE BY + SORT BY.",
            configFix       = Some("spark.sql.files.maxPartitionBytes=67108864  # 64 MB"),
            affectedStages  = Seq(s.stageId),
            metrics         = Map(
              "total_input_bytes" -> fmtBytes(total),
              "p50_partition"     -> fmtBytes(p50),
              "p95_partition"     -> fmtBytes(p95),
              "max_partition"     -> fmtBytes(pMax),
              "p95_p50_ratio"     -> fmtDouble(ratio, 2),
            ),
            estimatedImpact = Some(EstimatedImpact(
              summary     = s"p95/p50=${fmtDouble(ratio,1)}× — slow partitions bottleneck stage ${s.stageId}",
              savedTimeMs = savedMs,
              savedBytes  = None,
              confidence  = "medium",
            )),
          ))
        }
      }
    }
  }
}
