package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object IoClassifierAnalyzer extends Analyzer {

  private def coresForStage(stage: StageData, app: SparkAppModel): Int = {
    val from = stage.submissionTimeMs.getOrElse(0L)
    val to   = stage.completionTimeMs.getOrElse(Long.MaxValue)
    val cores = app.executors.values
      .filter(e => e.addedTimeMs < to && e.removedTimeMs.forall(_ > from))
      .map(_.totalCores).sum
    math.max(cores, 1)
  }

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val minDurationMs      = propLong(app,   "spark.sparklens.io.minDurationMs",     10000L)
    val ioFloorMbps        = propDouble(app, "spark.sparklens.io.ioFloorMbps",        2.0)
    val computeCeilingMbps = propDouble(app, "spark.sparklens.io.computeCeilingMbps", 1.0)
    val minIoBytes         = propLong(app,   "spark.sparklens.io.minIoBytes",         MB)

    app.stages.values.toSeq.flatMap { stage =>
      val dur = stage.durationMs
      if (dur < minDurationMs) Nil
      else {
        val shuffleRead  = stage.totalShuffleRemoteBytes + stage.totalShuffleLocalBytes
        val maxIoBytes   = Seq(
          stage.totalInputBytes,
          stage.totalOutputBytes,
          shuffleRead,
          stage.totalShuffleBytesWritten,
        ).max

        if (maxIoBytes < minIoBytes) Nil
        else {
          val cores      = coresForStage(stage, app)
          val durSec     = dur / 1000.0
          val throughput = maxIoBytes.toDouble / cores / durSec / MB

          if (throughput >= ioFloorMbps) {
            Seq(Issue(
              id              = s"io-bound-${stage.stageId}",
              severity        = Info,
              category        = "io",
              title           = s"I/O-Bound Stage ${stage.stageId} — ${fmtDouble(throughput, 1)} MB/s per core",
              description     =
                s"Stage ${stage.stageId} processed ${fmtBytes(maxIoBytes)} in ${fmtMs(dur)} across $cores core(s), " +
                s"reaching ~${fmtDouble(throughput, 1)} MB/s per core. The stage is limited by storage or network throughput.",
              recommendation  =
                "Apply predicate pushdown and column pruning to reduce data read. " +
                "Use Parquet or ORC (columnar) instead of CSV/JSON. " +
                "Enable AQE partition coalescing. " +
                "Cache the dataset if it is read multiple times.",
              configFix       = Some("spark.sql.adaptive.enabled=true"),
              affectedStages  = Seq(stage.stageId),
              metrics         = Map(
                "throughput_mb_per_core" -> fmtDouble(throughput, 2),
                "max_io_bytes"           -> maxIoBytes.toString,
                "cores"                  -> cores.toString,
                "duration_ms"            -> dur.toString,
              ),
              estimatedImpact = Some(configRisk),
            ))
          } else if (throughput < computeCeilingMbps) {
            Seq(Issue(
              id              = s"compute-bound-${stage.stageId}",
              severity        = Info,
              category        = "io",
              title           = s"Compute-Bound Stage ${stage.stageId} — Only ${fmtDouble(throughput, 2)} MB/s per core despite ${fmtMs(dur)} runtime",
              description     =
                s"Stage ${stage.stageId} ran for ${fmtMs(dur)} but only moved ${fmtBytes(maxIoBytes)} of data " +
                s"(~${fmtDouble(throughput, 2)} MB/s per core). The bottleneck is CPU or memory, not I/O.",
              recommendation  =
                "Investigate Python/Scala UDFs that bypass Catalyst (see UdfAnalyzer). " +
                "Check for data skew that serialises work onto one task (see SkewAnalyzer). " +
                "Review GC overhead (see GcAnalyzer).",
              affectedStages  = Seq(stage.stageId),
              metrics         = Map(
                "throughput_mb_per_core" -> fmtDouble(throughput, 2),
                "max_io_bytes"           -> maxIoBytes.toString,
                "cores"                  -> cores.toString,
                "duration_ms"            -> dur.toString,
              ),
              estimatedImpact = Some(configRisk),
            ))
          } else Nil
        }
      }
    }
  }
}
