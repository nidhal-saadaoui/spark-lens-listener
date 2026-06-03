package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object ShuffleLocalityAnalyzer extends Analyzer {
  private val MinShuffleBytes = 100L * MB

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val remoteRatioWarn = propDouble(app, "spark.sparklens.shuffleLocality.remoteRatioWarn", 0.70)
    val networkSpeedMbps = propLong(app,  "spark.sparklens.impact.networkSpeedMbps", 1024L)
    app.stages.values.toSeq.flatMap { stage =>
      val remote = stage.totalShuffleRemoteBytes
      val local  = stage.totalShuffleLocalBytes
      val total  = remote + local
      if (total < MinShuffleBytes) Nil
      else {
        val remoteRatio = remote.toDouble / total
        if (remoteRatio < remoteRatioWarn) Nil
        else {
          val penaltyMs = math.min(networkMs(remote, networkSpeedMbps), stage.durationMs)
          val impact    = EstimatedImpact(
            summary     = s"~${fmtBytes(remote)} fetched remotely, ~${fmtMs(penaltyMs)} network penalty (est. $networkSpeedMbps MB/s)",
            savedTimeMs = timeOpt(penaltyMs),
            savedBytes  = bytesOpt(remote),
            confidence  = "medium",
          )
          Seq(Issue(
            id              = s"shuffle-locality-${stage.stageId}",
            severity        = Warning,
            category        = "io",
            title           = s"High Remote Shuffle Read in Stage ${stage.stageId} — ${fmtDouble(remoteRatio * 100, 0)}% remote",
            description     = s"Stage ${stage.stageId} read ${fmtBytes(remote)} remotely out of ${fmtBytes(total)} total shuffle data (${fmtDouble(remoteRatio * 100, 0)}% remote). Cross-rack or cross-AZ reads are typically 3–10× slower than local disk reads and saturate shared cluster network bandwidth, slowing every other job running concurrently.",
            recommendation  = "Enable the external shuffle service so shuffle blocks are served from a long-lived process rather than the executor JVM (which may be reused). Pin executors to a single AZ to eliminate cross-AZ network costs. Enable shuffle compression to cut the bytes transferred.",
            configFix       = Some(
              "spark.shuffle.service.enabled=true\n" +
              "spark.io.compression.codec=lz4  # fast, lower CPU than snappy\n" +
              "spark.shuffle.compress=true\n" +
              "spark.shuffle.spill.compress=true"
            ),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "remote_bytes" -> remote.toString,
              "local_bytes"  -> local.toString,
              "remote_ratio" -> fmtDouble(remoteRatio, 3),
            ),
            estimatedImpact = Some(impact),
          ))
        }
      }
    }
  }
}
