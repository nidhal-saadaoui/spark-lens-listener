package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object ShuffleLocalityAnalyzer extends Analyzer {
  private val RemoteRatioWarn = 0.70
  private val MinShuffleBytes = 100L * MB

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      val remote = stage.totalShuffleRemoteBytes
      val local  = stage.totalShuffleLocalBytes
      val total  = remote + local
      if (total < MinShuffleBytes) Nil
      else {
        val remoteRatio = remote.toDouble / total
        if (remoteRatio < RemoteRatioWarn) Nil
        else Seq(Issue(
          id             = s"shuffle-locality-${stage.stageId}",
          severity       = Warning,
          category       = "io",
          title          = s"High Remote Shuffle Read in Stage ${stage.stageId} — ${fmtDouble(remoteRatio * 100, 0)}% remote",
          description    = s"Stage ${stage.stageId} read ${fmtBytes(remote)} remotely out of ${fmtBytes(total)} total shuffle data (${fmtDouble(remoteRatio * 100, 0)}% remote). Cross-rack or cross-AZ reads are typically 3–10× slower than local disk reads and saturate shared cluster network bandwidth, slowing every other job running concurrently.",
          recommendation = "Enable the external shuffle service so shuffle blocks survive executor loss without remote re-fetch. Configure rack-aware scheduling so Spark prefers co-located executors. On cloud clusters, pin executors to a single AZ. If data must cross AZs, enable shuffle compression to reduce transferred bytes.",
          configFix      = Some("spark.locality.wait=3s"),
          affectedStages = Seq(stage.stageId),
          metrics        = Map(
            "remote_bytes" -> remote.toString,
            "local_bytes"  -> local.toString,
            "remote_ratio" -> fmtDouble(remoteRatio, 3),
          ),
        ))
      }
    }
}
