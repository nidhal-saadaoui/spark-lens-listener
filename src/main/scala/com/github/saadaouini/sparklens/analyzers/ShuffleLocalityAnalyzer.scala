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
          title          = s"High Remote Shuffle Read in Stage ${stage.stageId} — ${f"${remoteRatio * 100}%.0f"}% remote",
          description    = s"Stage ${stage.stageId} read ${fmtBytes(remote)} remotely out of ${fmtBytes(total)} total shuffle data. Cross-rack or cross-AZ shuffle is expensive.",
          recommendation = "Enable external shuffle service and rack-aware scheduling. For cloud clusters, ensure executors are in the same AZ. Consider increasing locality wait (spark.locality.wait).",
          configFix      = Some("spark.locality.wait=3s"),
          affectedStages = Seq(stage.stageId),
          metrics        = Map(
            "remote_bytes" -> remote.toString,
            "local_bytes"  -> local.toString,
            "remote_ratio" -> f"$remoteRatio%.3f",
          ),
        ))
      }
    }
}
