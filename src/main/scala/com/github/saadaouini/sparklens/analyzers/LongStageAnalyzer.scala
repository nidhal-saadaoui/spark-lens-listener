package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object LongStageAnalyzer extends Analyzer {
  private val MinStages = 3

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val outlierRatio  = propDouble(app, "spark.sparklens.longStage.outlierRatio", 5.0)
    val minDurationMs = propLong(app,   "spark.sparklens.longStage.minStageSec", 30L) * 1000L

    val seen = scala.collection.mutable.Set[Int]()

    app.jobs.values.toSeq.flatMap { job =>
      val jobStages = job.stageIds.flatMap(id => app.stages.get(id))
      val durations = jobStages.map(_.durationMs).filter(_ > 0).sorted
      if (durations.size < MinStages) Nil
      else {
        val med = median(durations)
        if (med == 0) Nil
        else {
          jobStages.flatMap { stage =>
            val dur   = stage.durationMs
            val ratio = if (med > 0) dur.toDouble / med else 0.0
            if (dur < minDurationMs || ratio < outlierRatio || seen.contains(stage.stageId)) Nil
            else {
              seen += stage.stageId
              val ratioFmt = fmtDouble(ratio, 1)
              val diffMs   = dur - med
              val impact   = EstimatedImpact(
                summary     = s"Stage ran ${fmtMs(dur)} vs ${fmtMs(med)} job median — ${fmtMs(diffMs)} outlier overhead",
                savedTimeMs = timeOpt(diffMs),
                savedBytes  = None,
                confidence  = "high",
              )
              Seq(Issue(
                id              = s"long-stage-${stage.stageId}",
                severity        = Warning,
                category        = "reliability",
                title           = s"Long Stage ${stage.stageId} — ${fmtMs(dur)} (${ratioFmt}× job median)",
                description     = s"Stage ${stage.stageId} (${stage.name}) took ${fmtMs(dur)}, ${ratioFmt}× longer than the median stage in its job (${fmtMs(med)}). A single long stage serializes all downstream stages and dominates total job wall time.",
                recommendation  = "Check for data skew in this stage (SkewAnalyzer), too few tasks relative to available cores (StageParallelismAnalyzer), or excessive spill. If it is a join stage, broadcast the smaller side or enable AQE to let Spark optimise the join strategy at runtime.",
                configFix       = Some("spark.sql.adaptive.enabled=true"),
                affectedStages  = Seq(stage.stageId),
                metrics         = Map(
                  "duration_ms"   -> dur.toString,
                  "median_ms"     -> med.toString,
                  "outlier_ratio" -> ratioFmt,
                ),
                estimatedImpact = Some(impact),
              ))
            }
          }
        }
      }
    }
  }
}
