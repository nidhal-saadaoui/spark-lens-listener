package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object JobTimelineAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val gapWarnMs       = propLong(app,   "spark.sparklens.timeline.gapWarnMs",      60000L)
    val fragThresholdMs = propLong(app,   "spark.sparklens.timeline.fragThresholdMs", 2000L)
    val fragFraction    = propDouble(app, "spark.sparklens.timeline.fragFraction",    0.5)
    val minJobsForFrag  = propLong(app,   "spark.sparklens.timeline.minJobs",        20L).toInt

    // Only consider jobs with a real submission time (0 means not captured by older builds).
    val timedJobs = app.jobs.values.toSeq
      .filter(j => j.submissionTimeMs > 0L && j.completionTimeMs.isDefined)
      .sortBy(_.submissionTimeMs)

    // ── Gap detection ─────────────────────────────────────────────────────────
    val gapIssues: Seq[Issue] = timedJobs.sliding(2).toSeq.flatMap {
      case Seq(prev, next) =>
        val gap = next.submissionTimeMs - prev.completionTimeMs.getOrElse(prev.submissionTimeMs)
        if (gap < gapWarnMs) Nil
        else Seq(Issue(
          id              = s"timeline-gap-${prev.jobId}-${next.jobId}",
          severity        = Warning,
          category        = "io",
          title           = s"Execution Gap of ~${fmtMs(gap)} Between Jobs ${prev.jobId} and ${next.jobId}",
          description     =
            s"The pipeline was idle for ~${fmtMs(gap)} between job ${prev.jobId} and job ${next.jobId}. " +
            "This suggests the driver was doing blocking work: collecting results, running Python logic, " +
            "or waiting on an external dependency.",
          recommendation  =
            "Move driver-side computation into distributed stages. Use .persist() + lazy evaluation chains " +
            "rather than .collect() -> transform -> re-distribute. Profile the driver thread to find the bottleneck.",
          codeFix         = Some(
            "// Instead of:\n" +
            "val rows = df.collect()\n" +
            "val processed = rows.map(transform)\n" +
            "spark.createDataFrame(processed, schema)\n" +
            "// Prefer:\n" +
            "df.map(transform)"),
          affectedJobs    = Seq(prev.jobId, next.jobId),
          estimatedImpact = Some(EstimatedImpact(
            summary     = s"~${fmtMs(gap)} of cluster idle time between jobs",
            savedTimeMs = timeOpt(gap),
            savedBytes  = None,
            confidence  = "high",
          )),
        ))
      case _ => Nil
    }

    // ── Fragmentation detection ───────────────────────────────────────────────
    val totalJobs = app.jobs.size
    val smallJobs = app.jobs.values.count { j =>
      j.completionTimeMs.exists(end => end - j.submissionTimeMs < fragThresholdMs)
    }
    val fragIssues: Seq[Issue] =
      if (totalJobs < minJobsForFrag || smallJobs.toDouble / totalJobs < fragFraction) Nil
      else {
        val pct = f"${smallJobs.toDouble / totalJobs * 100}%.0f"
        Seq(Issue(
          id              = "timeline-fragmentation",
          severity        = Info,
          category        = "io",
          title           = s"Job Fragmentation — $smallJobs of $totalJobs Jobs Run Under ${fmtMs(fragThresholdMs)}",
          description     =
            s"$smallJobs out of $totalJobs jobs ($pct%) " +
            s"completed in under ${fmtMs(fragThresholdMs)}. " +
            "Many short Spark jobs accumulate scheduling overhead: job setup, stage planning, and task " +
            "serialisation can dominate compute cost at this scale.",
          recommendation  =
            "Batch small operations together. Use unionAll + a single action instead of a loop of small " +
            "actions. Consider foreachPartition instead of per-row jobs.",
          codeFix         = Some(
            "// Instead of:\n" +
            "rows.foreach(row => spark.createDataFrame(Seq(row), schema).write.append...)\n" +
            "// Prefer:\n" +
            "spark.createDataFrame(rows, schema).write.append..."),
          metrics         = Map(
            "small_jobs"   -> smallJobs.toString,
            "total_jobs"   -> totalJobs.toString,
            "threshold_ms" -> fragThresholdMs.toString,
          ),
          estimatedImpact = Some(configRisk),
        ))
      }

    gapIssues ++ fragIssues
  }
}
