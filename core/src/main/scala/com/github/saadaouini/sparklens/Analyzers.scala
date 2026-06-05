package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.analyzers._
import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

object Analyzers {
  val all: Seq[analyzers.Analyzer] = Seq(
    JobTimelineAnalyzer,
    SkewAnalyzer,
    TaskOverheadAnalyzer,
    SpillAnalyzer,
    JoinAnalyzer,
    GcAnalyzer,
    CacheAnalyzer,
    PreemptionAnalyzer,
    PlanAnalyzer,
    UdfAnalyzer,
    IoClassifierAnalyzer,
    ConfigAnalyzer,
    ExecutorSizingAnalyzer,
    SmallFilesAnalyzer,
    OutputSmallFilesAnalyzer,
    ShuffleLocalityAnalyzer,
    DriverBottleneckAnalyzer,
    CpuEfficiencyAnalyzer,
    SpeculationAnalyzer,
    StageFailureAnalyzer,
    MemoryPressureAnalyzer,
    StageParallelismAnalyzer,
    LongStageAnalyzer,
    PartitionImbalanceAnalyzer,
    SchedulerDelayAnalyzer,
    CriticalPathAnalyzer,
    DynamicAllocationAnalyzer,
    YarnAnalyzer,
    ScalingSimulatorAnalyzer,
  )

  def runAll(app: SparkAppModel): Seq[Issue] = {
    val grouped = group(all.flatMap(_.analyze(app)))
      .sortBy(i => (i.severity.order, -i.estimatedImpact.flatMap(_.savedTimeMs).getOrElse(0L)))
    linkRelated(grouped)
  }

  // Populates relatedIds on issues that share at least one affected stage with another
  // issue AND both have quantifiable savedTimeMs. Related issues often share a root cause
  // (e.g. coalesce(1) causing spill, low CPU, and single-task all on stage 1).
  private[sparklens] def linkRelated(issues: Seq[Issue]): Seq[Issue] = {
    val withSavings = issues.filter(_.estimatedImpact.flatMap(_.savedTimeMs).isDefined)
    issues.map { issue =>
      if (issue.affectedStages.isEmpty || issue.estimatedImpact.flatMap(_.savedTimeMs).isEmpty)
        issue
      else {
        val stageSet = issue.affectedStages.toSet
        val related  = withSavings
          .filter(other => other.id != issue.id && other.affectedStages.exists(stageSet.contains))
          .map(_.id)
        issue.copy(relatedIds = related)
      }
    }
  }

  // Strip trailing -<digits> to derive a stable group key (e.g. "spill-3" → "spill").
  // Issues sharing a group key are merged into one entry so the report stays readable
  // even when the same problem affects dozens of stages.
  private val trailingId = """-\d+$""".r

  private[sparklens] def group(issues: Seq[Issue]): Seq[Issue] =
    issues.groupBy(i => trailingId.replaceFirstIn(i.id, "")).values.toSeq.flatMap {
      case one if one.size == 1 => one
      case multi =>
        val sorted    = multi.sortBy(i => (i.severity.order, i.affectedStages.headOption.getOrElse(Int.MaxValue)))
        val rep       = sorted.head
        val extra     = sorted.size - 1
        val allStages = sorted.flatMap(_.affectedStages).distinct.sorted
        val allJobs   = sorted.flatMap(_.affectedJobs).distinct.sorted
        val suffix = (allStages.nonEmpty, allJobs.nonEmpty) match {
          case (true,  true)  => s" [+$extra more]"
          case (true,  false) => s" [+$extra more stages]"
          case (false, true)  => s" [+$extra more queries]"
          case _              => s" [+$extra more]"
        }
        val totalSavedMs    = multi.flatMap(_.estimatedImpact.flatMap(_.savedTimeMs)).sum
        val totalSavedBytes = multi.flatMap(_.estimatedImpact.flatMap(_.savedBytes)).sum
        val mergedImpact: Option[model.EstimatedImpact] = rep.estimatedImpact match {
          case Some(imp) =>
            Some(imp.copy(
              savedTimeMs = if (totalSavedMs > 0) Some(totalSavedMs) else None,
              savedBytes  = if (totalSavedBytes > 0) Some(totalSavedBytes) else None,
            ))
          case None if totalSavedMs > 0 || totalSavedBytes > 0 =>
            Some(model.EstimatedImpact(
              summary     = s"${multi.size} stages affected",
              savedTimeMs = if (totalSavedMs > 0) Some(totalSavedMs) else None,
              savedBytes  = if (totalSavedBytes > 0) Some(totalSavedBytes) else None,
              confidence  = "medium",
            ))
          case None => None
        }
        Seq(rep.copy(
          id              = trailingId.replaceFirstIn(rep.id, ""),
          title           = rep.title + suffix,
          affectedStages  = allStages,
          affectedJobs    = allJobs,
          estimatedImpact = mergedImpact,
        ))
    }
}
