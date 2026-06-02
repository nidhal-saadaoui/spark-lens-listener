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
  )

  def runAll(app: SparkAppModel): Seq[Issue] =
    group(all.flatMap(_.analyze(app)))
      .sortBy(i => (i.severity.order, -i.estimatedImpact.flatMap(_.savedTimeMs).getOrElse(0L)))

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
        val suffix    =
          if (allStages.nonEmpty)    s" [+$extra more stages]"
          else if (allJobs.nonEmpty) s" [+$extra more queries]"
          else                       s" [+$extra more]"
        Seq(rep.copy(
          id             = trailingId.replaceFirstIn(rep.id, ""),
          title          = rep.title + suffix,
          affectedStages = allStages,
          affectedJobs   = allJobs,
        ))
    }
}
