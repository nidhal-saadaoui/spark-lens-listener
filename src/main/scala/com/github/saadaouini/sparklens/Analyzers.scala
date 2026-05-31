package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.analyzers._
import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

object Analyzers {
  val all: Seq[analyzers.Analyzer] = Seq(
    SkewAnalyzer,
    SpillAnalyzer,
    JoinAnalyzer,
    GcAnalyzer,
    CacheAnalyzer,
    PreemptionAnalyzer,
    PlanAnalyzer,
    ConfigAnalyzer,
    SmallFilesAnalyzer,
    ShuffleLocalityAnalyzer,
    DriverBottleneckAnalyzer,
    CpuEfficiencyAnalyzer,
    SpeculationAnalyzer,
    StageFailureAnalyzer,
    MemoryPressureAnalyzer,
  )

  def runAll(app: SparkAppModel): Seq[Issue] =
    all.flatMap(_.analyze(app)).sortBy(_.severity.order)
}
