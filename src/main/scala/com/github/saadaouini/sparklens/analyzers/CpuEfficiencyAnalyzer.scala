package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object CpuEfficiencyAnalyzer extends Analyzer {
  private val LowCpuFraction  = 0.20
  private val MinRunTimeMs    = 30000L

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      val runTimeMs  = stage.totalExecutorRunTimeMs
      val cpuTimeNs  = stage.tasks.map(_.metrics.executorCpuTimeNs).sum
      if (runTimeMs < MinRunTimeMs || cpuTimeNs == 0) Nil
      else {
        val cpuFraction = (cpuTimeNs / 1000000.0) / runTimeMs  // ns → ms
        if (cpuFraction >= LowCpuFraction) Nil
        else {
          val pct = f"${cpuFraction * 100}%.0f%%"
          Seq(Issue(
            id             = s"cpu-${stage.stageId}",
            severity       = Info,
            category       = "io",
            title          = s"Low CPU Utilization in Stage ${stage.stageId} — $pct CPU",
            description    = s"Executors spent only $pct of their run time doing actual CPU work in stage ${stage.stageId} (${stage.name}). The rest was I/O wait, shuffle, or JVM overhead.",
            recommendation = "Check if the stage is I/O-bound (shuffle read/write dominating). Increase parallelism or use faster storage. For UDFs, consider vectorized UDFs with Arrow.",
            affectedStages = Seq(stage.stageId),
            metrics        = Map(
              "cpu_fraction"  -> f"$cpuFraction%.3f",
              "run_time_ms"   -> runTimeMs.toString,
              "cpu_time_ms"   -> (cpuTimeNs / 1000000).toString,
            ),
          ))
        }
      }
    }
}
