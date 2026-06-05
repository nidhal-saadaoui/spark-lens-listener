package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object CpuEfficiencyAnalyzer extends Analyzer {
  private val MinRunTimeMs = 30000L

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val lowCpuFraction = propDouble(app, "spark.sparklens.cpu.lowFraction", 0.20)
    app.stages.values.toSeq.flatMap { stage =>
      val runTimeMs  = stage.totalExecutorRunTimeMs
      val cpuTimeNs  = if (stage.hasExactAggregates) stage.exactExecutorCpuTimeNs
                       else stage.tasks.map(_.metrics.executorCpuTimeNs).sum
      if (runTimeMs < MinRunTimeMs || cpuTimeNs == 0) Nil
      else {
        val cpuFraction = (cpuTimeNs / 1000000.0) / runTimeMs
        if (cpuFraction >= lowCpuFraction) Nil
        else {
          val pct        = s"${fmtDouble(cpuFraction * 100, 0)}%"
          val wallClockMs = stage.durationMs
          val idleMs      = (wallClockMs * (1.0 - cpuFraction)).toLong
          val impact  = EstimatedImpact(
            summary     = s"~${fmtMs(idleMs)} wall-clock idle time ($pct CPU utilisation in stage ${stage.stageId})",
            savedTimeMs = timeOpt(idleMs),
            savedBytes  = None,
            confidence  = "medium",
          )
          Seq(Issue(
            id              = s"cpu-${stage.stageId}",
            severity        = Info,
            category        = "io",
            title           = s"Low CPU Utilization in Stage ${stage.stageId} — $pct CPU",
            description     = s"Executors spent only $pct of their run time on CPU computation in stage ${stage.stageId} (${stage.name}) — the rest was I/O wait, shuffle network, or JVM overhead. You are paying for cores that are mostly idle.",
            recommendation  = "Look at the stage metrics to identify the bottleneck: if shuffleReadBytes dominate, the stage is network-bound — reduce shuffles or enable compression. If inputBytes dominate, the source files are too small — compact them. For Python UDFs, switch to pandas UDFs (Arrow-based) which process batches and hand control back to the JVM quickly.",
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "cpu_fraction"  -> fmtDouble(cpuFraction, 3),
              "run_time_ms"   -> runTimeMs.toString,
              "cpu_time_ms"   -> (cpuTimeNs / 1000000).toString,
            ),
            estimatedImpact = Some(impact),
          ))
        }
      }
    }
  }
}
