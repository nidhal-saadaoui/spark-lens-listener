package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object MemoryPressureAnalyzer extends Analyzer {
  private val GcFractionThreshold   = 0.10
  private val SpillBytesThreshold   = 100L * MB
  private val MinRunTimeMs          = 10000L

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.stages.values.toSeq.flatMap { stage =>
      val runTime = stage.totalExecutorRunTimeMs
      val gcTime  = stage.totalGcTimeMs
      val spill   = stage.totalDiskSpillBytes

      if (runTime < MinRunTimeMs) Nil
      else {
        val gcFraction = if (runTime > 0) gcTime.toDouble / runTime else 0.0
        val hasGc      = gcFraction >= GcFractionThreshold
        val hasSpill   = spill >= SpillBytesThreshold

        if (hasGc && hasSpill) {
          val gcPct = f"${gcFraction * 100}%.0f%%"
          Seq(Issue(
            id             = s"memory-pressure-${stage.stageId}",
            severity       = Critical,
            category       = "reliability",
            title          = s"Memory Pressure in Stage ${stage.stageId} — GC $gcPct + ${fmtBytes(spill)} Spill",
            description    = s"Stage ${stage.stageId} (${stage.name}) shows both high GC ($gcPct of executor time) and significant disk spill (${fmtBytes(spill)}). The executor heap is genuinely undersized for the data volume.",
            recommendation = "Increase spark.executor.memory. Reduce the number of cached objects in memory. Enable off-heap memory for shuffle (spark.memory.offHeap.enabled=true).",
            configFix      = Some("spark.executor.memory=<increase>  spark.memory.offHeap.enabled=true  spark.memory.offHeap.size=2g"),
            affectedStages = Seq(stage.stageId),
            metrics        = Map(
              "gc_fraction"  -> f"$gcFraction%.3f",
              "spill_bytes"  -> spill.toString,
            ),
          ))
        } else Nil
      }
    }
}
