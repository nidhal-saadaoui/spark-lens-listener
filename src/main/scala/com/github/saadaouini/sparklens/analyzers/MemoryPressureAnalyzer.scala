package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object MemoryPressureAnalyzer extends Analyzer {
  private val MinRunTimeMs = 10000L

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val gcFractionThreshold = propDouble(app, "spark.sparklens.memoryPressure.gcFraction", 0.10)
    val spillBytesThreshold = propLong(app,   "spark.sparklens.memoryPressure.spillMb",   100L) * MB
    val diskSpeedMbps       = propLong(app,   "spark.sparklens.impact.diskSpeedMbps", 200L)
    app.stages.values.toSeq.flatMap { stage =>
      val runTime = stage.totalExecutorRunTimeMs
      val gcTime  = stage.totalGcTimeMs
      val spill   = stage.totalDiskSpillBytes

      if (runTime < MinRunTimeMs) Nil
      else {
        val gcFraction = if (runTime > 0) gcTime.toDouble / runTime else 0.0
        val hasGc      = gcFraction >= gcFractionThreshold
        val hasSpill   = spill >= spillBytesThreshold

        if (hasGc && hasSpill) {
          val gcPct       = s"${fmtDouble(gcFraction * 100, 0)}%"
          val spillMs     = diskMs(spill, diskSpeedMbps)
          val totalCostMs = gcTime + spillMs
          val impact      = EstimatedImpact(
            summary     = s"~${fmtMs(gcTime)} GC overhead + ~${fmtMs(spillMs)} spill I/O = ~${fmtMs(totalCostMs)} combined memory pressure cost",
            savedTimeMs = timeOpt(totalCostMs),
            savedBytes  = bytesOpt(spill),
            confidence  = "medium",
          )
          Seq(Issue(
            id              = s"memory-pressure-${stage.stageId}",
            severity        = Critical,
            category        = "reliability",
            title           = s"Memory Pressure in Stage ${stage.stageId} — GC $gcPct + ${fmtBytes(spill)} Spill",
            description     = s"Stage ${stage.stageId} (${stage.name}) shows both high GC ($gcPct of executor time) and significant disk spill (${fmtBytes(spill)}). The executor heap is genuinely undersized for the data volume.",
            recommendation  = "Increase spark.executor.memory. Reduce the number of cached objects in memory. Enable off-heap memory for shuffle (spark.memory.offHeap.enabled=true).",
            configFix       = Some("spark.executor.memory=<increase>  spark.memory.offHeap.enabled=true  spark.memory.offHeap.size=2g"),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "gc_fraction"  -> fmtDouble(gcFraction, 3),
              "spill_bytes"  -> spill.toString,
            ),
            estimatedImpact = Some(impact),
          ))
        } else Nil
      }
    }
  }
}
