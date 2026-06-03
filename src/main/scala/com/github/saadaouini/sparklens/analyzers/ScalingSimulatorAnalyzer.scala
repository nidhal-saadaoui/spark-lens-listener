package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{EstimatedImpact, Info, Issue, SparkAppModel, StageData, Warning}

import scala.collection.mutable
import scala.util.Try

/**
 * Simulates job wall-clock time at different executor counts using the stage DAG and
 * average task durations calibrated against the actual run.
 *
 * For dynamic-allocation jobs, detects whether the maxExecutors ceiling was the binding
 * constraint and projects the benefit of raising it.
 *
 * Model assumptions (stated explicitly in the issue metrics):
 *   - Stages process `numTasks` tasks; avg task time = totalExecutorRunTimeMs / taskCount
 *   - Skewed stages use p95 task time instead of avg (straggler-bound)
 *   - Stages are scheduled in topological order; parallel stages share the executor pool
 *   - Projected durations are calibrated so that sim@actual == actual app duration
 *   - Executor ramp-up latency is not modelled (optimistic for DA jobs)
 */
object ScalingSimulatorAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val appDuration = app.durationMs.getOrElse(0L)

    // Require a meaningful run and at least 3 timed stages with task data
    val timedStages = app.stages.values.filter(s =>
      s.submissionTimeMs.isDefined &&
      s.completionTimeMs.isDefined &&
      s.numTasks > 0 &&
      s.totalExecutorRunTimeMs > 0
    ).toSeq
    if (timedStages.size < 3 || appDuration < 10000L) return Nil

    // ── Cluster config ──────────────────────────────────────────────────────
    val execCores     = app.executors.values.map(_.totalCores)
    val coresPerExec  = (if (execCores.isEmpty) None else Some(execCores.max))
                          .orElse(app.prop("spark.executor.cores").flatMap(s => Try(s.toInt).toOption))
                          .getOrElse(4)
    val peakExecutors = peakConcurrentExecutors(app)
    if (peakExecutors <= 0) return Nil

    val daEnabled = app.prop("spark.dynamicAllocation.enabled").exists(_.equalsIgnoreCase("true"))
    val daMaxCap  = app.prop("spark.dynamicAllocation.maxExecutors")
                      .flatMap(s => Try(s.toInt).toOption)
    val ceilingHit = daEnabled && daMaxCap.exists(cap => peakExecutors >= math.max(1, cap * 9 / 10))

    // ── Stage model ─────────────────────────────────────────────────────────
    val stageById = timedStages.map(s => s.stageId -> s).toMap
    val sorted    = topologicalSort(stageById)

    // Effective task time: use p95 for skewed stages (straggler sets the wall-clock floor)
    def effectiveTaskMs(stage: StageData): Long = {
      val avg = math.max(1L, stage.totalExecutorRunTimeMs / math.max(1, stage.totalTaskCount))
      if (stage.tasks.size >= 10) {
        val p95 = percentile(stage.tasks.map(_.metrics.executorRunTimeMs).sorted, 95)
        math.max(avg, p95)
      } else avg
    }

    // ── DAG simulation at N executors ────────────────────────────────────────
    // Parallel stages (no dependency between them) compete for the same executor pool.
    // We approximate this by splitting the pool evenly among concurrently-active stages
    // during the simulation sweep.
    def simulateMs(executors: Int): Long = {
      val totalCores = executors * coresPerExec
      val endTime    = mutable.Map[Int, Long]()

      sorted.foreach { stage =>
        val parents        = stage.parentIds.flatMap(endTime.get)
        val parentEnd      = if (parents.isEmpty) 0L else parents.max
        val taskMs         = effectiveTaskMs(stage)
        val effectiveCores = math.min(stage.numTasks, totalCores)
        val stageMs        = math.ceil(stage.numTasks.toDouble / effectiveCores).toLong * taskMs
        endTime(stage.stageId) = parentEnd + stageMs
      }
      val vals = endTime.values; if (vals.isEmpty) 0L else vals.max
    }

    val simAtCurrent = simulateMs(peakExecutors)
    if (simAtCurrent <= 0) return Nil

    // Calibrate: scale all projections so sim@current == actual
    val calibFactor    = appDuration.toDouble / simAtCurrent
    val modelAccuracy  = if (calibFactor > 2.5 || calibFactor < 0.4) "low" else "medium"

    def projected(executors: Int): Long =
      (simulateMs(executors) * calibFactor).toLong

    // ── Scale points ─────────────────────────────────────────────────────────
    val halfN  = math.max(1, peakExecutors / 2)
    val twoN   = peakExecutors * 2
    val threeN = peakExecutors * 3
    val fourN  = peakExecutors * 4

    val pHalf  = projected(halfN)
    val pTwo   = projected(twoN)
    val pThree = projected(threeN)
    val pFour  = projected(fourN)

    def pctChange(ms: Long): String = {
      val delta = (ms.toDouble / appDuration - 1.0) * 100
      if (delta >= 0) f"+${delta.round}%%" else f"${delta.round}%%"
    }

    val savedMs2x = math.max(0L, appDuration - pTwo)

    // Diminishing-returns flag: < 15% additional gain going from 2× to 4×
    val gainTwoToFour   = if (pTwo > 0) (pTwo - pFour).toDouble / pTwo * 100 else 0.0
    val diminishing     = gainTwoToFour < 15.0 && pTwo < appDuration

    // Serial-bottleneck fraction: the critical-path floor as a fraction of app time.
    // When parallel stages dominate, this tells us "how much of the benefit is
    // blocked by serial stages regardless of executor count".
    val criticalPaths   = sorted.map(s =>
      s.parentIds.flatMap(id => app.stages.get(id)).map(_.durationMs).sum +
      app.stages.get(s.stageId).map(_.durationMs).getOrElse(0L)
    )
    val criticalPathMs  = if (criticalPaths.isEmpty) 0L else criticalPaths.max
    val serialFloorPct  = if (appDuration > 0) criticalPathMs.toDouble / appDuration * 100 else 0.0

    // ── Issue ────────────────────────────────────────────────────────────────
    val severity = if (ceilingHit) Warning else Info

    val title = if (ceilingHit)
      s"DA ceiling (maxExecutors=${daMaxCap.getOrElse(peakExecutors)}) limits scaling — " +
      s"${fmtDouble(((appDuration - pTwo).toDouble / appDuration * 100).max(0), 0)}% projected gain at ${twoN}"
    else
      s"Executor scaling analysis — ${fmtDouble(((appDuration - pTwo).toDouble / appDuration * 100).max(0), 0)}% projected gain at ${twoN} executors"

    val ceilingNote = if (ceilingHit)
      s"The job ran with dynamic allocation and peaked at $peakExecutors/${daMaxCap.getOrElse(peakExecutors)} executors " +
      s"(ceiling was binding). Raising maxExecutors to $twoN is projected to save ~${fmtMs(savedMs2x)} per run. "
    else ""

    val diminishingNote = if (diminishing)
      s"Beyond ${twoN} executors, returns diminish (${fmtDouble(gainTwoToFour, 0)}% gain from ${twoN}→${fourN}x); " +
      s"serial stages account for ${fmtDouble(serialFloorPct, 0)}% of app time and set a floor on scaling benefit. "
    else ""

    val confidenceNote = if (modelAccuracy == "low")
      s"Model confidence: low (calibration factor ${fmtDouble(calibFactor, 2)}× — " +
      s"large driver/shuffle overhead not captured). Treat projections as directional only. "
    else ""

    val description =
      s"${ceilingNote}Simulated by replaying the stage DAG (${timedStages.size} stages) at " +
      s"different executor counts; avg task durations calibrated to actual run. $diminishingNote$confidenceNote"

    val configFix = if (ceilingHit)
      Some(s"spark.dynamicAllocation.maxExecutors=$twoN  # projected ~${fmtMs(pTwo)} (${pctChange(pTwo)})")
    else None

    val metrics: Map[String, String] = Map(
      s"actual  ($peakExecutors exec)"     -> fmtMs(appDuration),
      s"sim     (${halfN} exec, 0.5×)"    -> s"~${fmtMs(pHalf)}  (${pctChange(pHalf)})",
      s"sim     (${twoN} exec, 2×)"       -> s"~${fmtMs(pTwo)}  (${pctChange(pTwo)})",
      s"sim     (${threeN} exec, 3×)"     -> s"~${fmtMs(pThree)}  (${pctChange(pThree)})",
      s"sim     (${fourN} exec, 4×)"      -> s"~${fmtMs(pFour)}  (${pctChange(pFour)})",
      "model_confidence"                   -> modelAccuracy,
      "serial_floor_pct"                   -> fmtDouble(serialFloorPct, 1),
    ) ++ (if (ceilingHit) Map("da_ceiling" -> s"$peakExecutors/${daMaxCap.getOrElse(peakExecutors)}") else Map.empty)

    Seq(Issue(
      id              = "scaling-estimate-0",
      severity        = severity,
      category        = "scaling",
      title           = title,
      description     = description,
      recommendation  = if (ceilingHit)
        s"Raise spark.dynamicAllocation.maxExecutors from ${daMaxCap.getOrElse(peakExecutors)} to at least ${twoN}."
      else if (savedMs2x > appDuration * 0.1)
        s"Consider increasing the executor count beyond $peakExecutors if cluster capacity allows."
      else
        s"The job is near the scaling limit set by its serial stages. Focus on reducing stage dependencies or fixing skew instead.",
      configFix       = configFix,
      affectedStages  = Nil,
      metrics         = metrics,
      estimatedImpact = if (savedMs2x > 0) Some(EstimatedImpact(
        summary     = s"~${fmtMs(savedMs2x)} saved per run at ${twoN} executors",
        savedTimeMs = Some(savedMs2x),
        savedBytes  = None,
        confidence  = modelAccuracy,
      )) else None,
    ))
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def peakConcurrentExecutors(app: SparkAppModel): Int = {
    if (app.executors.isEmpty) return 0
    val adds    = app.executors.values.map(e => (e.addedTimeMs, +1))
    val removes = app.executors.values.flatMap(e => e.removedTimeMs.map(t => (t, -1)))
    val events  = (adds ++ removes).toSeq.sortBy(_._1)
    var peak = 0; var cur = 0
    events.foreach { case (_, d) => cur += d; if (cur > peak) peak = cur }
    peak
  }

  private def topologicalSort(stages: Map[Int, StageData]): Seq[StageData] = {
    val result   = mutable.ArrayBuffer[StageData]()
    val inResult = mutable.Set[Int]()
    def visit(s: StageData): Unit = {
      if (inResult(s.stageId)) return
      s.parentIds.flatMap(stages.get).foreach(visit)
      result += s
      inResult += s.stageId
    }
    stages.values.toSeq.sortBy(_.stageId).foreach(visit)
    result.toSeq
  }
}
