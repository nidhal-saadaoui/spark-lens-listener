package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object CriticalPathAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val stages = app.stages
    if (stages.size < 3) return Nil

    val appDuration = app.durationMs.getOrElse(0L)
    if (appDuration < 10000L) return Nil

    val warnFraction = propDouble(app, "spark.sparklens.criticalPath.warnFraction", 0.85)
    val minChain     = propLong  (app, "spark.sparklens.criticalPath.minChain",      3L).toInt

    // DP: longest path (by cumulative duration) from any root to this stage.
    val memo = scala.collection.mutable.Map[Int, Long]()
    def longestPathTo(id: Int): Long = memo.getOrElseUpdate(id, {
      val dur     = stages.get(id).map(_.durationMs).getOrElse(0L)
      val parents = stages.get(id).map(_.parentIds.filter(stages.contains)).getOrElse(Nil)
      val parentMax = if (parents.isEmpty) 0L else parents.map(longestPathTo).max
      parentMax + dur
    })

    val criticalPathMs = if (stages.isEmpty) 0L else stages.keys.map(longestPathTo).max
    val criticalFraction = if (appDuration > 0) criticalPathMs.toDouble / appDuration else 0.0

    if (criticalFraction < warnFraction) return Nil

    // Walk back from the tip of the critical path to reconstruct the chain.
    def criticalChain(id: Int): List[Int] = {
      val parents = stages.get(id).map(_.parentIds.filter(stages.contains)).getOrElse(Nil)
      if (parents.isEmpty) List(id)
      else id :: criticalChain(parents.maxBy(longestPathTo))
    }
    val tipId = stages.keys.maxBy(longestPathTo)
    val chain = criticalChain(tipId).reverse

    if (chain.size < minChain) return Nil

    val chainMs       = chain.map(id => stages.get(id).map(_.durationMs).getOrElse(0L)).sum
    val bottleneckId  = chain.maxBy(id => stages.get(id).map(_.durationMs).getOrElse(0L))
    val bottleneckMs  = stages.get(bottleneckId).map(_.durationMs).getOrElse(0L)
    val bottleneckPct = fmtDouble(bottleneckMs.toDouble / chainMs * 100, 0)
    val severity      = if (criticalFraction > 0.95) Warning else Info

    Seq(Issue(
      id              = s"critical-path-serial-$tipId",
      severity        = severity,
      category        = "plan",
      title           = s"Critical Path Is ${fmtDouble(criticalFraction * 100, 0)}% of Wall Time — ${chain.size}-Stage Sequential Chain",
      description     =
        s"The longest dependency chain has ${chain.size} stages (${fmtMs(chainMs)}) and " +
        s"accounts for ${fmtDouble(criticalFraction * 100, 0)}% of the ${fmtMs(appDuration)} " +
        s"wall time. Adding more executors will not reduce this: the bottleneck is data " +
        s"dependencies, not parallelism. Stage $bottleneckId dominates the chain " +
        s"($bottleneckPct% of chain time, ${fmtMs(bottleneckMs)}).",
      recommendation  =
        s"Optimise stage $bottleneckId first — it drives the critical path. " +
        "Look for spill, skew, or expensive sorts in that stage. " +
        "Also review whether any stages in the chain could be restructured to run in parallel " +
        "(e.g., pre-compute independent aggregations before the join that combines them).",
      affectedStages  = chain,
      metrics         = Map(
        "critical_path_ms"  -> criticalPathMs.toString,
        "app_duration_ms"   -> appDuration.toString,
        "critical_fraction" -> fmtDouble(criticalFraction, 3),
        "chain_length"      -> chain.size.toString,
        "bottleneck_stage"  -> bottleneckId.toString,
        "bottleneck_ms"     -> bottleneckMs.toString,
      ),
      estimatedImpact = Some(configRisk),
    ))
  }
}
