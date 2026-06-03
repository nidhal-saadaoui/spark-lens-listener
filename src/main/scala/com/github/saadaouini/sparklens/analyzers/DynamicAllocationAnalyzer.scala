package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object DynamicAllocationAnalyzer extends Analyzer {

  // YARN physical-memory-kill patterns — intentionally narrow to avoid overlap
  // with PreemptionAnalyzer (general executor loss) and YarnAnalyzer (virtual
  // memory OOM, which has a different fix). "virtual" explicitly excluded so
  // vmem kills are handled separately in YarnAnalyzer.
  private val yarnMemKill =
    """(?i)(exit.?code.?137|ExitCode.?137|killed.*exceeding.*(?:physical )?memory)""".r

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val props = app.sparkProperties
    def get(k: String)                 = props.get(k)
    def getOrElse(k: String, d: String) = props.getOrElse(k, d)

    val dynEnabled = getOrElse("spark.dynamicAllocation.enabled", "false").toLowerCase == "true"
    if (!dynEnabled) return Nil

    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // ── 1. No shuffle protection ─────────────────────────────────────────────
    val essEnabled      = getOrElse("spark.shuffle.service.enabled",                     "false").toLowerCase == "true"
    val shuffleTracking = getOrElse("spark.dynamicAllocation.shuffleTracking.enabled",   "false").toLowerCase == "true"

    if (!essEnabled && !shuffleTracking) {
      issues += Issue(
        id              = "dynalloc-no-shuffle-protection",
        severity        = Critical,
        category        = "reliability",
        title           = "Dynamic Allocation Enabled Without Shuffle Protection",
        description     =
          "Dynamic allocation is active but neither shuffle tracking nor the External " +
          "Shuffle Service is enabled. When an idle executor is removed, its shuffle " +
          "map output is destroyed. Any downstream task that needs that output will " +
          "fail with FetchFailed and force the entire parent stage to rerun — silently " +
          "and repeatedly, hiding a correctness risk as a performance issue.",
        recommendation  =
          "On Spark 3.x enable shuffle tracking (no cluster-side setup required). " +
          "On Spark 2.x or CDH/HDP clusters that already run the External Shuffle " +
          "Service on every NodeManager, enable it via the second option instead.",
        configFix       = Some(
          if (majorVersion(app) >= 3)
            "spark.dynamicAllocation.shuffleTracking.enabled=true\n" +
            "# OR if your cluster runs the External Shuffle Service:\n" +
            "# spark.shuffle.service.enabled=true"
          else
            "spark.shuffle.service.enabled=true\n" +
            "# shuffleTracking.enabled requires Spark 3.0+"
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── 2. Executor churn ────────────────────────────────────────────────────
    val churnLifetimeMs = propLong  (app, "spark.sparklens.dynalloc.churnLifetimeMs", 30000L)
    val churnWarnPct    = propDouble(app, "spark.sparklens.dynalloc.churnPct",        25.0)
    val minRemoved      = propLong  (app, "spark.sparklens.dynalloc.minRemovedExecs",  3L).toInt

    val removed    = app.executors.values.filter(_.removedTimeMs.isDefined).toSeq
    val shortLived = removed.filter(e => e.removedTimeMs.get - e.addedTimeMs < churnLifetimeMs)

    if (removed.size >= minRemoved && shortLived.size.toDouble / removed.size * 100 >= churnWarnPct) {
      val churnPct     = fmtDouble(shortLived.size.toDouble / removed.size * 100, 0)
      val lifetimes    = shortLived.map(e => e.removedTimeMs.get - e.addedTimeMs).sorted
      val medianLifeMs = if (lifetimes.nonEmpty) percentile(lifetimes, 50) else 0L
      issues += Issue(
        id              = "dynalloc-executor-churn",
        severity        = Warning,
        category        = "config",
        title           = s"Executor Churn — ${shortLived.size} of ${removed.size} Executors Lived < ${fmtMs(churnLifetimeMs)}",
        description     =
          s"${shortLived.size} of ${removed.size} removed executors ($churnPct%) lived less than " +
          s"${fmtMs(churnLifetimeMs)} (median lifetime of churned executors: ${fmtMs(medianLifeMs)}). " +
          s"Each YARN container start costs 10–30 s of allocation overhead. Executors that are " +
          s"released between closely-spaced stages and immediately re-requested pay this overhead " +
          s"for near-zero compute benefit.",
        recommendation  =
          "Raise executorIdleTimeout so executors survive the gaps between stages. " +
          "Set cachedExecutorIdleTimeout=infinity to prevent releasing executors that " +
          "hold cached RDD partitions. If the workload has predictable bursts, " +
          "setting minExecutors > 0 keeps a warm pool ready.",
        configFix       = Some(
          "spark.dynamicAllocation.executorIdleTimeout=120s\n" +
          "spark.dynamicAllocation.cachedExecutorIdleTimeout=infinity"
        ),
        metrics         = Map(
          "churned_executors"         -> shortLived.size.toString,
          "total_removed_executors"   -> removed.size.toString,
          "churn_pct"                 -> churnPct,
          "median_churned_lifetime_ms"-> medianLifeMs.toString,
          "churn_threshold_ms"        -> churnLifetimeMs.toString,
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── 3. YARN container OOM kill ────────────────────────────────────────────
    // Narrow pattern: only matches memory-limit kills (exit 137 / "exceeding memory").
    // General executor loss and YARN preemption are handled by PreemptionAnalyzer.
    // Exclude virtual-memory kills — those are handled by YarnAnalyzer with a
    // completely different recommendation (vmem-check-enabled vs memoryOverhead).
    val yarnOomKilled = app.executors.values.filter { e =>
      e.removalReason.exists { r =>
        yarnMemKill.findFirstIn(r).isDefined && !r.toLowerCase.contains("virtual")
      }
    }.toSeq

    if (yarnOomKilled.nonEmpty) {
      val hosts = yarnOomKilled.map(_.host).distinct.take(5).mkString(", ")
      issues += Issue(
        id              = "dynalloc-yarn-oom-kill",
        severity        = Warning,
        category        = "reliability",
        title           = s"${yarnOomKilled.size} Executor(s) Killed by YARN Memory Enforcer",
        description     =
          s"${yarnOomKilled.size} executor(s) on $hosts were killed by YARN's memory " +
          s"enforcer (exit 137), not by JVM OutOfMemoryError. This means total process " +
          s"memory — JVM heap + off-heap (Arrow buffers, Python UDF pickling, native " +
          s"libraries, JVM metadata) — exceeded the YARN container limit. The JVM never " +
          s"logs this: the container is SIGKILLed and the executor simply disappears.",
        recommendation  =
          "Raise executor memory overhead to give the container more off-heap headroom. " +
          "If running PySpark with Arrow or pandas UDFs, overhead requirements are higher " +
          "than the default 10%. Start with 20% and tune from there. Do not increase " +
          "heap (spark.executor.memory) — it is off-heap memory that overflowed.",
        configFix       = Some(
          "spark.executor.memoryOverheadFactor=0.2\n" +
          "# or explicit: spark.executor.memoryOverhead=2g"
        ),
        metrics         = Map(
          "yarn_oom_killed"  -> yarnOomKilled.size.toString,
          "affected_hosts"   -> yarnOomKilled.map(_.host).distinct.mkString(", "),
        ),
        estimatedImpact = Some(configRisk),
      )
    }

    // ── 4. Running at maxExecutors ceiling ────────────────────────────────────
    // Only fires when maxExecutors is explicitly set to a finite value.
    // If the property is absent or set to Int.MaxValue the ceiling is not meaningful.
    val maxExecConfig = get("spark.dynamicAllocation.maxExecutors")
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(v => v > 0 && v < Int.MaxValue)

    maxExecConfig.foreach { ceiling =>
      // Reconstruct peak concurrent executor count from add/remove timeline.
      case class Ev(timeMs: Long, delta: Int)
      val events = app.executors.values.toSeq.flatMap { e =>
        Seq(Ev(e.addedTimeMs, +1)) ++ e.removedTimeMs.map(t => Ev(t, -1))
      }.sortBy(_.timeMs)

      var current = 0
      var peak    = 0
      events.foreach { ev => current += ev.delta; if (current > peak) peak = current }

      if (peak >= ceiling) {
        issues += Issue(
          id              = "dynalloc-maxexecutors-ceiling",
          severity        = Info,
          category        = "config",
          title           = s"Job Hit the maxExecutors Ceiling ($ceiling) — Cluster Capacity May Be Underused",
          description     =
            s"The job reached the spark.dynamicAllocation.maxExecutors=$ceiling limit " +
            s"with $peak concurrent executors during peak load. Spark could not request " +
            s"additional containers even if cluster capacity was available, forcing tasks " +
            s"to queue rather than run in parallel.",
          recommendation  =
            s"If the YARN queue has available capacity, raise maxExecutors. " +
            s"Check YARN ResourceManager UI to confirm free containers are actually " +
            s"available before increasing the limit — if the cluster is already at " +
            s"capacity the ceiling is not the bottleneck.",
          configFix       = Some(s"spark.dynamicAllocation.maxExecutors=${ceiling * 2}  # example — verify cluster quota first"),
          metrics         = Map(
            "peak_concurrent_executors" -> peak.toString,
            "configured_ceiling"        -> ceiling.toString,
          ),
          estimatedImpact = Some(configRisk),
        )
      }
    }

    // ── 5. Scale-up lag after idle gap ────────────────────────────────────────
    // When executors are released during an idle gap between jobs, the next job
    // must wait for YARN to allocate fresh containers — visible as high scheduler
    // delay on the first stage. This check connects the cause (idle gap → scale down)
    // to the symptom (task launch delay) that SchedulerDelayAnalyzer also flags,
    // giving a more specific recommendation (minExecutors / idleTimeout).
    val gapWarnMs   = propLong(app, "spark.sparklens.timeline.gapWarnMs",     60000L)
    val delayWarnMs = propLong(app, "spark.sparklens.schedulerDelay.warnMs",   2000L)

    val sortedJobs = app.jobs.values.toSeq
      .filter(j => j.completionTimeMs.isDefined)
      .sortBy(_.completionTimeMs.get)

    sortedJobs.sliding(2).foreach {
      case Seq(prev, next) =>
        val gapMs = next.submissionTimeMs - prev.completionTimeMs.get
        if (gapMs >= gapWarnMs) {
          // Find stages in the next job that have high median task launch delay
          val laggingStages = next.stageIds.flatMap(app.stages.get).filter { s =>
            val submitMs = s.submissionTimeMs.getOrElse(0L)
            val delays   = s.tasks
              .filter(t => !t.failed && !t.killed && t.launchTimeMs >= submitMs)
              .map(_.launchTimeMs - submitMs).sorted
            delays.size >= 3 && percentile(delays, 50) >= delayWarnMs
          }
          laggingStages.foreach { s =>
            val submitMs   = s.submissionTimeMs.getOrElse(0L)
            val delays     = s.tasks
              .filter(t => !t.failed && !t.killed && t.launchTimeMs >= submitMs)
              .map(_.launchTimeMs - submitMs).sorted
            val p50DelayMs = percentile(delays, 50)
            val p95DelayMs = percentile(delays, 95)
            issues += Issue(
              id              = s"dynalloc-scaleup-lag-${s.stageId}",
              severity        = Info,
              category        = "config",
              title           = s"Dynamic Allocation Scale-Up Lag in Stage ${s.stageId} — ${fmtMs(p50DelayMs)} wait after ${fmtMs(gapMs)} idle gap",
              description     =
                s"A ${fmtMs(gapMs)} idle gap between jobs caused dynamic allocation to " +
                s"release executors. When stage ${s.stageId} started, YARN needed a median " +
                s"of ${fmtMs(p50DelayMs)} (p95: ${fmtMs(p95DelayMs)}) to allocate fresh " +
                s"containers before the first task could launch. All cluster cores were idle " +
                s"during this window.",
              recommendation  =
                s"Set minExecutors > 0 to keep a warm pool of containers alive across " +
                s"job boundaries. Alternatively, raise executorIdleTimeout beyond the " +
                s"${fmtMs(gapMs)} gap so executors are not released between jobs.",
              configFix       = Some(
                s"spark.dynamicAllocation.minExecutors=2  # keeps a warm pool between jobs\n" +
                s"# or extend idle timeout beyond the ${fmtMs(gapMs)} gap:\n" +
                s"# spark.dynamicAllocation.executorIdleTimeout=${gapMs / 1000 + 30}s"
              ),
              affectedStages  = Seq(s.stageId),
              metrics         = Map(
                "idle_gap_ms"              -> gapMs.toString,
                "median_launch_delay_ms"   -> p50DelayMs.toString,
                "p95_launch_delay_ms"      -> p95DelayMs.toString,
                "tasks_sampled"            -> delays.size.toString,
              ),
              estimatedImpact = Some(EstimatedImpact(
                summary     = s"${fmtMs(p50DelayMs)} YARN allocation wait on every run after a ${fmtMs(gapMs)} idle gap",
                savedTimeMs = timeOpt(p50DelayMs),
                savedBytes  = None,
                confidence  = "medium",
              )),
            )
          }
        }
      case _ =>
    }

    issues.toSeq
  }
}
