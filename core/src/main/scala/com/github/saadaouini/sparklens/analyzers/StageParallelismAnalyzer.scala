package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object StageParallelismAnalyzer extends Analyzer {

  // ── Plan-cause detection ──────────────────────────────────────────────────

  private case class SingleTaskCause(
    planNote:       String,         // what was found, shown in description
    recommendation: String,
    codeFix:        Option[String],
    configFix:      Option[String] = None,
  )

  private def coaleseCause(suggestedN: Int) = SingleTaskCause(
    planNote       = "Coalesce 1 node detected in the query plan",
    recommendation = s"Remove the coalesce(1) call, or replace it with coalesce($suggestedN) " +
                     "to limit parallelism without collapsing to a single task.",
    codeFix        = Some(s"// Remove or replace: .coalesce(1)\ndf.coalesce($suggestedN)"),
  )

  private def repartitionCause(suggestedN: Int) = SingleTaskCause(
    planNote       = "repartition(1) (RoundRobinPartitioning(1)) detected in the query plan",
    recommendation = s"Replace repartition(1) with repartition($suggestedN) to distribute " +
                     "the work across all available cores.",
    codeFix        = Some(s"// Replace: .repartition(1)\ndf.repartition($suggestedN)"),
  )

  private def singlePartitionCause = SingleTaskCause(
    planNote       = "Exchange SinglePartition detected — a global sort or unpartitioned " +
                     "aggregate is forcing all data through one reducer",
    recommendation = "Add PARTITION BY to window functions, avoid global ORDER BY without LIMIT, " +
                     "or enable AQE so Spark can coalesce partitions without forcing a single one.",
    codeFix        = Some("// Add partitioning to window:\nWindow.partitionBy(\"key\").orderBy(\"ts\")\n" +
                          "// Or for global sort, consider bucketed tables instead"),
    configFix      = Some("spark.sql.adaptive.enabled=true"),
  )

  private def collectLimitCause = SingleTaskCause(
    planNote       = "CollectLimit node detected — a LIMIT clause in the write path is " +
                     "forcing all data to the driver before writing",
    recommendation = "Remove the LIMIT, or replace df.limit(n).write with " +
                     "df.write after filtering the dataset upstream.",
    codeFix        = Some("// Replace: df.limit(n).write.parquet(path)\n" +
                          "df.filter(...).write.parquet(path)"),
  )

  private def takeOrderedCause = SingleTaskCause(
    planNote       = "TakeOrderedAndProject node detected — ORDER BY … LIMIT is pulling " +
                     "all data to a single reducer to guarantee global order",
    recommendation = "If global ordering is not required, remove the ORDER BY or compute " +
                     "the top-N per partition first. If ordering is required, write the data " +
                     "first and sort on read, or use bucketed tables.",
    codeFix        = Some("// If only approximate top-N is needed:\ndf.sortWithinPartitions(\"col\").write.parquet(path)"),
  )

  private def unknownCause(suggestedN: Int) = SingleTaskCause(
    planNote       = "no SQL plan available for this stage — cause could not be determined from plan",
    recommendation = "Check whether repartition(1) or coalesce(1) was called upstream. " +
                     "A CollectLimit or TakeOrderedAndProject node in the plan also forces " +
                     "single-task execution. Increase partitions to distribute work across cores.",
    codeFix        = Some(s"df.repartition($suggestedN)"),
  )

  /** Walk stage → jobs → SQL executions to find the plan that produced this stage. */
  private def sqlForStage(stageId: Int, app: SparkAppModel): Option[SqlExecutionData] = {
    val jobIdsForStage = app.jobs.values
      .filter(_.stageIds.contains(stageId))
      .map(_.jobId)
      .toSet
    app.sqlExecutions.values.find(sql => sql.jobIds.exists(jobIdsForStage.contains))
  }

  /** Inspect the plan (structured tree preferred, text fallback) for the node
   *  that forced single-partition execution. */
  private def detectCause(sql: SqlExecutionData, suggestedN: Int): SingleTaskCause = {
    // Structured plan: exact node-level detection
    sql.planTree match {
      case Some(tree) =>
        val nodes = tree.flatten
        if (nodes.exists(n => n.nodeName == "Coalesce" &&
              n.simpleString.split("\\s+").lastOption.contains("1")))
          coaleseCause(suggestedN)
        else if (nodes.exists(n => n.nodeName == "Exchange" &&
              n.simpleString.contains("RoundRobinPartitioning(1)")))
          repartitionCause(suggestedN)
        else if (nodes.exists(n => n.nodeName == "Exchange" &&
              n.simpleString.contains("SinglePartition")))
          singlePartitionCause
        else if (nodes.exists(_.nodeName == "CollectLimit"))
          collectLimitCause
        else if (nodes.exists(_.nodeName == "TakeOrderedAndProject"))
          takeOrderedCause
        else
          textFallback(sql.physicalPlan, suggestedN)
      case None =>
        textFallback(sql.physicalPlan, suggestedN)
    }
  }

  private def textFallback(plan: String, suggestedN: Int): SingleTaskCause =
    if      (plan.contains("Coalesce 1"))                 coaleseCause(suggestedN)
    else if (plan.contains("RoundRobinPartitioning(1)"))  repartitionCause(suggestedN)
    else if (plan.contains("SinglePartition"))            singlePartitionCause
    else if (plan.contains("CollectLimit"))               collectLimitCause
    else if (plan.contains("TakeOrderedAndProject"))      takeOrderedCause
    else                                                   unknownCause(suggestedN)

  // ── Analyzer entry point ──────────────────────────────────────────────────

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val singleTaskMinMs     = propLong(app, "spark.sparklens.stageParallelism.singleTaskMinMs", 5000L)
    val totalCoresForSingle = app.executors.values.map(_.totalCores).sum
    val suggestedN          = math.max(totalCoresForSingle, 2)

    // ── 1. Single-task stages ─────────────────────────────────────────────
    val singleTaskIssues: Seq[Issue] = app.stages.values.toSeq.flatMap { stage =>
      if (stage.numTasks != 1 || stage.durationMs < singleTaskMinMs) Nil
      else {
        val dur      = stage.durationMs
        val idleCores = math.max(0, totalCoresForSingle - 1)
        val savedMs  = dur * (suggestedN - 1) / suggestedN
        val impact   = EstimatedImpact(
          summary     = s"${fmtMs(dur)} stage ran on 1 task — $idleCores cores sat idle",
          savedTimeMs = timeOpt(savedMs),
          savedBytes  = None,
          confidence  = "medium",
        )

        // Look up SQL plan and detect the cause definitively
        val maybeSql  = sqlForStage(stage.stageId, app)
        val cause     = maybeSql.map(detectCause(_, suggestedN))
                                .getOrElse(unknownCause(suggestedN))
        val sqlNote   = maybeSql.map(s => s""" for "${s.description.take(80)}"""").getOrElse("")
        val callSite  = stage.callSite

        val description =
          s"Stage ${stage.stageId}${if (callSite.nonEmpty) s" ($callSite)" else ""} " +
          s"ran as a single task for ${fmtMs(dur)}, leaving all other cluster resources idle. " +
          s"Cause: ${cause.planNote}$sqlNote."

        Seq(Issue(
          id              = s"single-task-${stage.stageId}",
          severity        = Warning,
          category        = "io",
          title           = s"Single-Task Stage ${stage.stageId} — Entire Stage Runs on One Executor",
          description     = description,
          recommendation  = cause.recommendation,
          configFix       = cause.configFix,
          codeFix         = cause.codeFix,
          affectedStages  = Seq(stage.stageId),
          metrics         = Map(
            "num_tasks"    -> "1",
            "duration_ms"  -> dur.toString,
            "idle_cores"   -> idleCores.toString,
          ),
          estimatedImpact = Some(impact),
        ))
      }
    }

    // ── 2. Low-parallelism stages ─────────────────────────────────────────
    val minCores    = propLong(app,   "spark.sparklens.stageParallelism.minCores",             8L).toInt
    val utilRatio   = propDouble(app, "spark.sparklens.stageParallelism.underutilizationRatio", 0.5)
    val minStageSec = propLong(app,   "spark.sparklens.stageParallelism.minStageSec",          10L)

    val totalCores  = totalCoresForSingle
    val coreIssues: Seq[Issue] = if (totalCores < minCores) Nil
    else {
      val threshold = math.max(1, (totalCores * utilRatio).toInt)
      app.stages.values.toSeq.flatMap { stage =>
        val dur = stage.durationMs
        if (dur < minStageSec * 1000L || stage.numTasks >= threshold || stage.numTasks == 1) Nil
        else {
          val utilPct   = fmtDouble(stage.numTasks.toDouble / totalCores * 100, 0)
          val idleCores = totalCores - stage.numTasks
          val impact    = EstimatedImpact(
            summary     = s"$idleCores of $totalCores cores idle for ${fmtMs(dur)} — stage under-parallelised at $utilPct%",
            savedTimeMs = timeOpt((dur * (1.0 - stage.numTasks.toDouble / threshold)).toLong),
            savedBytes  = None,
            confidence  = "medium",
          )
          Seq(Issue(
            id              = s"low-parallelism-${stage.stageId}",
            severity        = Info,
            category        = "io",
            title           = s"Low Parallelism in Stage ${stage.stageId} — ${stage.numTasks} tasks on $totalCores cores ($utilPct% utilization)",
            description     = s"Stage ${stage.stageId} (${stage.name}) ran ${stage.numTasks} tasks on a cluster with $totalCores available cores, using only $utilPct% of available parallelism. The stage ran for ${fmtMs(dur)} with most cores idle.",
            recommendation  = s"Increase the number of partitions to at least $threshold (${(utilRatio * 100).toInt}% of $totalCores cores). For shuffle stages raise spark.sql.shuffle.partitions; for scan stages lower spark.sql.files.maxPartitionBytes or call repartition() after the scan.",
            configFix       = Some(s"spark.sql.shuffle.partitions=$threshold"),
            codeFix         = Some(s"df.repartition($threshold)"),
            affectedStages  = Seq(stage.stageId),
            metrics         = Map(
              "num_tasks"   -> stage.numTasks.toString,
              "total_cores" -> totalCores.toString,
              "util_pct"    -> utilPct,
              "duration_ms" -> dur.toString,
            ),
            estimatedImpact = Some(impact),
          ))
        }
      }
    }

    singleTaskIssues ++ coreIssues
  }
}
