package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object JoinAnalyzer extends Analyzer {
  private val BroadcastThreshold = 10L * MB

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val largeBroadcastWarn    = propLong(app, "spark.sparklens.join.largeBroadcastGb",      1L) * GB
    val excessiveShuffleCount = propLong(app, "spark.sparklens.join.excessiveShuffleCount", 4L).toInt
    val networkSpeedMbps      = propLong(app, "spark.sparklens.impact.networkSpeedMbps", 1024L)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // Aggregate shuffle bytes written from stages linked to a SQL execution's jobs.
    def shuffleBytesForExec(sql: SqlExecutionData): Long =
      (for {
        jobId   <- sql.jobIds
        job     <- app.jobs.get(jobId).toSeq
        stageId <- job.stageIds
        stage   <- app.stages.get(stageId).toSeq
      } yield stage.totalShuffleRemoteBytes + stage.totalShuffleLocalBytes).sum

    app.sqlExecutions.values.foreach { sql =>
      val plan = sql.physicalPlan
      val desc = sql.description

      // Prefer planTree for join-type detection: planTree reflects the FINAL executed plan
      // (updated by SparkListenerSQLAdaptiveExecutionUpdate for AQE queries) so a join that
      // Spark auto-converted to BroadcastHashJoin at runtime is not reported as SortMergeJoin.
      // Fall back to text search when planTree is absent (e.g. tests with no planInfo).
      val hasSMJ = sql.planTree.fold(plan.contains("SortMergeJoin"))(
        _.nodesNamed("SortMergeJoin").nonEmpty
      )
      val hasBroadcast = sql.planTree.fold(
        plan.contains("BroadcastHashJoin") || plan.contains("BroadcastNestedLoopJoin")
      )(t => t.nodesNamed("BroadcastHashJoin").nonEmpty || t.nodesNamed("BroadcastNestedLoopJoin").nonEmpty)

      // SortMergeJoin where broadcast threshold might help
      if (hasSMJ) {
        val broadcastThresholdConf = app.prop("spark.sql.autoBroadcastJoinThreshold")
          .map(s => scala.util.Try(s.toLong).getOrElse(10L * MB)).getOrElse(10L * MB)

        if (broadcastThresholdConf < 0) {
          val shuffleBytes = shuffleBytesForExec(sql)
          val penaltyMs    = networkMs(shuffleBytes, networkSpeedMbps)
          val smjImpact    = EstimatedImpact(
            summary     = s"~${fmtBytes(shuffleBytes)} shuffled per run; with broadcast: 0 shuffle bytes, ~${fmtMs(penaltyMs)} saved",
            savedTimeMs = timeOpt(penaltyMs),
            savedBytes  = bytesOpt(shuffleBytes),
            confidence  = "medium",
          )
          issues += Issue(
            id              = s"join-broadcast-disabled-${sql.executionId}",
            severity        = Info,
            category        = "join",
            title           = s"""Broadcast Join Disabled — SortMergeJoin in "${desc.take(80)}"""",
            description     = "spark.sql.autoBroadcastJoinThreshold=-1 prevents Spark from automatically broadcasting small tables, forcing expensive SortMergeJoins.",
            recommendation  = "Re-enable auto-broadcast or explicitly hint small tables with df.hint(\"broadcast\").",
            configFix       = Some("spark.sql.autoBroadcastJoinThreshold=10485760  # 10 MB"),
            codeFix         = Some("small_df.hint(\"broadcast\").join(large_df, \"key\")"),
            affectedJobs    = sql.jobIds,
            estimatedImpact = Some(smjImpact),
          )
        }
      }

      if (hasBroadcast) {
        val broadcastThreshold = app.prop("spark.sql.autoBroadcastJoinThreshold")
          .map(s => scala.util.Try(s.toLong).getOrElse(10L * MB)).getOrElse(10L * MB)

        if (broadcastThreshold >= largeBroadcastWarn) {
          issues += Issue(
            id              = s"join-large-broadcast-${sql.executionId}",
            severity        = Warning,
            category        = "join",
            title           = s"""Oversized Broadcast Threshold (${fmtBytes(broadcastThreshold)}) in "${desc.take(80)}"""",
            description     = s"spark.sql.autoBroadcastJoinThreshold is set to ${fmtBytes(broadcastThreshold)}. Broadcasting tables this large can cause driver OOM and slow down serialization.",
            recommendation  = "Keep broadcast threshold below 200 MB. Use bucket joins for large-large table joins instead.",
            configFix       = Some("spark.sql.autoBroadcastJoinThreshold=209715200  # 200 MB max"),
            affectedJobs    = sql.jobIds,
            estimatedImpact = Some(configRisk),
          )
        }
      }

      val shuffleCount: Int = sql.planTree match {
        case Some(tree) =>
          tree.nodesContaining("Exchange").count(!_.nodeName.contains("BroadcastExchange"))
        case None =>
          val planForCounting = {
            val sep = plan.indexOf("\n\n(")
            if (sep > 0) plan.substring(0, sep) else plan
          }
          val allExchanges   = planForCounting.sliding("Exchange".length).count(_ == "Exchange")
          val broadcastCount = planForCounting.sliding("BroadcastExchange".length).count(_ == "BroadcastExchange")
          allExchanges - broadcastCount
      }
      if (shuffleCount >= excessiveShuffleCount) {
        val totalShuffleBytes = shuffleBytesForExec(sql)
        val penaltyMs         = networkMs(totalShuffleBytes, networkSpeedMbps)
        val excessImpact      = EstimatedImpact(
          summary     = s"$shuffleCount shuffle exchanges, ~${fmtBytes(totalShuffleBytes)} total shuffle per run, ~${fmtMs(penaltyMs)} network cost",
          savedTimeMs = timeOpt(penaltyMs),
          savedBytes  = bytesOpt(totalShuffleBytes),
          confidence  = "low",
        )
        issues += Issue(
          id              = s"join-excessive-shuffle-${sql.executionId}",
          severity        = Warning,
          category        = "join",
          title           = s"""Excessive Shuffles in "${desc.take(80)}" ($shuffleCount exchanges)""",
          description     = s"The query plan contains $shuffleCount shuffle exchanges. Each exchange sorts and repartitions the entire dataset across the network — $shuffleCount exchanges means the data crosses the network $shuffleCount times.",
          recommendation  = "Restructure the query to reduce shuffles: combine multiple groupBy steps into one, pre-partition source data by the join/group key using bucket tables, and cache shared intermediate results used in multiple branches. Enable AQE so Spark can coalesce small post-shuffle partitions at runtime.",
          configFix       = Some("spark.sql.adaptive.enabled=true"),
          affectedJobs    = sql.jobIds,
          metrics         = Map("exchange_count" -> shuffleCount.toString),
          estimatedImpact = Some(excessImpact),
        )
      }
    }

    issues.toSeq
  }
}
