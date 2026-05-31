package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object JoinAnalyzer extends Analyzer {
  private val BroadcastThreshold    = 10L * MB
  private val LargeBroadcastWarn    = 1L  * GB
  private val ExcessiveShuffleCount = 4

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    app.sqlExecutions.values.foreach { sql =>
      val plan = sql.physicalPlan
      val desc = sql.description

      // SortMergeJoin where broadcast threshold might help
      if (plan.contains("SortMergeJoin")) {
        val broadcastThresholdConf = app.prop("spark.sql.autoBroadcastJoinThreshold")
          .map(s => scala.util.Try(s.toLong).getOrElse(10L * MB)).getOrElse(10L * MB)

        // Heuristic: if broadcast is explicitly disabled (-1) flag it
        if (broadcastThresholdConf < 0) {
          issues += Issue(
            id             = s"join-broadcast-disabled-${sql.executionId}",
            severity       = Info,
            category       = "join",
            title          = s"""Broadcast Join Disabled — SortMergeJoin in "${desc.take(80)}"""",
            description    = "spark.sql.autoBroadcastJoinThreshold=-1 prevents Spark from automatically broadcasting small tables, forcing expensive SortMergeJoins.",
            recommendation = "Re-enable auto-broadcast or explicitly hint small tables with df.hint(\"broadcast\").",
            configFix      = Some("spark.sql.autoBroadcastJoinThreshold=10485760  # 10 MB"),
            codeFix        = Some("small_df.hint(\"broadcast\").join(large_df, \"key\")"),
            affectedJobs   = sql.jobIds,
          )
        }
      }

      // Oversized broadcast (risk of driver OOM)
      if (plan.contains("BroadcastHashJoin") || plan.contains("BroadcastNestedLoopJoin")) {
        val broadcastThreshold = app.prop("spark.sql.autoBroadcastJoinThreshold")
          .map(s => scala.util.Try(s.toLong).getOrElse(10L * MB)).getOrElse(10L * MB)

        if (broadcastThreshold >= LargeBroadcastWarn) {
          issues += Issue(
            id             = s"join-large-broadcast-${sql.executionId}",
            severity       = Warning,
            category       = "join",
            title          = s"""Oversized Broadcast Threshold (${fmtBytes(broadcastThreshold)}) in "${desc.take(80)}"""",
            description    = s"spark.sql.autoBroadcastJoinThreshold is set to ${fmtBytes(broadcastThreshold)}. Broadcasting tables this large can cause driver OOM and slow down serialization.",
            recommendation = "Keep broadcast threshold below 200 MB. Use bucket joins for large-large table joins instead.",
            configFix      = Some("spark.sql.autoBroadcastJoinThreshold=209715200  # 200 MB max"),
            affectedJobs   = sql.jobIds,
          )
        }
      }

      // Many shuffle exchanges in one job (over-complex plan)
      val shuffleCount = plan.sliding("Exchange".length).count(_ == "Exchange")
      if (shuffleCount >= ExcessiveShuffleCount) {
        issues += Issue(
          id             = s"join-excessive-shuffle-${sql.executionId}",
          severity       = Warning,
          category       = "join",
          title          = s"""Excessive Shuffles in "${desc.take(80)}" ($shuffleCount exchanges)""",
          description    = s"The query plan contains $shuffleCount shuffle exchanges. Each exchange is a full sort+repartition of the data across the network.",
          recommendation = "Enable AQE to auto-coalesce post-shuffle partitions. Combine multiple groupBy operations into one, pre-partition data by the join/group key, and cache shared intermediate results.",
          configFix      = Some("spark.sql.adaptive.enabled=true"),
          affectedJobs   = sql.jobIds,
          metrics        = Map("exchange_count" -> shuffleCount.toString),
        )
      }
    }

    issues.toSeq
  }
}
