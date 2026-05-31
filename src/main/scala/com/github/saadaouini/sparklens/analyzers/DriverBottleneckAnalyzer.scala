package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object DriverBottleneckAnalyzer extends Analyzer {
  private val LargeResultWarnBytes = 50L * MB
  private val LargeResultCritBytes = 500L * MB

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    // Large result sizes (collect() back to driver)
    app.stages.values.foreach { stage =>
      val resultSize = stage.totalResultSize
      if (resultSize >= LargeResultWarnBytes) {
        val severity = if (resultSize >= LargeResultCritBytes) Critical else Warning
        issues += Issue(
          id             = s"driver-result-${stage.stageId}",
          severity       = severity,
          category       = "io",
          title          = s"Large collect() to Driver in Stage ${stage.stageId} — ${fmtBytes(resultSize)}",
          description    = s"Stage ${stage.stageId} sent ${fmtBytes(resultSize)} of task results back to the driver. Large collects cause driver OOM and serialize executor parallelism.",
          recommendation = "Avoid collect() on large datasets. Write results directly to storage (df.write.parquet(...)) or use take(N) for sampling.",
          codeFix        = Some("df.write.parquet(\"s3://bucket/output/\")  // instead of df.collect()"),
          affectedStages = Seq(stage.stageId),
          metrics        = Map("result_bytes" -> resultSize.toString),
        )
      }
    }

    // SQL plans with driver-side operations
    app.sqlExecutions.values.foreach { sql =>
      val plan = sql.physicalPlan
      if (plan.contains("CollectLimit") || plan.contains("TakeOrderedAndProject")) {
        issues += Issue(
          id             = s"driver-collect-limit-${sql.executionId}",
          severity       = Info,
          category       = "io",
          title          = s"""Driver-Side Aggregation in "${sql.description.take(80)}"""",
          description    = "The query uses CollectLimit or TakeOrderedAndProject which gathers data to the driver. Fine for LIMIT, problematic for large N.",
          recommendation = "If fetching large amounts of data, write to storage instead of collecting to the driver.",
          affectedJobs   = sql.jobIds,
        )
      }
    }

    issues.toSeq
  }
}
