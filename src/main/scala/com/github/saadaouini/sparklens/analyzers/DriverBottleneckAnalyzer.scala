package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object DriverBottleneckAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    val largeResultWarnBytes = propLong(app, "spark.sparklens.driver.largeResultWarnMb",  50L) * MB
    val largeResultCritBytes = propLong(app, "spark.sparklens.driver.largeResultCritMb", 500L) * MB
    val networkSpeedMbps     = propLong(app, "spark.sparklens.impact.networkSpeedMbps", 1024L)
    val issues = scala.collection.mutable.ArrayBuffer[Issue]()

    app.stages.values.foreach { stage =>
      val resultSize = stage.totalResultSize
      if (resultSize >= largeResultWarnBytes) {
        val severity  = if (resultSize >= largeResultCritBytes) Critical else Warning
        val transferMs = networkMs(resultSize, networkSpeedMbps)
        val impact    = EstimatedImpact(
          summary     = s"~${fmtBytes(resultSize)} sent to driver, ~${fmtMs(transferMs)} transfer cost",
          savedTimeMs = timeOpt(transferMs),
          savedBytes  = bytesOpt(resultSize),
          confidence  = "medium",
        )
        issues += Issue(
          id              = s"driver-result-${stage.stageId}",
          severity        = severity,
          category        = "io",
          title           = s"Large collect() to Driver in Stage ${stage.stageId} — ${fmtBytes(resultSize)}",
          description     = s"Stage ${stage.stageId} sent ${fmtBytes(resultSize)} of task results back to the driver. Large collects cause driver OOM and serialize executor parallelism.",
          recommendation  = "Avoid collect() on large datasets. Write results directly to storage (df.write.parquet(...)) or use take(N) for sampling.",
          codeFix         = Some("df.write.parquet(\"s3://bucket/output/\")  // instead of df.collect()"),
          affectedStages  = Seq(stage.stageId),
          metrics         = Map("result_bytes" -> resultSize.toString),
          estimatedImpact = Some(impact),
        )
      }
    }

    app.sqlExecutions.values.foreach { sql =>
      val plan = sql.physicalPlan
      if (plan.contains("CollectLimit") || plan.contains("TakeOrderedAndProject")) {
        issues += Issue(
          id              = s"driver-collect-limit-${sql.executionId}",
          severity        = Info,
          category        = "io",
          title           = s"""Driver-Side Aggregation in "${sql.description.take(80)}"""",
          description     = "The query uses CollectLimit or TakeOrderedAndProject which gathers data to the driver. Fine for LIMIT, problematic for large N.",
          recommendation  = "If fetching large amounts of data, write to storage instead of collecting to the driver.",
          affectedJobs    = sql.jobIds,
          estimatedImpact = None,
        )
      }
    }

    issues.toSeq
  }
}
