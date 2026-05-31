package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

// All checks below match against Spark's FORMATTED physical plan description (Spark 3.x+).
// Node type names (CartesianProduct, SortMergeJoin, etc.) are stable identifiers in the
// Catalyst plan tree and unlikely to change. The CBO statistics strings ("Statistics(sizeInBytes=",
// "rowCount=") and the FORMATTED layout used for window detection (the "\n\n(" detail separator)
// are presentation-layer strings tied to Spark's explain format — fragile across Spark versions.
object PlanAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.sqlExecutions.values.toSeq.flatMap { sql =>
      val plan   = sql.physicalPlan
      val desc   = sql.description
      val issues = scala.collection.mutable.ArrayBuffer[Issue]()

      if (plan.contains("CartesianProduct")) {
        issues += Issue(
          id             = s"plan-cartesian-${sql.executionId}",
          severity       = Critical,
          category       = "plan",
          title          = s"""Cartesian Product in "${desc.take(80)}"""",
          description    = "A CartesianProduct node was found in the physical plan. This produces M×N rows and scales quadratically with dataset size.",
          recommendation = "Add an explicit join condition. If a cross join is intentional on small data, use df1.crossJoin(df2) to make the intent clear and ensure both sides are small.",
          affectedJobs   = sql.jobIds,
        )
      }

      // Detect unpartitioned window using the FORMATTED plan layout.
      // Spark's physicalPlanDescription uses ExplainMode.FORMATTED which has two sections:
      //   1. Tree (root→leaf): just node names and nesting
      //   2. Detailed (leaf→root by ascending node ID): each node with Arguments
      // An unpartitioned window's Exchange SinglePartition is a *child* of the Window node,
      // so it has a lower node ID and appears BEFORE Window in the detailed section.
      // A partitioned window's Exchange SinglePartition is a *parent* (outer aggregation),
      // so it has a higher node ID and appears AFTER Window in the detailed section.
      // For simple plan text (unit tests), fall back to indexOf ordering in the tree.
      val windowFires: Boolean = if (plan.contains("Window")) {
        val detailStart = plan.indexOf("\n\n(")
        if (detailStart < 0) {
          // Simple format: tree order matches execution order; SinglePartition after Window = child
          val wIdx = plan.indexOf("Window")
          wIdx >= 0 && plan.indexOf("SinglePartition", wIdx) >= 0
        } else {
          val detail = plan.substring(detailStart)
          val windowInDetail = detail.indexOf(") Window\n")
          if (windowInDetail < 0) false
          else {
            val spInDetail = detail.indexOf("SinglePartition")
            spInDetail >= 0 && spInDetail < windowInDetail
          }
        }
      } else false
      if (windowFires) {
        issues += Issue(
          id             = s"plan-window-nopart-${sql.executionId}",
          severity       = Warning,
          category       = "plan",
          title          = s"""Window Function Without PARTITION BY in "${desc.take(80)}"""",
          description    = "A window function runs without a PARTITION BY clause, forcing all data to a single partition — effectively serial execution.",
          recommendation = "Add PARTITION BY <column> to your window spec to distribute computation across partitions.",
          codeFix        = Some("Window.partitionBy(\"user_id\").orderBy(\"timestamp\")"),
          affectedJobs   = sql.jobIds,
        )
      }

      // Round-robin repartition (RoundRobinPartitioning) wastes a full shuffle
      if (plan.contains("RoundRobinPartitioning")) {
        issues += Issue(
          id             = s"plan-roundrobin-${sql.executionId}",
          severity       = Info,
          category       = "plan",
          title          = s"""Round-Robin Repartition in "${desc.take(80)}"""",
          description    = "repartition(N) without a column triggers a full shuffle with round-robin assignment. This is rarely necessary.",
          recommendation = "Use repartition(N, joinKey) to co-locate data by key, or coalesce(N) to reduce partitions without a full shuffle.",
          codeFix        = Some("df.repartition(200, col(\"join_key\"))"),
          affectedJobs   = sql.jobIds,
        )
      }

      // fragile: "Statistics(sizeInBytes=" and "rowCount=" are explain-output strings, not node names
      if (plan.contains("Statistics(sizeInBytes=") &&
          plan.contains("rowCount=") == false &&
          plan.contains("SortMergeJoin")) {
        issues += Issue(
          id             = s"plan-nocbo-${sql.executionId}",
          severity       = Info,
          category       = "plan",
          title          = s"""Missing Row Count Statistics — CBO Cannot Optimize "${desc.take(80)}"""",
          description    = "The query optimizer lacks row count statistics, preventing cost-based join reordering and broadcast decisions.",
          recommendation = "Run ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS for tables involved in the join.",
          codeFix        = Some("spark.sql(\"ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS\")"),
          affectedJobs   = sql.jobIds,
        )
      }

      issues.toSeq
    }
}
