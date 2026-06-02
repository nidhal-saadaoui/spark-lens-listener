package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

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
          id              = s"plan-cartesian-${sql.executionId}",
          severity        = Critical,
          category        = "plan",
          title           = s"""Cartesian Product in "${desc.take(80)}"""",
          description     = "A CartesianProduct node was found in the physical plan. This produces M×N rows and scales quadratically with dataset size.",
          recommendation  = "Add an explicit join condition. If a cross join is intentional on small data, use df1.crossJoin(df2) to make the intent clear and ensure both sides are small.",
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      // Text-based detection: for unpartitioned Window the child Exchange SinglePartition
      // appears AFTER "Window" in the tree section; for partitioned windows the
      // SinglePartition exchange belongs to the outer aggregation and appears BEFORE.
      val windowTextFires: Boolean = if (plan.contains("Window")) {
        val treePlan = {
          val sep = plan.indexOf("\n\n(")
          if (sep > 0) plan.substring(0, sep) else plan
        }
        val wIdx = treePlan.indexOf("Window")
        wIdx >= 0 && treePlan.indexOf("SinglePartition", wIdx) >= 0
      } else false

      // Plan-tree fallback: Window without PARTITION BY has Exchange(SinglePartition) as a
      // DESCENDANT of the Window node in the SparkPlanInfo tree.  For partitioned windows
      // the SinglePartition exchange is an ancestor (outer agg), not a descendant.
      val windowTreeFires: Boolean = sql.planTree.exists { tree =>
        tree.nodesNamed("Window").exists { w =>
          w.flatten.exists(n => n.simpleString.contains("SinglePartition"))
        }
      }

      val windowFires = windowTextFires || windowTreeFires
      if (windowFires) {
        issues += Issue(
          id              = s"plan-window-nopart-${sql.executionId}",
          severity        = Warning,
          category        = "plan",
          title           = s"""Window Function Without PARTITION BY in "${desc.take(80)}"""",
          description     = "A window function runs without a PARTITION BY clause, forcing all data to a single partition — effectively serial execution.",
          recommendation  = "Add PARTITION BY <column> to your window spec to distribute computation across partitions.",
          codeFix         = Some("Window.partitionBy(\"user_id\").orderBy(\"timestamp\")"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      if (plan.contains("RoundRobinPartitioning")) {
        issues += Issue(
          id              = s"plan-roundrobin-${sql.executionId}",
          severity        = Info,
          category        = "plan",
          title           = s"""Round-Robin Repartition in "${desc.take(80)}"""",
          description     = "repartition(N) without a column triggers a full shuffle with round-robin assignment. This is rarely necessary.",
          recommendation  = "Use repartition(N, joinKey) to co-locate data by key, or coalesce(N) to reduce partitions without a full shuffle.",
          codeFix         = Some("df.repartition(200, col(\"join_key\"))"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      if (plan.contains("Statistics(sizeInBytes=") &&
          !plan.contains("rowCount=") &&
          plan.contains("SortMergeJoin")) {
        issues += Issue(
          id              = s"plan-nocbo-${sql.executionId}",
          severity        = Info,
          category        = "plan",
          title           = s"""Missing Row Count Statistics — CBO Cannot Optimize "${desc.take(80)}"""",
          description     = "The query optimizer lacks row count statistics, preventing cost-based join reordering and broadcast decisions.",
          recommendation  = "Run ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS for tables involved in the join.",
          codeFix         = Some("spark.sql(\"ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS\")"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      issues.toSeq
    }
}
