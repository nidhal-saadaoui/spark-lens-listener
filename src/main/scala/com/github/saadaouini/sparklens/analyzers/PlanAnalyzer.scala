package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

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

      // SinglePartition in the Exchange node means all rows are sent to one executor
      if (plan.contains("Window") && plan.contains("SinglePartition")) {
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

      // Missing CBO statistics
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
