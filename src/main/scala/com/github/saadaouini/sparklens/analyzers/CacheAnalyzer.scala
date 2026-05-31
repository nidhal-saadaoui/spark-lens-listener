package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object CacheAnalyzer extends Analyzer {

  // Spark-internal RDD class names that are infrastructure, not user datasets.
  // "Python" covers PySpark's PythonRDD wrapper — it's an execution vehicle, not a dataset.
  private val InternalPrefixes = Set(
    "Map", "Parallel", "Shuffled", "HadoopRDD", "FileScan", "SQLExecution",
    "WholeStage", "Exchange", "Filter", "Project", "Scan", "Union", "Coalesced",
    "Zipped", "Python",
  )

  private def isInternal(name: String): Boolean =
    InternalPrefixes.exists(name.startsWith)

  def analyze(app: SparkAppModel): Seq[Issue] = {
    // Map each RDD name → set of job IDs that scanned it
    val rddToJobs = scala.collection.mutable.Map[String, Set[Int]]()

    for {
      (jobId, job) <- app.jobs
      stageId      <- job.stageIds
      stage        <- app.stages.get(stageId)
      rddName      <- stage.rddNames
      if rddName.nonEmpty && !isInternal(rddName)
    } {
      rddToJobs(rddName) = rddToJobs.getOrElse(rddName, Set.empty) + jobId
    }

    // Collect all RDD names that have caching configured across all stages — if the
    // user already called .cache()/.persist(), isCached is true and we suppress the issue.
    val cachedRddNames: Set[String] = app.stages.values.flatMap(_.rddCachedNames).toSet

    val rddIssues = rddToJobs.toSeq.collect {
      case (rddName, jobs) if jobs.size >= 2 && !cachedRddNames.contains(rddName) =>
        val sortedJobs = jobs.toSeq.sorted
        val jobNames   = sortedJobs.flatMap(id => app.jobs.get(id).map(j => s"'${j.name.take(60)}'"))
                                   .take(3).mkString(", ")
        Issue(
          id             = s"cache-${rddName.hashCode.abs}",
          severity       = Warning,
          category       = "cache",
          title          = s"Repeated Scan of '$rddName' Across ${jobs.size} Jobs Without Caching",
          description    = s"The dataset '$rddName' is read from scratch in ${jobs.size} jobs ($jobNames). Each job re-executes the full upstream lineage — reading source files, applying filters, and transforming data — when the result could be reused.",
          recommendation = "Call .cache() or .persist(StorageLevel.MEMORY_AND_DISK) before the first action, then .unpersist() when done. Use MEMORY_AND_DISK instead of MEMORY_ONLY to avoid re-computation if an executor is lost.",
          codeFix        = Some("val cached = df.persist(StorageLevel.MEMORY_AND_DISK)\ncached.count()  // materialise\n// ... use cached for subsequent jobs ...\ncached.unpersist()"),
          affectedJobs   = sortedJobs,
          metrics        = Map("job_count" -> jobs.size.toString, "rdd_name" -> rddName),
        )
    }

    // ── DataFrame / SQL repeated-scan detection ───────────────────────────────
    // When users call df.cache() Spark inserts InMemoryRelation into the plan.
    // If the same table appears in ≥ 3 SQL executions without InMemoryRelation,
    // the user is re-reading it from source every time.
    // Threshold 3 (not 2) avoids false positives for the common pattern of reading
    // a table twice in one pipeline (e.g. count then aggregate).
    val sqlIssues: Seq[Issue] = if (app.sqlExecutions.isEmpty) Nil else {
      val FileScanRe = """FileScan \w+ ([\w.`/]+)\[""".r

      // If a table appears under InMemoryRelation in ANY execution, df.cache() was called
      // on it somewhere — suppress that table globally across all executions.
      val cachedTableNames: Set[String] = app.sqlExecutions.values
        .filter(_.physicalPlan.contains("InMemoryRelation"))
        .flatMap(sql => FileScanRe.findAllMatchIn(sql.physicalPlan).map(_.group(1)))
        .toSet

      val tableCount = scala.collection.mutable.Map[String, Int]()
      app.sqlExecutions.values.foreach { sql =>
        FileScanRe.findAllMatchIn(sql.physicalPlan)
          .map(_.group(1))
          .toSet                                  // count each table once per execution
          .filterNot(cachedTableNames.contains)   // skip tables that are cached anywhere
          .foreach(t => tableCount(t) = tableCount.getOrElse(t, 0) + 1)
      }

      tableCount.toSeq.collect {
        case (table, count) if count >= 3 =>
          Issue(
            id             = s"cache-sql-${table.hashCode.abs}",
            severity       = Warning,
            category       = "cache",
            title          = s"Repeated Scan of '$table' Across $count SQL Executions Without Caching",
            description    = s"The table '$table' is read from source in $count separate SQL executions. Each execution re-reads the full dataset from storage when the result could be cached in memory.",
            recommendation = "Cache the DataFrame before the first action and unpersist when done.",
            codeFix        = Some("val cached = df.cache()\ncached.count()  // materialise\n// ... reuse cached ...\ncached.unpersist()"),
            metrics        = Map("execution_count" -> count.toString, "table" -> table),
          )
      }
    }

    rddIssues ++ sqlIssues
  }
}
