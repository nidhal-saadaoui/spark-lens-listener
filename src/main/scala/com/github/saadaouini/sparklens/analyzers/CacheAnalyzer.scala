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

    rddToJobs.toSeq.collect {
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
  }
}
