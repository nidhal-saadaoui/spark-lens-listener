package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Info, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CacheAnalyzerSpec extends AnyFlatSpec with Matchers {

  "CacheAnalyzer" should "return no issues for an empty app" in {
    CacheAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when an RDD appears in only one job" in {
    val s0 = stage(stageId = 0, rddNames = Seq("ParquetScan"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    CacheAnalyzer.analyze(app(stages = Map(0 -> s0), jobs = Map(0 -> j0))) shouldBe empty
  }

  it should "flag an RDD scanned in two different jobs" in {
    val s0 = stage(stageId = 0, rddNames = Seq("events"))
    val s1 = stage(stageId = 1, rddNames = Seq("events"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val issues = CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    ))
    val cacheIssues = issues.filter(_.category == "cache")
    cacheIssues should have size 1
    cacheIssues.head.severity shouldBe Warning
  }

  it should "include both job IDs in affected jobs" in {
    val s0 = stage(stageId = 0, rddNames = Seq("orders"))
    val s1 = stage(stageId = 1, rddNames = Seq("orders"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val issues = CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    ))
    issues.head.affectedJobs.sorted shouldBe Seq(0, 1)
  }

  it should "flag an RDD scanned in three jobs once with all three jobs listed" in {
    val stages = (0 to 2).map(i => i -> stage(stageId = i, rddNames = Seq("users"))).toMap
    val jobs   = (0 to 2).map(i => i -> job(jobId = i, stageIds = Seq(i))).toMap
    val issues = CacheAnalyzer.analyze(app(stages = stages, jobs = jobs))
    val cacheIssues = issues.filter(_.category == "cache")
    cacheIssues should have size 1
    cacheIssues.head.affectedJobs.sorted shouldBe Seq(0, 1, 2)
    cacheIssues.head.metrics("job_count") shouldBe "3"
  }

  it should "not flag different RDD names" in {
    val s0 = stage(stageId = 0, rddNames = Seq("users"))
    val s1 = stage(stageId = 1, rddNames = Seq("products"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  it should "skip internal Map-prefixed RDD names" in {
    val s0 = stage(stageId = 0, rddNames = Seq("MapPartitionsRDD"))
    val s1 = stage(stageId = 1, rddNames = Seq("MapPartitionsRDD"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  it should "not flag an RDD that is already cached" in {
    // rddCachedNames populated because user called .cache() — suppress the issue
    val s0 = stage(stageId = 0, rddNames = Seq("events"), rddCachedNames = Set("events"))
    val s1 = stage(stageId = 1, rddNames = Seq("events"), rddCachedNames = Set("events"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  it should "skip PySpark PythonRDD as an internal infrastructure name" in {
    val s0 = stage(stageId = 0, rddNames = Seq("PythonRDD"))
    val s1 = stage(stageId = 1, rddNames = Seq("PythonRDD"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  // ── DataFrame / SQL repeated-scan detection ─────────────────────────────

  it should "flag a table scanned in 5+ SQL executions without InMemoryRelation" in {
    val plan = "FileScan parquet default.orders[id#1L,amount#2] ..."
    val execs = (0 to 4).map(i => i.toLong -> sqlExec(id = i.toLong, plan = plan)).toMap
    val issues = CacheAnalyzer.analyze(app(sqlExecs = execs))
    val cacheIssues = issues.filter(_.category == "cache")
    cacheIssues should have size 1
    cacheIssues.head.title should include("default.orders")
    cacheIssues.head.metrics("execution_count") shouldBe "5"
  }

  it should "not flag a table scanned in only 4 SQL executions (below default threshold)" in {
    val plan = "FileScan parquet default.orders[id#1L] ..."
    val execs = (0 to 3).map(i => i.toLong -> sqlExec(id = i.toLong, plan = plan)).toMap
    CacheAnalyzer.analyze(app(sqlExecs = execs)).filter(_.category == "cache") shouldBe empty
  }

  it should "flag a spark_catalog-qualified table name with minExecCount=2 (Spark 3.5 integration)" in {
    // Spark 3.5.5 plan format: table name is 'spark_catalog.default.tableName'
    val countPlan = "*(2) HashAggregate(keys=[], functions=[count(1)])\n" +
      "+- *(1) ColumnarToRow\n" +
      "   +- FileScan parquet spark_catalog.default.user_events[] Batched: true, ..."
    val sumPlan   = "*(2) HashAggregate(keys=[], functions=[sum(val#8L)])\n" +
      "+- *(1) ColumnarToRow\n" +
      "   +- FileScan parquet spark_catalog.default.user_events[val#8L] Batched: true, ..."
    val execs = Map(0L -> sqlExec(id=0L, plan=countPlan), 1L -> sqlExec(id=1L, plan=sumPlan))
    val issues = CacheAnalyzer.analyze(app(
      sqlExecs = execs,
      props    = Map("spark.sparklens.cache.sql.minExecCount" -> "2"),
    ))
    issues.filter(_.id.startsWith("cache-sql")) should have size 1
  }

  it should "fire at a custom sql.minExecCount threshold" in {
    val plan = "FileScan parquet default.events[id#1L] ..."
    val execs = (0 to 2).map(i => i.toLong -> sqlExec(id = i.toLong, plan = plan)).toMap
    val issues = CacheAnalyzer.analyze(app(
      sqlExecs = execs,
      props    = Map("spark.sparklens.cache.sql.minExecCount" -> "3"),
    ))
    issues.filter(_.id.startsWith("cache-sql")) should have size 1
  }

  it should "not flag SQL executions that contain InMemoryRelation" in {
    val cachedPlan   = "InMemoryRelation [...]\n   +- FileScan parquet default.events[...]"
    val uncachedPlan = "FileScan parquet default.events[...]"
    val execs = (0 to 5).map(i =>
      i.toLong -> sqlExec(id = i.toLong, plan = if (i == 0) cachedPlan else uncachedPlan)
    ).toMap
    // exec 0 has InMemoryRelation → table is cached → all executions suppressed
    CacheAnalyzer.analyze(app(sqlExecs = execs)).filter(_.id.startsWith("cache-sql")) shouldBe empty
  }

  it should "downgrade SQL cache issue to Info for large estimated table sizes" in {
    // Realistic structure: each SQL execution triggers its own job and stage.
    // Each stage reads 10 GB.  Average = (5 × 10 GB) / 5 = 10 GB > 5 GB threshold → Info.
    val plan      = "FileScan parquet default.big_table[id#1L] ..."
    val tenGb     = 10L * 1024L * 1024L * 1024L
    val bigStages = (0 to 4).map(i => i -> stage(stageId = i).copy(
      hasExactAggregates = true,
      exactInputBytes    = tenGb,
    )).toMap
    val bigJobs = (0 to 4).map(i => i -> job(jobId = i, stageIds = Seq(i))).toMap
    val execs   = (0 to 4).map(i =>
      i.toLong -> sqlExec(id = i.toLong, plan = plan, jobIds = Seq(i))
    ).toMap
    val issues = CacheAnalyzer.analyze(app(
      stages   = bigStages,
      jobs     = bigJobs,
      sqlExecs = execs,
    ))
    val sqlIssues = issues.filter(_.id.startsWith("cache-sql"))
    sqlIssues should have size 1
    sqlIssues.head.severity shouldBe Info   // downgraded from Warning — table too large to cache
  }

  it should "not double-count stages shared across SQL executions when estimating size" in {
    // If multiple SQL executions share the same job+stage (edge case), the deduplication
    // must ensure that stage's bytes are counted only once, not N times.
    val plan  = "FileScan parquet default.shared_table[id#1L] ..."
    val tenGb = 10L * 1024L * 1024L * 1024L
    // All 5 executions link to the same job 0 / stage 0 — only one real scan happened.
    val sharedStage = stage(stageId = 0).copy(hasExactAggregates = true, exactInputBytes = tenGb)
    val sharedJob   = job(jobId = 0, stageIds = Seq(0))
    val execs       = (0 to 4).map(i =>
      i.toLong -> sqlExec(id = i.toLong, plan = plan, jobIds = Seq(0))
    ).toMap
    val issues = CacheAnalyzer.analyze(app(
      stages   = Map(0 -> sharedStage),
      jobs     = Map(0 -> sharedJob),
      sqlExecs = execs,
    ))
    val sqlIssues = issues.filter(_.id.startsWith("cache-sql"))
    sqlIssues should have size 1
    // With deduplication: unique stage bytes = 10 GB; avg = 10 GB / 5 execs = 2 GB < 5 GB → Warning
    // Without deduplication: sum = 5 × 10 GB = 50 GB; avg = 10 GB → Info (wrong — table was read once)
    sqlIssues.head.severity shouldBe Warning
  }

  it should "flag the RDD name in the issue title" in {
    val s0 = stage(stageId = 0, rddNames = Seq("clickstream"))
    val s1 = stage(stageId = 1, rddNames = Seq("clickstream"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val issues = CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    ))
    issues.head.title should include("clickstream")
  }

  // ── PlanNode-based guards ─────────────────────────────────────────────────

  it should "not flag SQL WholeStageCodegen RDD names (*(N) prefix)" in {
    // After .cache().count(), Spark logs the plan description as the RDD name.
    // These start with "*(N)" — they must be treated as internal infrastructure.
    val planName = "*(1) Project [id#0L, concat_ws( , first_name#2) AS name#5]\n+- *(1) Range (0, 300000, step=1)"
    val s0 = stage(stageId = 0, rddNames = Seq(planName))
    val s1 = stage(stageId = 1, rddNames = Seq(planName))
    val s2 = stage(stageId = 2, rddNames = Seq(planName))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val j2 = job(jobId = 2, stageIds = Seq(2))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1, 2 -> s2),
      jobs   = Map(0 -> j0, 1 -> j1, 2 -> j2),
    )).filter(_.category == "cache") shouldBe empty
  }

  it should "not flag SQL repeated scans when planTree contains InMemoryRelation" in {
    // After df.cache(), Spark inserts InMemoryRelation into the plan tree.
    // The CacheAnalyzer must suppress the finding when planTree confirms caching is active.
    val plan = "FileScan parquet default.orders[id#1L,amount#2] ..."
    val inMemoryTree = planNode("InMemoryRelation",
      children = Seq(planNode("FileScan", accumIds = Nil)))
    val execs = (0 to 4).map(i =>
      i.toLong -> sqlExec(id = i.toLong, plan = plan, planTree = Some(inMemoryTree))
    ).toMap
    CacheAnalyzer.analyze(app(sqlExecs = execs)).filter(_.category == "cache") shouldBe empty
  }

  it should "still flag repeated scans when planTree has no InMemoryRelation" in {
    val plan = "FileScan parquet default.orders[id#1L,amount#2] ..."
    val bareTree = planNode("Project",
      children = Seq(planNode("FileScan")))
    val execs = (0 to 4).map(i =>
      i.toLong -> sqlExec(id = i.toLong, plan = plan, planTree = Some(bareTree))
    ).toMap
    val issues = CacheAnalyzer.analyze(app(sqlExecs = execs)).filter(_.category == "cache")
    issues should have size 1
    issues.head.title should include("default.orders")
  }

  it should "attach estimatedImpact with savedBytes to SQL cache issue" in {
    val plan      = "FileScan parquet default.events[id#1L] ..."
    val oneGb     = 1L * 1024L * 1024L * 1024L
    val bigStages = (0 to 4).map(i => i -> stage(stageId = i).copy(
      hasExactAggregates = true,
      exactInputBytes    = oneGb,
    )).toMap
    val bigJobs = (0 to 4).map(i => i -> job(jobId = i, stageIds = Seq(i))).toMap
    val execs   = (0 to 4).map(i =>
      i.toLong -> sqlExec(id = i.toLong, plan = plan, jobIds = Seq(i))
    ).toMap
    val issues = CacheAnalyzer.analyze(app(
      stages = bigStages, jobs = bigJobs, sqlExecs = execs,
    )).filter(_.id.startsWith("cache-sql"))
    issues should not be empty
    val imp = issues.head.estimatedImpact
    imp shouldBe defined
    imp.get.savedBytes.exists(_ > 0) shouldBe true
    imp.get.savedTimeMs.exists(_ > 0) shouldBe true
    imp.get.confidence shouldBe "medium"
  }
}
