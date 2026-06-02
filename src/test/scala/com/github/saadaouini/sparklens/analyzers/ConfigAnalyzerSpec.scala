package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigAnalyzerSpec extends AnyFlatSpec with Matchers {

  "ConfigAnalyzer" should "flag AQE disabled by default" in {
    val issues = ConfigAnalyzer.analyze(app())
    issues.exists(_.id == "config-aqe-disabled") shouldBe true
  }

  it should "not flag AQE when enabled" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.sql.adaptive.enabled" -> "true")))
    issues.exists(_.id == "config-aqe-disabled") shouldBe false
  }

  it should "flag Java serializer" in {
    val issues = ConfigAnalyzer.analyze(app())
    issues.exists(_.id == "config-java-serializer") shouldBe true
  }

  it should "not flag Kryo serializer" in {
    val issues = ConfigAnalyzer.analyze(
      app(props = Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")))
    issues.exists(_.id == "config-java-serializer") shouldBe false
  }

  it should "flag default shuffle partitions when AQE is off" in {
    val issues = ConfigAnalyzer.analyze(
      app(props = Map("spark.sql.shuffle.partitions" -> "200")))
    issues.exists(_.id == "config-default-shuffle-partitions") shouldBe true
  }

  it should "not flag shuffle partitions when AQE is on" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map(
      "spark.sql.shuffle.partitions" -> "200",
      "spark.sql.adaptive.enabled"   -> "true",
    )))
    issues.exists(_.id == "config-default-shuffle-partitions") shouldBe false
  }

  it should "flag AQE skew join disabled when AQE is on" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map(
      "spark.sql.adaptive.enabled"          -> "true",
      "spark.sql.adaptive.skewJoin.enabled" -> "false",
    )))
    issues.exists(_.id == "config-aqe-skew-disabled") shouldBe true
  }

  it should "not flag skew join when AQE itself is off" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map(
      "spark.sql.adaptive.skewJoin.enabled" -> "false",
    )))
    issues.exists(_.id == "config-aqe-skew-disabled") shouldBe false
  }

  it should "flag low memory overhead when factor is below 0.15" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map(
      "spark.executor.memoryOverheadFactor" -> "0.1",
    )))
    issues.exists(_.id == "config-low-memory-overhead") shouldBe true
  }

  it should "not flag memory overhead when factor is adequate" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map(
      "spark.executor.memoryOverheadFactor" -> "0.2",
    )))
    issues.exists(_.id == "config-low-memory-overhead") shouldBe false
  }

  it should "not flag memory overhead when overhead is set explicitly" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map(
      "spark.executor.memoryOverhead" -> "1024",
    )))
    issues.exists(_.id == "config-low-memory-overhead") shouldBe false
  }

  it should "return all issues with Warning severity for AQE and serializer" in {
    val issues = ConfigAnalyzer.analyze(app())
    issues.filter(i => i.id == "config-aqe-disabled" || i.id == "config-java-serializer")
      .foreach(_.severity shouldBe Warning)
  }

  it should "attach low-confidence estimatedImpact to all config issues" in {
    val issues = ConfigAnalyzer.analyze(app())
    issues should not be empty
    issues.foreach { i =>
      i.estimatedImpact shouldBe defined
      i.estimatedImpact.get.confidence shouldBe "low"
      i.estimatedImpact.get.savedTimeMs shouldBe None
    }
  }

  // ── shuffle.file.buffer ───────────────────────────────────────────────────

  it should "flag small shuffle buffer when below 64k" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.shuffle.file.buffer" -> "32k")))
    issues.exists(_.id == "config-small-shuffle-buffer") shouldBe true
  }

  it should "not flag shuffle buffer when set to 1m" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.shuffle.file.buffer" -> "1m")))
    issues.exists(_.id == "config-small-shuffle-buffer") shouldBe false
  }

  // ── CBO histograms ────────────────────────────────────────────────────────

  it should "flag CBO histograms disabled by default" in {
    val issues = ConfigAnalyzer.analyze(app())
    issues.exists(_.id == "config-cbo-histogram-disabled") shouldBe true
  }

  it should "not flag CBO histograms when enabled" in {
    val issues = ConfigAnalyzer.analyze(
      app(props = Map("spark.sql.statistics.histogram.enabled" -> "true")))
    issues.exists(_.id == "config-cbo-histogram-disabled") shouldBe false
  }

  // ── task.maxFailures ──────────────────────────────────────────────────────

  it should "flag low task max failures when set to 2" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.task.maxFailures" -> "2")))
    issues.exists(_.id == "config-low-task-max-failures") shouldBe true
  }

  it should "not flag task max failures when set to 4 (default)" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.task.maxFailures" -> "4")))
    issues.exists(_.id == "config-low-task-max-failures") shouldBe false
  }

  // ── locality.wait ─────────────────────────────────────────────────────────

  it should "flag high locality wait when set to 10s" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.locality.wait" -> "10s")))
    issues.exists(_.id == "config-high-locality-wait") shouldBe true
  }

  it should "not flag locality wait when set to 1s" in {
    val issues = ConfigAnalyzer.analyze(app(props = Map("spark.locality.wait" -> "1s")))
    issues.exists(_.id == "config-high-locality-wait") shouldBe false
  }

  // ── reducer.maxReqsInFlight ───────────────────────────────────────────────

  it should "flag unlimited maxReqsInFlight on a large cluster" in {
    val execs = (0 until 25).map(i => i.toString -> executor().copy(totalCores = 10)).toMap
    // 250 total cores > 200 threshold
    val issues = ConfigAnalyzer.analyze(app(executors = execs))
    issues.exists(_.id == "config-max-reqs-in-flight") shouldBe true
  }

  it should "not flag maxReqsInFlight when explicitly capped" in {
    val execs = (0 until 25).map(i => i.toString -> executor().copy(totalCores = 10)).toMap
    val issues = ConfigAnalyzer.analyze(app(
      executors = execs,
      props     = Map("spark.reducer.maxReqsInFlight" -> "500"),
    ))
    issues.exists(_.id == "config-max-reqs-in-flight") shouldBe false
  }

  it should "not flag maxReqsInFlight on small clusters" in {
    // Only 1 executor × 4 cores = 4 total cores — well below threshold
    val issues = ConfigAnalyzer.analyze(app())
    issues.exists(_.id == "config-max-reqs-in-flight") shouldBe false
  }
}
