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
}
