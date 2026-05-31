package com.github.saadaouini.sparklens.analyzers

import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ShuffleLocalityAnalyzerSpec extends AnyFlatSpec with Matchers {

  "ShuffleLocalityAnalyzer" should "return no issues when shuffle is below threshold" in {
    val tasks = (1 to 5).map(_ => task(remoteShuffleBytes = 5L * MB, localShuffleBytes = 5L * MB))
    ShuffleLocalityAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when remote ratio is low" in {
    val tasks = (1 to 5).map(_ => task(
      remoteShuffleBytes = 20L * MB,
      localShuffleBytes  = 200L * MB,
    ))
    ShuffleLocalityAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "detect high remote shuffle ratio" in {
    val tasks = (1 to 5).map(_ => task(
      remoteShuffleBytes = 200L * MB,
      localShuffleBytes  = 10L * MB,
    ))
    val issues = ShuffleLocalityAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.metrics("remote_ratio").toDouble should be > 0.70
  }
}
