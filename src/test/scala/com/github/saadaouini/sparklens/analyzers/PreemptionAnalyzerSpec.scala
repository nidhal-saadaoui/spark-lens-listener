package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PreemptionAnalyzerSpec extends AnyFlatSpec with Matchers {

  "PreemptionAnalyzer" should "return no issues for an empty app" in {
    PreemptionAnalyzer.analyze(app()) shouldBe empty
  }

  it should "flag an executor removed with 'preempt' in the reason" in {
    val exc = executor("1", removedTimeMs = Some(500L), removalReason = Some("preempted by YARN"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("1" -> exc)))
    issues.exists(_.id == "preemption-executor-lost-0") shouldBe true
  }

  it should "flag an executor removed with 'kill' in the reason" in {
    val exc = executor("2", removedTimeMs = Some(500L), removalReason = Some("Container killed by NodeManager"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("2" -> exc)))
    issues.exists(_.id == "preemption-executor-lost-0") shouldBe true
  }

  it should "flag an executor removed with 'lost' in the reason" in {
    val exc = executor("3", removedTimeMs = Some(500L), removalReason = Some("executor lost"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("3" -> exc)))
    issues.exists(_.id == "preemption-executor-lost-0") shouldBe true
  }

  it should "not flag an executor that finished cleanly" in {
    val exc = executor("4", removedTimeMs = Some(1000L), removalReason = Some("finished successfully"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("4" -> exc)))
    issues.exists(_.id == "preemption-executor-lost-0") shouldBe false
  }

  it should "not flag an executor still running (no removed time)" in {
    val exc = executor("5", removedTimeMs = None, removalReason = Some("preempted"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("5" -> exc)))
    issues.exists(_.id == "preemption-executor-lost-0") shouldBe false
  }

  it should "give blacklisting advice when executor is killed by driver" in {
    val exc = executor("6", removedTimeMs = Some(500L), removalReason = Some("Executor killed by driver because it has been blacklisted"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("6" -> exc)))
    val issue = issues.find(_.id == "preemption-executor-lost-0").get
    issue.recommendation should include("blacklist")
    issue.configFix.get  should include("blacklist")
  }

  it should "give network advice when executor is lost via heartbeat timeout" in {
    val exc = executor("7", removedTimeMs = Some(500L), removalReason = Some("heartbeat timeout"))
    val issues = PreemptionAnalyzer.analyze(app(executors = Map("7" -> exc)))
    val issue = issues.find(_.id == "preemption-executor-lost-0").get
    issue.configFix.get should include("spark.network.timeout")
  }

  it should "report lost executor count in metrics" in {
    val excs = Map(
      "1" -> executor("1", removedTimeMs = Some(100L), removalReason = Some("lost")),
      "2" -> executor("2", removedTimeMs = Some(200L), removalReason = Some("lost")),
    )
    val issues = PreemptionAnalyzer.analyze(app(executors = excs))
    val lost = issues.find(_.id == "preemption-executor-lost-0").get
    lost.metrics("lost_executors") shouldBe "2"
  }

  it should "flag high killed task rate in a stage (>= 5%)" in {
    val tasks = (0 until 10).map(i => task(id = i, killed = i < 1)) // 10% killed
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = PreemptionAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.exists(_.id.startsWith("preemption-killed")) shouldBe true
    issues.find(_.id.startsWith("preemption-killed")).get.severity shouldBe Warning
  }

  it should "not flag killed task rate below 5%" in {
    // 0 killed out of 10 tasks
    val tasks = (0 until 10).map(i => task(id = i))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = PreemptionAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.exists(_.id.startsWith("preemption-killed")) shouldBe false
  }

  it should "not check kill rate for stages with fewer than 10 tasks" in {
    val tasks = (0 until 9).map(i => task(id = i, killed = i < 9)) // all killed, but < 10
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = PreemptionAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.exists(_.id.startsWith("preemption-killed")) shouldBe false
  }

  it should "not fire when kill rate is below a custom killedTaskRateWarn" in {
    // 7% kill rate — above default 5% but below custom 20%
    val tasks = (0 until 100).map(i => task(id = i, killed = i < 7))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.preemption.killedTaskRateWarn" -> "0.20"))
    PreemptionAnalyzer.analyze(a).exists(_.id.startsWith("preemption-killed")) shouldBe false
  }
}
