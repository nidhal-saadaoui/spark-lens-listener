# spark-lens-testing

Write ScalaTest specs that assert on spark-lens analysis results — performance contracts that catch regressions in CI before they hit production.

---

## Installation

```scala
// build.sbt
libraryDependencies += "io.github.nidhal-saadaoui" %% "spark-lens-testing" % "LATEST_VERSION" % Test
```

---

## Quick start

```scala
import com.github.saadaouini.sparklens.testing.SparkLensSpec
import com.github.saadaouini.sparklens.model.Critical
import org.scalatest.BeforeAndAfterEach

class MyJobSpec extends SparkLensSpec with BeforeAndAfterEach {

  // Reset any Spark config changes made inside analyse blocks between tests
  override def afterEach(): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    super.afterEach()
  }

  "aggregation job" should "not spill to disk" in {
    analyse { MyJob.run(spark) } should not(haveIssueOfCategory("spill"))
  }

  "job health" should "stay above 75" in {
    analyse { MyJob.run(spark) } should haveHealthScoreAbove(75)
  }

  "no critical issues" should "be present" in {
    analyse { MyJob.run(spark) } should haveNoIssuesOfSeverity(Critical)
  }
}
```

Run with `sbt test` — no Spark cluster needed.

---

## How `analyse {}` works

1. A fresh `SparkAppModelBuilder` is registered as a scoped `SparkListener` on the shared `SparkSession`
2. The block runs — all Spark events (tasks, stages, jobs, SQL executions, plan trees) are captured
3. The async listener bus is drained before reading results
4. `builder.build()` produces a `SparkAppModel`; `Analyzers.runAll()` runs all 29 analyzers
5. A `SparkLensResult` is returned; the listener is removed
6. The `SparkSession` is shared across all tests in the suite — config changes inside a block persist unless you reset them (use `BeforeAndAfterEach`)

---

## Matchers

All matchers support `should` and `should not(...)`.

| Matcher | Passes when |
|---|---|
| `haveIssue(id)` | An issue with that exact id OR with id as prefix is present. `"plan-cartesian"` matches `"plan-cartesian-3"` |
| `haveIssueOfCategory(cat)` | Any issue with that category is present |
| `haveIssueOfSeverity(sev)` | Any issue at that severity is present |
| `haveNoIssuesOfSeverity(sev)` | No issue at that severity exists |
| `haveHealthScoreAbove(n)` | `healthScore > n` |
| `haveHealthScoreBelow(n)` | `healthScore < n` |

**Issue categories:** `spill`, `skew`, `join`, `gc`, `cache`, `plan`, `config`, `io`, `reliability`, `scaling`

**Severity values:** `Critical`, `Warning`, `Info` (imported from `com.github.saadaouini.sparklens.model`)

---

## Failure messages

When an assertion fails, the **full text report** is embedded in the failure output:

```
MyJobSpec > aggregation job should not spill to disk *** FAILED ***
Did not expect any issue of category 'spill' but one was present.

======================================================================
  spark-lens  |  MyJobSpec  (local-xxx)
  Spark 3.5.0   |  Health: 70/100  |  1 critical
======================================================================

[✖ CRITICAL]  Disk Spill in Stage 3 — 2.1 GB spilled
  fix:   spark.executor.memory=8g
       Stage 3 spilled 2.1 GB to disk. Disk I/O is 10–100× slower than memory.
  stages: 3

  ──────────────────────────────────────────────────────────────────
  Quick wins
    spark.executor.memory=8g
       resolves 1 issue: spill-3
======================================================================
```

Access the report at any time for debugging:

```scala
val result = analyse { MyJob.run(spark) }
println(result.textReport)
```

---

## SparkLensResult accessors

```scala
val result = analyse { MyJob.run(spark) }

result.healthScore                         // Int (0–100)
result.issues                              // Seq[Issue]
result.hasIssue("plan-cartesian")          // Boolean
result.hasIssueOfCategory("spill")         // Boolean
result.hasIssueOfSeverity(Critical)        // Boolean
result.issuesOfCategory("spill")           // Seq[Issue]
result.issuesOfSeverity(Warning)           // Seq[Issue]
result.textReport                          // String — full text report
```

---

## FunSuite style

Use `SparkLensSuite` if you prefer `AnyFunSuite`:

```scala
import com.github.saadaouini.sparklens.testing.SparkLensSuite

class MyJobSuite extends SparkLensSuite {
  test("no spill") {
    analyse { MyJob.run(spark) } should not(haveIssueOfCategory("spill"))
  }
  test("health above 75") {
    analyse { MyJob.run(spark) } should haveHealthScoreAbove(75)
  }
}
```

---

## Size and time helpers

```scala
result.metrics("shuffleBytes").toLong should be < (500L).MB
result.metrics("durationMs").toLong   should be < (2L).minutes
```

| Helper | Value |
|---|---|
| `n.GB` | n × 1,073,741,824 |
| `n.MB` | n × 1,048,576 |
| `n.KB` | n × 1,024 |
| `n.minutes` | n × 60,000 ms |
| `n.seconds` | n × 1,000 ms |

---

## Config isolation between tests

Spark config changes made inside `analyse {}` persist on the shared `SparkSession`. Use `BeforeAndAfterEach` to reset them:

```scala
class MyJobSpec extends SparkLensSpec with BeforeAndAfterEach {
  override def afterEach(): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    super.afterEach()
  }
}
```

---

## JVM requirement

Spark 3.5 / Hadoop 3.3.4 use `Subject.getSubject()` which was removed in Java 21+. `spark-lens-testing` works on **JVM < 23**.

When sbt runs on JVM ≥ 23, `build.sbt` automatically detects a Java 17 installation for the forked test JVM. Discovery order:

1. `JAVA_17_HOME` environment variable
2. `/opt/homebrew/opt/openjdk@17` (macOS Homebrew)
3. `/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home`
4. `/usr/lib/jvm/java-17-openjdk-amd64` (Ubuntu/Debian)
5. `/usr/lib/jvm/java-17-openjdk` (RHEL/CentOS)

Set `JAVA_17_HOME` explicitly if your Java 17 is in a non-standard location.
