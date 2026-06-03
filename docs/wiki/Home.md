# SparkLens Listener

**Zero-config Spark performance analyzer.** Attach via `spark.extraListeners` and get actionable recommendations at the end of every job — no code changes, no Python, no extra services.

---

## What it does

SparkLens attaches to any Spark application as a `SparkListener`. It observes every event (tasks, stages, jobs, SQL executions) as your job runs, builds a complete application model, and at `onApplicationEnd` runs **28 analyzers** that detect performance anti-patterns and emit prioritized, fix-ready issues.

Issues are classified by severity (Critical / Warning / Info), grouped by type, ranked by estimated wall-clock savings, and written to your chosen output format (text, JSON, HTML, or structured log lines).

---

## Quick start

### Maven / SBT (Spark 3.x, Java 8+)

```bash
# spark-submit with --packages (downloads from Maven Central)
spark-submit \
  --packages io.github.nidhal-saadaoui:spark-lens_2.12:1.3.0 \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  --conf spark.sparklens.report.path=/tmp/report.txt \
  myJob.jar

# Or with a pre-built fat JAR
spark-submit \
  --driver-class-path spark-lens_2.12-1.3.0-assembly.jar \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  --conf spark.sparklens.report.path=/tmp/report.txt \
  myJob.jar
```

### Minimal configuration

| Property | Required | Description |
|---|---|---|
| `spark.extraListeners` | yes | Must include `com.github.saadaouini.sparklens.SparkLensListener` |
| `spark.sparklens.output` | yes | `text`, `json`, `html`, `log`, or comma-separated list |
| `spark.sparklens.report.path` | yes (for text/json/html) | Path to write the report |

---

## Compatibility

| | |
|---|---|
| Spark | 2.4 – 3.5+ |
| Scala | 2.12, 2.13 |
| Java | 8+ |
| Platforms | YARN, Kubernetes, Databricks, EMR, local |

> **Note:** The `config-aqe-disabled` and AQE-related checks only fire on Spark 3.x, where AQE exists. On Spark 2.x those checks are silently skipped.

---

## Wiki pages

| Page | Contents |
|---|---|
| [Configuration Reference](Configuration-Reference) | All 66+ `spark.sparklens.*` properties with defaults |
| [Analyzer Reference](Analyzer-Reference) | All 28 analyzers — what they detect and how to fix |
| [Output Formats](Output-Formats) | text, json, html, log formats; path config; placeholder tokens |
| [Deployment Guide](Deployment-Guide) | YARN, Kubernetes, Databricks, EMR, local mode |
| [Interpreting the Report](Interpreting-the-Report) | Health score, priority fixes, savings estimates, root cause linking |
| [Troubleshooting](Troubleshooting) | Common issues and how to diagnose them |

---

## How it works

```
SparkLensListener  →  SparkAppModelBuilder  →  SparkAppModel
      (events)          (mutable, live)         (immutable snapshot)
                                                       ↓
                                              Analyzers.runAll()
                                               (28 analyzers)
                                                       ↓
                                          Reporter (text/json/html/log)
```

SQL plan events arrive via `onOtherEvent` because the SQL module classes live outside `spark-core`. The listener captures `SparkPlanInfo` trees from `SparkListenerSQLExecutionStart` and resolves per-partition Exchange metrics at `SparkListenerSQLExecutionEnd`, giving analyzers per-partition byte counts for skew detection.

---

## Building from source

```bash
# Run all tests (Scala 2.12)
sbt test

# Cross-build and test both Scala versions
sbt "+test"

# Build fat assembly JAR
sbt "++2.12.20 assembly"
# Output: target/scala-2.12/spark-lens_2.12-<version>-assembly.jar
```
