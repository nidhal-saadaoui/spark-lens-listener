# Output Formats

SparkLens supports four output formats. Use `spark.sparklens.output` with a comma-separated list to activate one or more simultaneously.

```
--conf spark.sparklens.output=text,json
--conf spark.sparklens.report.path=/tmp/reports/{app_id}
```

When multiple formats share a base path, each gets its own extension (`.txt`, `.json`, `.html`, `.log`). A format-specific path always takes priority.

---

## Path resolution order

For each active format, the path is resolved as:

1. `spark.sparklens.report.path.<format>` (e.g., `spark.sparklens.report.path.json`)
2. `spark.sparklens.report.path` + `.{ext}` (when multiple formats are active)
3. `spark.sparklens.report.path` as-is (when only one format is active — backward-compatible)
4. No path configured:
   - `text`, `json`, `html` → throws `IllegalArgumentException` at app start
   - `log` → routes through SLF4J to driver log appenders (see [log format](#log) below)

**Remote paths** (HDFS, S3, GCS) are supported for all formats via Hadoop FileSystem:
```
--conf spark.sparklens.report.path=hdfs:///user/spark/reports/{app_id}
--conf spark.sparklens.report.path.json=s3a://my-bucket/reports/{app_id}.json
```

---

## Path placeholder tokens

Placeholders are resolved at `onApplicationEnd` when the full application model is available. Both `{token}` and `${token}` forms work.

| Token | Example value |
|---|---|
| `{app_id}` | `application_1748959200_0001` |
| `{app_name}` | `My_ETL_Job` (spaces → underscores) |
| `{spark_version}` | `3_5_1` (dots/dashes → underscores) |
| `{date}` | `2026-06-03` (UTC) |
| `{timestamp}` | `1748959200` (Unix epoch seconds) |

---

## text

A human-readable report written to a file (or HDFS/S3). Designed for quick review in a terminal or log viewer.

**Structure:**
1. Header bar — app name, app ID, Spark version, duration, health score, severity counts
2. Priority fixes — top 20 issues sorted by estimated savings (configurable via `spark.sparklens.report.maxPriorityFixes`)
3. Issue list — all issues with fix, description, config/code fix, affected stages, and root-cause notes
4. Quick wins — config properties grouped to show "one change resolves N issues"

**Example header:**
```
======================================================================
  spark-lens  |  My ETL Job  (application_1748959200_0001)
  Spark 3.5.0  |  Duration: 24.5min  |  Health: 6/100  |  1 critical  ·  5 warning  ·  7 info
======================================================================
```

**Example issue:**
```
  [✖ CRITICAL]  Disk Spill in Stage 1 (insertInto…)  (~1.5min per run)
    fix:   spark.sql.adaptive.enabled=true
           # or: spark.executor.memory=36g
       Stage spilled 17.8 GB to disk and 90.4 GB to memory across 1 task.
    stages: 1
    note:   likely shares root cause with: single-task, cpu, low-parallelism
```

---

## json

Machine-readable JSON. All issues are included (the quiet-info filter used in text output does not apply). Suitable for dashboards, alerting pipelines, or programmatic processing.

**Top-level structure:**
```json
{
  "spark_lens_version": "LATEST_VERSION",
  "app_id": "application_1748959200_0001",
  "app_name": "My ETL Job",
  "spark_version": "3.5.0",
  "duration_ms": 1470000,
  "health_score": 6,
  "issue_count": 13,
  "total_estimated_savings_ms": 54000,
  "top_actions": [
    {
      "config": "spark.sql.adaptive.enabled=true",
      "resolves": ["spill", "config-aqe-disabled"],
      "estimated_savings_ms": 91000
    }
  ],
  "issues": [
    {
      "id": "spill-1",
      "severity": "critical",
      "category": "spill",
      "title": "Disk Spill in Stage 1 (insertInto…)",
      "description": "Stage spilled 17.8 GB to disk…",
      "recommendation": "Tasks averaged…",
      "fixes": {
        "config": "spark.sql.adaptive.enabled=true\n# or: spark.executor.memory=36g"
      },
      "affected_stages": [1],
      "metrics": {
        "disk_bytes": "19109486592",
        "avg_peak_mem": "5726623744"
      },
      "estimated_impact": {
        "summary": "~17.8 GB spilled to disk…",
        "saved_time_ms": 91000,
        "saved_bytes": 19109486592,
        "confidence": "medium"
      }
    }
  ]
}
```

Fields with `null` or zero values are omitted for clean output.

---

## html

A self-contained interactive HTML dashboard. No external assets or JavaScript — the SparkLens logo, all CSS, all SVG charts, and all content are embedded inline. Can be opened directly in a browser, archived, or attached as a CI artifact.

**Dashboard visualizations (SVG, self-contained, no JS):**
- **Metrics summary panel** — health score, critical/warning/info counts, total duration, peak memory, shuffle bytes, GC time
- **Stage timeline** — Gantt chart with color coding: red = GC > 10% of stage time, orange = disk spill, blue = normal
- **Memory pressure timeline** — line chart tracking executor peak memory evolution across the job
- **Shuffle metrics breakdown** — horizontal bars comparing input bytes vs shuffle-written bytes per stage
- **GC timeline** — bar chart of GC pause duration per stage, color-coded by impact (red > 20%, orange > 10%)
- **Issue severity timeline** — maps when critical (red) and warning (amber) issues occur across the job's timeline

**Issue cards:**
- Color-coded expandable cards per issue (open by default)
- Severity badge, title, estimated savings badge, description, recommendation
- Config/code fix blocks, metrics table, stage/job pills with callsite tooltips

Useful for sharing reports with team members, attaching to incident tickets, or archiving in CI.

---

## log

Structured single-line key=value output. Designed for ingestion by log aggregation tools — Splunk, Datadog, CloudWatch, ELK.

**Summary line:**
```
[spark-lens] SUMMARY  app="My ETL Job" app_id=application_1748959200_0001 spark=3.5.0 health=6 issues=13 critical=1 warning=5 info=7 duration=24.5min savings=54.0s/run
```

**Per-issue lines:**
```
[spark-lens] CRITICAL id=spill-1 category=spill title="Disk Spill in Stage 1" savings=1.5min/run fix="spark.sql.adaptive.enabled=true" disk_bytes=19109486592 avg_peak_mem=5726623744 stages=1
[spark-lens] WARNING  id=single-task-1 category=io title="Single-Task Stage 1 — Entire Stage Runs on One Executor" savings=16.6min/run stages=1
```

**Routing when no path is configured:**

The `log` format is the only one that works without a path. At application end:
1. If log4j2 `LoggerContext` is still `STARTED` → routes through SLF4J so all configured appenders (console, Splunk TCP, Datadog forwarder, etc.) receive the lines
2. If log4j2 is already in its shutdown sequence → falls back to `System.err` (always captured by YARN/K8s/Databricks regardless of logging framework state)

Lines are emitted at `WARN` level for Critical/Warning issues and `INFO` level for Info issues, making them filterable by severity in your log aggregation tool.

**Grepping log output:**
```bash
grep "\[spark-lens\]" application.log
grep "\[spark-lens\] CRITICAL" application.log
```

---

## fail.on — CI/CD integration

Use `spark.sparklens.fail.on` to throw at application end if issues above a threshold severity are found. The job still completes normally; SparkLens throws only after all analysis is done.

```
--conf spark.sparklens.fail.on=critical
```

| Value | Throws if |
|---|---|
| `critical` | Any Critical issues found |
| `warning` | Any Critical or Warning issues found |
| `info` | Any issues found at all |
| `none` or unset | Never throws |

The thrown exception is a `RuntimeException` with a summary of up to 5 blocking issues and a suggestion to enable text output for the full report. This integrates cleanly with CI pipelines that check exit codes.
