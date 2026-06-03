# Troubleshooting

---

## No report is generated

**Check 1: Is SparkLens attached?**

Look for this line at application start in the driver log:
```
spark-lens attached (output=text, path=/tmp/report.txt)
```
If absent, `spark.extraListeners` is either not set or the JAR is not on the driver classpath.

**Check 2: Is `spark.sparklens.output` set?**

The default value is `off`. Without an explicit output format, SparkLens observes the job silently and emits nothing.

**Check 3: Is the path configured?**

`text`, `json`, and `html` formats require a path. If no path is set, SparkLens throws at startup:
```
IllegalArgumentException: spark-lens: output=text requires a report path.
Set spark.sparklens.report.path or spark.sparklens.report.path.text.
```

---

## Report path exists but the file is empty

The report is written at `onApplicationEnd` — if the driver is killed before the application ends cleanly (SIGKILL, OOM, unhandled exception before SparkContext.stop()), the report is never written.

**YARN cluster mode:** The ApplicationMaster container exits before `onApplicationEnd` if the driver crashes. Use `--conf spark.sparklens.output=log` (no path) so lines appear in the application log even if the report file is never created.

**Databricks:** Jobs that exceed the maximum run time may be killed without `onApplicationEnd` firing. Use a shorter analysis window or rely on the JSON output of completed runs.

---

## `log` format without a path — lines missing from driver log

SparkLens routes `log` output through SLF4J, which goes through all configured log4j/logback appenders. Lines can be missing if:

1. **`onApplicationEnd` fires after log4j shuts down.** This is common on Databricks and some EMR configurations where Spark's shutdown is entangled with the JVM shutdown hook sequence. SparkLens falls back to `System.err` in this case — check whether lines appear in stderr.

2. **Logger namespace filtered.** Some cluster log configurations suppress `com.github.saadaouini.*` at the root level. Check your log4j configuration for a root-level filter or a specific exclusion.

3. **Wrong appender.** If you want lines in a file appender, configure `spark.sparklens.report.path.log` instead of relying on SLF4J routing.

---

## `log` format — duplicate lines

If you configure both `spark.sparklens.output=log` and a path, the report is written to the file only. If you rely on SLF4J routing (no path), lines appear once per appender. No duplication.

---

## High memory usage from SparkLens

SparkLens uses reservoir sampling to cap task data at 10,000 tasks per stage. For extremely wide jobs (thousands of stages, millions of tasks total), the aggregate memory footprint grows. It is proportional to:

```
stages × min(actualTasks, 10000) × (task metrics size ~200 bytes)
```

For a 1,000-stage job with 10,000+ tasks per stage, this is ~2 GB. If this is a problem, SparkLens cannot be tuned to use less — consider running it only on a representative subset of your workload.

---

## Analysis takes too long

The analysis runs synchronously in `onApplicationEnd` and typically completes in milliseconds. If it appears slow, it is most likely due to writing the report to a slow network path (S3 with high latency, HDFS under load). Use a local path for testing and an async path strategy for production.

---

## Report shows wrong Spark version

SparkLens reads `spark.version` from `SparkListenerEnvironmentUpdate`. If this event fires before the listener is registered (rare in older Spark versions), it falls back to the version from `SPARK_VERSION` constant baked into the Spark JAR on the driver.

Check `app.sparkVersion` in the JSON output — if it shows an unexpected value, the environment update event was missed.

---

## `config-aqe-disabled` fires on Spark 2.x

This check is guarded behind `majorVersion >= 3` — AQE does not exist in Spark 2.x and the check is silently skipped. If you see it firing on a Spark 2.x job, check `sparkVersion` in the report header. A Hadoop-distribution version string like `2.3.2.3.1.0.0-78` is correctly parsed as major version 2.

---

## `executor-memory-underprovisioned` fires with an extremely high recommendation

This happens when a single-task (`coalesce(1)`) stage processes the full dataset and its peak execution memory dominates the recommendation. SparkLens automatically excludes single-task stages from memory sizing when multi-task stages exist.

If the issue still shows a very high recommendation, all your long-running stages are single-task. Fix `coalesce(1)` / `repartition(1)` first — after that, the memory recommendation from the remaining parallel stages will be much more reasonable.

---

## `preemption-executor-lost` shows wrong fix advice

The fix recommendation is based on the executor removal reason. If the advice looks wrong, check the `reason` field in the JSON report:

- `"Executor killed by driver because it has been blacklisted"` → blacklisting fix
- `"heartbeat timeout"` → network timeout fix
- `"Container killed by YARN for exceeding memory limits"` → `memoryOverheadFactor` fix
- Anything else → generic preemption advice

If the removal reason is not captured (empty or `unknown`), SparkLens falls back to generic advice.

---

## JSON output has missing fields

Fields with `null` or zero values are omitted from JSON for clean output. Specifically:
- `estimated_impact.saved_time_ms` — omitted when savings are zero (config-only issues)
- `estimated_impact.saved_bytes` — omitted when no byte savings
- `fixes` — omitted when neither `configFix` nor `codeFix` is present
- `metrics` — omitted when the metrics map is empty

This is intentional. Use `!= null` checks in consumers rather than assuming all fields are present.

---

## Report path placeholder `{app_id}` resolves to `unknown`

`app_id` comes from `SparkListenerApplicationStart.appId`. In some configurations (local mode, unit tests), the application ID is not set and defaults to `"unknown"`. In production Spark on YARN/K8s this is always populated.

---

## Integration with existing `spark.extraListeners`

SparkLens can coexist with other listeners. Use a comma-separated list:
```
--conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener,com.example.MyListener
```

Order does not matter — all listeners receive the same events independently.

---

## Getting help

- **Issues and bug reports:** [github.com/nidhal-saadaoui/spark-lens-listener/issues](https://github.com/nidhal-saadaoui/spark-lens-listener/issues)
- **Docs site:** [nidhal-saadaoui.github.io/spark-lens-listener](https://nidhal-saadaoui.github.io/spark-lens-listener/)
