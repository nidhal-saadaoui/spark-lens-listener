# spark-lens-listener

Zero-config Spark performance analyzer. Attach via `spark.extraListeners` — get actionable recommendations in your logs at the end of every job. No Python, no extra services, no code changes.

## Usage

```bash
spark-submit \
  --packages com.github.saadaouini:spark-lens_2.12:0.1.0 \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  myJob.jar
```

That's it. At application end, a report appears in the driver stdout. To write to a dedicated file instead, set `spark.sparklens.report.path`.

## Sample report

Sample output with `output=text` and `report.path=/tmp/report.txt`:
```
======================================================================
  spark-lens  |  daily-user-aggregation  (app-20241105-0042)
  Spark 3.5.0   |  Health: 64/100  |  3 warning  ·  2 info
======================================================================

  [⚠ WARNING ]  Java Serializer in Use — Switch to Kryo
           Java serialization is 10× slower than Kryo and produces 2–10× larger byte
           arrays. Every JVM shuffle write, broadcast variable, and RDD persist pays
           this cost.
        →  Switch to Kryo for JVM workloads. Register your domain classes for maximum
           performance (unregistered classes fall back to class-name strings).
           config: spark.serializer=org.apache.spark.serializer.KryoSerializer

  [⚠ WARNING ]  Adaptive Query Execution (AQE) Is Disabled
           AQE automatically coalesces shuffle partitions, handles skew, and switches
           join strategies at runtime. Disabling it forces static planning.
        →  Enable AQE for all production workloads on Spark 3.x.
           config: spark.sql.adaptive.enabled=true

  [⚠ WARNING ]  Excessive Shuffles in "count at ..." (6 exchanges) [+2 more queries]
           The query plan contains 6 shuffle exchanges. Each exchange sorts and
           repartitions the entire dataset across the network.
        →  Restructure the query to reduce shuffles. Enable AQE so Spark can coalesce
           small post-shuffle partitions at runtime.
           config: spark.sql.adaptive.enabled=true
           jobs:   0, 1, 2

  [ℹ INFO    ]  Low Executor Memory Overhead — Risk of Off-Heap OOM
           Executor memory is 4g with only 10% overhead. Native memory usage (Python
           UDFs, Arrow, native libs) can exceed this.
        →  Set memoryOverhead to at least 10% of executor memory.
           config: spark.executor.memoryOverheadFactor=0.2

  [ℹ INFO    ]  Default Shuffle Partitions (200) — May Be Too Few or Too Many
        →  Enable AQE to auto-tune, or set to 2–3× the number of executor cores.
           config: spark.sql.adaptive.enabled=true

======================================================================
```

Issues of the same type across multiple stages or queries are merged into a single
entry — e.g. `Disk Spill in Stage 3 [+4 more stages]` — so the report stays readable
on large pipelines.

## What it detects

| Analyzer | Category | Signal |
|---|---|---|
| SkewAnalyzer | skew | p95/p50 task duration ratio > 3× or top-5% tasks hold > 25% of stage time |
| SpillAnalyzer | spill | Total disk spill > 100 MB |
| JoinAnalyzer | join | Broadcast disabled on SMJ, broadcast threshold > 1 GB, > 4 shuffle exchanges |
| GcAnalyzer | gc | GC time > 10% of executor run time |
| CacheAnalyzer | cache | Same RDD scanned in multiple jobs without caching |
| PreemptionAnalyzer | preemption | Executor lost mid-job, task kill rate > 5% |
| PlanAnalyzer | plan | CartesianProduct, Window without PARTITION BY, round-robin repartition, missing CBO stats |
| ConfigAnalyzer | config | AQE disabled, Java serializer, default shuffle partitions, low memory overhead |
| SmallFilesAnalyzer | io | Input avg < 64 MB/task with majority of tasks reading tiny files |
| OutputSmallFilesAnalyzer | io | Output avg < 64 MB/task — downstream jobs will hit the small-files problem |
| ShuffleLocalityAnalyzer | io | > 70% of shuffle bytes read remotely |
| DriverBottleneckAnalyzer | io | collect() result > 50 MB, CollectLimit or TakeOrderedAndProject in plan |
| CpuEfficiencyAnalyzer | io | CPU utilization < 20% of executor run time |
| SpeculationAnalyzer | config | Speculative tasks firing — masking skew rather than fixing it |
| StageFailureAnalyzer | reliability | Stage retried (attempt > 0), task failure rate > 5% |
| MemoryPressureAnalyzer | reliability | GC > 10% and disk spill > 100 MB co-occurring in the same stage |
| StageParallelismAnalyzer | io | Stage tasks < 50% of available executor cores on a stage > 10 s |
| LongStageAnalyzer | reliability | Stage duration > 5× the median stage duration in its job |

## Configuration

All settings are optional and prefixed with `spark.sparklens.*`:

| Property | Default | Values | Description |
|---|---|---|---|
| `spark.sparklens.output` | `off` | `off` `text` `json` `html` | Output format. `off` = silent unless `fail.on` is set |
| `spark.sparklens.report.path` | *(stdout)* | local path or `hdfs://...` | Write report to a file instead of stdout |
| `spark.sparklens.fail.on` | *(none)* | `critical` `warning` `info` | Throw at app end if issues at this severity or above are found |

## CI quality gate

Fail the Spark application itself if critical issues are found:

```bash
spark-submit \
  --packages com.github.saadaouini:spark-lens_2.12:0.1.0 \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.fail.on=critical \
  myJob.jar
```

Exit code is non-zero if critical issues are detected — CI pipeline fails automatically.

## Write a report file

```bash
--conf spark.sparklens.output=html \
--conf spark.sparklens.report.path=/tmp/spark-lens-report.html
```

```bash
--conf spark.sparklens.output=json \
--conf spark.sparklens.report.path=hdfs:///user/spark/reports/myapp.json
```

The JSON schema is stable and suitable for CI parsers or dashboards:

```json
{
  "app_id": "app-20241105-0042",
  "app_name": "daily-user-aggregation",
  "spark_version": "3.5.0",
  "health_score": 64,
  "issue_count": 5,
  "issues": [{
    "id": "config-aqe-disabled",
    "severity": "warning",
    "category": "config",
    "title": "Adaptive Query Execution (AQE) Is Disabled",
    "description": "...",
    "recommendation": "...",
    "config_fix": "spark.sql.adaptive.enabled=true",
    "code_fix": null,
    "affected_stages": [],
    "affected_jobs": [],
    "metrics": {}
  }]
}
```

## Permanent cluster configuration

Add to `spark-defaults.conf` on every node — every job gets analyzed automatically:

```properties
spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener
spark.sparklens.output=text
spark.sparklens.fail.on=critical
```

## Health score

| Severity | Deduction |
|---|---|
| Critical | −25 pts |
| Warning | −10 pts |
| Info | −3 pts |

Score floors at 0. A job with no issues scores 100/100. Per-category caps prevent a
flood of config warnings from drowning out genuine criticals (max −30 for warnings,
max −15 for info).

## Configurable thresholds

All analyzer thresholds can be overridden per-job via Spark conf:

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.skew.minTasks` | `10` | Minimum tasks in a stage before skew is evaluated |
| `spark.sparklens.skew.warnP95Ratio` | `3.0` | p95/p50 task duration ratio that triggers a Warning |
| `spark.sparklens.skew.critP95Ratio` | `8.0` | p95/p50 ratio that escalates to Critical |
| `spark.sparklens.spill.warnDiskMb` | `100` | Total disk spill (MB) that triggers a Warning |
| `spark.sparklens.spill.critDiskMb` | `1024` | Total disk spill (MB) that escalates to Critical |
| `spark.sparklens.gc.warnFraction` | `0.10` | GC time / executor run time ratio for Warning |
| `spark.sparklens.gc.critFraction` | `0.20` | GC time / executor run time ratio for Critical |
| `spark.sparklens.cpu.lowFraction` | `0.20` | CPU fraction below which low-CPU Info is raised |
| `spark.sparklens.smallFiles.targetMb` | `128` | Target task input size (MB); avg below half triggers Warning |
| `spark.sparklens.outputSmallFiles.targetMb` | `128` | Target output size per task (MB); avg below half triggers Warning |
| `spark.sparklens.shuffleLocality.remoteRatioWarn` | `0.70` | Fraction of shuffle bytes read remotely for Warning |
| `spark.sparklens.driver.largeResultWarnMb` | `50` | Result size (MB) sent to driver that triggers Warning |
| `spark.sparklens.driver.largeResultCritMb` | `500` | Result size (MB) sent to driver that escalates to Critical |
| `spark.sparklens.join.largeBroadcastGb` | `1` | Broadcast threshold (GB) above which oversized-broadcast Warning fires |
| `spark.sparklens.join.excessiveShuffleCount` | `4` | Number of shuffle exchanges that triggers Warning |
| `spark.sparklens.preemption.killedTaskRateWarn` | `0.05` | Fraction of killed tasks per stage for Warning |
| `spark.sparklens.stageFailure.failedTaskRateWarn` | `0.05` | Fraction of failed tasks per stage for Warning |
| `spark.sparklens.memoryPressure.gcFraction` | `0.10` | GC fraction co-required with spill for memory-pressure Critical |
| `spark.sparklens.memoryPressure.spillMb` | `100` | Disk spill (MB) co-required with GC for memory-pressure Critical |
| `spark.sparklens.stageParallelism.minCores` | `8` | Minimum cluster core count before parallelism is evaluated |
| `spark.sparklens.stageParallelism.underutilizationRatio` | `0.5` | Tasks / total cores ratio below which low-parallelism Info fires |
| `spark.sparklens.stageParallelism.minStageSec` | `10` | Minimum stage duration (s) before parallelism is evaluated |
| `spark.sparklens.longStage.outlierRatio` | `5.0` | Stage duration / job-median ratio above which Warning fires |
| `spark.sparklens.longStage.minStageSec` | `30` | Minimum stage duration (s) before long-stage check applies |

## Build

Requires Java 17+ and sbt:

```bash
sbt test     # run tests
sbt assembly # build fat JAR
```

## License

Apache 2.0
