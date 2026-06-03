# spark-lens-listener

[![CI](https://github.com/nidhal-saadaoui/spark-lens-listener/actions/workflows/ci.yml/badge.svg)](https://github.com/nidhal-saadaoui/spark-lens-listener/actions/workflows/ci.yml)
[![maven](https://badges.mvnrepository.com/badge/io.github.nidhal-saadaoui/spark-lens/badge.svg?label=maven)](https://mvnrepository.com/artifact/io.github.nidhal-saadaoui/spark-lens)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-orange)](https://nidhal-saadaoui.github.io/spark-lens-listener/)

Zero-config Spark performance analyzer. Attach via `spark.extraListeners` — get actionable recommendations in your logs at the end of every job. No Python, no extra services, no code changes.

## Usage

```bash
spark-submit \
  --packages io.github.nidhal-saadaoui:spark-lens_2.12:1.2.0 \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  myJob.jar
```

That's it. At application end, a report appears in the driver stdout.

> **Note:** `spark.extraListeners=` **replaces** any existing listener list. To append
> spark-lens alongside another listener, comma-separate them:
> ```
> --conf spark.extraListeners=com.other.Listener,com.github.saadaouini.sparklens.SparkLensListener
> ```

> **Batch only:** spark-lens is designed for batch Spark applications. Structured Streaming
> apps share the same listener API but the stage/job model does not map cleanly to streaming
> micro-batches — results will be incomplete or misleading. To write to a dedicated file instead, set `spark.sparklens.report.path`.

## Sample report

Sample output with `output=text` and `report.path=/tmp/report.txt`:
```
======================================================================
  spark-lens  |  daily-user-aggregation  (app-20241105-0042)
  Spark 3.5.0   |  Health: 64/100  |  3 warning  ·  2 info
======================================================================

  Priority fixes (estimated savings per run):
  1. [Warning] Disk Spill in Stage 3 [+2 more stages]  ~4.2min
  2. [Warning] Data Skew in Stage 5  ~1.8min
  3. [Warning] Java Serializer in Use  ~45s

  [⚠ WARNING ]  Java Serializer in Use — Switch to Kryo
    fix:   spark.serializer=org.apache.spark.serializer.KryoSerializer
           Java serialization is 10× slower than Kryo and produces 2–10× larger byte arrays.

  [⚠ WARNING ]  Adaptive Query Execution (AQE) Is Disabled
    fix:   spark.sql.adaptive.enabled=true
           AQE automatically coalesces shuffle partitions, handles skew, and switches join strategies at runtime.

  [⚠ WARNING ]  Excessive Shuffles in "count at ..." (6 exchanges) [+2 more queries]
    fix:   spark.sql.adaptive.enabled=true
           The query plan contains 6 shuffle exchanges.
    jobs:   0, 1, 2

  [ℹ INFO    ]  Low Executor Memory Overhead — Risk of Off-Heap OOM
    fix:   spark.executor.memoryOverheadFactor=0.2
           Executor memory is 4g with only 10% overhead.

  [ℹ INFO    ]  Default Shuffle Partitions (200) — May Be Too Few or Too Many
    fix:   spark.sql.adaptive.enabled=true
           The default of 200 shuffle partitions is rarely optimal.

  ──────────────────────────────────────────────────────────────────
  Quick wins
    spark.sql.adaptive.enabled=true
       resolves 3 issues: config-aqe-disabled, config-default-shuffle-partitions, plan-excessive-shuffles

======================================================================
```

Issues of the same type across multiple stages or queries are merged into a single
entry — e.g. `Disk Spill in Stage 3 [+4 more stages]` — so the report stays readable
on large pipelines.

## What it detects

| Analyzer | Category | Signal |
|---|---|---|
| JobTimelineAnalyzer | io | Idle gap > 60 s between jobs (driver bottleneck), or > 70% of jobs complete in < 2 s across 50+ total jobs (job fragmentation) |
| SkewAnalyzer | skew | p95/p50 task duration ratio > 3×, top-5% tasks hold > 25% of stage time, or max task > 1.5× p75 (hidden outlier) |
| TaskOverheadAnalyzer | io | Executor deserialize time > 30% of run time — too many small tasks paying serialization overhead |
| SpillAnalyzer | spill | Total disk spill > 100 MB |
| JoinAnalyzer | join | Broadcast disabled on SMJ, broadcast threshold ≥ 1 GB, ≥ 4 shuffle exchanges, output > 5× input (exploding join) |
| GcAnalyzer | gc | GC time > 10% of executor run time |
| CacheAnalyzer | cache | Same RDD scanned in multiple jobs without caching |
| PreemptionAnalyzer | reliability | Executor lost mid-job, task kill rate > 5% |
| PlanAnalyzer | plan | CartesianProduct, Window without PARTITION BY, round-robin repartition, missing CBO stats |
| UdfAnalyzer | plan | Python UDF (PythonUDF / BatchEvalPython / ArrowEvalPython) or Scala UDF detected in physical plan |
| IoClassifierAnalyzer | io | Stage throughput ≥ 3 MB/s/core (stage is storage/network bound, not compute-bound) |
| ConfigAnalyzer | config | AQE disabled, Java serializer, default shuffle partitions (200), low memory overhead, AQE skew join disabled, small shuffle buffer (< 64k), CBO histograms disabled, low task.maxFailures (< 3), high locality.wait (> 5s), unlimited reducer.maxReqsInFlight on large clusters |
| ExecutorSizingAnalyzer | config | Executor memory under/over-provisioned vs p95 peak task memory; driver heap risk vs total result bytes; cluster cores vs max stage parallelism |
| SmallFilesAnalyzer | io | Input avg < 64 MB/task with majority of tasks reading tiny files |
| OutputSmallFilesAnalyzer | io | Output avg < 64 MB/task — downstream jobs will hit the small-files problem |
| ShuffleLocalityAnalyzer | io | > 70% of shuffle bytes read remotely |
| DriverBottleneckAnalyzer | io | collect() result > 50 MB, CollectLimit or TakeOrderedAndProject in plan |
| CpuEfficiencyAnalyzer | io | CPU utilization < 20% of executor run time |
| SpeculationAnalyzer | config | Speculative tasks firing — masking skew rather than fixing it |
| StageFailureAnalyzer | reliability | Stage retried (attempt > 0), task failure rate > 5% |
| MemoryPressureAnalyzer | reliability | GC > 10% and disk spill > 100 MB co-occurring in the same stage |
| StageParallelismAnalyzer | io | Stage tasks < 50% of available executor cores on a stage > 10 s, or entire stage runs as a single task |
| LongStageAnalyzer | reliability | Stage duration > 5× the median stage duration in its job |
| PartitionImbalanceAnalyzer | io | p95/p50 input partition size ratio > 3× — high-variance input partition sizes slow the stage (fat partitions become the bottleneck) |
| SchedulerDelayAnalyzer | config | Median task launch delay > 2 s after stage submission — tasks waiting idle before first execution |
| CriticalPathAnalyzer | plan | DAG critical path (via parentIds) ≥ 85% of app wall time across ≥ 3 sequential stages — serial dependency chain means adding executors won't help |

## Configuration

All settings are optional and prefixed with `spark.sparklens.*`:

| Property | Default | Values | Description |
|---|---|---|---|
| `spark.sparklens.output` | `off` | Comma-separated: `off` `text` `json` `html` `log` | One or more output formats. Examples: `text` `text,json` `log,json`. The `log` format writes one line per issue through the Java driver logger so issues appear inline in the Spark driver log. |
| `spark.sparklens.report.path` | *(stdout)* | local path or `hdfs://...` | Base path for all formats. With multiple formats each gets an extension (`.txt`, `.json`, `.html`, `.log`). |
| `spark.sparklens.report.path.<format>` | — | path | Per-format path override (highest priority). Available for each format: `.text` `.json` `.html` `.log`. Example: `spark.sparklens.report.path.json=hdfs:///reports/myapp.json` |
| `spark.sparklens.fail.on` | *(none)* | `critical` `warning` `info` | Throw at app end if issues at this severity or above are found |
| `spark.sparklens.skew.warnP95Ratio` | `3.0` | double | p95/p50 task duration ratio threshold for skew warning |
| `spark.sparklens.skew.p75WarnRatio` | `1.5` | double | max/p75 task duration ratio threshold for hidden-outlier skew |
| `spark.sparklens.join.explodingRatio` | `5.0` | double | Output/input byte ratio above which a join is flagged as exploding |
| `spark.sparklens.timeline.gapWarnMs` | `60000` | ms | Minimum idle gap between jobs to flag as a driver bottleneck |
| `spark.sparklens.timeline.fragThresholdMs` | `2000` | ms | Jobs completing faster than this count toward the fragmentation check |
| `spark.sparklens.io.ioFloorMbps` | `3.0` | MB/s | Per-core throughput above which a stage is I/O-bound |
| `spark.sparklens.timeline.fragFraction` | `0.7` | 0–1 | Fraction of short jobs required to flag fragmentation |
| `spark.sparklens.timeline.minJobs` | `50` | int | Minimum total jobs before fragmentation check fires |
| `spark.sparklens.plan.compileWarnMs` | `5000` | ms | Driver plan compilation time above which a slow-compile warning fires |
| `spark.sparklens.stageParallelism.singleTaskMinMs` | `5000` | ms | Minimum duration for a single-task stage to be flagged |

## CI quality gate

Fail the Spark application itself if critical issues are found:

```bash
spark-submit \
  --packages io.github.nidhal-saadaoui:spark-lens_2.12:1.2.0 \
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

To write multiple formats in one run with separate destinations:

```bash
--conf spark.sparklens.output=text,json \
--conf spark.sparklens.report.path.text=/tmp/report.txt \
--conf spark.sparklens.report.path.json=hdfs:///reports/myapp.json
```

The JSON schema is stable and suitable for CI parsers or dashboards:

```json
{
  "spark_lens_version": "1.2.0",
  "app_id": "app-20241105-0042",
  "app_name": "daily-user-aggregation",
  "spark_version": "3.5.0",
  "duration_ms": 120000,
  "health_score": 64,
  "issue_count": 5,
  "total_estimated_savings_ms": 45000,
  "top_actions": [
    {"config": "spark.sql.adaptive.enabled=true", "resolves": ["config-aqe-disabled"], "estimated_savings_ms": null}
  ],
  "issues": [{
    "id": "config-aqe-disabled",
    "severity": "warning",
    "category": "config",
    "title": "Adaptive Query Execution (AQE) Is Disabled",
    "description": "...",
    "recommendation": "...",
    "fixes": {"config": "spark.sql.adaptive.enabled=true"},
    "affected_stages": [],
    "affected_jobs": [],
    "metrics": {},
    "estimated_impact": {"summary": "...", "confidence": "low"}
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
| Critical | −30 pts |
| Warning | −10 pts |
| Info | −2 pts |

Score floors at 0. A job with no issues scores 100/100.

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
| `spark.sparklens.cache.sql.minExecCount` | `5` | Minimum SQL executions scanning the same table before repeated-scan Warning fires |
| `spark.sparklens.cache.sql.warnMaxGb` | `5` | Estimated table size (GB) above which repeated-scan is downgraded to Info (table may be too large to cache) |
| `spark.sparklens.preemption.killedTaskRateWarn` | `0.05` | Fraction of killed tasks per stage for Warning |
| `spark.sparklens.stageFailure.failedTaskRateWarn` | `0.05` | Fraction of failed tasks per stage for Warning |
| `spark.sparklens.memoryPressure.gcFraction` | `0.10` | GC fraction co-required with spill for memory-pressure Critical |
| `spark.sparklens.memoryPressure.spillMb` | `100` | Disk spill (MB) co-required with GC for memory-pressure Critical |
| `spark.sparklens.stageParallelism.minCores` | `8` | Minimum cluster core count before parallelism is evaluated |
| `spark.sparklens.stageParallelism.underutilizationRatio` | `0.5` | Tasks / total cores ratio below which low-parallelism Info fires |
| `spark.sparklens.stageParallelism.minStageSec` | `10` | Minimum stage duration (s) before parallelism is evaluated |
| `spark.sparklens.longStage.outlierRatio` | `5.0` | Stage duration / job-median ratio above which Warning fires |
| `spark.sparklens.longStage.minStageSec` | `30` | Minimum stage duration (s) before long-stage check applies |
| `spark.sparklens.partition.imbalance.minInputMb` | `100` | Minimum total input bytes (MB) before partition imbalance is evaluated |
| `spark.sparklens.partition.imbalance.p95p50Ratio` | `3.0` | p95/p50 input partition size ratio that triggers Info (5× triggers Warning) |
| `spark.sparklens.schedulerDelay.warnMs` | `2000` | Median task launch delay (ms) that triggers Info (5000 ms triggers Warning) |
| `spark.sparklens.schedulerDelay.minTasks` | `5` | Minimum task sample size before scheduler delay is evaluated |
| `spark.sparklens.criticalPath.warnFraction` | `0.85` | Critical path / app wall time fraction threshold |
| `spark.sparklens.criticalPath.minChain` | `3` | Minimum number of sequential stages in the chain |

## Build

Requires Java 11+ and sbt (Java 17 recommended, matches Spark 3.5.x):

```bash
sbt test     # run tests
sbt assembly # build fat JAR
```

## License

Apache 2.0
