# Configuration Reference

All configuration is passed as standard Spark properties via `--conf`, `SparkConf`, or `spark-defaults.conf`. Every property is optional and has a sensible default.

---

## Output & Reporting

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.output` | `off` | Comma-separated list of output formats: `off`, `text`, `json`, `html`, `log`. Example: `text,json` |
| `spark.sparklens.report.path` | *(none)* | Base path for all formats. Required for `text`, `json`, `html`. Optional for `log`. Supports [placeholder tokens](#path-placeholder-tokens). |
| `spark.sparklens.report.path.text` | *(none)* | Format-specific path override for text reports. |
| `spark.sparklens.report.path.json` | *(none)* | Format-specific path override for JSON reports. |
| `spark.sparklens.report.path.html` | *(none)* | Format-specific path override for HTML reports. |
| `spark.sparklens.report.path.log` | *(none)* | Format-specific path override for log reports. |
| `spark.sparklens.report.maxPriorityFixes` | `20` | Maximum number of issues shown in the "Priority fixes" header of the text report. |
| `spark.sparklens.fail.on` | *(none)* | Throw at application end if issues at this severity or above are found. Valid values: `critical`, `warning`, `info`. |

### Path placeholder tokens

Resolved at application end when the full app model is available. Both `{token}` and `${token}` forms are supported.

| Token | Resolves to | Example |
|---|---|---|
| `{app_id}` | Spark application ID | `application_1234567890_0001` |
| `{app_name}` | Application name (spaces → underscores) | `My_Spark_Job` |
| `{spark_version}` | Spark version string (special chars → underscores) | `3_5_1` |
| `{date}` | UTC date at report time | `2026-06-03` |
| `{timestamp}` | Unix epoch seconds at report time | `1748959200` |

**Example:**
```
--conf spark.sparklens.report.path=/data/reports/{app_name}/{date}/{app_id}.txt
```

---

## Impact estimation speed assumptions

Used by multiple analyzers to convert bytes to estimated wall-clock penalties. Override these to match your cluster's actual throughput.

| Property | Default | Used by |
|---|---|---|
| `spark.sparklens.impact.networkSpeedMbps` | `1024` MB/s | JoinAnalyzer, SkewAnalyzer, ShuffleLocalityAnalyzer, DriverBottleneckAnalyzer |
| `spark.sparklens.impact.diskSpeedMbps` | `200` MB/s | SpillAnalyzer, MemoryPressureAnalyzer |
| `spark.sparklens.impact.readSpeedMbps` | `512` MB/s | CacheAnalyzer |

---

## Analyzer thresholds

### CacheAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.cache.sql.minExecCount` | `5` | Minimum SQL executions scanning the same table before flagging. |
| `spark.sparklens.cache.sql.warnMaxGb` | `5` | Table avg size in GB above which severity drops to Info (too large to cache cheaply). |

### ConfigAnalyzer

No per-threshold properties. The analyzer reads Spark-native properties directly (`spark.sql.adaptive.enabled`, `spark.serializer`, etc.).

### CpuEfficiencyAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.cpu.lowFraction` | `0.20` | CPU time / executor run time threshold below which the issue fires. |

### CriticalPathAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.criticalPath.warnFraction` | `0.85` | Critical path as fraction of app wall time that triggers Warning (below this → Info). |
| `spark.sparklens.criticalPath.minChain` | `3` | Minimum stages in the dependency chain to report. |

### DriverBottleneckAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.driver.largeResultWarnMb` | `50` | Result bytes threshold for Warning. |
| `spark.sparklens.driver.largeResultCritMb` | `500` | Result bytes threshold for Critical. |

### DynamicAllocationAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.dynalloc.churnLifetimeMs` | `30000` | Executor lifetime below which it is counted as "short-lived". |
| `spark.sparklens.dynalloc.churnPct` | `25.0` | Percentage of short-lived executors that triggers the churn warning. |
| `spark.sparklens.dynalloc.minRemovedExecs` | `3` | Minimum removed executors before churn check runs. |

### ExecutorSizingAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.sizing.coreUtilWarnPct` | `30.0` | Maximum stage task count as % of total cores before flagging cluster over-provisioning. |

### GcAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.gc.warnFraction` | `0.10` | GC time / executor run time for Warning. |
| `spark.sparklens.gc.critFraction` | `0.20` | GC time / executor run time for Critical. |

### IoClassifierAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.io.minDurationMs` | `10000` | Minimum stage duration (ms) to analyze. |
| `spark.sparklens.io.ioFloorMbps` | `3.0` | Minimum throughput per core (MB/s) to consider I/O-bound. |
| `spark.sparklens.io.minIoBytes` | `1048576` | Minimum bytes read to qualify for analysis. |

### JobTimelineAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.timeline.gapWarnMs` | `60000` | Inter-job idle gap (ms) that triggers a warning. |
| `spark.sparklens.timeline.stageGapWarnMs` | `10000` | Intra-job inter-stage idle gap (ms) that triggers a warning. |
| `spark.sparklens.timeline.fragThresholdMs` | `2000` | Job duration below which it counts as "short" for fragmentation detection. |
| `spark.sparklens.timeline.fragFraction` | `0.70` | Fraction of short jobs that triggers the fragmentation issue. |
| `spark.sparklens.timeline.minJobs` | `50` | Minimum total jobs before fragmentation check runs. |

### JoinAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.join.largeBroadcastGb` | `1` | Broadcast join threshold (GB) above which the broadcast is considered oversized. |
| `spark.sparklens.join.excessiveShuffleCount` | `4` | Number of shuffle exchanges that triggers the "excessive shuffles" issue. |
| `spark.sparklens.join.explodingRatio` | `5.0` | Output/input byte ratio that triggers the exploding-join issue. |
| `spark.sparklens.join.explodingMinInputBytes` | `1048576` | Minimum input bytes before the explosion check runs. |

### LongStageAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.longStage.outlierRatio` | `5.0` | Stage duration / job median duration ratio that triggers the issue. |
| `spark.sparklens.longStage.minStageSec` | `30` | Minimum stage duration (seconds) to flag as long. |

### MemoryPressureAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.memoryPressure.gcFraction` | `0.10` | GC fraction required alongside spill to trigger the composite issue. |
| `spark.sparklens.memoryPressure.spillMb` | `100` | Spill bytes (MB) required alongside GC to trigger the issue. |

### OutputSmallFilesAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.outputSmallFiles.targetMb` | `128` | Target output file size (MB); stages producing smaller files are flagged. |

### PartitionImbalanceAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.partition.imbalance.minInputMb` | `100` | Minimum total input (MB) before imbalance check runs. |
| `spark.sparklens.partition.imbalance.p95p50Ratio` | `3.0` | p95 / p50 input-bytes ratio that triggers the issue. |

### PlanAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.plan.explodeRatio` | `5.0` | Output / input records or bytes ratio for the explode-row-explosion check. |
| `spark.sparklens.plan.compileWarnMs` | `5000` | Gap between SQL start event and first job submission (ms) that indicates slow compilation. |

### PreemptionAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.preemption.killedTaskRateWarn` | `0.05` | Fraction of killed tasks per stage that triggers a warning. |

### SchedulerDelayAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.schedulerDelay.warnMs` | `2000` | Median task launch delay (ms) for Info. |
| `spark.sparklens.schedulerDelay.minTasks` | `5` | Minimum sampled tasks to run the check. |

### ShuffleLocalityAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.shuffleLocality.remoteRatioWarn` | `0.70` | Remote shuffle bytes / total shuffle bytes ratio that triggers a warning. |

### SkewAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.skew.minTasks` | `10` | Minimum tasks in a stage to check for skew. |
| `spark.sparklens.skew.warnP95Ratio` | `3.0` | p95 / p50 task duration ratio for Warning. |
| `spark.sparklens.skew.critP95Ratio` | `8.0` | p95 / p50 task duration ratio for Critical. |
| `spark.sparklens.skew.p75WarnRatio` | `1.5` | max / p75 task duration ratio for the hidden-outlier check. |

### SmallFilesAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.smallFiles.targetMb` | `128` | Target read file size (MB); stages averaging below this are flagged. |

### SpillAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.spill.warnDiskMb` | `100` | Disk spill (MB) for Warning. |
| `spark.sparklens.spill.critDiskMb` | `1024` | Disk spill (MB) for Critical. |

### StageFailureAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.stageFailure.failedTaskRateWarn` | `0.05` | Failed task fraction per stage that triggers a warning. |

### StageParallelismAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.stageParallelism.singleTaskMinMs` | `5000` | Stage duration (ms) below which a single-task stage is not flagged. |
| `spark.sparklens.stageParallelism.minCores` | `8` | Minimum cluster cores before low-parallelism check runs. |
| `spark.sparklens.stageParallelism.underutilizationRatio` | `0.50` | Task count / total cores threshold below which the stage is flagged. |
| `spark.sparklens.stageParallelism.minStageSec` | `10` | Minimum stage duration (seconds) for low-parallelism check. |

### TaskOverheadAnalyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.overhead.deserializeRatioWarn` | `0.30` | Deserialization time / executor run time that triggers a warning. |
| `spark.sparklens.overhead.minStageSec` | `5` | Minimum stage duration (seconds) for the check. |

### YARN Analyzer

| Property | Default | Description |
|---|---|---|
| `spark.sparklens.yarn.queueWaitWarnMs` | `30000` | Gap between app start and first executor allocation (ms) for Warning. |
| `spark.sparklens.yarn.hotNodeMinFailed` | `5` | Minimum failed tasks on one host to run the hot-node check. |
| `spark.sparklens.yarn.hotNodePct` | `50.0` | Percentage of total failures on one host that flags it as a hot node. |
