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

That's it. At application end, a report appears in the driver logs.

## Configuration

All settings are optional and prefixed with `spark.sparklens.*`:

| Property | Default | Values | Description |
|---|---|---|---|
| `spark.sparklens.output` | `off` | `off` `text` `json` `html` | Output format. `off` = silent unless `fail.on` is set |
| `spark.sparklens.report.path` | *(stdout)* | local path or `hdfs://...` | Write report to a file instead of stdout |
| `spark.sparklens.fail.on` | *(none)* | `critical` `warning` `info` | Throw at app end if issues at this severity or above are found |

## What it detects

| Analyzer | Category | Signal |
|---|---|---|
| SkewAnalyzer | skew | Max/median task duration ratio > 3× |
| SpillAnalyzer | spill | Disk spill > 100 MB |
| JoinAnalyzer | join | Broadcast disabled on SMJ, oversized broadcast, excessive shuffles |
| GcAnalyzer | gc | GC time > 10% of executor run time |
| CacheAnalyzer | cache | Same RDD scanned in multiple jobs without caching |
| PreemptionAnalyzer | preemption | Executor lost mid-job, high task kill rate |
| PlanAnalyzer | plan | CartesianProduct, Window without PARTITION BY, round-robin repartition |
| ConfigAnalyzer | config | AQE disabled, Java serializer, default shuffle partitions |
| SmallFilesAnalyzer | io | Many tiny input files (avg < 64 MB/task) |
| ShuffleLocalityAnalyzer | io | > 70% of shuffle bytes read remotely |
| DriverBottleneckAnalyzer | io | Large collect() result > 50 MB, CollectLimit in plan |
| CpuEfficiencyAnalyzer | io | CPU utilization < 20% of executor run time |
| SpeculationAnalyzer | config | Speculative tasks actively firing (masking skew) |
| StageFailureAnalyzer | reliability | Stage retried (attempt > 0), high task failure rate |
| MemoryPressureAnalyzer | reliability | GC overhead + disk spill co-occurring in the same stage |

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

## Permanent cluster configuration

Add to `spark-defaults.conf` on every node — every job gets analyzed automatically:

```properties
spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener
spark.sparklens.output=text
spark.sparklens.fail.on=critical
```

## Health score

| Deduction | Severity |
|---|---|
| −25 pts | Critical |
| −10 pts | Warning |
| −3 pts | Info |

Score floors at 0. A job with no issues scores 100/100.

## Build

Requires Java 17+ and sbt, or use Docker:

```bash
./build.sh test     # run tests (Scala 2.12 + 2.13)
./build.sh package  # build JARs
```

## License

Apache 2.0
