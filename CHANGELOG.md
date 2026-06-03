# Changelog

All notable changes to spark-lens are documented here.
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [1.2.0] — 2026-06-03

### New analyzers (3)
- **PartitionImbalanceAnalyzer** — flags stages where input partition p95/p50 size ratio exceeds 3× (warns at 5×); fat partitions become the bottleneck while most tasks finish early
- **SchedulerDelayAnalyzer** — flags stages where the median task launch delay after stage submission exceeds 2 s; surfaces locality-wait and executor-saturation issues
- **CriticalPathAnalyzer** — builds the stage DAG from `parentIds` and warns when the critical path accounts for ≥ 85% of app wall time across ≥ 3 sequential stages; adding executors won't help when the bottleneck is data dependencies

### ConfigAnalyzer — 5 new checks
- `spark.shuffle.file.buffer` below 64 k — excessive disk syscalls on every shuffle write
- `spark.sql.statistics.histogram.enabled` disabled — CBO uses row count only, missing column histograms
- `spark.task.maxFailures` below 3 — transient errors abort jobs in cloud environments
- `spark.locality.wait` above 5 s — task launch delayed on cloud storage with uniform access time
- `spark.reducer.maxReqsInFlight` unlimited on clusters with > 200 cores — risk of shuffle OOM

### ExecutorSizingAnalyzer
- Switched from average task peak memory to **p95** of the task sample for memory demand calculation — less pessimistic, more representative of real concurrent load
- Metric key renamed from `peak_task_memory` to `p95_task_memory` in JSON output

### HtmlReporter
- Added **metrics table** — raw key/value measurements shown for each issue
- Added **estimated impact badge** — confidence level and time saved displayed in the issue summary line
- Added **callSite tooltip** on stage pills — hover to see the user code line that submitted the stage

### Output
- `BuildInfo.version` field added; JSON output now includes `"spark_lens_version"` in every report
- Multiple output formats can be specified in a single run via comma-separated `spark.sparklens.output` (e.g. `text,json`)
- `log` format: one structured `[spark-lens]` line per issue written through the Java driver logger — compatible with Datadog, Splunk, CloudWatch

### CI/CD
- Fixed `docs.yml`: landing page and Scaladoc now both published to `gh-pages`; previously only Scaladoc was served
- Added concurrency cancellation to CI and Docs workflows
- Integration test extended with 4 new scenarios (StageParallelism, OutputSmallFiles, SmallFiles, IoClassifier)
- Release workflow now triggers docs rebuild automatically after Maven Central publish

---

## [1.0.1] — 2025-05-31

### Fixes
- Corrected sbt-dynver configuration for Sonatype Central publishing
- Fixed Scala 2.13 cross-compilation warnings (`-Xfatal-warnings`)

---

## [1.0.0] — 2025-05-31

### Initial release — 23 analyzers

**Skew & spill**
- SkewAnalyzer — p95/p50 task duration ratio, concentration check, hidden-outlier detection
- SpillAnalyzer — disk and memory spill with Warning/Critical thresholds

**Joins & plans**
- JoinAnalyzer — broadcast disabled, oversized threshold, excessive shuffles, exploding join
- PlanAnalyzer — CartesianProduct, Window without PARTITION BY, round-robin repartition, missing CBO stats
- UdfAnalyzer — Python UDF and Scala UDF detection

**Memory & GC**
- GcAnalyzer — GC fraction of executor run time
- MemoryPressureAnalyzer — GC + spill co-occurrence
- ExecutorSizingAnalyzer — executor memory, driver heap, cluster core utilisation

**I/O**
- SmallFilesAnalyzer — input avg below target task size
- OutputSmallFilesAnalyzer — output avg below target task size
- ShuffleLocalityAnalyzer — remote shuffle byte fraction
- IoClassifierAnalyzer — per-core throughput indicating storage-bound stage
- CpuEfficiencyAnalyzer — CPU fraction of executor run time
- DriverBottleneckAnalyzer — large collect(), CollectLimit, TakeOrderedAndProject
- JobTimelineAnalyzer — inter-job idle gaps, job fragmentation
- TaskOverheadAnalyzer — deserialise time fraction

**Reliability**
- StageFailureAnalyzer — stage retries, task failure rate
- PreemptionAnalyzer — executor loss, task kill rate
- SpeculationAnalyzer — speculative task activity
- LongStageAnalyzer — stage duration outlier within job
- StageParallelismAnalyzer — single-task stage, low core utilisation

**Configuration**
- ConfigAnalyzer — AQE, serialiser, shuffle partitions, memory overhead, skew-join flag
- CacheAnalyzer — repeated RDD/table scan without caching

**Output formats**: `text`, `json`, `html`
**Estimated impact**: savedTimeMs, savedBytes, confidence on every issue
**CI gate**: `spark.sparklens.fail.on=critical`
