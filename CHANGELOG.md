# Changelog

All notable changes to spark-lens are documented here.
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [1.6.1] — 2026-06-05

### Internal
- Release pipeline: bypass sbt-sonatype `USER_MANAGED` limitation by uploading the bundle
  directly via the Sonatype Central API with `publishingType=AUTOMATIC`

---

## [1.6.0] — 2026-06-05

### Multi-module build + spark-lens-testing artifact

**Build refactor — 3 subprojects:**
- `spark-lens-core` — model, analyzers, reporters (shared logic, no duplication)
- `spark-lens` — production listener (`SparkLensListener`, `SparkAppModelBuilder`), depends on core
- `spark-lens-testing` — test utilities, depends on core + listener

All 29 analyzers live in `spark-lens-core` exactly once. Both the production listener and the test utilities consume them as a dependency.

**New: `spark-lens-testing` artifact**

Lets users write ScalaTest specs that assert on spark-lens analysis results — performance contract tests that catch regressions before production:

```scala
libraryDependencies += "io.github.nidhal-saadaoui" %% "spark-lens-testing" % "1.6.0" % Test
```

```scala
class MyJobSpec extends SparkLensSpec {
  "cartesian join" should "be flagged" in {
    analyse { spark.range(100).crossJoin(spark.range(10)).count() } should haveIssue("plan-cartesian")
  }
  "aggregation" should "not spill" in {
    analyse { MyJob.run(spark) } should not(haveIssueOfCategory("spill"))
  }
}
```

Components:
- `SparkLensSpec` (FlatSpec) / `SparkLensSuite` (FunSuite) — traits with shared local SparkSession and `analyse {}` block
- `SparkLensAnalyser.run {}` — attaches a scoped listener to the existing session, flushes async events, builds `SparkAppModel`, runs all analyzers
- `SparkLensMatchers` — `haveIssue`, `haveIssueOfCategory`, `haveIssueOfSeverity`, `haveHealthScoreAbove/Below`, `haveNoIssuesOfSeverity`
- `SparkLensResult` — wraps `SparkAppModel` + `Seq[Issue]` with convenience accessors

**New: `Scoring` object in core**

`Reporter.healthScore`, `issueClusterGroups`, and `deduplicatedSavingsMs` extracted to public `object Scoring` in `spark-lens-core`. `Reporter` now delegates to it. `SparkLensResult` uses `Scoring.healthScore` directly.

---

## [1.5.0] — 2026-06-04

### Interactive HTML dashboard with performance timelines
Enhanced `HtmlReporter` output with six self-contained SVG-based visualizations:

1. **Metrics Summary Panel** — Health score, critical/warning/info counts, plus key metrics: total duration, peak executor memory, total shuffle bytes, GC time
2. **Stage Timeline** — Gantt-style chart showing stage execution duration with color coding: red if GC > 10% of stage time, orange if disk spill detected, blue otherwise. Enables visual correlation of problems with when they occur
3. **Memory Pressure Timeline** — Line chart tracking executor peak memory evolution throughout the job, highlighting memory pressure peaks
4. **Shuffle Metrics Breakdown** — Stacked horizontal bar chart comparing input bytes (blue) vs shuffle output bytes (red) per shuffle stage, useful for identifying data expansion/reduction patterns
5. **GC Timeline** — Bar chart showing GC pause duration at stage boundaries, color-coded by impact (red >20%, orange >10%, blue normal). Includes hover tooltips with duration and percentage overhead
6. **Issue Severity Timeline** — Timeline showing when critical (red) vs warning (amber) issues occur in the job, mapped to affected stages for visual correlation with execution timeline

All visualizations:
- Are self-contained SVG with no external dependencies or JavaScript
- Scale dynamically to data ranges (no hardcoded min/max values)
- Include native SVG tooltips (via `<title>` elements) for hover inspection
- Gracefully omit themselves if no relevant data exists (e.g., no GC events → no GC timeline)
- Use a Tailwind-inspired color palette (red #dc2626, amber #f59e0b, blue #2563eb)

### New analyzer: ScalingSimulatorAnalyzer
Answers "how much would more executors help?" from a single run — closes the main gap vs Qubole Sparklens.

- Replays the stage DAG (topological order) at 0.5×, 2×, 3×, 4× current executor count
- Avg task duration = `totalExecutorRunTimeMs / taskCount`; calibrated so sim@actual ≈ real wall-clock
- Skewed stages use p95 task duration (straggler sets the floor regardless of parallelism)
- Detects when dynamic-allocation `maxExecutors` ceiling was binding (peak ≥ 90% of cap) → **Warning** + `configFix: spark.dynamicAllocation.maxExecutors=<2×>`
- Reports `serial_floor_pct`: fraction of app time locked in serial stage dependencies (limiting factor on scaling beyond a certain point)
- `model_confidence: low` emitted when calibration factor > 2.5× (unmodelled driver/shuffle overhead dominates)
- Diminishing-returns note when 2× → 4× executors yields < 15% additional improvement
- `estimatedImpact.savedTimeMs` = projected saving at 2× executors (used for priority-fix ranking)

Sample output:
```
actual  (20 exec)        24.3m
sim     (10 exec, 0.5×)  ~38.1m  (+57%)
sim     (40 exec, 2×)    ~14.1m  (-42%)
sim     (60 exec, 3×)    ~11.8m  (-51%)
sim     (80 exec, 4×)    ~11.2m  (-54%)
serial_floor_pct         38.2
model_confidence         medium
```

### Internal
- Added `StageData.totalTaskCount` computed property (follows existing `hasExactAggregates` pattern)
- Analyzer count: 29 (was 28)

---

## [1.4.1] — 2026-06-03

### Listener overhead measurement
- SparkLens now self-times its `onTaskEnd` callback (the hot path). Every report footer line shows: `SparkLens: 2.5M task events · 340ms listener overhead (0.1% of app time)`
- JSON output gains a `"listener_overhead_ms"` field (omitted when no task events were recorded)
- Driver log receives a `WARN` when SparkLens overhead exceeds 5% of app duration, naming the task count so users can decide whether to disable the listener on that workload

### Savings deduplication by root-cause cluster
- `total_estimated_savings_ms` in JSON now takes the **maximum** savings per root-cause cluster and sums across clusters, capped at app duration. Issues linked via `relatedIds` no longer each contribute independently — a `coalesce(1)` causing 4 issues each claiming 30 min now correctly reports 30 min total, not 120 min
- Priority fixes section in text report now collapses related issues into a single entry with a `(+N covered)` suffix. The root fix appears once at the top of the ranked list; the count tells you how many additional issues the same fix resolves

---

## [1.4.0] — 2026-06-03

### ExecutorSizingAnalyzer — 3 new sizing checks
- **executor-cores-gc-pressure** — warns when app-wide GC fraction >15% AND coresPerExec >2. High GC with many cores means concurrent tasks share too little heap. Recommends halving cores (doubles heap per task) + G1GC
- **executor-storage-execution-conflict** — warns when the job caches RDDs/DataFrames AND has significant disk spill AND spark.memory.storageFraction=0.5 (default). Indicates storage memory is evicting execution pages. Recommends lowering to 0.3
- **executor-offheap-not-configured** — warns when Python/Arrow UDF patterns (PythonUDF, ArrowEvalPython, etc.) are detected AND spark.memory.offHeap.enabled is not true. Recommends enabling off-heap at 25% of executor memory for untracked Python subprocess + Arrow buffer usage

### Analysis accuracy
- Health score now consolidates root-cause clusters before scoring (issues linked via relatedIds count as one toward health deduction, not independently)
- JoinAnalyzer + SkewAnalyzer savings are now capped at SQL execution duration (prevents inflated estimates when network-based penalties exceed query wall-clock time)
- SpeculationAnalyzer now detects non-idempotent write stages (insertInto, .write., SaveAsTable) and escalates to Critical when speculation fires on them (risk of duplicate rows)
- ExecutorSizingAnalyzer now excludes single-task stages (coalesce(1), repartition(1)) from memory sizing when multi-task stages exist (prevents inflation from full-dataset single-task stages)

### Report improvements
- HTML report now includes an interactive bar chart of top issues by estimated savings
- Priority fixes now show up to 20 issues by default (was 3, configurable via spark.sparklens.report.maxPriorityFixes)
- Text report shows "likely shares root cause with:" note for issues that share affected stages

### Configuration
- Added spark.sparklens.timeline.stageGapWarnMs (default 10000 ms) — threshold for intra-job driver idle gaps between stages
- Added spark.sparklens.sizing.gcWarnCoresFraction (default 0.15) — GC fraction threshold for core-reduction recommendation
- Added spark.sparklens.report.maxPriorityFixes (default 20) — configurable priority fixes limit in text report

### New checks
- **config-parallelism-mismatch** — warns when spark.default.parallelism and spark.sql.shuffle.partitions differ by >2×
- **driver-stage-gap** — warns on intra-job driver idle time >10s between consecutive stages within the same job

### Documentation
- CLAUDE.md updated with current analyzer count (28, not 21)
- GitHub wiki expanded from 6 to 7 pages with full analyzer reference and troubleshooting guide
- README updated with logo and current version references
- Added HTML report logo (SparkLens brand embedded in self-contained reports)

### Internal
- Added RootCauseClusters union-find in Reporter trait for health score consolidation
- Added relatedIds field to Issue model for root-cause linking
- Analyzers.linkRelated() post-processing computes related-issue clusters based on shared affected stages and estimated impact

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
