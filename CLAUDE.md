# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests (both Scala versions)
sbt "+test"

# Run all tests (default Scala 2.12 only — faster for iteration)
sbt test

# Run a single test class
sbt "testOnly com.github.saadaouini.sparklens.analyzers.SkewAnalyzerSpec"

# Run tests matching a name pattern
sbt "testOnly com.github.saadaouini.sparklens.analyzers.*"

# Build fat assembly JAR (Scala 2.12)
sbt "++2.12.20 assembly"
# Output: target/scala-2.12/spark-lens_2.12-<version>-assembly.jar

# Run integration test locally (requires PySpark installed)
SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")
JAR=$(ls target/scala-2.12/spark-lens_2.12-*-assembly.jar | head -1)
${SPARK_HOME}/bin/spark-submit \
  --master "local[2]" \
  --driver-class-path "${JAR}" \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  --conf spark.sparklens.report.path=/tmp/spark-lens-report.txt \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sparklens.cache.sql.minExecCount=3 \
  integration-test/demo_job.py
```

`scalacOptions` includes `-Xfatal-warnings` — all compiler warnings fail the build.

## Architecture

### Data flow

```
SparkLensListener (SparkListener)
    │  delegates all events to
    ▼
SparkAppModelBuilder          ← mutable, accumulates during job
    │  build() at onApplicationEnd
    ▼
SparkAppModel                 ← immutable snapshot
    │  passed to
    ▼
Analyzers.runAll              ← runs all 21 analyzers, groups + sorts issues
    │
    ▼
Reporter (Text / JSON / HTML) ← writes report to stdout or file path
```

**SparkLensListener** is the Spark `extraListeners` entry point. It delegates every listener event to `SparkAppModelBuilder` and at `onApplicationEnd` calls `builder.build()`, then `Analyzers.runAll()`, then the selected reporter. SQL plan events arrive via `onOtherEvent` (not typed listener methods) because `SparkListenerSQLExecutionStart` etc. live in the SQL module.

**SparkAppModelBuilder** keeps two parallel data structures per stage:
- `StageSummary` — exact running totals for every task metric (never sampled)
- `stageTasks` buffer — reservoir-sampled up to 10K tasks per stage using Vitter's Algorithm R

At `onStageCompleted` the exact totals populate `StageData.exactXxx` fields and `hasExactAggregates = true`. The `totalXxx` computed methods on `StageData` return `exactXxx` when the flag is set, otherwise fall back to summing the task sample. All analyzers should use the `total*` methods, not `exactXxx` directly.

The builder also captures `SparkPlanInfo` trees from SQL execution start events, collects per-task accumulator updates for Exchange nodes, and resolves them at `SqlExecutionEnd` so SkewAnalyzer has per-partition byte counts.

### Analyzer contract

Each analyzer is an `object` extending `Analyzer`. The only public method is:
```scala
def analyze(app: SparkAppModel): Seq[Issue]
```

All thresholds must be read via `propLong` / `propDouble` (not hardcoded) to make them configurable at runtime via `spark.sparklens.*` properties. The `Analyzer` base trait provides `percentile`, `concentration`, `fmtBytes`, `fmtMs`, `fmtDouble`, and the `MB`/`GB` constants.

**Dual plan parsing pattern** — analyzers that inspect SQL plans must handle two cases:
1. `sql.planTree` (structured `PlanNode` tree built from `SparkPlanInfo`) — preferred; reflects AQE rewrites
2. `sql.physicalPlan` (text string) — fallback; used when planTree is absent

The standard idiom:
```scala
val hasSMJ = sql.planTree.fold(plan.contains("SortMergeJoin"))(
  _.nodesNamed("SortMergeJoin").nonEmpty
)
```

`PlanNode.nodesNamed` is exact match; `nodesContaining` is substring match. `flatten` is DFS over the whole subtree.

**Issue IDs** must end with `-${stageId}` or `-${executionId}` (a trailing integer). `Analyzers.group` strips the trailing `-N` to merge issues of the same type across stages/queries into a single report entry. If you break this convention, grouping silently stops working.

**`estimatedImpact`**: Use `ImpactEstimator.configRisk` only for config checks where no runtime data is available. For issues with actual measurements (bytes, task durations), compute a real `EstimatedImpact`. `timeOpt` and `bytesOpt` return `None` for zero values so the JSON output omits the field cleanly.

### Issue model fields

```scala
Issue(
  id,             // e.g. "skew-warn-3"  — must end in -<int> for grouping
  severity,       // Critical | Warning | Info
  category,       // "skew" | "spill" | "join" | "gc" | "io" | "plan" | "config" | "reliability"
  title,          // one line, shown first in text report
  description,    // one sentence explaining what was observed
  recommendation, // what to do about it
  configFix,      // Option[String]: spark property=value to set
  codeFix,        // Option[String]: code snippet
  affectedStages, // Seq[Int] — populated from stage.stageId
  affectedJobs,   // Seq[Int] — populated from sql.jobIds or job.jobId
  metrics,        // Map[String,String] — raw numbers shown in report and JSON
  estimatedImpact // Option[EstimatedImpact] — savedTimeMs, savedBytes, confidence
)
```

### Test fixtures

All analyzer tests use the hand-crafted helpers in `AnalyzerFixtures` (no Spark context needed):

```scala
// Build a minimal app with one stage and one executor
val a = app(
  stages    = Map(0 -> stage(stageId = 0, submitMs = Some(0L), completeMs = Some(30000L))
                   .copy(exactInputBytes = 200L * MB, hasExactAggregates = true)),
  executors = Map("0" -> executor(id = "0").copy(totalCores = 4)),
  props     = Map("spark.sparklens.io.ioFloorMbps" -> "1.0"),
)
```

Key fixture methods: `task()`, `stage()`, `job()`, `executor()`, `app()`, `sqlExec()`, `planNode()`.

### Plan text parsing fragility

Several checks in `PlanAnalyzer` and `JoinAnalyzer` depend on the exact format of Spark's FORMATTED physical plan text. The two fragile markers called out in `build.sbt` comments are:
- `"Statistics(sizeInBytes="` — CBO stats check
- `"\n\n("` — boundary between plan tree section and per-node detail section

If the Spark version is bumped, validate these checks against a real `FORMATTED` plan dump from the new version. `PlanAnalyzerSpec` contains the exact plan strings used as test cases.

### Adding a new analyzer

1. Create `src/main/scala/.../analyzers/MyAnalyzer.scala` as an `object` extending `Analyzer`
2. Create `src/test/scala/.../analyzers/MyAnalyzerSpec.scala`
3. Add `MyAnalyzer` to the `all` list in `Analyzers.scala`

The order in `Analyzers.all` is the execution order. Issues are re-sorted after grouping by `(severity.order, -savedTimeMs)`, so position in `all` only affects tie-breaking within the same estimated savings.
