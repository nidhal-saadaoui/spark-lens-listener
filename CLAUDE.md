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
Analyzers.runAll              ← runs all 29 analyzers, groups + sorts issues
    │
    ▼
Reporter (Text / JSON / HTML) ← writes report to stdout or file path
```

**SparkLensListener** is the Spark `extraListeners` entry point. It delegates every listener event to `SparkAppModelBuilder` and at `onApplicationEnd` calls `builder.build()`, then `Analyzers.runAll()`, then all active reporters.

`spark.sparklens.output` accepts a comma-separated list of formats (`text,json`, `log,json`, etc.). Each format is emitted independently. Path resolution per format: `spark.sparklens.report.path.<format>` → base `spark.sparklens.report.path` + extension → stdout (or driver logger for `log`). The `log` format without a path writes through the Java logger so it appears inline in the Spark driver log. SQL plan events arrive via `onOtherEvent` (not typed listener methods) because `SparkListenerSQLExecutionStart` etc. live in the SQL module.

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

All thresholds must be read via `propLong` / `propDouble` (not hardcoded) to make them configurable at runtime via `spark.sparklens.*` properties. The `Analyzer` base trait provides `percentile`, `concentration`, `median`, `fmtBytes`, `fmtMs`, `fmtDouble`, `treeSection`, `checkPlan`, `majorVersion`, and the `MB`/`GB` constants.

**Dual plan parsing pattern** — analyzers that inspect SQL plans must handle two cases:
1. `sql.planTree` (structured `PlanNode` tree built from `SparkPlanInfo`) — preferred; reflects AQE rewrites
2. `sql.physicalPlan` (text string) — fallback; used when planTree is absent

Use the `checkPlan` helper from `Analyzer` (preferred):
```scala
val hasSMJ = checkPlan(sql,
  textCheck = treeSection(sql.physicalPlan).contains("SortMergeJoin"),
  treeCheck = _.nodesNamed("SortMergeJoin").nonEmpty,
)
```

Or the manual fold idiom (equivalent):
```scala
val hasSMJ = sql.planTree.fold(plan.contains("SortMergeJoin"))(
  _.nodesNamed("SortMergeJoin").nonEmpty
)
```

`PlanNode.nodesNamed` is exact match; `nodesContaining` is substring match. `flatten` is DFS over the whole subtree. `treeSection` strips the per-node detail blocks below `\n\n(` so text searches stay anchored to the plan tree.

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
  estimatedImpact, // Option[EstimatedImpact] — savedTimeMs, savedBytes, confidence
  relatedIds,     // Seq[String] — IDs of issues sharing affected stages (populated by Analyzers.linkRelated)
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

## Documentation update rule

**After every code change, verify and update all of the following before committing:**

### README.md
- `## What it detects` table — one row per analyzer; add new analyzers, remove deleted ones
- `## Configuration` table — every `spark.sparklens.*` property exposed by the change must appear here with its default value and description
- `## Configurable thresholds` table — every new threshold property must be listed
- JSON schema example — reflect any new top-level fields added to `JsonReporter`
- Version references — update when cutting a release (search for the previous version string)

### CHANGELOG.md
- Every PR/commit that changes behaviour (new analyzer, new check, bug fix, output change, new config property) gets an entry under the current unreleased version block
- New analyzers: describe the signal, threshold, and severity
- Bug fixes: describe what was wrong and what changed
- Breaking changes (JSON field renamed, output format change): mark explicitly

### docs/wiki/ pages
| Page | When to update |
|---|---|
| `Interpreting-the-Report.md` | New output fields, changed scoring, new savings formulas, new report sections |
| `Deployment-Guide.md` | New config properties that affect deployment, new supported platforms |
| `Troubleshooting.md` | New known failure modes, new edge cases discovered during implementation |

### Accuracy checks — run these mentally after every change
1. **Analyzer count** — `Analyzers.all.size` must match every mention of "N analyzers" in README, CHANGELOG, and this file
2. **Config properties** — every `propLong`/`propDouble` call with a `spark.sparklens.*` key must have a corresponding row in the README configuration tables
3. **JSON schema** — every field added to or removed from `JsonReporter.render` must be reflected in the README JSON example
4. **Issue categories** — if a new `category` string is introduced, add it to the `category` comment in the Issue model fields section above
5. **Version consistency** — `spark_lens_version` in the JSON example, `--packages` version in README, and the latest CHANGELOG heading must all match when releasing

## Testing requirement

**Before committing any new feature or analyzer, run the full test suite and integration test to verify no regressions:**

```bash
# 1. Run all unit tests (both Scala versions — required before any commit)
sbt "+test"

# 2. Build the assembly JAR
sbt "++2.12.20 assembly"

# 3. Run the integration test against the demo job
SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")
JAR=$(ls target/scala-2.12/spark-lens_2.12-*-assembly.jar | head -1)
${SPARK_HOME}/bin/spark-submit \
  --master "local[2]" \
  --driver-class-path "${JAR}" \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text,json \
  --conf spark.sparklens.report.path.text=/tmp/spark-lens-report.txt \
  --conf spark.sparklens.report.path.json=/tmp/spark-lens-report.json \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sparklens.cache.sql.minExecCount=3 \
  --conf spark.sparklens.stageParallelism.singleTaskMinMs=500 \
  --conf spark.sparklens.io.minDurationMs=1000 \
  --conf spark.sparklens.io.ioFloorMbps=0.1 \
  integration-test/demo_job.py

# 4. Verify report contains expected issues (quick sanity check)
grep -q "Cartesian Product" /tmp/spark-lens-report.txt
grep -q "Broadcast Join Disabled" /tmp/spark-lens-report.json
grep -q "Executor scaling analysis" /tmp/spark-lens-report.json
```

**Failure modes to watch:**
- Unit test failures → fix the bug immediately; do not commit
- Assembly build failures → likely Scala 2.12 incompatibility; check for 2.13+ only APIs
- Integration test failures → likely Python/Spark version incompatibility; check CloudPickle and PySpark compatibility
- Missing expected issues in report → verify the analyzer is registered in `Analyzers.all` and the issue ID naming convention is followed (must end with `-<int>` for grouping)
- JSON report missing → verify `spark.sparklens.output=...json` and `spark.sparklens.report.path.json=...` are set

**This is non-negotiable.** Every commit must pass `sbt "+test"` and the integration test must generate a valid report with expected issue types. No exceptions.
