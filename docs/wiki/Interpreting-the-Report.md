# Interpreting the Report

Understanding what the numbers mean — and what they don't — is as important as reading the issues themselves.

---

## Health score

The health score is a simple linear deduction from 100:

| Severity | Deduction per issue |
|---|---|
| Critical | −30 pts |
| Warning | −10 pts |
| Info | −2 pts |

**Examples:**
- 1 Critical → 70/100
- 1 Critical + 5 Warning → 20/100
- No issues → 100/100

The score is floored at 0. There are no per-category caps — many Critical issues can push the score all the way to zero.

> **Note:** The score reflects the number of detected issues, not their root-cause complexity. Five issues caused by a single `coalesce(1)` call all count independently. After fixing the root cause, all five issues disappear and the score jumps.

---

## Priority fixes

The priority fixes section at the top of the text report shows the top 20 issues (configurable via `spark.sparklens.report.maxPriorityFixes`) ranked by **estimated wall-clock savings**, regardless of severity.

```
Priority fixes (estimated savings per run):
  1. [WARNING] Single-Task Stage 1 — Entire Stage Runs on One Executor  ~16.7min  (68% of app time)
  2. [INFO]    Low CPU Utilization in Stage 2 — 2% CPU [+1 more stages]  ~6.9min
  3. [CRITICAL] Disk Spill in Stage 1 (insertInto…)  ~1.5min  (6% of app time)
```

The `(X% of app time)` label is shown when savings are ≤100% of app duration. When an issue's savings estimate exceeds the total app duration (which can happen for I/O-based estimates), the percentage is suppressed to avoid misleading output.

---

## Estimated savings — what they mean

Every savings estimate is a model approximation. Read them as relative comparisons ("fixing this issue saves more than fixing that one") rather than precise predictions.

### How savings are computed

| Analyzer | Savings formula |
|---|---|
| SpillAnalyzer | `min(diskBytes / diskSpeedMbps, stage.durationMs)` — I/O penalty, capped at stage duration |
| CpuEfficiencyAnalyzer | `stage.durationMs × (1 − cpuFraction)` — recoverable wall-clock time |
| GcAnalyzer | `stage.durationMs × gcFraction` — wall-clock fraction lost to GC |
| TaskOverheadAnalyzer | `stage.durationMs × deserializeRatio` — wall-clock fraction lost to JVM setup |
| StageParallelismAnalyzer (single-task) | `stageDuration × (N−1)/N` — gain from parallelizing across N cores |
| JoinAnalyzer | `min(networkMs(shuffleBytes, speed), sqlDurationMs)` — capped at SQL execution time |
| PreemptionAnalyzer (killed tasks) | `stage.durationMs × killedFraction` — fraction of stage time wasted on killed work |
| Config checks | No savings — uses `configRisk` (confidence: low) |

### Caveats

1. **I/O-based estimates are rough.** `diskSpeedMbps` and `networkSpeedMbps` are configurable assumptions. Your cluster's actual throughput may differ.

2. **Savings don't account for concurrency.** If stages run in parallel, fixing one doesn't reduce wall-clock time by the full amount — only the critical path matters.

3. **Grouped-issue savings are summed.** When multiple stages of the same type are merged (e.g., `cpu [+1 more stages]`), the displayed savings is the sum across all stages. This is the total potential gain if ALL affected stages are fixed.

4. **Overlapping savings.** Multiple issues can share the same root cause. Fixing `coalesce(1)` simultaneously eliminates the single-task issue, the spill, the low-CPU, and the low-parallelism issues. The combined savings in the priority list appear additive but the actual gain is from one fix.

---

## Grouping and the `[+N more stages]` label

When the same type of issue fires on multiple stages, SparkLens merges them into one report entry:

- **Title:** `Low CPU Utilization in Stage 2 — 2% CPU [+1 more stages]`
- **Affected stages:** `2, 5`
- **Savings:** sum of savings from both stages

The first stage listed in the title is the most severe or earliest. The merged savings represents the total potential gain across all affected stages.

---

## Root cause linking (`relatedIds`)

In the text and JSON reports, issues that share affected stages and both have quantifiable savings show a `relatedIds` cross-reference:

```
  [✖ CRITICAL]  Disk Spill in Stage 1
    ...
    note:   likely shares root cause with: single-task, cpu
```

This note means: fixing the root cause (likely `coalesce(1)`) will probably resolve all three issues simultaneously. You don't need to address each independently.

---

## Quick wins

The quick wins section groups issues by the Spark property needed to fix them:

```
Quick wins
    spark.sql.adaptive.enabled=true
       resolves 2 issues: spill-1, config-aqe-disabled
    spark.executor.memory=30.5g
       resolves 2 issues: executor-memory-underprovisioned, memory-pressure-1
```

When multiple analyzers recommend different values for the same property (e.g., SpillAnalyzer computes `12g` and ExecutorSizingAnalyzer computes `30.5g`), the higher value is shown — satisfying the most demanding requirement.

---

## Confidence levels

Each issue's `estimatedImpact` has a confidence field:

| Confidence | Meaning |
|---|---|
| `high` | Metric is directly observed (GC time, job duration, stage failure) |
| `medium` | Derived from observed data with a model assumption (disk speed, network speed) |
| `low` | Configuration risk — impact depends entirely on workload characteristics |

Low-confidence issues (config checks, fragmentation) are never shown in the text report's priority fixes section — they have no quantifiable `savedTimeMs`.

---

## Severity vs savings — which to act on first?

The priority fixes section already combines both signals (sorted by savings, but severity is displayed). As a general rule:

- **Critical issues:** Act immediately. These indicate active data correctness risk (speculation + write stage), complete stage failure, or memory pressure causing disk spill cascades.
- **Warning issues with high savings:** Act next. Single-task stages, significant spills, and memory under-provisioning usually have concrete, cheap fixes.
- **Info issues:** Optimize after the above. Config checks and small-file patterns are typically single-property changes with broad benefits.

---

## When "% of app time" is not shown

The percentage label is suppressed when estimated savings > total app duration. This can happen when:

- An I/O-based penalty (spill bytes / disk speed) is large relative to a short stage
- A grouped issue sums savings from many stages

In these cases the absolute time label (e.g., `~1.5min`) is still shown and is meaningful for comparison purposes.
