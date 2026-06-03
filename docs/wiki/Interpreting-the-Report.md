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

> **Note:** The score uses root-cause clustering before scoring. Issues linked by `relatedIds` (i.e. they share affected stages and both have quantifiable savings) count as one cluster — fixing the root cause removes all of them at once. Without clustering, a single `coalesce(1)` causing spill + low-CPU + single-task + low-parallelism would deduct 4×10 = 40 pts for what is effectively one fix.

---

## Priority fixes

The priority fixes section at the top of the text report shows the top 20 issues (configurable via `spark.sparklens.report.maxPriorityFixes`) ranked by **estimated wall-clock savings**, regardless of severity.

Issues that share a root cause are **collapsed into a single entry** — the representative (highest savings) appears with a `(+N covered)` suffix showing how many co-located issues the same fix would also resolve.

```
Priority fixes (estimated savings per run):
  1. [WARNING] Single-Task Stage 1 — Entire Stage on One Executor  ~16.7min  (68%)  (+2 covered)
  2. [INFO]    Low CPU Utilization in Stage 4 — 2% CPU  ~6.9min
  3. [CRITICAL] Disk Spill in Stage 7 (insertInto…)  ~1.5min  (6% of app time)
```

In entry 1, `(+2 covered)` means the same fix (likely `repartition(N)`) also resolves 2 other issues — spill and low-parallelism — that are grouped under this entry. You do not need to act on each independently.

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

4. **Root-cause deduplication.** `total_estimated_savings_ms` (JSON) and the priority fixes list both deduplicate by root-cause cluster: for issues linked via `relatedIds`, only the **maximum** savings in each cluster contributes to the total (not the sum). The total is also capped at app duration. This means a `coalesce(1)` cluster with 4 issues each claiming 30 min reports 30 min, not 120 min. Individual issue `estimatedImpact.savedTimeMs` values are still the raw per-issue estimates.

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

---

## SparkLens self-instrumentation footer

Every text report ends with a footer line showing SparkLens's own cost:

```
  SparkLens: 2.5M task events · 42ms listener overhead  (0.0% of app time)
```

- **task events** — number of `onTaskEnd` callbacks processed on the driver
- **listener overhead** — total wall-clock time SparkLens spent inside those callbacks (nanoTime-measured, converted to ms)
- **% of app time** — only shown when ≥ 0.1%; suppressed for negligible values

The JSON report includes `"listener_overhead_ms"` for the same figure.

If overhead exceeds 5% of app duration, the driver log receives a `WARN` with the task count and a suggestion to disable the listener on that workload. For context: on a modern 16-core driver, processing one million tasks takes roughly 100–200 ms.
