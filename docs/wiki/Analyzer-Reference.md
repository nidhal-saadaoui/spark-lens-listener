# Analyzer Reference

SparkLens runs **28 analyzers** in the order listed below. Each analyzer is independent — it reads from the immutable `SparkAppModel` snapshot and emits zero or more issues. Issues are then grouped by type, cross-referenced for root-cause overlap, and sorted by severity + estimated savings.

---

## 1. JobTimelineAnalyzer

**Category:** `io`  
**Issue IDs:** `timeline-gap-{prev}-{next}`, `timeline-fragmentation`, `driver-stage-gap-{stageId}`

Detects idle time in the application timeline where Spark executors sit unused.

| Issue | Severity | Fires when |
|---|---|---|
| Inter-job gap | Warning | Idle gap between consecutive jobs > 60s (`spark.sparklens.timeline.gapWarnMs`) |
| Job fragmentation | Info | ≥70% of jobs complete in <2s AND total jobs ≥50 |
| Intra-job stage gap | Warning | Driver idles >10s between consecutive stages within a job (`spark.sparklens.timeline.stageGapWarnMs`) |

**Root cause:** Driver blocking on `collect()`, Python logic, external API calls, or heavyweight computation between actions.

**Fix:** Move driver-side computation into distributed stages; replace `collect() → transform → re-distribute` with `df.map(transform)`; batch small operations into a single action.

---

## 2. SkewAnalyzer

**Category:** `skew`  
**Issue IDs:** `skew-warn-{stageId}`, `skew-crit-{stageId}`, `skew-warn-exchange-{hash}-{execId}`, `skew-crit-exchange-{hash}-{execId}`

Detects data skew via task duration distribution and per-partition byte imbalance in shuffle Exchange nodes.

| Issue | Severity | Fires when |
|---|---|---|
| Task duration skew | Critical | p95/p50 ≥8× OR top-5% tasks hold ≥50% of run time |
| Task duration skew | Warning | p95/p50 ≥3× OR top-5% tasks hold ≥25% of run time |
| Exchange byte skew | Critical | top-5% partitions hold ≥50% of shuffle bytes |
| Exchange byte skew | Warning | top-5% partitions hold ≥25% of shuffle bytes |

**Requires:** ≥10 tasks sampled (`spark.sparklens.skew.minTasks`); p50 ≥500ms for duration signals.

**Fix:**
- Enable `spark.sql.adaptive.skewJoin.enabled=true` (AQE auto-splits hot partitions)
- Apply two-phase key salting: add random suffix before aggregation, strip after
- Use `repartition(n, key)` to distribute data more evenly

---

## 3. TaskOverheadAnalyzer

**Category:** `io`  
**Issue IDs:** `task-overhead-{stageId}`  
**Severity:** Warning

Fires when JVM task deserialization overhead (measured from `executorDeserializeTime`) accounts for ≥30% of total executor run time. This is the signature of **too many tiny tasks** — JVM startup cost dominates actual work.

**Fix:** Increase `spark.sql.files.maxPartitionBytes` (256 MB recommended); enable `spark.sql.adaptive.coalescePartitions.enabled=true`; coalesce partitions after shuffles to target ≥128 MB per task.

---

## 4. SpillAnalyzer

**Category:** `spill`  
**Issue IDs:** `spill-{stageId}`

| Severity | Fires when |
|---|---|
| Critical | Disk spill ≥1 GB (`spark.sparklens.spill.critDiskMb`) |
| Warning | Disk spill ≥100 MB (`spark.sparklens.spill.warnDiskMb`) |

Uses avg task peak execution memory vs executor heap to produce a concrete memory recommendation. Savings estimated at 200 MB/s disk speed (configurable).

**Fix (typical):**
```
spark.sql.adaptive.enabled=true
# or: spark.executor.memory=<computed from avg peak memory × 2>
```

> **Note:** If the spill is caused by `coalesce(1)`, fixing the coalesce also fixes the spill — check for a `single-task` issue on the same stage.

---

## 5. JoinAnalyzer

**Category:** `join`  
**Issue IDs:** `join-broadcast-disabled-{execId}`, `join-large-broadcast-{execId}`, `join-excessive-shuffle-{execId}`, `join-exploding-{execId}`

| Issue | Severity | Fires when |
|---|---|---|
| Broadcast disabled | Info | `spark.sql.autoBroadcastJoinThreshold=-1` with a SortMergeJoin present |
| Oversized broadcast | Warning | Broadcast threshold ≥1 GB — risk of driver OOM |
| Excessive shuffles | Warning | ≥4 non-broadcast Exchange nodes in the plan |
| Exploding join | Warning | Output bytes >5× input bytes (likely many-to-many join on non-unique key) |

Uses `planTree` (the AQE-updated final plan) when available; falls back to text search. Savings capped at SQL execution duration.

**Fix:** Re-enable auto-broadcast; use `df.hint("broadcast")`; restructure queries to reduce shuffle count; add a selective filter before a fan-out join; use semi-joins for existence checks.

---

## 6. GcAnalyzer

**Category:** `gc`  
**Issue IDs:** `gc-{stageId}`

| Severity | Fires when |
|---|---|
| Critical | GC time ≥20% of executor run time (`spark.sparklens.gc.critFraction`) |
| Warning | GC time ≥10% of executor run time (`spark.sparklens.gc.warnFraction`) |

Requires ≥10s total executor run time. Savings estimated as `stage.durationMs × gcFraction` (wall-clock fraction).

**Fix:**
```
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
# AND increase spark.executor.memory
```

Reduce object churn: use primitive arrays, avoid boxed collections, don't hold large intermediate DataFrames in memory.

---

## 7. CacheAnalyzer

**Category:** `cache`  
**Issue IDs:** `cache-{hash}`, `cache-sql-{hash}`

Detects redundant re-scans of the same dataset across multiple jobs or SQL executions.

| Issue | Severity | Fires when |
|---|---|---|
| RDD repeated scan | Warning | Same RDD name scanned in ≥2 jobs without a `cache()` call |
| SQL repeated scan | Warning | Same table scanned ≥5 times across SQL executions (`spark.sparklens.cache.sql.minExecCount`) AND avg table size <5 GB |
| SQL repeated scan (large) | Info | Same as above but avg size >5 GB — warn about memory cost |

**Fix:**
```scala
val cached = df.persist(StorageLevel.MEMORY_AND_DISK)
cached.count()  // materialise
// ... use cached in multiple jobs ...
cached.unpersist()
```

---

## 8. PreemptionAnalyzer

**Category:** `preemption`  
**Issue IDs:** `preemption-executor-lost-0`, `preemption-killed-{stageId}`

| Issue | Severity | Fires when |
|---|---|---|
| Executor lost | Warning | Any executor removed mid-job (reason contains: lost, kill, preempt, timeout, heartbeat) |
| High task kill rate | Warning | ≥5% of tasks killed in a stage (`spark.sparklens.preemption.killedTaskRateWarn`) |

The fix recommendation varies by removal reason:

| Reason pattern | Recommended fix |
|---|---|
| `killed by driver`, `blacklist` | Investigate task failures; adjust `spark.blacklist.task.maxTaskAttemptsPerExecutor` |
| `lost`, `heartbeat`, `timeout` | Increase `spark.network.timeout` and `spark.executor.heartbeatInterval` |
| `container`, `overhead`, `memory limit` | Increase `spark.yarn.executor.memoryOverheadFactor=0.2` |
| Other | Generic YARN/K8s memory overhead advice |

---

## 9. PlanAnalyzer

**Category:** `plan`  
**Issue IDs:** `plan-cartesian-{execId}`, `plan-window-nopart-{execId}`, `plan-roundrobin-{execId}`, `plan-nocbo-{execId}`, `plan-explode-{execId}`, `plan-slow-compile-{execId}`

Inspects SQL physical plans for structural anti-patterns.

| Issue | Severity | Fires when |
|---|---|---|
| Cartesian product | Critical | `CartesianProduct` node in plan |
| Window without PARTITION BY | Warning | Window function with `Exchange SinglePartition` descendant |
| Round-robin repartition | Info | `RoundRobinPartitioning(N)` in plan |
| Missing CBO statistics | Info | SortMergeJoin present but `rowCount` absent from Statistics |
| Row explosion | Warning | Output records or bytes >5× input (`spark.sparklens.plan.explodeRatio`) |
| Slow compilation | Warning | >5s gap between SQL start and first job launch |

---

## 10. UdfAnalyzer

**Category:** `plan`  
**Issue IDs:** `plan-udf-{execId}`  
**Severity:** Warning

Detects Python UDFs (`PythonUDF`, `BatchEvalPython`, `ArrowEvalPython`) and opaque Scala UDFs in the physical plan.

**Why it matters:** UDFs bypass Catalyst optimizations (predicate pushdown, column pruning, Tungsten). Python UDFs require row-by-row JVM↔Python serialization.

**Fix:** Replace with native `org.apache.spark.sql.functions` equivalents. For Python, use pandas UDFs (Arrow-based) which process batches rather than rows.

---

## 11. IoClassifierAnalyzer

**Category:** `io`  
**Issue IDs:** `io-bound-{stageId}`  
**Severity:** Info

Identifies stages that are I/O-bound (throughput ≥3 MB/s per core). These stages spend more time moving data than computing.

**Fix:** Apply predicate pushdown and column pruning; use columnar formats (Parquet/ORC); enable AQE partition coalescing; cache if the dataset is read multiple times.

---

## 12. ConfigAnalyzer

**Category:** `config`

Checks Spark configuration for known anti-patterns. All issues use `configRisk` (low confidence, no quantifiable time savings — impact depends on workload).

| Issue ID | Severity | Fires when |
|---|---|---|
| `config-aqe-disabled` | Warning | AQE not enabled — **Spark 3.x only** |
| `config-java-serializer` | Warning | Java serializer in use instead of Kryo |
| `config-default-shuffle-partitions` | Info | `shuffle.partitions=200` (default) AND AQE disabled |
| `config-low-memory-overhead` | Info | `memoryOverheadFactor` <15% with no explicit overhead set |
| `config-aqe-skew-disabled` | Info | AQE on but `skewJoin.enabled` not true — **Spark 3.x only** |
| `config-small-shuffle-buffer` | Info | `spark.shuffle.file.buffer` <64 KB (default 32 KB causes excessive syscalls) |
| `config-cbo-histogram-disabled` | Info | Column histograms disabled — poor join ordering |
| `config-low-task-max-failures` | Warning | `spark.task.maxFailures` <3 |
| `config-high-locality-wait` | Info | `spark.locality.wait` >5s (unnecessary on cloud storage) |
| `config-max-reqs-in-flight` | Info | Unlimited fetch requests with >200 cores (connection storm risk) |
| `config-parallelism-mismatch` | Info | `default.parallelism` and `shuffle.partitions` differ by >2× |

---

## 13. ExecutorSizingAnalyzer

**Category:** `config`  
**Issue IDs:** `executor-memory-underprovisioned`, `executor-memory-overprovisioned`, `driver-memory-underprovisioned`, `cluster-cores-overprovisioned`

Analyses whether executor memory, driver memory, and cluster size are appropriate for the observed workload. Uses p95 task peak memory across **multi-task stages** (single-task coalesce stages excluded to prevent inflation).

| Issue | Severity | Fires when |
|---|---|---|
| Executor under-provisioned | Warning (if spill), Info | Peak concurrent demand >85% of execution pool |
| Executor over-provisioned | Info | Peak concurrent demand <25% of pool AND no spill AND memory >2 GB |
| Driver under-provisioned | Critical (>80%), Warning (>40%) | Task results exceed 40% of driver heap |
| Cluster over-provisioned | Info | Widest stage uses <30% of total cores |

---

## 14. SmallFilesAnalyzer

**Category:** `io`  
**Issue IDs:** `small-files-{stageId}`  
**Severity:** Warning

Flags stages reading many small files (average input per task <64 MB from a 128 MB target). Small files cause excessive metadata operations.

**Fix:** Compact source files: Delta `OPTIMIZE`, Hudi compaction, Iceberg `rewrite_data_files`. Increase `spark.sql.files.maxPartitionBytes=268435456` (256 MB).

---

## 15. OutputSmallFilesAnalyzer

**Category:** `io`  
**Issue IDs:** `output-small-files-{stageId}`  
**Severity:** Warning

Flags stages writing many small output files. Small output files create problems for downstream readers.

**Fix:**
```scala
df.coalesce(targetTasks).write.parquet(outputPath)
// or on Delta: OPTIMIZE table_name
```

---

## 16. ShuffleLocalityAnalyzer

**Category:** `io`  
**Issue IDs:** `shuffle-locality-{stageId}`  
**Severity:** Warning

Fires when >70% of shuffle bytes are fetched from remote executors (cross-rack or cross-AZ reads are 3–10× slower than local disk).

**Fix:**
```
spark.shuffle.service.enabled=true
spark.io.compression.codec=lz4
spark.shuffle.compress=true
```
Pin executors to a single AZ to eliminate cross-AZ network costs.

---

## 17. DriverBottleneckAnalyzer

**Category:** `io`  
**Issue IDs:** `driver-result-{stageId}`, `driver-collect-limit-{execId}`

| Issue | Severity | Fires when |
|---|---|---|
| Large collect | Critical (≥500 MB), Warning (≥50 MB) | Task results sent to driver exceed threshold |
| Driver aggregation | Warning | `CollectLimit` or `TakeOrderedAndProject` in SQL plan |

**Fix:**
```scala
// Instead of:
val rows = df.collect()
// Prefer:
df.write.parquet("s3://bucket/output/")
```

---

## 18. CpuEfficiencyAnalyzer

**Category:** `io`  
**Issue IDs:** `cpu-{stageId}`  
**Severity:** Info

Fires when CPU time / executor run time <20% over a 30s minimum stage duration. Indicates executors are mostly waiting (I/O, shuffle network, JVM overhead) rather than computing.

**Savings estimated as:** `stage.durationMs × (1 - cpuFraction)` — wall-clock time recoverable if the bottleneck is eliminated.

**Fix:** Identify the bottleneck from stage metrics: shuffle-dominant → reduce shuffles or enable compression; input-dominant → compact source files; Python → switch to pandas (Arrow) UDFs.

---

## 19. SpeculationAnalyzer

**Category:** `config`  
**Issue IDs:** `speculation-active`, `speculation-configured-not-firing`

| Issue | Severity | Fires when |
|---|---|---|
| Speculation active on write stage | **Critical** | `spark.speculation=true` AND speculative tasks ran AND a write stage is present (insertInto, .write., saveAsTable) |
| Speculation active (compute only) | Warning | `spark.speculation=true` AND speculative tasks ran |
| Configured but not firing | Info | `spark.speculation=true` AND no speculative tasks launched |

> **Critical risk:** Speculative tasks on write stages can commit duplicate rows to non-idempotent sinks (Hive, JDBC). Set `spark.speculation=false` on any job with a write stage, or ensure your sink is idempotent.

---

## 20. StageFailureAnalyzer

**Category:** `reliability`  
**Issue IDs:** `job-failed-{jobId}`, `stage-retry-{stageId}`, `task-failure-{stageId}`

| Issue | Severity | Fires when |
|---|---|---|
| Job failed | Critical | `job.status == FAILED` |
| Stage retried | Warning | `stage.attemptId > 0` |
| High task failure rate | Warning | ≥5% of tasks failed in a stage |

**Fix:** Check driver logs for the root exception. Common causes: executor OOM (increase memory), shuffle fetch failures, transient HDFS/S3 errors, task `maxFailures` too low.

---

## 21. MemoryPressureAnalyzer

**Category:** `reliability`  
**Issue IDs:** `memory-pressure-{stageId}`  
**Severity:** Critical

A **composite** signal — fires only when BOTH high GC (≥10%) AND significant disk spill (≥100 MB) are present in the same stage. This combination strongly indicates the executor heap is genuinely undersized.

**Fix:**
```
spark.executor.memory=<computed from avgPeakMemory × 2>
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=2g
```

---

## 22. StageParallelismAnalyzer

**Category:** `io`  
**Issue IDs:** `single-task-{stageId}`, `low-parallelism-{stageId}`

| Issue | Severity | Fires when |
|---|---|---|
| Single-task stage | Warning | `numTasks == 1` AND stage duration ≥5s |
| Low parallelism | Info | Task count <50% of cluster cores AND duration ≥10s (single-task stages excluded) |

For single-task stages the analyzer walks the SQL plan to identify the root cause:

| Plan pattern | Cause | Recommended fix |
|---|---|---|
| `Coalesce 1` | `coalesce(1)` call | Remove or replace with `coalesce(N)` |
| `RoundRobinPartitioning(1)` | `repartition(1)` call | Replace with `repartition(N)` |
| `Exchange SinglePartition` | Global sort or unpartitioned aggregate | Add `PARTITION BY`; enable AQE |
| `CollectLimit` | `LIMIT` in write path | Remove LIMIT or filter upstream |
| `TakeOrderedAndProject` | `ORDER BY … LIMIT` | Use bucketed tables; sort on read |

---

## 23. LongStageAnalyzer

**Category:** `reliability`  
**Issue IDs:** `long-stage-{stageId}`  
**Severity:** Warning

Fires when a stage duration is >5× the job's median stage duration AND ≥30s. Long outlier stages serialize downstream execution and inflate total wall-clock time.

**Fix:** Check SkewAnalyzer results for the same stage; check for spill; enable AQE to optimize join strategies; broadcast the smaller join side.

---

## 24. PartitionImbalanceAnalyzer

**Category:** `io`  
**Issue IDs:** `partition-imbalance-{stageId}`

| Severity | Fires when |
|---|---|
| Warning | p95/p50 input bytes ratio ≥5× |
| Info | p95/p50 input bytes ratio ≥3× (`spark.sparklens.partition.imbalance.p95p50Ratio`) |

Requires ≥10 tasks with input data AND ≥100 MB total input.

**Fix:**
```
spark.sql.files.maxPartitionBytes=67108864  # 64 MB for finer-grained partitioning
```
Or: `repartition(n, partitionKey)` to enforce even distribution.

---

## 25. SchedulerDelayAnalyzer

**Category:** `config`  
**Issue IDs:** `scheduler-delay-{stageId}`

| Severity | Fires when |
|---|---|
| Warning | Median task launch delay ≥5s |
| Info | Median task launch delay ≥2s (`spark.sparklens.schedulerDelay.warnMs`) |

Task launch delay = time from stage submission to task first-run on an executor. Requires ≥5 sampled tasks.

**Fix:**
```
spark.locality.wait=0s   # cloud storage has uniform access time — no benefit in waiting
# OR enable dynamic allocation to release idle executors that cause congestion
spark.dynamicAllocation.enabled=true
```

---

## 26. CriticalPathAnalyzer

**Category:** `plan`  
**Issue IDs:** `critical-path-serial-{tipStageId}`

| Severity | Fires when |
|---|---|
| Warning | Critical path ≥95% of app wall time |
| Info | Critical path 85–95% of app wall time |

Uses dynamic programming on the stage dependency graph to find the longest cumulative-duration chain of dependent stages, then identifies the single bottleneck stage.

> **Key insight:** Adding more executors cannot help when the bottleneck is data dependencies. Fix the bottleneck stage itself.

---

## 27. DynamicAllocationAnalyzer

**Category:** `config`, `reliability`  
**Issue IDs:** `dynalloc-no-shuffle-protection`, `dynalloc-executor-churn`, `dynalloc-yarn-oom-kill`, `dynalloc-maxexecutors-ceiling`, `dynalloc-scaleup-lag-{jobId}`

Only fires when `spark.dynamicAllocation.enabled=true`.

| Issue | Severity | Fires when |
|---|---|---|
| No shuffle protection | **Critical** | Neither `shuffleTracking.enabled` nor `shuffle.service.enabled` is set — removed executors lose their shuffle data, causing silent stage reruns |
| Executor churn | Warning | ≥25% of removed executors lived <30s |
| YARN OOM kill | Warning | Executor removed with exit code 137 (physical memory enforcer) |
| Max executors ceiling | Info | Peak concurrent executors hit the configured `maxExecutors` |
| Scale-up lag | Info | >60s idle between jobs causes >2s median launch delay in the next job |

---

## 28. YarnAnalyzer

**Category:** `config`, `reliability`, `io`  
**Issue IDs:** `yarn-queue-wait`, `yarn-vmem-oom-kill`, `yarn-hot-node-failure`, `yarn-shuffle-disk-full`, `yarn-pyspark-overhead-risk`

YARN-specific checks. The analyzer auto-detects YARN via the master URL or executor removal reason patterns.

| Issue | Severity | Fires when |
|---|---|---|
| Queue wait | Warning (≥120s), Info (≥30s) | Gap between app start and first executor allocation |
| Virtual memory OOM | Warning | Executor killed with "virtual memory" in reason |
| Hot node failure | Warning | One host accounts for ≥50% of task failures |
| Shuffle disk full | Warning | Task errors contain disk-full patterns (ENOSPC, DiskSpaceException) |
| PySpark low overhead | Warning | Python UDFs detected AND `memoryOverheadFactor` <20% |

For PySpark jobs with pandas/Arrow UDFs, set `spark.yarn.executor.memoryOverheadFactor=0.4` — Python subprocess + Arrow buffers + cloudpickle all count against the YARN container limit.
