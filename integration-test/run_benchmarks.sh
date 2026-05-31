#!/usr/bin/env bash
# run_benchmarks.sh — execute all spark-lens before/after scenarios and compare
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORT_DIR=/tmp/spark-lens-benchmarks
mkdir -p "$REPORT_DIR"

# ── Resolve assembly JAR ──────────────────────────────────────────────────────
JAR=$(ls "$PROJECT_DIR"/target/scala-2.12/*assembly*.jar 2>/dev/null | tail -1)
if [[ -z "$JAR" ]]; then
  echo "ERROR: no assembly JAR found in $PROJECT_DIR/target/scala-2.12/" >&2
  echo "Build it first with: sbt '++2.12.20 assembly'" >&2
  exit 1
fi
echo "Using JAR: $JAR"

SCENARIOS="skew cache join_broadcast join_shuffles config_aqe config_serializer plan_cartesian plan_window driver_collect"

FAIL_COUNT=0
PASS_COUNT=0

for scenario in $SCENARIOS; do
  for phase in before after; do
    REPORT_PATH="$REPORT_DIR/${scenario}_${phase}.json"
    echo ""
    echo "=== Running $scenario / $phase ==="

    docker run --rm \
      -v "$JAR":/opt/spark-lens.jar:ro \
      -v "$SCRIPT_DIR/scenarios.py":/opt/scenarios.py:ro \
      -v "$REPORT_DIR":/opt/reports \
      apache/spark:3.5.5 \
      /opt/spark/bin/spark-submit \
        --master "local[2]" \
        --driver-class-path /opt/spark-lens.jar \
        --conf "spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener" \
        --conf "spark.sparklens.output=json" \
        --conf "spark.sparklens.report.path=/opt/reports/${scenario}_${phase}.json" \
        /opt/scenarios.py "$scenario" "$phase" \
      2>&1 | grep -v "^[0-9]\{2\}/[0-9]\{2\}/[0-9]\{2\}" | grep -v "^INFO" | grep -v "^WARN" | grep -v "^DEBUG" || true

    if [[ -f "$REPORT_PATH" ]]; then
      echo "  [OK] report: $REPORT_PATH"
      PASS_COUNT=$((PASS_COUNT + 1))
    else
      echo "  [WARN] no report produced for $scenario/$phase"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
  done
done

echo ""
echo "=== All scenarios complete: $PASS_COUNT produced reports, $FAIL_COUNT missing ==="
echo ""
echo "=== Comparison Report ==="
python3 "$SCRIPT_DIR/compare_reports.py" "$REPORT_DIR"
