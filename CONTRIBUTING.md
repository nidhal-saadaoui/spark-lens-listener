# Contributing to SparkLens Listener

Thank you for considering a contribution! This guide covers everything you need to get started.

## Ways to contribute

- Report bugs via [GitHub Issues](https://github.com/nidhal-saadaoui/spark-lens-listener/issues)
- Suggest new analyzers or improvements
- Submit pull requests for bug fixes or new features

## Development setup

**Requirements:** JDK 8+, SBT 1.x, Python + PySpark (for integration tests only)

```bash
git clone https://github.com/nidhal-saadaoui/spark-lens-listener.git
cd spark-lens-listener
sbt test
```

## Running tests

```bash
# Unit tests (both Scala versions — required before any PR)
sbt "+test"

# Single test class
sbt "testOnly com.github.saadaouini.sparklens.analyzers.SkewAnalyzerSpec"

# Integration test (requires PySpark)
SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")
JAR=$(ls target/scala-2.12/spark-lens_2.12-*-assembly.jar | head -1)
${SPARK_HOME}/bin/spark-submit \
  --master "local[2]" \
  --driver-class-path "${JAR}" \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  integration-test/demo_job.py
```

## Adding a new analyzer

1. Create `src/main/scala/.../analyzers/MyAnalyzer.scala` extending `Analyzer`
2. Implement `def analyze(app: SparkAppModel): Seq[Issue]`
3. Read thresholds via `propLong` / `propDouble` — never hardcode them
4. Create `src/test/scala/.../analyzers/MyAnalyzerSpec.scala` using `AnalyzerFixtures`
5. Register it in `Analyzers.scala` (`Analyzers.all` list)
6. Add a row to the `## What it detects` table in `README.md`
7. Document any new `spark.sparklens.*` properties in the `## Configuration` table

Issue IDs must end with `-<int>` (e.g. `"skew-warn-3"`) for the grouping logic to work.

## Pull request checklist

- [ ] `sbt "+test"` passes (both Scala versions)
- [ ] Integration test produces a valid report
- [ ] New `spark.sparklens.*` properties are documented in `README.md`
- [ ] `CHANGELOG.md` has an entry under the current unreleased block
- [ ] No hardcoded thresholds — all configurable via `propLong`/`propDouble`

## Code style

- Standard Scala formatting; no autoformatter enforced — match the style of surrounding code
- `-Xfatal-warnings` is enabled; all compiler warnings fail the build
- No comments unless the *why* is non-obvious

## License

By contributing you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).
