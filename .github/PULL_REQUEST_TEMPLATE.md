## What does this PR do?

<!-- One paragraph summary -->

## Checklist

- [ ] `sbt "+test"` passes (both Scala 2.12 and 2.13)
- [ ] Integration test produces a valid report
- [ ] New `spark.sparklens.*` properties documented in `README.md`
- [ ] `CHANGELOG.md` updated under the current unreleased block
- [ ] No hardcoded thresholds (all read via `propLong`/`propDouble`)
- [ ] New analyzer registered in `Analyzers.all` and issue IDs end with `-<int>`

## Type of change

- [ ] Bug fix
- [ ] New analyzer
- [ ] Config / threshold change
- [ ] Output format change (JSON / text / HTML)
- [ ] Refactor / cleanup
- [ ] Docs
