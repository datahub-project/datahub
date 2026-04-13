# Known Flaky / Pre-existing CI Failures

When monitoring CI for a release, failures listed here should be treated as **pre-existing or
infrastructure flakiness** — they do not block a release unless they were passing before your
changes and are now failing.

Always verify: check recent run history for the same workflow to confirm the pattern.

---

## `spark smoke test`

- **Workflow:** `spark smoke test` (release event trigger)
- **Failing since:** commit `244b6dcbac` (April 10, 2026) — predates v1.5.0.7
- **Pattern:** Fails on release-triggered runs; unrelated to metadata-ingestion or connector changes
- **How to verify:** `gh run list --repo acryldata/datahub --workflow "spark smoke test" --limit 10`
- **Safe to ignore if:** No changes to `metadata-ingestion-modules/spark-lineage/`, `spark-smoke-test/`, or Spark-related source

---

## `Metadata Ingestion` — `test_nifi_ingest_cluster`

- **Workflow:** `Metadata Ingestion` → `ci (3.11, testIntegrationBatch3)`
- **Failing test:** `tests/integration/nifi/test_nifi.py::test_nifi_ingest_cluster`
- **Failure mode:** Golden file mismatch — "Metadata files differ"
- **Pattern:** Intermittent; same commit will pass on a re-run. Push-triggered runs on the same SHA pass consistently.
- **How to verify:** Check if another run of `Metadata Ingestion` on the same commit succeeded: `gh run list --repo acryldata/datahub --workflow "Metadata Ingestion" --limit 10`
- **Safe to ignore if:** The same commit has at least one passing `Metadata Ingestion` run (any trigger)

---

## Adding New Entries

When you encounter a new CI failure during a release and determine it is pre-existing or flaky,
add an entry here with:

```markdown
## `<Workflow Name>` — `<test name if applicable>`

- **Workflow:** full workflow name
- **Failing since:** commit SHA and date
- **Pattern:** description of failure pattern
- **How to verify:** command to check history
- **Safe to ignore if:** specific conditions
```
