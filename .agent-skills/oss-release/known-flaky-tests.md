# Known Flaky / Pre-existing CI Failures

When monitoring CI for a release, failures listed here should be treated as **pre-existing or
infrastructure flakiness** — they do not block a release unless they were passing before your
changes and are now failing.

Always verify: check recent run history for the same workflow to confirm the pattern.

One entry below (`No solution found when resolving dependencies`) is not flakiness at all but a
**release-timing race in our own tooling** — it is documented here because it surfaces as a
connector-test failure and has a specific, non-obvious remedy.

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

## Connector tests — `No solution found when resolving dependencies`

**This is not connector flakiness.** It is a release-timing race in our own tooling, listed here
because that is where a release engineer looks first. The connector code and the tests are fine —
the wheel simply wasn't resolvable from pypi yet when the job ran.

- **Workflow:** `Nightly Connector Tests` (`acryldata/connector-tests` → `nightly_tests.yaml`),
  dispatched by `prep` Step 6
- **Failure signature:** the install step fails immediately with

  ```
  + uv pip install --build-constraint .../build-constraints.txt --constraint .../constraints.txt \
      'acryl-datahub[testing-utils,bigquery,bigquery-usage]==<version>'
  error: No solution found when resolving dependencies
  ```

- **Cause:** `pypi-release metadata-ingestion` reporting `success` only means `twine upload`
  returned. Pypi's index and CDN edges converge a little later, so a dispatch fired right after
  the workflow succeeds can resolve against an index that doesn't list the version yet.
- **Pattern:** only ever happens within minutes of a release; **a re-run of the same job
  succeeds** with no code change. Jobs for older versions never show it.
- **Mitigation already in place:** `wait-for-pypi-release.sh` enforces a settle window
  (`PYPI_SETTLE_SECONDS`, default 120s after the pypi-release run completed) and
  `dispatch-connector-tests.sh` refuses to dispatch until that gate passes. This reduces the
  race; it does not eliminate it.
- **Remedy:** **re-run the failed connector-test jobs.** Do NOT re-cut the RC, do NOT open a
  connector bug, and do NOT block stable promotion on this first failure — promote once the
  re-run is green.
- **How to verify it's this and not a real dependency problem:** the version resolves when you
  try it yourself (`pip index versions acryl-datahub` / `pip download acryl-datahub==<version>`),
  and only install steps failed — no test assertions did.

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
