# Ingestion Test Conventions

Rules for `metadata-ingestion/tests/`. Commands: `metadata-ingestion/AGENTS.md`. Test
principles shared by every module (value over coverage, anti-patterns to avoid): root
`AGENTS.md`, "Testing Strategy".

## Where things go

- `tests/unit/`: fast, no Docker, no network. `tests/integration/`: Docker or recorded
  services. `tests/conftest.py` adds the `integration` marker and assigns batches by weight;
  do not hand-write `integration_batch_N` unless pinning with `integration_no_balance`.
- Mirror the source tree and split by concern (`test_<source>_config.py`, `_lineage.py`,
  `_usage.py`) instead of growing one module.
- Shared fakes and helpers: `tests/test_helpers/` (`MockDataHubGraph` in
  `graph_helpers.py`, `run_and_get_pipeline` in `state_helpers.py`, `docker_helpers.py`).
  Reuse before writing a new one.
- Golden files: compare with `check_golden_file` / `check_goldens_stream`
  (`src/datahub/testing/mce_helpers.py`); regenerate with
  `../gradlew :metadata-ingestion:testSingle -PtestFile="tests/unit/x.py --update-golden-files"`
  (flag defined in `src/datahub/testing/pytest_hooks.py`). Review golden diffs; never
  regenerate blindly.
- Fixture data uses generic placeholder names (`my_db.my_schema.events`, `col_a`); no
  customer identifiers, ticket IDs or real credentials. Redaction tests use obvious
  placeholder secrets.

## A test must be able to fail

- Revert the fix locally and confirm the new test goes red before pushing.
- Never mock the unit under test. When an assertion depends on `report.failures` or
  `report.warnings`, use a real `SourceReport`: a `Mock(spec=SourceReport)` attribute is
  truthy, so `if report.failures:` passes vacuously.
- No vacuous assertions (`[] == []`, a lone `is not None`). Assert the produced work units
  or aspects, or the report counters and `report.failures` / `report.warnings` entries.
- Every behaviour change ships a failure-path test: a denied or capped listing must leave
  `report.failures` non-empty; a dropped object must bump its counter.
- Assert on `title=` and counters, not exact message prose. Asserting on the `context`
  string alone passes even when `context=` is removed, because the exception text is
  appended to it.
- Never insert a function between a decorator such as `@time_machine.travel(...)` and its
  target. Renaming a test so pytest stops collecting it is a silent delete; grep the old name.

## Hygiene

- `pytest`, not `unittest`: plain classes or functions, `assert x`, `assert not x`,
  `assert result is None`. Imports at module top; an import inside a test function needs a
  stated reason (optional dependency, import side effect) or reviewers will ask for one.
- Keep tests concise; shared setup goes in fixtures, not copy-pasted blocks.
- Do not leak threads, processes or temp dirs; `tests/conftest.py` reports threads still
  alive at exit. Close native clients explicitly.
- Integration images must run on both arm64 and amd64 runners.
- Repo meta-checks live beside the code they guard: `tests/unit/test_package_data.py`,
  `tests/unit/test_source_doc_headings.py`, `tests/unit/test_ingestion_source_tiles_completeness.py`.
