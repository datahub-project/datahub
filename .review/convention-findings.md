# Convention Review — snowflake-openflow

**Scope**: `snowflake_openflow.py`, `snowflake_openflow_{config,models,query,report}.py`,
`tests/unit/snowflake_openflow/`, `tests/integration/snowflake_openflow/`,
`docs/sources/openflow/`. Full 15-row rubric re-run after commit `d7c3b96f7ea`
(retry-composition fix + SHOW-truncation warning + docs update).

## Critical

(none)

## High

(none)

## Medium

(none)

## Low

(none)

## Summary

**0 Critical / 0 High / 0 Medium / 0 Low.** All 15 rubric rows pass. Highlights of this
pass, given the prompt's focus on the two changes since the last review:

- **Retry composition (the bug this re-review targets) is fixed and verified.**
  `_query_rows` is a bare seam again; `_query_rows_with_retry` (used by the inventory
  SHOW and `_paged_history`) and the two independent `_retrying()` call sites in
  `_read_connector_url` / `_read_connector_config` each wrap exactly one layer. I
  independently reran `test_stage_get_retries_a_transient_connection_error` and the
  "retried at exactly one layer, not nested" assertion in `test_source.py` — both pass.
  Nesting would have doubled worst-case attempts and, more importantly, would have let
  a retry re-enter `_download_connector_config`'s per-attempt `TemporaryDirectory` from
  underneath, defeating the guard against a partial file leaking into the next attempt.
- **`_warn_if_show_truncated`** is well-scoped: constant `title`/`message` (LiteralString
  compliant), dynamic `object_type` in `context`, boundary-tested at and just below
  `_SHOW_ROW_CAP` (both tests pass).
- **Docs**: the two new Limitations bullets (entity-volume scaling with replicated
  tables; SHOW row-cap truncation) are present in `snowflake-openflow_post.md`, and all
  9 tests in `test_docs.py` pass, including the bridging-registry enumeration test that
  pins `CONNECTOR_DEFINITION_PLATFORM`'s keys to the documented table.
- Independently re-ran `ruff format --check`, `ruff check`, and
  `mypy --config-file setup.cfg` against all four in-scope source files plus both test
  directories — all clean. Reran `tests/unit/snowflake_openflow/` — 226 passed.
- Verified `data-platforms.yaml` carries the `openflow` entry and that the
  `data-platforms` bootstrap step version was bumped (v18) in this connector's own
  history — the platform-registration row is not a dangling YAML-only edit.
- No manual f-string URNs, no mixed URN-construction primitives, no dead private
  methods, no banned constructs (`print`, `time.sleep`, bare/swallowed `except`), no
  `TypedDict`/pydantic duplication, no nested config models bypassing the mixin
  contract.

Full detail and rationale for each row is recorded in `summary.notes` of
`convention-findings.json`.
