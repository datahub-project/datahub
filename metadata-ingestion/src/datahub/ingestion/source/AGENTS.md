# Connector Conventions

Rules for sources under `src/datahub/ingestion/source/`. Module commands and layout:
`metadata-ingestion/AGENTS.md`. Config and style basics: `metadata-ingestion/developing.md`.
Review-time mirror of these rules: `metadata-ingestion/.cursor/BUGBOT.md`.

## Strategic over tactical

- Fix or extend the framework (`src/datahub/ingestion/api/`, `source/common/`,
  `src/datahub/sql_parsing/`, `src/datahub/sdk/`, `src/datahub/utilities/`) rather than
  patch one connector. A second connector needing the same thing is the signal to build a
  shared abstraction.
- Search those packages before adding a helper; reviewers reject re-implementations
  (pagination loops, URN builders, subtype constants outside `source/common/subtypes.py`).
- Never add a one-off mode flag to a shared helper for one connector; wrap at the call site.
- If a connector-local workaround is unavoidable, say so in the PR and name the framework gap.

## Structure

- Split by duty: `constants.py`, `models.py`, `config.py`, `client.py`, `source.py`, plus
  `lineage.py`, `mapper.py`, `usage.py` as needed. `source/airbyte/` is the reference layout.
- Hoist magic strings into module-level constants; pre-compile regexes in `constants.py`.
- No top-of-file docstrings in connector modules; comments only where they add context.
  Remove leftover agent artifacts (agent signatures, comments that narrate the diff).
- Lazy imports only for opt-in features the connector does not always depend on.
- Parse external API responses into dataclasses; never pass raw response dicts around.
- XML: never the stdlib `xml` parser; use `defusedxml` as `source/sap_common/edmx.py` does.

## Config semantics

Naming, descriptions, defaults and validators: `developing.md`, "Guidelines for Ingestion
Configs". On top of that:

- Filter with `AllowDenyPattern`; derive variants with `with_deny(...)`
  (`src/datahub/configuration/common.py`), never mutate `pattern.deny` in place.
- Rename a field with `pydantic_renamed_field` (`source/ldap.py`), deprecate with
  `pydantic_field_deprecated` (`source/looker/looker_config.py`), remove with
  `pydantic_removed_field`.
- A field lands in the same PR as the code that reads it; an unread field is a doc lie.
- Retyping a field needs a `mode="before"` validator so old recipes still parse.
- Nothing un-deepcopyable (`threading.Lock`, clients) on a `ConfigModel`; keep it on the source.
- Describe a shared field once, in the shared base class.
- Cross-platform references take a `PlatformDetail` (`src/datahub/configuration/source_common.py`);
  do not invent per-connector `*_platform_instance` fields.

## URNs, platform instance, env, casing

- One URN builder per source, consistent with its casing config: prefer
  `make_dataset_urn_with_platform_instance` (`src/datahub/emitter/mce_builder.py`).
  `DatasetUrn.create_from_ids` never lowercases, so mixing the two splits entities.
- Match the URN the owning source emits (lowercasing, `convert_urns_to_lowercase`,
  multi-part names); a mismatch creates duplicate entities. See
  `docs/dev_guides/lineage_urn_casing.md`.
- Register new platforms in
  `metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml`
  rather than emitting `dataPlatformInfo` from a source; self-registration (`source/sqlmesh/`)
  is the exception for a platform only that source creates.

## Reporting

- Operator-visible events go through `self.report.warning/failure/info(...)` and counters,
  not `logger`. Skipped or inaccessible objects get a `num_*_skipped` counter.
- `title=` and `message=` are constants; the variable part goes in `context=`. Entries
  dedupe on title and message (`src/datahub/ingestion/api/source.py`), so an f-string
  message floods the report.
- `exc=` appends the exception text to `context`; redacting `context` alone redacts nothing.
- `report.info` logs at INFO by default; per-entity info needs `log=False` or a counter.
- `logger.warning` inside a pydantic validator never reaches the structured report.
- Show progress with explicit stages: `report.new_stage(...)`
  (`src/datahub/ingestion/source_report/ingestion_stage.py`; see `source/snowflake/snowflake_v2.py`).

## Partial reads and stale-entity removal

- A capped, denied, timed-out or otherwise partial listing is `report.failure`, not
  `warning`: only `report.failures` stops `StaleEntityRemovalHandler` from soft-deleting
  everything it did not see (`source/state/stale_entity_removal_handler.py`). Example:
  `source/bigquery_v2/bigquery_schema_gen.py`.
- Never drop data silently: every `except` on the ingestion path ends in a `report.*` call
  or a counter. No broad `except Exception: pass`.
- Release per-run resources (connections, temp tables, warehouses) in `finally` or `close()`.

## Errors, retries, performance

- Classify errors: retry transient ones (429, 5xx, timeouts) with backoff, fail fast on
  permanent ones (auth, 404). Pattern: `tenacity` in `source/snowflake/snowflake_connection.py`.
- No GMS or source-API round-trip per entity. Batch, cache per run, and parallelise with
  `src/datahub/utilities/threaded_iterator_executor.py`.
- Bound every accumulator (`src/datahub/utilities/lossy_collections.py` or streaming);
  give long extraction loops a query budget or timeout config.

## Lineage

- SQL lineage: `SqlParsingAggregator` via `create_lineage_from_sql_statements`
  (`src/datahub/sql_parsing/sql_parsing_aggregator.py`, `src/datahub/sql_parsing/sqlglot_lineage.py`)
  with a platform map; no per-connector sqlglot dialects. Mirror existing connectors for
  known-URN mapping, platform instance / env, casing, multi-part names and temp tables.
- Column lineage: resolve upstream and downstream schemas from the DataHub graph when
  available, load known URNs from the platform / instance / env mapping, and match columns
  case-insensitively.

## Secrets

- Never write secrets into the process environment to feed an SDK: `/proc/<pid>/environ`
  and any code in the process can read them. Inject via a settings object, constructor
  argument or credential provider (`source/looker/looker_lib_wrapper.py`,
  `_DataHubLookerApiSettings`). `SecretStr` for every password, token or key.
- A credential inside a plain `str` field (URL userinfo, presigned query, connection string)
  is invisible to `_register_secret_fields`. Register it from a `model_validator(mode="after")`
  as `register_sqlalchemy_uri_secrets` (`source/sql/sql_config.py`) and
  `register_artifact_url_secrets` (`source/dbt/dbt_core.py`) do, using `sqlalchemy_uri_secrets`
  (`source/sql/sqlalchemy_uri.py`) or `http_url_secrets` (`source/common/object_store_files.py`).
  Register the whole query string, not individual parameters.
- A `redact_*` helper at a `report.*` call site is a deterministic floor for strings we
  build and the only thing keeping the in-memory report clean. Use it in addition to
  registration, never instead of it.
- Never hand-roll substring redaction with `str.replace`; the registry guards short and
  common values (`MIN_SECRET_LENGTH`, `src/datahub/masking/secret_registry.py`).
- Masking is installed by the CLI and executor (`initialize_secret_masking`,
  `src/datahub/masking/bootstrap.py`); a programmatic `Pipeline` that skips it logs raw.
- Test with a realistic exception that carries the credential, not a bare `ValueError`.

## New connectors and PRs

- Registration chain: `setup.py` entry point → `../gradlew :metadata-ingestion:updateLockFile`
  → `../gradlew :metadata-ingestion:connectorRegistry` regenerates `datahub.json` (never by hand)
  → UI tile in `datahub-web-react/src/app/ingestV2/source/builder/sources.json` (checked by
  `tests/unit/test_ingestion_source_tiles_completeness.py`) → logos → docs per
  `docs/sources/AGENTS.md` → subtypes in `source/common/subtypes.py`.
- Vendored code keeps its licence header and gets an entry in the repo-root `NOTICE`.
- Before committing, run the connector in debug mode to a file sink and read the full log.
  Credentials for live tests stay outside the checkout and never reach the PR.
- Breaking recipe, URN or default changes: entry in `docs/how/breaking-changes.md` under the
  next unreleased version, leading with the PR number, plus the connector's `_pre.md`.
  Prefer transparent upgrades and dual-read over breaking existing recipes.
- One connector per PR; shared framework changes go in their own PR first. The PR
  description must match the diff. Rebase before requesting review.

## Style opinions not enforced by tooling

Follow these when ruff and mypy are silent; the goal is to move each into ruff or drop it.

- Prefer `Dict` and `List` from `typing` over builtin generics in connector annotations
  (ruff cannot enforce this direction; `UP006` enforces the opposite and is not enabled).
- No walrus operator in connector code (no ruff rule exists).
