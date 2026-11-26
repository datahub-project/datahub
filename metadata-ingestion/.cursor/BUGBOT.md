# metadata-ingestion Bugbot rules

Authoring rules these mirror: `src/datahub/ingestion/source/AGENTS.md`, `tests/AGENTS.md`.

## Secrets

If connector or ingestion code sets `os.environ[...]` (or otherwise writes secrets
into the process environment) to feed a third-party SDK, then:

- High security. Inject via settings object, constructor, or credential provider
  (see `looker_lib_wrapper.py` `_DataHubLookerApiSettings`); env vars leak.

If a Pydantic config adds password / token / API-key fields as plain `str`, or a
credential inside a URL / connection-string field is not registered, then:

- High security. Use `SecretStr`, or register the value from a `model_validator`.

If Python uses the stdlib `xml` package to parse untrusted XML, then:

- High security. Use a safe XML library (see HANA-related ingestion code).

## Packaging

If `metadata-ingestion/setup.py` changes (deps, entry points, extras) but
`pyproject.toml` / `uv.lock` / `constraints.txt` are not regenerated in the same PR,
or those generated files are hand-edited, then:

- High: CI `checkLockFile` will fail. Run the project's lockfile update task.

If a new connector is added under `src/datahub/ingestion/source/` without the
full registration chain, then:

- High packaging/UI gap. Expect an entry point in `setup.py`, `datahub.json`
  regenerated via Gradle (never hand-edited), a UI tile in
  `datahub-web-react/src/app/ingestV2/source/builder/sources.json`, logos,
  `uv.lock` refresh, and subtypes in the shared subtypes module.

## Stale-entity removal

If a source with stateful ingestion hits a listing cap, permission error, timeout
or partial read and reports it via `report.warning` (or only logs it) and carries
on, then:

- High data loss. Only `report.failures` suppresses soft-deletion.

## Tests that cannot fail

If a test mocks the unit under test, uses `Mock(spec=SourceReport)`, asserts
`[] == []` or a lone `is not None`, inserts a function between a decorator and
its target, or renames a test so pytest no longer collects it, then:

- High. Ask for a test that goes red when the fix is reverted, plus a failure path.

## Reporting

If a `report.warning`, `report.failure` or `report.info` call gets an f-string
`message=`, a `logger.warning` sits inside a pydantic validator, per-entity
`report.info` calls lack `log=False`, or `context=` is redacted while `exc=`
carries the raw exception, then:

- Medium. Constant `title=` / `message=`, variable part in `context=`; register secrets.

## Lineage

If connector code hand-rolls SQL lineage or per-connector sqlglot dialects instead
of `SqlParsingAggregator` / `create_lineage_from_sql_statements` with a platform
map, or emits column lineage without graph / known-URN schema resolution, then:

- Medium–High. Prefer the central aggregator or a shared dialect fix.

## URNs and config

If dataset URNs are built with `DatasetUrn.create_from_ids` or manual casing
instead of `make_dataset_urn_with_platform_instance`, or a connector invents
`*_platform_instance` fields instead of reusing `PlatformDetail`, then:

- High duplicate-entity risk.

If an `AllowDenyPattern` is mutated in place (use `with_deny`), a config field has
no reader in the same PR, a `threading.Lock` or client lives on a config model, or
a field is retyped without a `mode="before"` validator, then:

- Medium.

## Performance, retries, framework reuse

If the code makes one GMS or API call per entity, grows a collection unbounded
across the run, or retries without backoff and error classification, then:

- Medium–High.

If a connector adds a one-off flag to a shared helper, re-implements a helper that
exists in `source/common/`, `sql_parsing/` or `utilities/`, or patches locally
what the framework should own, then:

- Medium. Ask for the shared abstraction or an explicit note of the framework gap.

## Breaking recipe / URN changes

If an ingestion source removes, renames or incompatibly redefines recipe keys,
changes URN format or casing (`convert_urns_to_lowercase`, platform-instance
lowercasing), or changes default hierarchy/region/scan behavior, then:

- High. Prefer transparent upgrade / dual-read; identity changes need a dedup plan.
  Flag missing `docs/how/breaking-changes.md` and `*_pre.md` entries. Optional new keys are fine.

## Identity / corp user status

If a PR touches LDAP/OIDC/Okta/user-provisioning emitters for `corpuser`, then:

- Flag if `corpUserStatus` is not emitted alongside `corpUserInfo`; login depends
  on `corpUserStatus`, not deprecated `corpUserInfo.active`.

## Artifacts and hygiene

If comments contain agent markers or signatures, tests import inside functions, a
source emits `dataPlatformInfo`, vendored code lacks its licence header and a
`NOTICE` entry, or fixtures carry customer identifiers or ticket IDs, then:

- Medium.
