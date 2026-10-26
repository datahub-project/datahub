# metadata-ingestion Bugbot rules

## Secrets

If connector or ingestion code sets `os.environ[...]` (or otherwise writes secrets
into the process environment) to feed a third-party SDK, then:

- High security. Inject via settings object, constructor, or credential provider
  (see `looker_lib_wrapper.py` `_DataHubLookerApiSettings`).
- Body: "Secrets in the process environment become global process state and can
  leak to child processes and diagnostics; prefer programmatic injection."

If a Pydantic config adds password / token / API-key fields as plain `str`, then:

- High security. Use `SecretStr` (or equivalent) so values are not logged/serialized.

If Python uses the stdlib `xml` package to parse untrusted XML, then:

- High security. Use a safe XML library (see HANA-related ingestion code).

## Packaging

If `metadata-ingestion/setup.py` changes (deps, entry points, extras) but
`pyproject.toml` / `uv.lock` / `constraints.txt` are not regenerated in the same
PR, then:

- High: CI `checkLockFile` will fail. Run the project's lockfile update task.

If a PR hand-edits `pyproject.toml`, `uv.lock`, or `constraints.txt` without a
corresponding `setup.py` change, then:

- Flag: those files are generated from `setup.py` and will be overwritten.

If a new connector is added under `src/datahub/ingestion/source/` without the
full registration chain, then:

- High packaging/UI gap. Expect entry point in `setup.py`, `datahub.json`, UI
  form pieces, logos, `uv.lock` refresh, and subtypes in the shared subtypes
  module (not defined only locally).

## Lineage

If connector code sets per-connector sqlglot dialects or hand-rolls SQL lineage
(or otherwise bypasses `SqlParsingAggregator` /
`create_lineage_from_sql_statements` with a platform map — e.g. Tableau native
SQL, SQLAlchemy dialect crutches), then:

- Flag correctness/consistency risk. Prefer the central aggregator / sqlglot or a
  shared dialect fix over a local parser.

If a connector emits column-level lineage but leaves edges coarse (no schema
resolve from graph / known URNs / case-insensitive column match when peers do),
then:

- Medium–High incomplete lineage. Best-effort resolve upstream/downstream schemas.

## Breaking recipe / URN changes

If an ingestion source **removes, renames, or incompatibly redefines** recipe
config keys, changes URN format, or changes default hierarchy/region/scan
behavior in a breaking way, then:

- High. Prefer transparent upgrade / dual-read; do not break existing recipes.
- Flag missing entries in `docs/how/updating-datahub.md` and connector `*_pre.md`.
- Do not flag backward-compatible **additions** of optional recipe keys as
  breaking.

If a PR changes URN normalization / `convert_urns_to_lowercase` / platform-instance
casing, then:

- High when platform-instance (or other identity segments) are lowercased without
  a migration / dedup plan — creates duplicate entities and breaks assertions.

## Identity / corp user status

If a PR touches LDAP/OIDC/Okta/user-provisioning emitters for `corpuser`, then:

- Flag if `corpUserStatus` is not emitted alongside `corpUserInfo`.
- Login/eligibility depends on `corpUserStatus`, not deprecated
  `corpUserInfo.active`.
