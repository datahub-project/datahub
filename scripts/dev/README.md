# DataHub development wrapper

Use `scripts/dev/datahub-dev.sh --help` for the full Docker and remote-runner
commands. The wrapper also supports a Docker-free stack on JDK 25:

```bash
scripts/dev/datahub-dev.sh start --local
scripts/dev/datahub-dev.sh status
eval "$(scripts/dev/datahub-dev.sh shell-env)"
datahub profile current
datahub graphql --query '{ me { corpUser { urn } } }' --format json
scripts/dev/datahub-dev.sh stop
```

The backend uses the [GMS-lite launcher](../../metadata-service/gms-lite/README.md).
It builds GMS, upgrade and embedded OpenSearch artifacts when missing, then runs
the launcher detached. Existing artifacts are reused; rebuild them with Gradle
when source changes. The start command prints the log path for `tail -f`.

Vite serves the UI at `http://localhost:<9002 + slot * 1000>` with hot reload and
no Play server. Log in as `datahub` / `datahub`. The plugin reads `GMS_LITE_STACK_FILE`
from the worktree's `stack.json`, handles sessions, and proxies GraphQL and OpenAPI
to GMS. `frontend.log` contains startup and HMR output; use `tail -f <data-dir>/frontend.log`.

- `start --local --processes N`: N isolated GMS instances, starting at the slot's
  GMS port. Ports use the Docker registry's `base + slot * 1000` scheme. Every GMS
  port is reserved in the shared registry, including additional processes.

- `--opensearch 2|3`: select the embedded engine for a new stack. The default comes
  from `embeddedOpenSearchDefault` in root `gradle.properties` (currently 3), with
  `EMBEDDED_OPENSEARCH_VERSION` as an override. Restarts retain the recorded major.
  Changing an existing stack's major requires `--fresh`; its old indices are removed
  on the old node before starting with fresh H2 and search data. Unmarked legacy
  stacks retain major 3.
- `--no-frontend`: start only the backend. Restarts retain this choice; `--frontend` re-enables Vite.
- `--skip-build`: require existing GMS, upgrade and OpenSearch artifacts.
- `--fresh`: reset this stack's data and indices before starting, including env overrides.
- `--auth` / `--no-auth`: enable or disable authentication for this local backend.
  Omitting both keeps the recorded setting (enabled for new stacks).
- `env set KEY=VALUE`, `env list`, `env restart`: maintain the worktree's
  `env-overrides.env`, passed to the launcher through `--env-file`. Restart retains
  the previous process count, authentication and frontend settings. The launcher protects its
  per-instance storage, port and authentication settings from env overrides.
- `stop`: stop GMS and Vite processes, retaining data, profiles and the registry reservation.
- `reset`: stop and warm-start with the recorded options.
- `rebuild`: rebuild the GMS artifact and warm-start with the recorded options.
- `wait`: wait for all GMS instances to become healthy.
- `nuke --keep-data`: stop and remove stale H2 locks; retain data and profiles.
- `nuke`: remove this stack's indices, data, profiles and registry entry; no restart.
- `instances clean`: reclaim marked, stopped stacks whose recorded worktrees no
  longer exist, including their indices and profiles. Stale Docker and remote entries
  are removed from the registry only. Unrecognized local entries are retained.
  Stacks missing their registry entry are left for manual inspection.

Persistent stack data lives in `~/.datahub/local/stacks/<worktree-id>/`. The registry
and each stack's `instance.json` record `local_owner: datahub-dev-oss-local-v1` and
`opensearch_dir` and `opensearch_version`. Only matching entries and data directories
can be changed. The wrapper uses `~/.datahub/local/opensearch-<major>/` for the shared
search node. The two majors can run side by side; their data is never shared.
`status` shows the recorded major. The unversioned `~/.datahub/local/opensearch/`
directory is reserved for another tool and is never used.
Stop and nuke leave that node running for other stacks; its manager checks process
identity and major version before reusing it. Index deletion enumerates exact
names under this stack's launcher-derived prefixes.

Before starting, the wrapper reads the node's effective shard limit, including
replicas. It estimates 86 shards per GMS instance (measured on the OSS OpenSearch 3
stack), preferring a larger measurement from ready stacks when available. A warm
start reuses existing shards; a full node produces an error before GMS bootstrap.
Concurrent standalone launcher starts can still consume the remaining budget.

Each ready instance gets a CLI profile under `~/.datahub/profiles/`:

- Default: `local-<worktree-id>`.
- Other instances: `local-<worktree-id>-gms-N`.

Profiles contain the GMS URL, admin token and expiry from `stack.json`. The wrapper
and CLI share dependency-free path and private atomic-write helpers. The wrapper
writes JSON, a YAML subset, so it does not require an installed CLI or PyYAML.
It refreshes only profiles with its matching ownership metadata and refuses name
collisions. It never writes `~/.datahub/current-profile` or `~/.datahubenv`.
`shell-env` exports `DATAHUB_PROFILE` and the legacy `DATAHUB_GMS_URL`, and clears
an inherited `DATAHUB_GMS_TOKEN`. Explicit profiles own their credentials.
A restart refreshes the token. Reducing the process count removes obsolete owned
profiles. Stop leaves profiles; nuke removes them. As with other named profiles,
select a profile explicitly for another GMS instance:

```bash
datahub --profile local-<worktree-id>-gms-1 graphql --query '{ me { corpUser { urn } } }'
```

Run the offline tests and lint through Gradle:

```bash
./gradlew :scripts:dev:test :metadata-service:gms-lite:launcherSafetyTest
./gradlew :scripts:dev:lintFix
./gradlew :metadata-ingestion:lintFix
```

`-PdevPythonFiles=datahub_dev.py,datahub_dev_local.py,tests/test_datahub_dev_local.py`
limits scripts lint to selected paths. Tests isolate HOME, the registry and search
data. Standalone agent experiment scripts are excluded from pytest collection.
The GitHub scripts workflow also runs the dev wrapper tests.
