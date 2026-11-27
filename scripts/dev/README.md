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
- `logs [service] [-n N] [-f]`: tail a log the stack wrote (`gms`, `gms-N`, `system-update`,
  `frontend`, `launcher`, `opensearch`, ...). No argument lists them. Docker mode runs
  `docker compose logs`.
- `frontend --local --preview --detach`: serve the built bundle in the background. Returns once
  `/health` answers; the PID is recorded so `status`, `stop` and `nuke` see it.
- `nuke --keep-data`: stop and remove stale H2 locks; retain data and profiles.
- `nuke`: remove this stack's indices, data, profiles and registry entry; no restart.
- `instances clean`: preview cleanup; `--yes` reclaims marked, stopped stacks whose recorded worktrees no
  longer exist, including their indices and profiles. Stale Docker and remote entries
  are removed from the registry only. Unrecognized local entries are retained.
  `--orphans --yes` also reclaims owned, unregistered stopped stacks whose worktrees
  are gone. It never infers ownership of unrecognized directories.

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

## Local stack inventory and limits

`indices list|drop|prune` and `instances repair` use the stacks' ownership records.
Repair only restores a recorded owned stack when its ports are free of registry
claims. Cleanup refuses unrecognized or mismatched state; directories without a
valid ownership record are never inferred to be disposable.

`max_docker_free_instances` limits **running** Docker-free (local) stacks (default four);
`max_docker_instances` limits Docker Compose stacks separately (default four). A stack counts only when its
registry entry and instance record match the ownership markers. A live
start-in-progress marker or ownership-verified launcher, service or managed frontend
PID establishes liveness. Unknown or reused PIDs and HTTP health alone do not establish
liveness. Foreign entries have no verified process contract, so they do not count
toward the running limit; their slots and every recorded port remain reserved,
including Docker and remote entries. The deprecated `max_local_instances` key is read
only as a fallback when `max_docker_free_instances` is absent.

Allocation searches every slot whose core and declared extension ports fit in
`1..65535`, independently of the running limit (57 slots with the default ports).
This bounded search accommodates old reservations without changing the capacity
limit. Occupied, overlapping and browser-forbidden ports are skipped. Reusing a
stopped reservation obeys the same running limit; an idempotent start of an already
running stack is allowed. A limit error names running stack IDs/worktrees and
explains how to stop one or raise `max_docker_free_instances` in
`~/.datahub/dev/config.json`. Stopped Docker-free reservations do not exhaust the
Docker slot search range.

### Local extension interface

`extensions/datahub_local_extension.py` defines the stdlib-only `LocalExtension` Protocol.
A provider is a module attribute with `id` and `schema_version`, plus:

- `add_arguments(parser)` declares optional CLI settings. `None` means keep the
  persisted value. The registry records the module, identity, version and options.
- `describe(options)` declares port bases and build prerequisites before allocation.
  Prerequisites can be required or optional; optional failures are logged.
- `describe(options)` also declares `layer_dirs`, extra env layers inserted after
  `env/docker-free` and before the job folder, `artifact_globs` for build prerequisites,
  and `search_plugins` for required search-node capabilities. `env_aliases` declares
  names-only compatibility aliases, applied within a service before protected values.

- `placeholders(ctx)` supplies additional `${GMS_LITE_*}` names without replacing core names.
- `services(ctx)` returns argv, working directory, declared port roles, readiness URL,
  timeout and required status. Every service resolves `all.env` and its own files;
  the hook receives no GMS environment and returns no environment dictionaries.
  The launcher adds `DATAHUB_GMS_TOKEN` after resolution and owns each process group.
  `protected_keys` names derived values to pin from the declared files; it contains no values.
- `after_ready(ctx)`, `after_stop(ctx)` and `after_nuke(ctx)` receive the same instance
  identity. Nuke contexts contain persisted ports/options, without live credentials.

Each extension keeps its env files under `env/extensions/<name>/`, with the same shape as
`env/` (`common/`, `docker-free/` and their `local/` and `ci/` subfolders), so `env/common`
and `env/docker-free` hold only the base values.

To use an extension, point `local_extension` at it from `extensions/datahub_dev_ext.py`
(untracked; `extend_config(config)` returns the changed config). Without one, `start --local`
behaves as before.

The wrapper passes `--extension module:attribute`, `--extension-options` and
`--extension-slot` explicitly to the detached launcher. It does not depend on a
parent-process import or implicitly reread wrapper configuration. The provider cannot
change the core's ownership predicates.

For an isolated check, set a temporary HOME and an absolute JAVA_HOME before running
`scripts/dev/tests/local_stack_e2e.sh`. Never run a cleanup check with the developer's
real HOME.

### Boot diagnostics and CI options

The adopted launcher records `timings.json` next to `stack.json`. It reports
`warm`, `total_s` and cumulative `phases`: `opensearch`, `usage_event_index`,
`system_update`, `restore_indices`, `gms_boot`, `admin_token` and
`service:<role>` for each declared extension service. Completed phases survive a
failed boot. Ready logs include the phase split and data-directory size; `status`
includes timings when available. The wrapper adds `datahub_dev.opensearch_ensure_s`
(preparation, including shard checks), `stack_json_wait_s` and, when started,
`frontend_ready_s`. The launcher total excludes wrapper preparation and frontend startup.

`start --local --timeout 600 --verbose` bounds discovery readiness and streams
`launcher.log`. A failed start prints its last 40 lines. Tokens and client secrets
remain in private discovery/profile files and are not printed by the launcher.

`GMS_LITE_OPENSEARCH_READY_TIMEOUT` and `GMS_LITE_GMS_HEALTH_TIMEOUT` accept
non-negative whole seconds (defaults 900 and 600). The existing separate shared-node
health/recovery timeout stays unchanged. `GMS_LITE_UPGRADE_JAVA_OPTS` supplies extra
JVM arguments for SystemUpdate and RestoreIndices. `GMS_LITE_HEAP` sets the wrapper's
GMS/upgrade heap. Each GMS defaults `ES_BULK_ACK_AFTER_TRANSFER=true` so queue sync
waits for search visibility, and pins `DATAHUB_BASE_URL` to its assigned frontend URL
so OAuth issuer discovery works on every slot.

CI measures a lean boot with no frontend, a 2 GB heap, 240-second readiness bounds and a
600-second start timeout. It publishes the timing table and archives logs and timings on every
run, before removing that stack and running the existing lifecycle checks. Discovery files are
never uploaded.
