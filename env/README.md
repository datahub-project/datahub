# Service environment files

Env files for GMS, system-update and optional services in DataHub's local stacks. Today only the Docker-free stack
(`metadata-service/gms-lite/bin/gms-lite-stack.py`, started by `scripts/dev/datahub-dev.sh start --local`)
reads them. The Docker quickstart still uses `docker/profiles/*` and `smoke-test/smoke.*.env`.

## Layout

```
env/
  common/        values that are the same on every stack and job
  docker-free/   values for the Docker-free stack (H2, pgqueue, embedded OpenSearch, ports)
  extensions/<name>/   one launcher extension's values, with the same common/ and docker-free/
                       folders, plus any conditional folders the extension adds
smoke-test/env/              job layer for the pytest smoke tests (--job smoke-test)
e2e-test/ui/playwright/env/  job layer for Playwright (--job playwright)
```

Each layer folder can hold `all.env` (every service) and one file per service: `gms.env`,
`system-update.env`, and `<service>.env` for each optional service. Each layer can also have `local/` and
`ci/` subfolders with the same file names. `ci/` applies when `CI` is set, `local/` otherwise;
`--mode local|ci` overrides that. Every file is optional, so only create one when it has values.

## Precedence

For one service, from lowest to highest:

1. The launcher's own environment. It only passes through keys no file sets.
2. For each layer, in the order `env/common`, `env/docker-free`, the extension's layers (below),
   then the `--job` folder: `all.env`, then the service's files, then the same names in
   `local/` or `ci/`.
3. Ad-hoc files: each `--env-file [SCOPE:]PATH` in command-line order. `datahub-dev.sh env set`
   writes to one of these (`~/.datahub/local/stacks/<worktree-id>/env-overrides.env`).
4. The shell, only for keys whose final value is written `KEY=${KEY:-default}` (as in compose).

Service files: `system-update` reads `gms.env` and then `system-update.env`. `restore-indices` reads
only `gms.env`, so it never gets `EBEAN_AUTOCREATE=true`, which drops every table.

## Values

- Plain values are literal: `KEY=value`. Comments go on their own line.
- `${GMS_LITE_*}` placeholders come from the launcher: `GMS_LITE_REPO_ROOT`, `GMS_LITE_INSTANCE`,
  `GMS_LITE_INSTANCE_DIR`, `GMS_LITE_INSTANCE_DIR_URI`, `GMS_LITE_INDEX_PREFIX`, the
  `GMS_LITE_*_PORT` values, `GMS_LITE_AUTH` and the per-stack secrets. An unknown placeholder
  stops the launcher.
- `KEY=${KEY:-default}` lets the shell set `KEY`. Without a shell value the default applies, and it
  may use placeholders. Keep stack plumbing (ports, paths, secrets) as plain values.
- Write `$$` for a literal `$`. Any other `$` stops the launcher.

## Adding a value

1. Pick the lowest layer that is true for every case that should get the value: `env/common` if
   every stack and job wants it, a job folder if only that job does, `ci/` or `local/` if it
   depends on the machine.
2. Put it in `all.env` or in the file of the one service that reads it, with a comment saying why.
3. Check the result: `python3 metadata-service/gms-lite/bin/gms-lite-stack.py --print-env [--job smoke-test] [--mode ci]`
   prints every service's final values and the file each came from (secrets redacted). Each
   start writes the same report to `<data-dir>/resolved-env.json`.
4. Values copied from the Docker quickstart (`env/common`, `smoke-test/env`) must stay
   equal to it until Docker reads these files too. `python3 smoke-test/check_env_parity.py`
   compares the two smoke envs and lists every difference that isn't in
   `smoke-test/env-parity-allowlist.txt`. When you change a smoke value, change it on both sides or
   add an allowlist line with a reason. Protected keys are not compared, and the job layer must
   not set them.

## Isolation and extensions

An extension declares extra placeholders and layer directories through
`describe(options).layer_dirs` and `placeholders(ctx)`. Its layers apply after
`env/docker-free` and before job layers. Each extension keeps its files under
`env/extensions/<name>/`, laid out like `env/` itself, so its settings cannot collide with
future base env files. The layout matches `env/`, but the precedence does not: the extension's
layers apply after all of the base layers, so a key in its `common/` overrides the same key in
`env/docker-free/`. It lists its folders in the same order as the base layers: `common/`, then
`docker-free/` (each with its `local/` or `ci/` subfolder), then any conditional folders it adds.

The generic hook contract lives in `scripts/dev/extensions/datahub_local_extension.py`.

Each process inherits only unset shell keys and resolves its own files. System-client
credentials are shared in `all.env`; token signing keys and salt belong to `gms.env`.
A service never receives GMS's resolved environment. The launcher adds a GMS access
token to services after GMS readiness and redacts it in reports.

After resolution, a protected set pins search host/port, URI, credentials, TLS,
proxy and prefix settings, H2 and spool paths, object storage, authentication secrets
and core listening ports. Search aliases inherited from the shell are neutralized as
in the original launcher. Protected values are marked `source: protected`, including
in `--print-env` and `resolved-env.json`; they cannot be redirected by job or ad-hoc
files. Only SystemUpdate can create a new H2 schema; RestoreIndices never reads
`system-update.env`.
