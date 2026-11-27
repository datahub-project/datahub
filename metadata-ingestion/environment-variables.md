---
description: "Every environment variable the DataHub CLI and Python SDK read: what it does, its type, its default, and how it interacts with your recipe and CLI flags."
---

# Environment Variables

The DataHub CLI and the `acryl-datahub` Python SDK read their configuration from environment
variables as well as from `~/.datahubenv`, CLI flags and the recipe itself. This page is the
full reference for those variables.

It does **not** cover the server. For GMS, the frontend and the consumers, see
[Deployment Environment Variables](../docs/deploy/environment-vars.md).

## Setting variables

```shell
export DATAHUB_GMS_URL=https://my-instance.acryl.io/gms
export DATAHUB_GMS_TOKEN=<your-token>
datahub ingest -c recipe.yml
```

They can also be set per command (`DATAHUB_DEBUG=true datahub ingest -c recipe.yml`), in a
`.env` file your shell sources, or through the usual `env:` / `envFrom:` blocks in Docker and
Kubernetes. For ingestion run from the DataHub UI, set them on the ingestion executor.

## Precedence

There are two separate chains, depending on what the variable configures.

**Connection settings** — where DataHub is and how to authenticate. The first one set wins:

1. **A selected profile** — `--profile`, or `DATAHUB_PROFILE`. This is resolved before the
   environment is consulted, so a selected profile beats `DATAHUB_GMS_URL`.
2. **An explicit CLI argument**, such as `datahub init --token <token>`
3. **A server environment variable** — `DATAHUB_GMS_URL`, or the `DATAHUB_GMS_HOST` /
   `DATAHUB_GMS_PORT` pair
4. **The saved connection files** — written by `datahub init`

Two traps worth knowing. `DATAHUB_GMS_TOKEN` on its own does **not** take effect: the
environment is only used when it supplies a server, so a token with no `DATAHUB_GMS_URL` is
ignored and the saved file is used instead, token and all. And `DATAHUB_GMS_PORT` overrides
`DATAHUB_GMS_URL` rather than supplementing it.

`DATAHUB_SKIP_CONFIG=true` skips the saved files entirely, so a missing environment variable
becomes an error rather than a silent fallback.

**Defaults for recipe fields** — the opposite order, because the recipe is more specific:

1. **The recipe field**, such as `sink.config.max_threads`
2. **An environment variable**, such as `DATAHUB_REST_SINK_DEFAULT_MAX_THREADS`
3. **The built-in default** — the Default column below

These variables set the default for recipes that do not configure the field themselves; a recipe
that does is unaffected. Rows that work this way name their recipe field in the Description
column.

## Secrets

Variables marked **Secret** carry credentials. Do not commit them, and prefer
[secret resolution](../docs/secret-resolution.md) over putting them in a shell profile or an
image.

Two things to know when ingestion runs on the DataHub executor rather than your laptop:

- A recipe's `extra_env_vars` **cannot** override the `DATAHUB_GMS_*` connection settings, the
  system client credentials, any `DATAHUB_AUTH_*` variable, or the proxy variables. The executor
  rejects the run rather than ignoring the override, so a recipe cannot redirect its own run to
  a different DataHub or escalate its own access. Set those on the executor itself.
- Secret masking is on by default. `DATAHUB_DISABLE_SECRET_MASKING` turns it off in the SDK,
  which puts credentials into run logs — and those logs are visible to anyone who can see the
  ingestion run in the UI. The executor's `DATAHUB_ENABLE_SECRET_MASKING` is **not** a usable
  off switch: the executor overwrites it with `true` for every task it starts, so a recipe
  cannot disable masking for its own run.

## Reading the tables

- **Type** is how the value is parsed. Boolean variables differ in what they accept: some take
  only `true`, others also accept `1`. Always check the accepted values in the Description
  column rather than assuming — `NO_COLOR=1`, for instance, does **not** disable colour.
- **Default** is what the code falls back to when the variable is unset. `_(unset)_` means there
  is no fallback and the feature stays off or is configured elsewhere.
- Every row is tagged with its tier, and each section is ordered by it. **_Basic_** comes
  first: the variables most people need. **_Advanced_** is tuning and escape hatches, safe to
  set but with defaults that suit nearly everyone. **_Experimental_** comes last and may change
  or disappear without a deprecation cycle.
- ⚠️ marks a variable whose effect goes beyond what its name suggests — it can create duplicate
  entities, drop lineage, or put credentials in logs. The description says what actually
  happens. Read it before changing one of these in an existing deployment.
- **Detected, not set by you** marks variables DataHub or the surrounding platform sets and the
  SDK only reads. Setting them by hand usually does nothing useful and sometimes breaks things.
- **Deprecated** rows name their replacement. They still work, for now.

## Reference

{{ inline /docs/generated/python-sdk/env_vars.md.snippet }}

## See also

- [Recipes](recipe_overview.md) — recipe fields, several of which these variables set defaults for
- [DataHub CLI](../docs/cli.md) — the commands that read these variables
- [External OAuth providers](../docs/authentication/external-oauth-providers.md) — the full
  `DATAHUB_AUTH_*` flow and its precedence rules
- [DataHub sink](sink_docs/datahub.md) — sink configuration in depth
- [API tracing](../docs/advanced/api-tracing.md) — `DATAHUB_EMIT_MODE` and emitter tracing
- [Telemetry](../docs/deploy/telemetry.md) — what telemetry collects and how to turn it off
- [Deployment Environment Variables](../docs/deploy/environment-vars.md) — the server-side equivalent
