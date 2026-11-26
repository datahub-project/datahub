# datahub init and profiles

## init

The init command is used to tell `datahub` about where your DataHub instance is located. The CLI will point to localhost DataHub by default.

**_Note_**: Provide your GMS instance's host when the prompt asks you for the DataHub host.

### Interactive Mode

```shell
# Interactive mode - prompts for input
datahub init
/Users/user/.datahubenv already exists. Overwrite? [y/N]: y
Configure which datahub instance to connect to
Enter your DataHub host [http://localhost:8080]: http://localhost:8080
Enter your DataHub access token []:
```

### Non-Interactive Mode with Username/Password

The CLI can automatically generate tokens from your username and password credentials. This is useful for quickstart instances, automation, and CI/CD pipelines.

```shell
# Quickstart (local instance with default credentials)
datahub init --profile local --username datahub --password datahub

# Custom credentials with longer token duration
datahub init --profile local --username datahub --password datahub --token-duration ONE_MONTH

# For long-running jobs or CI/CD
datahub init --profile local --username datahub --password datahub --token-duration ONE_WEEK
```

### DataHub Cloud Example

For DataHub Cloud instances, you can use an existing token:

```shell
# Interactive
datahub init
Enter your DataHub host [http://localhost:8080]: https://<your-instance-id>.acryl.io/gms
Enter your DataHub access token []: <token generated from https://<your-instance-id>.acryl.io/settings/tokens>

# Non-interactive
datahub init --profile acme --host https://acme.example.com/gms --token "<token>"
```

### Environment variables supported

Connection environment variables take precedence over the saved machine default. An explicit
`--profile` or `DATAHUB_PROFILE` takes precedence over connection environment variables, as described
in [Selection order](#selection-order).

- `DATAHUB_PROFILE` - Select a named connection for this shell or session.
- `DATAHUB_SKIP_CONFIG` (default `false`) - Set to `true` to skip loading saved connection files. Cannot be combined with `--profile`; `DATAHUB_PROFILE` is ignored when it is set.
- `DATAHUB_GMS_URL` (default `http://localhost:8080`) - Set to a URL of GMS instance
- `DATAHUB_GMS_HOST` (default `localhost`) - Set to a host of GMS instance. Prefer using `DATAHUB_GMS_URL` to set the URL.
- `DATAHUB_GMS_PORT` (default `8080`) - Set to a port of GMS instance. Prefer using `DATAHUB_GMS_URL` to set the URL.
- `DATAHUB_GMS_PROTOCOL` (default `http`) - Set to a protocol like `http` or `https`. Prefer using `DATAHUB_GMS_URL` to set the URL.
- `DATAHUB_GMS_TOKEN` (default `None`) - Used for communicating with DataHub Cloud.
- `DATAHUB_CLIENT_CERT_PATH` (default `None`) - Path to a PEM file containing the client certificate to present when GMS sits behind a proxy that requires mutual TLS (mTLS). The file may be a combined PEM (cert + private key) if `DATAHUB_CLIENT_KEY_PATH` is not set. Applies to **all** outbound HTTPS from the CLI / Python SDK — `datahub init`, `datahub ingest`, the `datahub-rest` sink, and any `DataHubGraph` client.
- `DATAHUB_CLIENT_KEY_PATH` (default `None`) - Path to a PEM file containing the private key matching `DATAHUB_CLIENT_CERT_PATH`. Required only when the cert and key live in separate files (e.g. the kubernetes `tls`-secret layout, where `tls.crt` and `tls.key` are mounted side by side).
- `DATAHUB_USERNAME` (default `None`) - Username for generating access tokens via `datahub init`. Used with `DATAHUB_PASSWORD` for non-interactive authentication.
- `DATAHUB_PASSWORD` (default `None`) - Password for generating access tokens via `datahub init`. Used with `DATAHUB_USERNAME` for non-interactive authentication.
- `DATAHUB_TELEMETRY_ENABLED` (default `true`) - Set to `false` to disable telemetry. If CLI is being run in an environment with no access to public internet then this should be disabled.
- `DATAHUB_TELEMETRY_TIMEOUT` (default `10`) - Set to a custom integer value to specify timeout in secs when sending telemetry.
- `DATAHUB_DEBUG` (default `false`) - Set to `true` to enable debug logging for CLI. Can also be achieved through `--debug` option of the CLI. This exposes sensitive information in logs, enabling on production instances should be avoided especially if UI ingestion is in use as logs can be made available for runs through the UI.
- `DATAHUB_VERSION` (default `quickstart`) - Set to a specific version to run quickstart with the particular version of docker images (`quickstart`, a release `v*`, or a commit pin `sha-<short_sha>`).
- `DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH` (default unset) - Only for cases where column-level lineage aspects grow too large. Set it to N (at least `1`) to cut `transformOperation` in fine-grained lineage to its first N characters followed by `...`; values of at most N + 3 characters are kept unchanged. With `incremental_lineage`, changing the value leaves entries stored under the old value until a run with `incremental_lineage: false`.
- `ACTIONS_VERSION` (default `quickstart`) - Set to a specific version to run quickstart with that image tag of `datahub-actions` container.
- `DATAHUB_ACTIONS_IMAGE` (default `acryldata/datahub-actions`) - Set to `-slim` to run a slimmer actions container without pyspark/deequ features.
- `DATAHUB_RECORDING_PASSWORD` - Password for encrypting/decrypting recording archives. Used by `--record` and `--replay` commands.
- `INGESTION_ARTIFACT_DIR` - Directory to save recordings when S3 upload is disabled. If not set, recordings are saved to a temp directory.
- `DATAHUB_REPORT_FAILURE_SAMPLE_SIZE` / `DATAHUB_REPORT_WARNING_SAMPLE_SIZE` / `DATAHUB_REPORT_INFO_SAMPLE_SIZE` (default `10`) - How many failure/warning/info entries to retain and show in the **final** report. Recipe flags: `report_failure_sample_size`, `report_warning_sample_size`, `report_info_sample_size`.
- `DATAHUB_PROGRESS_REPORT_MAX_FAILURES` / `DATAHUB_PROGRESS_REPORT_MAX_WARNINGS` / `DATAHUB_PROGRESS_REPORT_MAX_INFOS` (default `10`) - How many entries to display in the **interim** 60-second progress reports. The final report is unaffected. Recipe flags: `progress_report_max_failures`, `progress_report_max_warnings`, `progress_report_max_infos`.
- `DATAHUB_CALLER` (default: auto-detected) - Override the caller label sent in the User-Agent header to GMS. The CLI auto-detects common callers (e.g. coding agents, CI systems), but you can set this explicitly for custom tools or to distinguish environments (e.g. `airflow-prod-etl`).

```shell
DATAHUB_SKIP_CONFIG=false
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_GMS_TOKEN=
DATAHUB_TELEMETRY_ENABLED=true
DATAHUB_TELEMETRY_TIMEOUT=10
DATAHUB_DEBUG=false
DATAHUB_REPORT_FAILURE_SAMPLE_SIZE=10
DATAHUB_REPORT_WARNING_SAMPLE_SIZE=10
```

## Profiles

Use named profiles when you work with more than one DataHub instance. Each named connection lives
in `~/.datahub/profiles/<name>/.datahubenv`. Your existing `~/.datahubenv` remains the reserved
`default` profile. Profiles created by overriding `HOME` under that directory already work.
Telemetry, browser discovery, recipes, and caches continue to use your normal home directory.

```bash
datahub init --profile local --host http://localhost:8080 --username datahub --password datahub
datahub init --profile acme --host https://acme.example.com/gms --token "<token>" --description "Acme instance"
datahub search orders --profile acme
datahub --profile acme profile current --format json
```

The global `--profile` flag works before or after the subcommand. Names match
`[a-z0-9][a-z0-9._-]{0,62}`. Initialization supports token, username/password, SSO, OAuth, and support
authentication. It creates or refreshes a profile without changing the active connection.

### Selection order

The first applicable source wins:

1. `--profile <name>` for one invocation.
2. `DATAHUB_PROFILE` for a shell, agent session, directory environment, or CI job.
3. Existing GMS connection environment variables: `DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`, and the existing host, port, and protocol variables.
4. `~/.datahub/current-profile` for the machine default.
5. `~/.datahubenv`, the `default` profile.

An unknown profile is an error, including a stale active pointer. The CLI lists the profiles and
servers available and never falls back to another instance. If an explicit profile conflicts with
the environment's GMS URL, the profile wins and stderr identifies both servers. `--profile` with
`DATAHUB_SKIP_CONFIG=true` is an error; under `DATAHUB_SKIP_CONFIG=true` an inherited `DATAHUB_PROFILE` is ignored. Without an explicit profile, the skip-config behavior
and connection environment precedence stay unchanged. Environment-configured OAuth
(`DATAHUB_AUTH_TYPE`) keeps its precedence over static tokens, but is not applied to an explicit
profile while `DATAHUB_GMS_URL` names another server, because its tokens belong to that server.

A profile selects a server together with its credential. Unlike `AWS_PROFILE`, which exported
AWS credentials can still override, `DATAHUB_PROFILE` deliberately outranks `DATAHUB_GMS_URL` and
`DATAHUB_GMS_TOKEN`, so a token exported for one instance is never sent to another. Recipe
references to `${DATAHUB_GMS_URL}` and `${DATAHUB_GMS_TOKEN}` follow the same rule.

The active pointer (`profile use`) is a convenience for interactive use and is shared by every
shell and agent on the machine. To switch one shell only, run
`eval "$(datahub profile use --shell acme)"`, which sets `DATAHUB_PROFILE` there. Scripts and
agents should pin a profile with `--profile` or `DATAHUB_PROFILE` rather than rely on the pointer;
`profile current` and the write-target line say when the pointer made the choice.

`datahub profile current --format json` reports `name`, `server`, `source`, and `token_expires`
(JWT expiry in epoch seconds, or null). Check the server before a write. Write commands announce
named targets on stderr. `datahub ingest run` writes through the recipe's sink, so it announces
that destination instead. A recipe whose `datahub-rest` sink names a different server than an
explicit `--profile` is refused before anything runs. Under `DATAHUB_PROFILE` or the active pointer
the recipe's sink wins with a warning naming both servers. A recipe without a sink writes to the
selected profile. The Python SDK uses the same rules through `load_client_config(profile=...)`,
including clients that load their default connection through `DataHubGraph`.

### Manage connections

| Command                                              | Behavior                                                                                                                                                                                                                      |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `datahub profile list [--format json]`               | Lists names, servers, token status, descriptions, and an active marker. Never displays tokens.                                                                                                                                |
| `datahub profile current [--format json]`            | Reports the selected profile and why it was chosen.                                                                                                                                                                           |
| `datahub profile use acme [--shell]`                 | Changes the machine default. Requires `--yes` without a terminal. `use default` removes the active pointer. `--shell` instead prints `export DATAHUB_PROFILE=acme` for the current shell.                                     |
| `datahub profile save acme [--from local] [--force]` | Copies a stored connection, defaulting to `~/.datahubenv`. Ignores environment credentials and refuses an existing destination without `--force`. An OAuth session is not copied; run `init --oauth` on the copy for its own. |
| `datahub profile show acme`                          | Displays server, expiry, auth mode, and path with the token redacted. `--show-token` reveals it explicitly.                                                                                                                   |
| `datahub profile remove acme`                        | Deletes the profile directory after confirmation; if it was active, `default` becomes active. Requires `--yes` without a terminal and refuses `default`.                                                                      |
| `datahub profile rename acme acme-staging`           | Moves the directory and updates the active pointer if needed.                                                                                                                                                                 |
| `datahub profile doctor [--fix]`                     | Reports expired tokens, legacy keys, loose permissions, and stray files. Only `--fix` applies repairs. Never deletes stray files.                                                                                             |

Profiles store one credential each, in plaintext YAML with directory permissions `0700` and file
permissions `0600`. OAuth refresh updates the profile it read from. OAuth sessions are per
profile: two profiles for the same identity provider each need their own `init --oauth` login. New descriptions live under
the top-level `profile.description` key, preserving compatibility with older configuration readers.

### Initialize safely from scripts

Without `--profile`, init chooses its destination from `DATAHUB_PROFILE`, then the machine default,
then `default`. `DATAHUB_GMS_URL` supplies the new host, not a destination profile. Without a terminal,
init refuses to change an existing profile to another server and suggests creating a separate
profile. Refreshing credentials against the same server still succeeds. Pass `--force` only when
you intend to replace the connection. Token-duration defaults stay `ONE_MONTH` for localhost and
`ONE_HOUR` for remote hosts.

### DataHub CLI connection profiles

- Never run `datahub init` without `--profile <name>`. Name the profile after the instance (`acme`, `acme-staging`).
- Never run `datahub profile use`; it changes the default for the user's other shells.
- Pass `--profile <name>` on every command, unless the session already sets `DATAHUB_PROFILE`.
- Before any write (`put`, `delete`, `ingest`, upserts), run `datahub profile current --format json` and confirm the server is the one the user asked for.
- If a token has expired, re-run `datahub init --profile <same name>` with the same host. Do not create a new profile.
