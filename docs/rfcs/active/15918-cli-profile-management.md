- Start Date: 2026-01-18
- RFC PR: [#15918](https://github.com/datahub-project/datahub/pull/15918)
- Discussion Issue: (if any)
- Implementation PR(s): [#21214](https://github.com/datahub-project/datahub/pull/21214) (merged)

# DataHub CLI Profile Management

## Summary

People working across DataHub instances need a reliable way to select a connection without overwriting another session's credentials. Add named connection profiles under `~/.datahub/profiles/<name>/.datahubenv`, while keeping `~/.datahubenv` as the `default` profile. The CLI and Python SDK share resolution rules, and automation cannot silently repoint an existing profile to another server.

## Basic example

```bash
datahub init --profile local --host http://localhost:8080 --token "<token>"
datahub init --profile acme --host https://acme.example.com/gms --token "<token>"
datahub profile list
datahub --profile acme profile current --format json
datahub search orders --profile acme

# Keep selection within this shell or agent session.
export DATAHUB_PROFILE=acme
datahub profile current --format json

# Preserve an existing login without logging in again.
datahub profile save acme-staging
```

Creating a profile never changes the active profile. Humans can explicitly change their machine default with `datahub profile use acme`, or switch one shell with `eval "$(datahub profile use --shell acme)"`. Agents pass `--profile` or set `DATAHUB_PROFILE` for their own session instead.

## Motivation

The CLI currently has one connection slot, `~/.datahubenv`. Users working across instances sometimes override `HOME` to maintain separate copies. That also moves browser discovery, telemetry, and caches. It gives users no standard way to list connections or inspect the selected target.

More seriously, non-interactive `datahub init` silently overwrites an existing configuration. An agent asked to connect to staging can change the human's default, allowing a later write to reach the wrong instance. We need explicit, inspectable selection and safe credential refresh without requiring migration for existing users.

## Requirements

- Select a named connection on every CLI command and in SDK clients.
- Report which profile and server a command uses and why.
- Reject unknown profiles instead of falling back to another instance.
- Preserve legacy configuration, environment precedence, and `DATAHUB_SKIP_CONFIG` behavior when profiles are not explicitly selected.
- Reuse profiles created by the `HOME` workaround without migration.
- Prevent non-interactive initialization from silently changing an existing profile's server.
- Keep credentials out of discovery output and write refreshed OAuth credentials back to their source profile.

### Extensibility

Keep the existing `.datahubenv` schema and a shared resolver so future credential storage and selection sources can use the same command surface. New metadata belongs at the top level, where older clients ignore unknown keys.

## Non-Requirements

This proposal does not move telemetry, ingestion recipes, caches, or `datahub lite` state into profiles; lite keeps its local state in `~/.datahubenv` whichever profile is selected. It does not introduce environment interpolation, agent scope configuration, a keychain backend, or confirmation requirements for destructive commands.

## Detailed design

### On-disk layout

Each named profile is a directory holding exactly the file `datahub init` already writes, so the format does not change:

```
~/.datahubenv                         # the "default" profile (unchanged, legacy location)
~/.datahub/
  current-profile                     # one line: name of the active profile (absent = default)
  profiles/
    acme/
      .datahubenv                     # gms.server, gms.token, optional oauth block
    acme-staging/
      .datahubenv
    local/
      .datahubenv
  telemetry-config.json, datapack-cache/, quickstart/ …   # stay global, never per profile
```

Rules:

- `default` is reserved and always means `~/.datahubenv`. Existing users and CI see no change.
- Names match `[a-z0-9][a-z0-9._-]{0,62}`.
- A profile exists when `profiles/<name>/.datahubenv` exists. Other files in the directory are ignored, so folders created by the HOME hack still count.
- `current-profile` is a plain text file, not a symlink, so it works on Windows.
- Permissions: the CLI creates profile directories `0700` and `.datahubenv` files `0600`. Today `persist_raw_datahub_config` uses the process umask, typically `0644`.

The `.datahubenv` schema stays the same: `gms.server`, `gms.token`, optional `oauth`. One optional top-level key is added: `profile.description`, shown by `datahub profile list`. New keys always go at the top level, never under `gms:` — `DatahubClientConfig` inherits `extra="forbid"`, so a new key under `gms:` would make older CLIs fail to load the file. The top-level `DatahubConfig` model ignores unknown keys.

### Command surface

| Surface                              | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--profile <name>` (short flag TBD)  | Selects the profile for this one invocation. Accepted before or after the subcommand (`datahub --profile acme search x` and `datahub search x --profile acme` are equivalent).                                                                                                                                                                                                                                                         |
| `DATAHUB_PROFILE=<name>`             | Same as the flag, for a whole shell, CI job or agent session.                                                                                                                                                                                                                                                                                                                                                                          |
| `datahub init --profile <name> …`    | Writes `~/.datahub/profiles/<name>/.datahubenv`, creating the directory. `--profile` is the global option, not an `init` option, so there is one way to select the destination. All existing init modes (`--token`, `--username/--password`, `--sso`, `--oauth`, `--support`) work unchanged. Never changes the active profile. Adds `--description`, which is kept when `init` is re-run without it.                                  |
| `datahub profile list`               | Name, server, token status (valid until / expired / no expiry), marker on the active one (none when `DATAHUB_GMS_URL` is in effect). Never prints tokens. `--format json`.                                                                                                                                                                                                                                                             |
| `datahub profile current`            | Resolved profile, its server, and why it was chosen. `--format json` returns `{"name", "server", "source", "token_expires"}`.                                                                                                                                                                                                                                                                                                          |
| `datahub profile use <name>`         | Writes `~/.datahub/current-profile`. `use default` removes the file. Non-interactive use requires `--yes`. `--shell` instead prints `export DATAHUB_PROFILE=<name>` for `eval`, switching one shell and leaving the pointer alone; plain `use` prints that alternative.                                                                                                                                                                |
| `datahub profile save <name>`        | Copies the `default` profile (`~/.datahubenv`) into a new named profile, so an existing setup becomes a profile without logging in again. Reads the file directly, never `DATAHUB_GMS_URL`/`DATAHUB_GMS_TOKEN`. Refuses an existing name unless `--force`. `--from <profile>` copies another profile instead. Does not copy an OAuth refresh token: refresh tokens rotate on use, so two profiles sharing one would revoke each other. |
| `datahub profile show <name>`        | Server, token expiry, auth mode (static / OAuth with refresh), file path. Token redacted unless `--show-token`.                                                                                                                                                                                                                                                                                                                        |
| `datahub profile remove <name>`      | Deletes the profile directory after a TTY confirmation; non-TTY requires `--yes`. Refuses `default`. If it was the active profile, removes `current-profile` and says `default` is now active.                                                                                                                                                                                                                                         |
| `datahub profile rename <old> <new>` | Moves the directory and updates `current-profile` if it pointed at `<old>`.                                                                                                                                                                                                                                                                                                                                                            |
| `datahub profile doctor`             | Flags expired tokens, legacy keys (`gms_url`/`gms_token`), loose permissions, and stray non-config files, and prints the fix for each. Changes nothing without `--fix`; never deletes non-config files.                                                                                                                                                                                                                                |

Implicit profile creation is off: `--profile foo` on any command other than `init` fails if `foo` does not exist, and lists the profiles that do. A typo must never fall through to the default instance.

### Resolution

The first source that names a profile wins:

| Order | Source                                                                              | Scope                                                                |
| ----- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| 1     | `--profile <name>` flag                                                             | one command                                                          |
| 2     | `DATAHUB_PROFILE` env var                                                           | one shell, session or directory (direnv, agent harness settings, CI) |
| 3     | `DATAHUB_GMS_URL` (+ `DATAHUB_GMS_TOKEN`, and the existing HOST/PORT/PROTOCOL vars) | unchanged from today                                                 |
| 4     | `~/.datahub/current-profile`                                                        | the user's machine                                                   |
| 5     | `~/.datahubenv`                                                                     | the `default` profile                                                |

Conflicts are loud, never silent:

- Explicit profile (1 or 2) plus `DATAHUB_GMS_URL` pointing at a different server: the profile wins and the CLI prints one stderr warning naming both.
- `DATAHUB_SKIP_CONFIG=true` plus `--profile`: usage error. An inherited `DATAHUB_PROFILE` is ignored under `DATAHUB_SKIP_CONFIG`, because executors set it for subprocesses that may inherit a user's shell environment. Otherwise `DATAHUB_SKIP_CONFIG` behaves exactly as today.
- A named profile that does not exist (from any source, including a stale `current-profile` or `DATAHUB_PROFILE`) is an error. The CLI never falls back to `default`. A profile named by `--profile` or `DATAHUB_PROFILE` is checked before any command runs; a stale `current-profile` is reported when a command reads its connection, so `version` and `--help` still work. The error names the fix (`datahub profile use default`), and `profile list` still works so the user can choose. An empty `DATAHUB_PROFILE` or `current-profile` counts as unset.
- `DATAHUB_AUTH_TYPE` (env-configured OAuth) keeps its current precedence rules relative to tokens. When an explicit profile wins over a `DATAHUB_GMS_URL` naming another server, env OAuth is not applied to the profile, since its tokens belong to that other server; the CLI warns. When `DATAHUB_GMS_URL` names the profile's own server (ignoring a trailing slash), env OAuth still applies.

A profile selects a server together with its credential. This is deliberately stronger than `AWS_PROFILE`, which exported AWS credentials can still override: `DATAHUB_PROFILE` outranks `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN`. AWS signs requests per region and endpoint; DataHub sends a bearer token, so pairing one instance's server with another's token would leak the token. Every consumer of the connection follows the same pairing (see Implementation).

`datahub profile current` reports the source, e.g. `acme  https://acme.example.com/gms  (from DATAHUB_PROFILE)`. When the machine pointer chose the profile, the line says so explicitly (`from current-profile, the machine-wide default set by datahub profile use`), so a change made from another terminal is visible before a write. The same line goes to stderr from commands that write through the CLI connection (`put`, `delete`, `ingest deploy`/`mcps`/`rollback`, upserts) when the resolved profile is not `default`. The announcement never fails a command: a stale pointer is reported by the command only if it actually reads the connection.

`datahub ingest run` and `ingest replay --live-sink` write through the recipe's sink, not necessarily the selected profile, so announcing the profile would confirm the wrong target. They check the loaded recipe after `${VAR}` substitution instead:

| Recipe sink                            | No named profile | `DATAHUB_PROFILE` or pointer   | Explicit `--profile`            |
| -------------------------------------- | ---------------- | ------------------------------ | ------------------------------- |
| none (default sink)                    | unchanged        | announces the profile          | announces the profile           |
| `datahub-rest` on the profile's server | unchanged        | announces it                   | announces it                    |
| `datahub-rest` on another server       | unchanged        | warns naming both; recipe wins | error naming both; nothing runs |
| other sink types (file, Kafka)         | unchanged        | notes the recipe decides       | notes the recipe decides        |

An explicit `--profile` is a direct statement of intent, and it is new, so refusing a contradiction breaks no existing invocation. `DATAHUB_PROFILE` and the pointer are session-wide, so a refusal there would fail every recipe with its own sink; they warn. The motivating case: `--profile staging` with a recipe sink of `${DATAHUB_GMS_URL}` while `DATAHUB_GMS_URL` is exported for prod. Recipe substitution for `datahub ingest` stays environment-only, since executors depend on it; the check compares the substituted server. A recipe's separate `datahub_api` (stateful-ingestion) server is not checked yet.

Where `datahub init` writes: with `--profile`, that profile. Without it, the profile resolved from sources 1, 2, 4 and 5, printed in the success message. `DATAHUB_GMS_URL` is an input to init, not a destination.

Token duration defaults are unchanged (`ONE_MONTH` for localhost, `ONE_HOUR` for remote).

### Guardrails for automation

The CLI enforces these rules:

1. `init` no longer silently repoints a profile. If stdin is not a TTY, the target profile exists, and the new host differs from the stored server, `init` exits non-zero. Re-running against the same server (token refresh) succeeds. `--force` overrides.
2. The refusal derives a profile name from the host and prints a command to run as-is:

```
Error: ~/.datahubenv (profile "default") points at https://acme.example.com/gms.
Refusing to repoint it to https://acme-staging.example.com/gms from a non-interactive session.
To add a separate profile for this instance, run:
  datahub init --profile acme-staging --host https://acme-staging.example.com/gms <same auth flags>
Then pass --profile acme-staging (or set DATAHUB_PROFILE=acme-staging) on later commands.
To overwrite anyway, add --force.
```

3. `profile use` needs `--yes` when non-interactive.
4. Writes announce their target on stderr; `ingest run` announces the recipe's real destination.
5. An ingest recipe whose sink contradicts an explicit `--profile` is refused before the pipeline starts.
6. "No config" and "unknown profile" errors list available profiles and servers.

### Security

- `0700` directories, `0600` files, including the legacy `~/.datahubenv` when next written.
- Writes, including OAuth refresh, are atomic (temporary file, then rename) and follow symlinks, so a `~/.datahubenv` kept in a dotfiles repo stays linked.
- No tokens in `list`, `current`, or the stderr target line; `show` redacts unless `--show-token`, which agent context never mentions.
- Expiry from the JWT `exp` claim, reusing the existing OAuth session decoder.
- One copy per credential: sessions and worktrees select a profile by name, never copy tokens.
- OAuth refresh writes back to the profile it read from. OAuth sessions are per profile: two profiles for the same identity provider each need their own `init --oauth` login (see Future Work).

### Implementation

1. `cli/config_utils.py`: replace the import-time constant `DATAHUB_CONFIG_PATH` with `resolve_profile() -> ResolvedProfile` (dataclass: name, path, source, server), cached per process and resettable for tests. `get_raw_client_config`, `persist_raw_datahub_config`, `_ensure_datahub_config` and `refresh_oauth_token_if_needed` take the resolved path. Keep `DATAHUB_CONFIG_PATH`/`CONDENSED_DATAHUB_CONFIG_PATH` importable for compatibility (other modules import them) but stop using them for reads/writes. Resolve the home directory with `os.path.expanduser`, not `Path.home()`, which raises when `HOME` is unset and the UID has no passwd entry (arbitrary-UID containers).
2. `load_client_config(profile: Optional[str] = None)`: insert resolution at the positions above. This covers the SDK, `DataHubGraph` and the REST sink. Code that reads env vars or `~/.datahubenv` before calling it must follow the selected profile too, so the server and token never come from different instances: `chunking_config.py` loads both defaults from one `load_client_config()` call (localhost only when nothing is configured), and the recipe secret resolver answers `${DATAHUB_GMS_URL}`/`${DATAHUB_GMS_TOKEN}` from a profile chosen by name (sources 1, 2, 4) ahead of the environment, leaving a field the profile lacks unresolved rather than taking it from the environment. Other recipe variables keep environment precedence.
3. `entrypoints.py`: global `--profile` on the `datahub` group, stored on the Click context and in the resolver, and exported as `DATAHUB_PROFILE` for the command's duration so worker threads, spawned processes and child `datahub` commands target the same profile. An argv pre-pass in `main()` lifts `--profile X` / `--profile=X` from after the subcommand (stop at `--`). No subcommand defines `--profile`, including `init` (`docker_cli.py` only passes it to docker compose as a subprocess argument).
4. `init`: reads the global profile, adds `--description`; non-TTY same-server check replaces the silent overwrite; success messages name the profile.
5. New `cli/profile_cli.py` with list/current/use/save/show/remove/rename/doctor, using the existing agent-aware help command class in `entrypoints.py`.
6. Resources: rewrite `INIT_AGENT_CONTEXT.md`, add `PROFILE_AGENT_CONTEXT.md`.
7. Announce write targets from a hook on each command's `invoke`, installed by `enable_auto_decorators`, rather than by replacing callbacks: telemetry detects its own wrappers by walking the callback, and an instrumented callback must stay the same object. `ingest_cli.py` runs the recipe sink check above.
8. Replace hardcoded `~/.datahubenv` strings in `cli_utils.py` and `ingest_cli.py` error messages with the resolved path.
9. Tests in `tests/unit/cli/`: resolution order and each conflict, non-TTY repoint refusal and same-server refresh, unknown profile errors, `profile save` ignoring env vars, OAuth refresh target file, file permissions, argv pre-pass.

## How we teach this

A profile is a named connection to one instance with one credential. Start with `datahub init --profile local` and use `datahub profile current --format json` to inspect the target before a write. The CLI guide documents selection order and all management commands.

Rewrite `INIT_AGENT_CONTEXT.md` to lead with explicit profile selection and explain non-interactive repoint refusal. Add `PROFILE_AGENT_CONTEXT.md`, available through `datahub profile --agent-context` and appended to non-TTY `datahub profile --help`. Fix the root `AGENTS.md` example to use `--profile local`.

Every agent-facing surface carries these rules:

```markdown
## DataHub CLI connection profiles

- Never run `datahub init` without `--profile <name>`. Name the profile after the instance (`acme`, `acme-staging`).
- Never run `datahub profile use`; it changes the default for the user's other shells.
- Pass `--profile <name>` on every command, unless the session already sets `DATAHUB_PROFILE`.
- Before any write (`put`, `delete`, `ingest`, upserts), run `datahub profile current --format json` and confirm the server is the one the user asked for.
- If a token has expired, re-run `datahub init --profile <same name>` with the same host. Do not create a new profile.
```

## Drawbacks

Profiles add configuration state and selection rules that users must learn. A stale `current-profile` (for example, a profile directory deleted by hand) now stops a command, requiring an explicit repair; `profile remove` avoids this by resetting the pointer itself. Credentials remain plaintext on disk, protected by restrictive permissions rather than a keychain. The non-interactive repoint guard changes behavior for scripts that reused a configuration file across different servers.

## Alternatives

- **One `~/.datahub/config.yaml` containing every profile.** The earlier version of this RFC and [PR #15919](https://github.com/datahub-project/datahub/pull/15919) used this design, including ancestor-walk selection, environment interpolation, and `require_confirmation`. A per-profile directory reuses the existing file format and existing `HOME` workaround layouts. It avoids a new migration and keeps selection separate from independent destructive-command confirmations.
- **`~/.datahub/sessions.json` and `session use` copying into `~/.datahubenv`.** [PR #12458](https://github.com/datahub-project/datahub/pull/12458) proposed sessions, including the useful `session save` idea. Copying credentials into a shared slot changes the connection for other shells and can leave multiple copies of refreshed credentials. This design selects the source file directly and retains the save command as an explicit migration operation.
- **A committed per-directory pointer file.** A profile name is local to a machine and may not exist for another contributor. A committed pointer can silently select an unintended target. Directory-specific selection can already use `DATAHUB_PROFILE` through direnv or agent session settings. A future pointer must remain local and uncommitted.
- **No machine-wide pointer; shell-scoped selection only, as in AWS.** AWS has no persistent active profile: `AWS_PROFILE` or `--profile` select per shell or command. That is easier to reason about when several terminals or agents run at once, but it removes the persistent switch users asked for (switching staging and prod without editing shell startup files) and departs from today's behavior, where `~/.datahubenv` is already one machine-wide setting. kubectl (`current-context`), gcloud (`configurations activate`) and Docker (`docker context use`) all keep a machine default under a per-shell environment variable and a per-command flag, the same three layers as here. The pointer is the lowest-precedence named source, `profile use --shell` offers AWS-style shell scoping, agent context forbids `profile use`, and the target line names the pointer when it made the choice.
- **Multiple tokens per profile.** Read and write credentials introduce another selection rule and ambiguity about which token a command uses. Keep one credential per profile, as in npm, AWS, kubectl, and Snowflake. A future read-only credential uses a separate profile and can optionally declare top-level access metadata.

## Rollout / Adoption Strategy

| Who                                                                      | What changes                                                                                        |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| Single-instance users                                                    | Nothing. `~/.datahubenv` is the `default` profile.                                                  |
| CI using `DATAHUB_GMS_URL` / `DATAHUB_GMS_TOKEN`                         | Nothing.                                                                                            |
| CI re-running `datahub init` against a different host on a reused runner | Breaking: needs `--force` (or `--profile`). Document in `docs/how/breaking-changes.md`.             |
| HOME-hack users                                                          | `HOME=~/.datahub/profiles/x datahub …` becomes `datahub --profile x …`. The old form keeps working. |
| SDK scripts calling `load_client_config()`                               | Pick up `DATAHUB_PROFILE` and `current-profile` automatically; add a `profile=` argument.           |

`datahub profile save <name>` turns an existing `~/.datahubenv` into a named profile (idea credited to the earlier `session save` in PR #12458).

Document the non-interactive `init` change in the upgrade guide. Ship profile commands, shared SDK resolution, agent context, and permissions together. Existing users can adopt named profiles incrementally without moving their default file.

## Future Work

- A per-directory `.datahub-profile` file. Per-directory pinning is covered by `DATAHUB_PROFILE` via direnv or agent harness settings. Can be added later as a source between `DATAHUB_GMS_URL` and `current-profile`; if added it stays local (never committed).
- Read-only tokens: one credential per profile (as with npm, AWS, kubectl, Snowflake). `datahub init --profile acme --read-only` would store only a read-only token; write access is a separate profile. At most an optional top-level `access: read` key.
- Local dev environment integration: `scripts/dev/datahub-dev.sh` registers a `local-<worktree>` profile for the instance it starts and `shell-env` exports `DATAHUB_PROFILE`; `instances clean` removes it. Follow-up PR.
- Shared login sessions: AWS lets several profiles reference one named SSO session with a shared, refreshed token cache. Here each profile holds its own OAuth session, since copying a rotating refresh token would make profiles revoke each other. Sharing would need a separate session object with coordinated refresh.
- Per-setting provenance (as in `aws configure list`): report where the server, token and auth mode each came from, not only which profile was selected.
- A confirmation decorator for destructive commands (raised in review of #15919), independent of profiles.

An operating-system keychain backend can later sit behind the same layout.

## Unresolved questions

- Naming clash: some agent skills already call their scope config files "profiles".
- Should read commands also print the stderr target line?
- Short flag: is `-P` worth taking alongside the global `-C/--context`?
