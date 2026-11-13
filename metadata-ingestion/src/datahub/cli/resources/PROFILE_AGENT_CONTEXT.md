# DataHub profile agent context

## DataHub CLI connection profiles

- Never run `datahub init` without `--profile <name>`. Name the profile after the instance (`acme`, `acme-staging`).
- Never run `datahub profile use`; it changes the default for the user's other shells.
- Pass `--profile <name>` on every command, unless the session already sets `DATAHUB_PROFILE`.
- Before any write (`put`, `delete`, `ingest`, upserts), run `datahub profile current --format json` and confirm the server is the one the user asked for.
- If a token has expired, re-run `datahub init --profile <same name>` with the same host. Do not create a new profile.

## Select and verify

Selection order: `--profile`, `DATAHUB_PROFILE`, existing GMS connection environment variables,
`~/.datahub/current-profile`, then `~/.datahubenv` (`default`). The global flag works before or after
the subcommand. Unknown profiles stop the command. An explicit profile overrides a conflicting GMS
URL with a warning, and cannot be combined with `DATAHUB_SKIP_CONFIG=true` (`DATAHUB_PROFILE` is ignored under it).

```bash
datahub profile list --format json
datahub --profile acme profile current --format json
DATAHUB_PROFILE=acme datahub profile current --format json
```

`current` returns `name`, `server`, `source`, and `token_expires` (JWT expiry in epoch seconds, or null).
Confirm the server before writing. Writes announce a named target on stderr. For `datahub ingest run`, the recipe's
sink decides the destination: omit the sink to write to the selected profile. `--profile` with a
recipe sink on another server is refused. Discovery output never
includes tokens.

## Maintain profiles

`datahub profile save acme` copies the stored default connection, ignoring environment credentials. It does not copy an OAuth refresh token.
`--from local` copies another profile, and `--force` explicitly replaces an existing destination.
Use `profile show acme` to inspect authentication mode and expiry, `profile rename acme acme-staging`
to rename a profile, and `profile doctor` to diagnose expired tokens, legacy keys, permissions, and
stray files. Doctor changes nothing without `--fix` and never deletes non-config files.

`profile use` changes the machine default and requires `--yes` without a TTY. Agents must not use it.
`profile use --shell <name>` only prints `export DATAHUB_PROFILE=<name>` and changes nothing; prefer
setting `DATAHUB_PROFILE` or passing `--profile` directly.
`profile remove` deletes a profile directory (resetting to `default` if it was active) and requires `--yes` without a TTY. The reserved `default`
profile cannot be removed or renamed.
