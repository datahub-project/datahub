# DataHub Init CLI - Agent Context

Best practices for AI agents using `datahub init` to set up authentication.

## What it does

Writes `~/.datahubenv` with the GMS URL and an access token. Run once before using any other
CLI commands that require authentication.

## Quickstart (local instance)

```bash
# Default credentials on localhost — no --host, no --force needed
datahub init --username datahub --password datahub
```

Non-interactive signals (credentials via flags or env vars) trigger all agent-friendly defaults
automatically — no extra flags required.

## Key defaults

| Situation                               | Default behaviour                     |
| --------------------------------------- | ------------------------------------- |
| `--host` omitted + credentials provided | silently uses `http://localhost:8080` |
| `--token-duration` omitted, localhost   | `ONE_MONTH`                           |
| `--token-duration` omitted, remote host | `ONE_HOUR`                            |
| Config file already exists, non-TTY     | silently overwrites (no prompt)       |
| Config file already exists, TTY         | prompts for confirmation              |

## Common scenarios

```bash
# Local instance — minimal form
datahub init --username datahub --password datahub

# Local instance — explicit duration override
datahub init --username datahub --password datahub --token-duration NO_EXPIRY

# Remote instance — always pass --host explicitly
datahub init --host https://your-instance.acryl.io/gms --username alice --password secret

# Already have a token — skip credential exchange
datahub init --host https://your-instance.acryl.io/gms --token <your-token>

# CI/CD — fully non-interactive via env vars
export DATAHUB_GMS_URL=https://prod.example.com/gms
export DATAHUB_GMS_TOKEN=<your-token>
datahub init
```

## Environment variables

| Variable            | CLI equivalent |
| ------------------- | -------------- |
| `DATAHUB_GMS_URL`   | `--host`       |
| `DATAHUB_GMS_TOKEN` | `--token`      |
| `DATAHUB_USERNAME`  | `--username`   |
| `DATAHUB_PASSWORD`  | `--password`   |

CLI flags take precedence over environment variables.

## Available token durations

`ONE_HOUR`, `ONE_DAY`, `ONE_WEEK`, `ONE_MONTH`, `THREE_MONTHS`, `SIX_MONTHS`, `ONE_YEAR`,
`NO_EXPIRY`
