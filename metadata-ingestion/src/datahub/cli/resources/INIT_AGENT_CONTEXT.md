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

## SSO browser login

For DataHub instances using SSO (OIDC/SAML), use `--sso` to authenticate via browser:

```bash
# Opens browser — complete SSO, CLI captures session and generates token
datahub init --sso --host https://your-instance.example.com/gms

# Custom token duration
datahub init --sso --host https://your-instance.example.com/gms --token-duration ONE_MONTH
```

**Prerequisites** (one-time setup):

```bash
pip install 'acryl-datahub[sso]'   # or: uv pip install 'acryl-datahub[sso]'
```

`--sso` opens the computer's default browser. If that is Chrome, Edge or
Firefox, there is nothing else to install.

Playwright cannot automate Safari. If Safari is the default, run
`playwright install chromium` once, and login opens that Chromium instead. If
no browser can be opened at all, the error says which one to install.

Login runs in its own browser profile under `~/.datahub/sso-browser-profiles`
(directory 0700), not the user's everyday one, so by default the first run
signs in — `--seed-profile` is what skips even that. The profile is reused per
instance, so a still-valid identity provider session skips the login form on
later runs.

- `--fresh-login` deletes both stores for this instance — the saved browser
  profiles and any remembered session — to sign in as somebody else.
- `--seed-profile DIR` copies an existing profile in, but only while the CLI's
  own profile directory is still empty. Anything a failed earlier attempt left
  behind counts, so `--fresh-login` is what makes a seed apply again. The seed
  must come from the same engine as the browser being driven, and for Chromium
  must be the user data directory rather than a single profile inside it; both
  mismatches are refused.
- `--remember-session` stores the login cookies in `~/.datahub/sso-sessions`
  (directory 0700, per-instance JSON files 0600) and replays them, for
  providers whose session cookie no browser persists. The next run then tries
  the login headlessly first and opens a visible browser if that does not
  authenticate.

All three require `--sso`.

`--sso` is mutually exclusive with `--token`, `--username`, and `--password`.
If Playwright is not installed, the command prints step-by-step install instructions and exits.

### Support login (DataHub Cloud)

For the support team debugging customer instances, add `--support` to use the
`/support/authenticate` login path:

```bash
datahub init --sso --support --host https://customer.acryl.io/gms
```

`--support` requires `--sso`.

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
