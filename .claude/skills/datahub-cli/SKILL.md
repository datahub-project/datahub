---
name: datahub-cli
description: >
  Operate the datahub CLI against any DataHub instance (local, staging, production):
  init a connection profile, run GraphQL queries and mutations, discover operations.
  Use when asked to query or change metadata on a running instance from the shell.
---

# DataHub CLI operations

Safety rules for profiles and writes live in root `AGENTS.md`, "DataHub CLI connection
profiles", and apply to every command here.

## Common operations

These commands work against **any** DataHub instance — local dev, staging, or production.
Provide connection details via environment variables:

```bash
export DATAHUB_GMS_URL=http://localhost:8080  # or your instance URL
export DATAHUB_GMS_TOKEN=<your-token>         # omit if auth is not required
```

### Init (setup authentication)

`datahub init --profile local` writes the local connection to
`~/.datahub/profiles/local/.datahubenv`. Pass `--profile local` on subsequent commands.

```bash
# Quickstart: local instance with default credentials
datahub init --profile local --username datahub --password datahub

# Full agent best-practices guide (defaults, env vars, all scenarios)
datahub init --agent-context
```

### GraphQL

`datahub graphql` executes queries and mutations against the DataHub GraphQL API and can
introspect the live schema to discover available operations.

```bash
# Discover what's available
datahub graphql --list-operations --format json

# Inspect a specific operation's arguments
datahub graphql --describe dataset --format json

# Preview a query before executing
datahub graphql --query "{ me { corpUser { urn } } }" --dry-run

# Execute a query
datahub graphql --query "{ me { corpUser { urn username } } }" --format json
```

For full agent best practices (discovery, dry-run, error codes, common recipes):

```bash
datahub graphql --agent-context
```
