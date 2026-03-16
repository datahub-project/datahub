# Agent Rules for `dh` CLI

Invariants for AI agents using the `dh` CLI to manage DataHub on k3d.

## Output format

Always use `--output json` when parsing command output. Without it, output is
human-readable colored text that is not reliably parseable.

```bash
dh status --output json
dh list --output json
dh deploy --output json
```

All commands emit **NDJSON** (newline-delimited JSON) to stdout:

- Progress lines: `{"type": "event", "level": "info|warn|error", "msg": "...", "ts": "..."}`
- Final result: `{"type": "result", "success": true|false, "data": {...}, "error": null|"..."}`

To get only the result line:

```bash
dh status --output json | grep '"type":"result"' | jq .
```

## Dry-run before mutations

Use `--dry-run` before executing any mutating command in automated workflows.
Dry-run validates prerequisites and prints what would be executed without making changes.

```bash
dh deploy --dry-run --output json
dh cluster-up --dry-run --output json
dh infra-down --dry-run --output json
```

## Prerequisite order

Commands must be run in dependency order. Violating this causes `die()` with a
non-zero exit code and a `{"type": "result", "success": false, ...}` result line.

```
cluster-up → infra-up → deploy
                      ↓
             teardown → infra-down → cluster-down
```

## Check status before acting

Always check status before deploying or tearing down:

```bash
dh status --output json | grep '"type":"result"' | jq '.data.healthy'
```

If `healthy` is `false`, inspect `data.pods` for pod failures before proceeding.

## Worktree isolation

Each git worktree gets its own Kubernetes namespace, MySQL database, and
Elasticsearch indices. Set `DH_WORKTREE_NAME` explicitly to avoid name
collisions when running from scripts that don't have a git worktree context:

```bash
DH_WORKTREE_NAME=my-feature dh deploy --output json
```

## Discover available commands

Use `dh describe` to get machine-readable schemas for all commands:

```bash
dh describe            # all commands
dh describe deploy     # specific command options and types
```

## Environment variables

All configuration can be overridden via environment variables:

| Variable | Default | Description |
|---|---|---|
| `K3D_CLUSTER_NAME` | `datahub-dev` | k3d cluster name |
| `DATAHUB_VERSION` | `head` | Docker image tag |
| `K3D_HTTP_PORT` | `80` | Ingress HTTP port |
| `K3D_HTTPS_PORT` | `443` | Ingress HTTPS port |
| `DH_WORKTREE_NAME` | auto-derived | Override worktree name |
| `DH_LOCAL_SERVICES` | `gms,frontend` | Services to build with `--local` |

## Error handling

On failure, `dh` exits with code 1 and emits a result line with `"success": false`.
Check `result.error` for the human-readable error message.

```bash
output=$(dh deploy --output json 2>/dev/null)
result=$(echo "$output" | grep '"type":"result"')
success=$(echo "$result" | jq -r '.success')
if [ "$success" != "true" ]; then
  echo "Deploy failed: $(echo "$result" | jq -r '.error')"
fi
```

## Input validation

- Service names: `gms`, `frontend`, `upgrade`, `mysql-setup`, `elasticsearch-setup`
- Profiles: `minimal`, `consumers`, `backend` (cumulative, can repeat `--profile`)
- Chart version: semver string (e.g. `1.3.0`) or omit to use latest
- Worktree names: alphanumeric + hyphens, max 20 chars (auto-sanitized)
