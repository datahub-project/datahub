---
name: local-datahub
description: Deploy a local DataHub instance on Kubernetes using the k3d/dh CLI. Use when you need to spin up, rebuild, or inspect a local DataHub deployment — especially for validating code changes against a live instance.
---

# Local DataHub on Kubernetes (k3d/dh)

## What is it?

`k3d/dh` is a Python CLI that manages DataHub deployments on local k3d Kubernetes clusters. It replaces Docker Compose quickstart with a real K8s environment, supporting multiple isolated DataHub instances (one per git worktree).

## When to use

**Use this when you need to:**
- Validate code changes against a running DataHub instance
- Run smoke tests or Cypress E2E tests against a real deployment
- Test Helm chart changes or deployment configuration
- Debug GMS or frontend behavior with a live backend
- Reproduce issues that only appear in Kubernetes
- Test with multiple DataHub versions side-by-side (via worktrees)

**Don't use this when:**
- Running unit tests (use `./gradlew` tasks directly)
- Only changing Python ingestion code (use `./gradlew :metadata-ingestion:testQuick`)
- Only changing frontend code without backend interaction (use `yarn start`)

## CLI commands

All commands are run from the repo root via `k3d/dh`.

### Cluster lifecycle

```bash
k3d/dh cluster-up      # Create k3d cluster with Traefik ingress
k3d/dh cluster-down    # Delete the cluster entirely
```

### Infrastructure (shared across worktrees)

```bash
k3d/dh infra-up        # Deploy MySQL, Kafka, Elasticsearch, Schema Registry
k3d/dh infra-down      # Remove infrastructure (warns if worktrees still deployed)
```

### Deploy DataHub

```bash
k3d/dh deploy                  # Deploy using published Docker images (tag: head)
k3d/dh deploy --local          # Build GMS + frontend from local source, import to k3d
k3d/dh deploy --version v0.14  # Use a specific image tag
k3d/dh deploy --debug          # Enable JDWP debug ports (GMS:5001, Frontend:5002)
k3d/dh deploy --profile minimal          # Disable optional consumers
k3d/dh deploy --set key=value            # Override Helm values
k3d/dh deploy -f extra-values.yaml       # Additional Helm values file
k3d/dh deploy --kustomize ./my-overlay   # Use a Kustomize overlay
```

### Rebuild and reload (fast iteration)

```bash
k3d/dh reload                        # Rebuild GMS + frontend, restart pods
k3d/dh reload --services gms         # Rebuild only GMS
k3d/dh reload --no-build             # Skip build, just restart pods
```

### Status and inspection

```bash
k3d/dh status              # Show cluster, infra, and deployment status
k3d/dh status -o json      # Machine-readable JSON (has url, gms_url, namespace, etc.)
k3d/dh list                # List all deployed worktree instances
```

### Teardown

```bash
k3d/dh teardown       # Remove this worktree's DataHub deployment
k3d/dh infra-down     # Remove shared infrastructure
k3d/dh cluster-down   # Delete the entire k3d cluster
```

## Typical workflows

### First-time setup

```bash
k3d/dh cluster-up
k3d/dh infra-up
k3d/dh deploy --local
```

### Iterate on GMS changes

```bash
# Make code changes to metadata-service/...
k3d/dh reload --services gms    # Rebuilds GMS via Gradle, imports image, restarts pod
```

### Iterate on frontend changes

```bash
k3d/dh reload --services frontend
```

### Check deployment URLs

```bash
k3d/dh status -o json | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Frontend: {d[\"url\"]}'); print(f'GMS: {d[\"gms_url\"]}')"
```

### Get credentials

Credentials are in `datahub-frontend/conf/user.props` (format: `username:password`):

```bash
head -1 datahub-frontend/conf/user.props
```

## How it works internally

### Naming and isolation

Each git worktree gets isolated resources derived from its directory name:
- **Namespace**: `dh-{name}` (e.g., `dh-k3d-quickstart`)
- **Database**: `datahub_{name}` in the shared MySQL
- **ES index prefix**: `{name}_`
- **Kafka topic prefix**: `{name}_`
- **Frontend URL**: `{name}.datahub.localhost`
- **GMS URL**: `gms.{name}.datahub.localhost`

The name is derived from the git repo basename with `datahub.`/`datahub-` prefix stripped, or overridden via `$DH_WORKTREE_NAME`.

### Local image builds (`--local`)

When deploying with `--local`:
1. Builds Docker images via Gradle (delegates to `./gradlew` docker tasks)
2. Imports images into k3d cluster via `k3d image import`
3. Sets per-service Helm overrides (`image.tag`, `image.pullPolicy=Never`)

By default builds `gms` and `frontend`. Override via `$DH_LOCAL_SERVICES` env var.

### Key source files

| File | Purpose |
|------|---------|
| `k3d/src/dh/cli.py` | Click CLI entry point |
| `k3d/src/dh/deploy.py` | Deployment orchestration, status, GMS ingress |
| `k3d/src/dh/images.py` | Gradle builds, k3d import, git change detection |
| `k3d/src/dh/naming.py` | Worktree name derivation, URL/namespace helpers |
| `k3d/src/dh/config.py` | K3dConfig dataclass with env-based defaults |
| `k3d/src/dh/cluster.py` | k3d cluster lifecycle |
| `k3d/src/dh/infra.py` | Shared infrastructure (MySQL, Kafka, ES) |
| `k3d/src/dh/profiles.py` | Deployment profiles (minimal, consumers, backend) |
| `k3d/values/datahub-worktree.yaml.tpl` | Helm values template (variable substitution) |

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `K3D_CLUSTER_NAME` | `datahub-dev` | k3d cluster name |
| `DATAHUB_VERSION` | `head` | Default Docker image tag |
| `DH_WORKTREE_NAME` | *(auto-derived)* | Override worktree name |
| `DH_LOCAL_SERVICES` | `gms,frontend` | Services to build with `--local` |
| `K3D_HTTP_PORT` | `80` | Host HTTP port |
| `K3D_HTTPS_PORT` | `443` | Host HTTPS port |

## Running k3d CLI unit tests

The CLI has 112 unit tests (no cluster required):

```bash
cd k3d && uv run pytest -v
```

## Running smoke/Cypress tests against the deployment

See the `k3d-tests` skill for the full workflow on running the repo's smoke tests and Cypress E2E tests against a live k3d deployment.

## k3d-specific gotchas

### Always use `--local` for Cypress tests

Deploying without `--local` pulls pre-built Docker Hub images (`head` tag), which may not match the Cypress test specs in your branch. This causes false failures (e.g., V2 theme "Not Found", missing UI elements, `"Welcome back, undefined"`). Always use `k3d/dh deploy --local` when running Cypress.

### `ES_BULK_REFRESH_POLICY` must be `NONE`

The Helm values template (`k3d/values/datahub-worktree.yaml.tpl`) should set `ES_BULK_REFRESH_POLICY=NONE`. Using `WAIT_UNTIL` forces ES to refresh the shard on each write (~3s per op), causing massive consumer backlog with 5000+ queued messages. CI uses `NONE` by default.

### `wait_for_writes_to_sync()` uses `docker exec`

The smoke test utility `smoke-test/tests/consistency_utils.py` checks Kafka consumer lag via `docker exec` on the broker container. In k3d, Kafka runs inside a Kubernetes pod, so this fails and falls back to a static sleep (insufficient for tests that need data consistency). When running in k3d, set `USE_STATIC_SLEEP=true` with a generous `ELASTICSEARCH_REFRESH_INTERVAL_SECONDS`, or rewrite the lag check to use GMS's Kafka lag REST endpoints:

```
GET /openapi/operations/kafka/mcp/consumer/offsets?skipCache=true&detailed=true
GET /openapi/operations/kafka/mcl/consumer/offsets?skipCache=true&detailed=true
GET /openapi/operations/kafka/mcl-timeseries/consumer/offsets?skipCache=true&detailed=true
```

These return `totalLag`, `maxLag`, `medianLag`, `avgLag` and work in any environment.

### Cypress `baseUrl` override

`cypress.config.js` hardcodes `baseUrl: "http://localhost:9002/"`. In k3d the frontend is at `{name}.datahub.localhost`. The env var `CYPRESS_BASE_URL` does **not** work — Cypress requires `CYPRESS_baseUrl` (camelCase after prefix). Safest approach: pass `--config baseUrl=http://{name}.datahub.localhost` directly to `npx cypress run`.

### Resource constraints on single-node cluster

Aggressive testing can overwhelm the MCL consumer, causing 5k+ lag and effective downtime. Recovery: `kubectl rollout restart deployment/<gms-deployment> -n <namespace>`. Monitor lag via:

```bash
kubectl exec -n <namespace> <kafka-pod> -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```
