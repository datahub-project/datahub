# k3d Quickstart for DataHub (Multi-Worktree)

Run DataHub on a local Kubernetes cluster (k3d = k3s-in-Docker), with isolated deployments per git worktree sharing a single set of infrastructure services.

## Architecture

```
k3d cluster: datahub-dev (port 80 → Traefik ingress)
│
├── Namespace: datahub-infra (shared, deployed once)
│   ├── MySQL 8.0
│   ├── Kafka (KRaft)
│   ├── Elasticsearch 7.10.1
│   └── Schema Registry
│
├── Namespace: dh-main (worktree: datahub/)
│   ├── GMS (embedded MAE/MCE/PE consumers)
│   ├── Frontend → main.datahub.localhost
│   ├── Actions
│   └── Setup jobs (transient)
│
└── Namespace: dh-feature-x (worktree: datahub.feature-x/)
    ├── GMS, Frontend, Actions
    ├── Frontend → feature-x.datahub.localhost
    └── Setup jobs
```

**Per-worktree data isolation** via shared infra:

- **MySQL**: Separate database per worktree (`datahub_main`, `datahub_feature_x`)
- **Elasticsearch**: Index prefix per worktree (`main_datasetindex_v2`, `feature_x_datasetindex_v2`)
- **Kafka**: Topic prefix per worktree (`main_MetadataChangeProposal_v1`, ...)

**Resource budget:**

| Component | Memory Request | Memory Limit |
|-----------|---------------|--------------|
| Shared infra (total) | ~1.15Gi | ~2.3Gi |
| Per worktree (total) | ~0.9Gi | ~2.7Gi |
| **2 worktrees** | **~3Gi** | **~7.3Gi** |

## Prerequisites

- Docker Desktop (or colima/lima) running
- `devbox shell` (installs k3d, kubectl, helm, k9s)

Or install manually: [k3d](https://k3d.io), [kubectl](https://kubernetes.io/docs/tasks/tools/), [helm](https://helm.sh/docs/intro/install/)

## Quick Start

```bash
# Enter devbox shell (installs all tools)
devbox shell

# 1. Create cluster and deploy shared infra
devbox run k3d-up

# 2. Deploy DataHub for the current worktree
devbox run k3d-deploy

# 3. Open in browser
open http://main.datahub.localhost   # (or whatever your worktree name is)
```

## CLI Reference

All commands are available via `k3d/dh.sh <command>` or `devbox run k3d-*` shortcuts.

### Cluster Management

```bash
k3d/dh.sh cluster-up          # Create the k3d cluster
k3d/dh.sh cluster-down        # Delete the k3d cluster (destroys everything)
```

### Shared Infrastructure

```bash
k3d/dh.sh infra-up            # Deploy MySQL, Kafka, ES, Schema Registry
k3d/dh.sh infra-down          # Remove shared infra (warns if worktrees exist)
k3d/dh.sh infra-down --force  # Force remove even with active worktrees
```

### Per-Worktree Deployment

```bash
k3d/dh.sh deploy                       # Deploy using published images (tag: head)
k3d/dh.sh deploy --version v0.14.1     # Deploy a specific version
k3d/dh.sh deploy --local               # Build & import local images, then deploy
k3d/dh.sh deploy --debug               # Enable JDWP debug ports (GMS:5001, Frontend:5002)
k3d/dh.sh deploy --local --debug       # Local images + debug mode
k3d/dh.sh teardown                     # Remove this worktree's deployment + data
```

### Deployment Profiles

Profiles select pre-configured deployment variants. They are **cumulative** — multiple profiles can be combined:

| Profile | What it does | Use case |
|---------|-------------|----------|
| `minimal` | No Actions, no managed ingestion | Fast iteration, lower memory |
| `consumers` | Standalone MAE/MCE/PE consumers (not embedded in GMS) | Testing consumer architecture |
| `backend` | No Frontend | Running frontend locally via `yarn start` |

```bash
# Single profile
k3d/dh deploy --profile minimal

# Combine profiles
k3d/dh deploy --profile minimal --profile consumers

# Profiles compose with --debug and other flags
k3d/dh deploy --profile backend --debug

# User overrides always win (last --set takes precedence)
k3d/dh deploy --profile minimal --set acryl-datahub-actions.enabled=true
```

**Values loading order** (last wins):

1. Rendered base template
2. Profile values files (in `--profile` order)
3. Per-worktree overrides (`k3d/values/overrides/<name>.yaml`)
4. User `-f`/`--values` files
5. `--set` args: profiles → debug → user (user always wins)

### Status & Discovery

```bash
k3d/dh.sh status              # Full status: cluster, infra, all deployments
k3d/dh.sh list                # List all deployed worktree instances
```

### Local Image Management

```bash
k3d/dh.sh import-images                       # Build & import gms,frontend
k3d/dh.sh import-images --services gms        # Import only GMS
k3d/dh.sh import-images --services gms,frontend,upgrade  # Multiple services
```

Available services: `gms`, `frontend`, `upgrade`, `mysql-setup`, `elasticsearch-setup`

### Hot Reload

Rebuild images and restart only the affected deployments — the k3d equivalent of `./gradlew quickstartReload`:

```bash
k3d/dh.sh reload                              # Rebuild gms,frontend → reimport → restart
k3d/dh.sh reload --services gms               # Rebuild only GMS
k3d/dh.sh reload --services gms,frontend,upgrade  # Rebuild multiple services
k3d/dh.sh reload --no-build                   # Skip build, just reimport existing images and restart
```

Requires a prior `dh deploy --local`. Only long-running deployments (GMS, Frontend) are restarted — job-only services (upgrade, mysql-setup, etc.) are imported but not restarted.

## Devbox Script Shortcuts

```bash
devbox run k3d-up              # cluster-up + infra-up
devbox run k3d-deploy          # deploy (published images)
devbox run k3d-deploy-local    # deploy --local (build from source)
devbox run k3d-status          # status
devbox run k3d-teardown        # teardown current worktree
devbox run k3d-down            # cluster-down (destroys everything)
```

## How Worktree Names Work

The worktree name is derived automatically from your git repository directory:

| Directory | Worktree Name | Namespace | URL |
|-----------|--------------|-----------|-----|
| `datahub/` | `main` | `dh-main` | `main.datahub.localhost` |
| `datahub.feature-x/` | `feature-x` | `dh-feature-x` | `feature-x.datahub.localhost` |
| `datahub-my-branch/` | `my-branch` | `dh-my-branch` | `my-branch.datahub.localhost` |

Override with: `DH_WORKTREE_NAME=custom-name k3d/dh.sh deploy`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATAHUB_VERSION` | `head` | Docker image tag |
| `DH_WORKTREE_NAME` | *(auto-derived)* | Override worktree name |
| `K3D_HTTP_PORT` | `80` | Host port for HTTP ingress |
| `K3D_HTTPS_PORT` | `443` | Host port for HTTPS ingress |
| `DH_LOCAL_SERVICES` | `gms,frontend` | Services to build for `--local` |

## DNS / Hostname Resolution

`*.localhost` resolves to `127.0.0.1` natively on macOS — no `/etc/hosts` editing needed.

On Linux, you may need to add entries:

```bash
echo "127.0.0.1 main.datahub.localhost" | sudo tee -a /etc/hosts
```

## Debugging

### JDWP Remote Debugging

Deploy with `--debug` to enable JDWP debug ports and dev-friendly settings (equivalent to `./gradlew quickstartDebug`):

```bash
k3d/dh.sh deploy --debug           # or --local --debug
```

This sets:
- **GMS (port 5001)**: JDWP agent, `SEARCH_SERVICE_ENABLE_CACHE=false`, `LINEAGE_SEARCH_CACHE_ENABLED=false`, `DATAHUB_SERVER_TYPE=dev`
- **Frontend (port 5002)**: JDWP agent

Then port-forward to attach your IDE debugger:

```bash
kubectl port-forward -n dh-main deployment/datahub-main-datahub-gms 5001:5001
kubectl port-forward -n dh-main deployment/datahub-main-datahub-frontend 5002:5002
```

### General Troubleshooting

```bash
# Watch pods
kubectl get pods -n dh-main -w

# Logs
kubectl logs -n dh-main -l app.kubernetes.io/name=datahub-gms -f

# Shell into GMS
kubectl exec -it -n dh-main deploy/datahub-main-datahub-gms -- bash

# Port-forward GMS directly
kubectl port-forward -n dh-main svc/datahub-main-datahub-gms 8080:8080

# Interactive cluster browser
k9s
```

## Customization

### Using a different port (port 80 is taken)

```bash
K3D_HTTP_PORT=8080 k3d/dh.sh cluster-up
# Then access via: http://main.datahub.localhost:8080
```

### Overriding Helm values

Create a file `k3d/values/overrides.yaml` and pass it manually:

```bash
helm upgrade --install datahub-main datahub/datahub \
  -n dh-main \
  -f <generated-values> \
  -f k3d/values/overrides.yaml
```

## Comparison with Docker Compose Quickstart

| Feature | Docker Compose | k3d |
|---------|---------------|-----|
| Setup time | ~2 min | ~5 min (first time) |
| Memory (1 instance) | ~8Gi | ~2Gi |
| Multiple instances | Port conflicts | Hostname-based routing |
| Shared infra | No | Yes (MySQL, Kafka, ES) |
| Local images | Volume mounts | `k3d image import` |
| Production-like | No | Yes (real K8s) |
| Ingress/routing | Port mapping | Traefik + hostnames |
