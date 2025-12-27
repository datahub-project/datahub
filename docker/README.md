---
title: "Deploying with Docker"
hide_title: true
---

# Docker Images

## Prerequisites

You need to install [docker](https://docs.docker.com/install/) and
[docker-compose](https://docs.docker.com/compose/install/) (if using Linux; on Windows and Mac compose is included with
Docker Desktop).

Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs, 8GB RAM, 2GB Swap
area.

_If you prefer not to use Docker Desktop (which requires a license for commercial use), you can opt for free and open-source alternatives such as [Podman Desktop](https://podman-desktop.io/) or [Rancher Desktop](https://rancherdesktop.io/). To configure them, you can add the following aliases to your `~/.bashrc` file:_

```bash
# podman
alias docker=podman
alias docker-compose="podman compose"

# Rancher (or nerdctl)
alias docker=nerdctl
alias docker-compose="nerdctl compose"
```

## Quickstart

The easiest way to bring up and test DataHub is using DataHub [Docker](https://www.docker.com) images
which are continuously deployed to [Docker Hub](https://hub.docker.com/u/acryldata) with every commit to repository.

You can easily download and run all these images and their dependencies with our
[quick start guide](../docs/quickstart.md).

DataHub Docker Images:

Do not use `latest` or `debug` tags for any of the image as those are not supported and present only due to legacy reasons. Please use `head` or tags specific for versions like `v0.8.40`. For production we recommend using version specific tags not `head`.

- [acryldata/datahub-ingestion](https://hub.docker.com/r/acryldata/datahub-ingestion/)
- [acryldata/datahub-gms](https://hub.docker.com/repository/docker/acryldata/datahub-gms/)
- [acryldata/datahub-frontend-react](https://hub.docker.com/repository/docker/acryldata/datahub-frontend-react/)
- [acryldata/datahub-mae-consumer](https://hub.docker.com/repository/docker/acryldata/datahub-mae-consumer/)
- [acryldata/datahub-mce-consumer](https://hub.docker.com/repository/docker/acryldata/datahub-mce-consumer/)
- [acryldata/datahub-upgrade](https://hub.docker.com/r/acryldata/datahub-upgrade/)
- [acryldata/datahub-elasticsearch-setup](https://hub.docker.com/r/acryldata/datahub-elasticsearch-setup/)
- [acryldata/datahub-mysql-setup](https://hub.docker.com/r/acryldata/datahub-mysql-setup/)
- [acryldata/datahub-postgres-setup](https://hub.docker.com/r/acryldata/datahub-postgres-setup/)
- [acryldata/datahub-actions](https://hub.docker.com/r/acryldata/datahub-actions). Do not use `acryldata/acryl-datahub-actions` as that is deprecated and no longer used.

## Image Variants

The `datahub-ingestion` and `datahub-actions` images are available in multiple variants optimized for different use cases:

| Variant          | Base OS      | Image Size | Use Case                                |
| ---------------- | ------------ | ---------- | --------------------------------------- |
| `full` (default) | Ubuntu 24.04 | Largest    | All connectors, maximum compatibility   |
| `slim`           | Ubuntu 24.04 | Medium     | Common connectors, good balance         |
| `slim-alpine`    | Alpine 3.22  | Small      | Minimal footprint, security-focused     |
| `locked`         | Ubuntu 24.04 | Medium     | Air-gapped environments (pypi disabled) |
| `locked-alpine`  | Alpine 3.22  | Smallest   | Air-gapped slim-alipine (pypi disabled) |

### Variant Tag Format

```
acryldata/datahub-ingestion:v0.x.y          # full (default)
acryldata/datahub-ingestion:v0.x.y-slim     # slim
acryldata/datahub-ingestion:v0.x.y-slim-alpine
acryldata/datahub-ingestion:v0.x.y-locked
acryldata/datahub-ingestion:v0.x.y-locked-alpine
```

### datahub-ingestion Feature Matrix

| Feature               | full | slim | slim-alpine | locked | locked-alpine |
| --------------------- | :--: | :--: | :---------: | :----: | :-----------: |
| Core CLI & REST/Kafka | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| S3 / GCS / Azure Blob | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| Snowflake             | Yes  | Yes  |      -      |   -    |       -       |
| BigQuery              | Yes  | Yes  |      -      |   -    |       -       |
| Redshift              | Yes  | Yes  |      -      |   -    |       -       |
| MySQL / PostgreSQL    | Yes  | Yes  |      -      |   -    |       -       |
| ClickHouse            | Yes  | Yes  |      -      |   -    |       -       |
| dbt                   | Yes  | Yes  |      -      |   -    |       -       |
| Looker / LookML       | Yes  | Yes  |      -      |   -    |       -       |
| Tableau / PowerBI     | Yes  | Yes  |      -      |   -    |       -       |
| Superset              | Yes  | Yes  |      -      |   -    |       -       |
| Glue                  | Yes  | Yes  |      -      |   -    |       -       |
| Spark lineage (JRE)   | Yes  |  -   |      -      |   -    |       -       |
| Oracle client         | Yes  |  -   |      -      |   -    |       -       |
| Runtime `pip install` | Yes  | Yes  |     Yes     |   -    |       -       |

### datahub-actions Feature Matrix

| Feature                      | full | slim | slim-alpine | locked | locked-alpine |
| ---------------------------- | :--: | :--: | :---------: | :----: | :-----------: |
| Core actions                 | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| Kafka / Executor             | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| Slack / Teams                | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| Tag / Term / Doc propagation | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| Snowflake tag propagation    | Yes  | Yes  |      -      |  Yes   |       -       |
| Bundled CLI venvs            | Yes  | Yes  |     Yes     |  Yes   |      Yes      |
| Runtime `pip install`        | Yes  | Yes  |     Yes     |   -    |       -       |

> **Note:** Alpine variants exclude Snowflake tag propagation due to `spacy`/`blis` lacking pre-built musl wheels.

### CI Testing Coverage

| Image Variant   | Tested in CI | Notes                       |
| --------------- | :----------: | --------------------------- |
| `full`          |     Yes      | Smoke tests run on every PR |
| `slim`          |     Yes      | Smoke tests run on every PR |
| `slim-alpine`   |  Build only  | Manual testing only         |
| `locked`        |  Build only  |                             |
| `locked-alpine` |  Build only  | Manual testing only         |

### Choosing the Right Variant

- **`full`**: Use when you need maximum connector coverage or aren't sure what you'll need
- **`slim`**: Recommended for most production deployments with standard cloud data stacks
- **`slim-alpine`**: Best for security-sensitive environments or when image size matters (e.g., S3/GCS ingestion only)
- **`locked` / `locked-alpine`**: Required for air-gapped environments where runtime package installation is prohibited

### Building Alpine Variants Locally

Alpine variants are not built by default. To include them:

```bash
./gradlew :docker:datahub-ingestion:docker -PincludeAlpineVariants=true
```

Dependencies:

- [Elasticsearch](elasticsearch-setup)
- [MySQL](mysql)
- [(Optional) Neo4j](neo4j)

### Ingesting demo data.

If you want to test ingesting some data once DataHub is up, use the `./docker/ingestion/ingestion.sh` script or `datahub docker ingest-sample-data`. See the [quickstart guide](../docs/quickstart.md) for more details.

## Using Docker Images During Development

See [Using Docker Images During Development](../docs/docker/development.md).

## Building And Deploying Docker Images

We use GitHub Actions to build and continuously deploy our images. There should be no need to do this manually; a
successful release on Github will automatically publish the images.

### Building images

> This is **not** our recommended development flow and most developers should be following the
> [Using Docker Images During Development](../docs/docker/development.md) guide.

To build the full images (that we are going to publish), you need to run the following:

```
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker compose -p datahub build
```

This is because we're relying on builtkit for multistage builds. It does not hurt also set `DATAHUB_VERSION` to
something unique.

### Community Built Images

As the open source project grows, community members would like to contribute additions to the docker images. Not all contributions to the images can be accepted because those changes are not useful for all community members, it will increase build times, add dependencies and possible security vulns. In those cases this section can be used to point to `Dockerfiles` hosted by the community which build on top of the images published by the DataHub core team along with any container registry links where the result of those images are maintained.

# DataHub Docker Nuke Tasks

This document describes the nuke task system for cleaning up DataHub Docker containers and volumes.

## Overview

The nuke tasks provide a way to completely remove DataHub containers and volumes for different project namespaces. This is useful for:

- Cleaning up test environments
- Resetting development setups
- Isolating different project instances
- Troubleshooting container issues

## Available Tasks

### All Configurations Have Nuke Tasks

Every quickstart configuration automatically gets a nuke task for targeted cleanup:

- **`quickstartNuke`** - Removes containers and volumes for the default project namespace (`datahub`)
- **`quickstartDebugNuke`** - Removes containers and volumes for the debug configuration (`datahub`)
- **`quickstartCypressNuke`** - Removes containers and volumes for the cypress configuration (`dh-cypress`)
- **`quickstartDebugMinNuke`** - Removes containers and volumes for the debug-min configuration (`datahub`)
- **`quickstartDebugConsumersNuke`** - Removes containers and volumes for the debug-consumers configuration (`datahub`)
- **`quickstartPgNuke`** - Removes containers and volumes for the postgres configuration (`datahub`)
- **`quickstartPgDebugNuke`** - Removes containers and volumes for the debug-postgres configuration (`datahub`)
- **`quickstartSlimNuke`** - Removes containers and volumes for the backend configuration (`datahub`)
- **`quickstartSparkNuke`** - Removes containers and volumes for the spark configuration (`datahub`)
- **`quickstartStorageNuke`** - Removes containers and volumes for the storage configuration (`datahub`)
- **`quickstartBackendDebugNuke`** - Removes containers and volumes for the backend-debug configuration (`datahub`)

### Project Namespace Behavior

- **Default project namespace (`datahub`)**: Most configurations use this, so their nuke tasks will clean up containers in the same namespace
- **Custom project namespace (`dh-cypress`)**: The cypress configuration uses its own namespace for isolation

## Usage

### Basic Usage

```bash
# Remove containers and volumes for specific configurations
./gradlew quickstartDebugNuke      # For debug configuration
./gradlew quickstartCypressNuke    # For cypress configuration
./gradlew quickstartDebugMinNuke   # For debug-min configuration
./gradlew quickstartPgNuke         # For postgres configuration

# For general cleanup of all containers
./gradlew quickstartDown
```

### When to Use Each Task

- **Use specific nuke tasks when:**

  - You want to clean up a specific configuration environment
  - You need targeted cleanup without affecting other configurations
  - You're working with a particular development setup (debug, postgres, cypress, etc.)

- **Use `quickstartDown` when:**
  - You want to stop all running containers
  - You need a general cleanup option
  - You want to ensure all environments are stopped

## How It Works

1. **Volume Management**: Each nuke task sets `removeVolumes = true` for relevant configurations
2. **Container Cleanup**: Tasks are finalized by appropriate `ComposeDownForced` operations
3. **Project Isolation**: Each task operates within its own Docker Compose project namespace
4. **Configuration Respect**: Tasks respect the `projectName` settings in `quickstart_configs`

## Configuration

The nuke tasks are automatically generated based on the `quickstart_configs` in `docker/build.gradle`. To add a new nuke task:

1. Add a configuration to `quickstart_configs`:

   ```gradle
   'quickstartCustom': [
       profile: 'debug',
       modules: [...],
       // Optional: custom project name for isolation
       additionalConfig: [
           projectName: 'dh-custom'
       ]
   ]
   ```

2. The task `quickstartCustomNuke` will be automatically created

## Troubleshooting

### Task Not Found

- Ensure the configuration exists in `quickstart_configs`
- Check that the task name follows the pattern `{configName}Nuke`

### Containers Not Removed

- Verify the project namespace is correct
- Check that the configuration has the right `projectName` setting
- Ensure the task is targeting the correct `ComposeDownForced` operations

### Volume Persistence

- Check if `preserveVolumes` is set to `true` in the configuration
- Verify the `removeVolumes` setting is properly applied

## Related Commands

- **Start services**: `./gradlew quickstartDebug`, `./gradlew quickstartCypress`
- **Stop services**: `./gradlew quickstartDown`
- **Reload services**: `./gradlew debugReload`, `./gradlew cypressReload`

## Examples

### Complete Development Environment Reset

```bash
# Clean up specific debug environment
./gradlew quickstartDebugNuke

# Start fresh
./gradlew quickstartDebug
```

### Cypress Environment Isolation

```bash
# Clean up cypress environment
./gradlew quickstartCypressNuke

# Start fresh cypress environment
./gradlew quickstartCypress
```

### Mixed Environment Management

```bash
# Clean up only cypress (leaving main environment intact)
./gradlew quickstartCypressNuke

# Clean up only debug environment (leaving cypress intact)
./gradlew quickstartDebugNuke

# Clean up only postgres environment (leaving others intact)
./gradlew quickstartPgNuke
```
