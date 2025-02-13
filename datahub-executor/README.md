# datahub-executor

## Local Development

You can deploy the service with locally built docker containers
using `docker/dev.sh` or `docker/dev-without-neo4j.sh`. By default, the containers will be
deployed in hot reloading mode, which will allow you to edit files locally without needing
to restart the containers.

To deploy the service locally on your own (port 9004):

```sh
./gradlew datahub-executor && cd datahub-executor && source .venv/bin/activate && ./scripts/dev.sh
```

Note that you should stop any running Docker containers for `datahub-executor` before running this, or you'll
see port conflicts

## Environment Variables and Use Cases

The monitor service will launch and run in different modes depending on the values of some environment variables. Here are the environment variables and their various meanings. The section below will describe the combinations of variables needed for specific use cases.

The main switch we have to change the behavior/mode of this service is the DATAHUB_EXECUTOR_MODE variable.
This can be set to coordinator or worker.

When set to coordinator, it can further be configured with: - MONITORS_ENABLED: True (default) or False. This will enable or disable the monitor/assertion fetcher and - scheduler. - INGESTION_ENABLED: True (default) or False. This will enable or disable the ingestion fetcher and scheduler. - EMBEDDED_WORKER_ENABLED: True (default) or False. This will enable a local (non-celery/SQS) worker that will process any execution requests coming in with executorId equal to EMBEDDED_WORKER_ID - EMBEDDED_WORKER_ID: Usually "default". Incoming requests with this executorId will be processed locally and not sent out on the an SQS queue for processing.

When set to worker, the only other environment variable to configure is EXECUTOR_ID. This will configure the celery task worker to only listen on the SQS queue configured for that specific executorId.

### Use Case: Local Development mode.

A single node, executorId set to "default"

- DATAHUB_EXECUTOR_MODE: coordinator
- MONITORS_ENABLED: True
- INGESTION_ENABLED: True
- EMBEDDED_WORKER_ENABLED: True
- EMBEDDED_WORKER_ID: default

### Use Case: Multiple nodes, multiple SQS queues

For the server/scheduler and "default" task worker.

- DATAHUB_EXECUTOR_MODE: coordinator
- MONITORS_ENABLED: True
- INGESTION_ENABLED: True
- EMBEDDED_WORKER_ENABLED: True
- EMBEDDED_WORKER_ID: default

For the remote task worker(s)

- DATAHUB_EXECUTOR_MODE: worker
- EXECUTOR_ID: remoteExecutor1 or remoteExecutor2, etc...

## Dependency management

Note that this section is copied from the integrations-service README.md. Both use the same setup for dependencies.

### Adding dependencies

We use [uv](https://github.com/astral-sh/uv)'s compile and sync subcommands to manage dependencies. To add a new dependency:

```sh
# First, add the dependency to pyproject.toml
vim pyproject.toml

# Then run the following command to update the lockfile and install deps.
./scripts/lockfile.sh
```

### Updating dependencies

```sh
# First, update the dependency's lower bound in pyproject.toml
vim pyproject.toml

# Update the lockfiles and venv:
./scripts/lockfile.sh
```

Alternative approach: updating requirements without updating pyproject.toml.

```sh
# Upgrade a single package:
./scripts/lockfile.sh --upgrade-package <package>

# Upgrade all packages:
./scripts/lockfile.sh --upgrade

# Either way, run the same lockfile and venv update commands as above.
```

### Updating lockfiles after a merge

```sh
./scripts/lockfile.sh
```
