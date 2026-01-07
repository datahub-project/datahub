# Docker Compose Profiles - Acryl Addendum

Note that common gradle tasks like `./gradlew quickstartDebug` will run a Saas specifc configuration using a different
project name `acryl`. This will allow preserving an OSS quickstart instance and avoid having to nuke both setups.

The `acryl` configuration will automatically disable `actions` and run both the `executor` and `integration-service` components.

See the sections below for the following helpful configuration options:

- Set the `DATAHUB_COMPOSE_PROJECT_NAME` variable when using the python CLI.
- Disable authentication and authorization for local development.

## Acryl Environment Variables

### DataHub CLI

Since the python CLI expects the project name `datahub`, set this environment variable to use `acryl` instead.

```shell
# datahub cli
export DATAHUB_COMPOSE_PROJECT_NAME="acryl"
```

### Acryl & DataHub: Disable Authentication

Add to your shell.

```shell
DATAHUB_LOCAL_COMMON_ENV=${HOME}/.datahub/docker/common/env.local
```

Include this content in the file `env.local`, referenced in the environment variable `DATAHUB_LOCAL_COMMON_ENV`.

```shell
REST_API_AUTHORIZATION_ENABLED=false
METADATA_SERVICE_AUTH_ENABLED=false
```

### Cloudsmith Private PyPI (for local development builds)

If you need to install packages from the private Cloudsmith PyPI repository (e.g., for `datahub-executor` development),
you'll need to configure an API key since developers authenticate via Okta SSO.

**Setup:**

1. Log in to [Cloudsmith](https://cloudsmith.io) using Okta SSO
2. Navigate to **User Settings** → **API Keys**
3. Generate a new API key (or use an existing one)
4. Add to your shell profile:

```shell
export CLOUDSMITH_API_KEY="your-api-key-here"
export UV_EXTRA_INDEX_URL="https://token:${CLOUDSMITH_API_KEY}@dl.cloudsmith.io/basic/datahub/datahub-cloud/python/simple/"
```

This is required for:

- `./gradlew :datahub-executor:installDev` - Installing executor dependencies locally
- `./gradlew :datahub-executor:build` - Building and testing the executor
- Local Docker image builds with `./gradlew :datahub-executor:docker` or `./gradlew quickstartDebug`

**Note:** When `UV_EXTRA_INDEX_URL` is set, local Docker builds (including `quickstartDebug`) will automatically
use it to access Cloudsmith. If you're using pre-built images from ECR, Cloudsmith access is not needed at runtime.

### Docker Compose CLI

If using the docker compose CLI, use the following environment variable to select the `acryl` base docker-compose file. CI also uses
this approach to run smoke-tests. This is in a separate file to avoid complications that may arise from git merges.

```shell
# Docker Compose Commands
export COMPOSE_FILE="docker-compose.acryl.yml"
```

Docker Repo:

To run `docker compose --profile` off acryl-main, you will need to set DATAHUB_REPO to access images built
off the acryl-main branch.

For example: `DATAHUB_REPO="795586375822.dkr.ecr.us-west-2.amazonaws.com" docker compose --profile debug up`
