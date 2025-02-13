# datahub-integrations-service

## Local Development

```sh
../gradlew build

# Use the ../docker/dev*.sh scripts to run datahub with the integrations service.
# Hot reloading should work, but you'll need to rebuild if dependencies change.
```

### Development with Remote GMS

In certain scenarios, you might want to run integrations service and test its
functionality by calling its API-s directly. In that scenario, you still need to
provide it a valid GMS instance for it to be able to resolve connections and
other dependencies.
For example, you might want to point it to `dev01`.
Here is how you can do it.

First, export the following env variables:

```sh
export DATAHUB_GMS_PROTOCOL='https'
export DATAHUB_GMS_HOST='dev01.acryl.io/api/gms'
export DATAHUB_GMS_PORT=''  # this ensures that integrations service does not try to provide a port number
export DATAHUB_GMS_API_TOKEN='<INSERT_PERSONAL_ACCESS_TOKEN_HERE>'
```

Then build and run integrations service

```sh
# execute this from inside the `datahub-integrations-service` folder (this folder)
../gradlew installDev # this installs all the dependencies
source venv/bin/activate
# start the application on port 9003
uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 ${EXTRA_UVICORN_ARGS:-} --reload
```

You can now go to `http://localhost:9003` and access the integrations service
Swagger UI.

## Build machinery

- The project is set up to have an editable/development dependency on acryl-datahub via the metadata-ingestion directory. If metadata-ingestion updates its dependencies, those won't get picked up automatically and you may need to re-run the `./scripts/lockfile.sh` command.
- For local development, dependencies are installed in `venv` in this directory. VSCode should automatically detect this directory and load it appropriately.
- In Docker, no venv is created. Instead, dependencies are installed into the global environment.
- In Docker development mode, the file mounts are set up so that any code changes are detected and the server automatically hot reloads accordingly. Note that the image will still need to be rebuilt if the dependencies change.

## Dependency management

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
