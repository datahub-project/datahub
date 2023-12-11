# datahub-integrations-service

## Local Development

```sh
../gradlew build

# Use the ../docker/dev*.sh scripts to run datahub with the integrations service.
# Hot reloading should work, but you'll need to rebuild if dependencies change.
```

## Build machinery

- The project is set up to have an editable/development dependency on acryl-datahub via the metadata-ingestion directory. If metadata-ingestion updates its dependencies, those won't get picked up automatically and you may need to re-run the `./scripts/lockfile.sh` command.
- For local development, dependencies are installed in `venv` in this directory. VSCode should automatically detect this directory and load it appropriately.
- In Docker, no venv is created. Instead, dependencies are installed into the global environment.
- In Docker development mode, the file mounts are set up so that any code changes are detected and the server automatically hot reloads accordingly. Note that the image will still need to be rebuilt if the dependencies change.

## Dependency management

### Adding dependencies

We use [pip-tools](https://github.com/jazzband/pip-tools) (`pip-compile` and `pip-sync`) to manage dependencies. To add a new dependency:

```sh
# First, add the dependency to pyproject.toml

# Then run the following command to update the lockfile.
./scripts/lockfile.sh

# Finally, update the venv.
pip-sync requirements.txt requirements-dev.txt && pip install -e .
```

### Updating dependencies

```sh
# First, update the dependency's lower bound in pyproject.toml

# Update the lockfiles and venv:
./scripts/lockfile.sh
pip-sync requirements.txt requirements-dev.txt && pip install -e .
```

Alternative approach: updating requirements without updating pyproject.toml.

```sh
# Upgrade a single package:
pip-compile -o requirements.txt pyproject.toml requirements-local.in --upgrade-package <package>

# Upgrade all packages:
pip-compile -o requirements.txt pyproject.toml requirements-local.in --upgrade

# Either way, run the same lockfile and venv update commands as above.
```

### Updating lockfiles after a merge

```sh
./scripts/lockfile.sh
```
