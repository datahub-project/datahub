# datahub-integrations-service

## Local Development

```sh
../gradlew build

# Use the ../docker/dev*.sh scripts to run datahub with the integrations service.
# Hot reloading should work, but you'll need to rebuild if dependencies change.
```

## Build machinery

- The pyproject is set up to have an editable/development dependency on acryl-datahub via the metadata-ingestion directory. If metadata-ingestion updates its dependencies, those won't get picked up and you may need to re-run the poetry add command to refresh the `poetry.lock` file.
- For local development, dependencies are installed in `venv` in this directory. VSCode should automatically detect this directory and load it appropriately.
- In Docker, no venv is created. Instead, dependencies are installed into the global environment.
- In Docker development mode, the file mounts are set up so that any code changes are detected and the server automatically hot reloads accordingly. Note that the image will still need to be rebuilt if the dependencies change.

## Adding dependencies

We use `pip-tools` (`pip-compile` and `pip-sync`) to manage dependencies. To add a new dependency:

```sh
# First, add a dependency to pyproject.toml
#...

# Then run the following command to update the lockfile.
./scripts/lockfile.sh

# Finally, update the venv.
pip-sync requirements.txt requirements-dev.txt
pip install -e .
```

## Troubleshooting

#### Poetry complains that `acryl-datahub` does not match the package name

This is a known issue with Poetry: https://github.com/python-poetry/poetry/issues/5070.

I've commented with a workaround on that issue, although it's not ideal because it requires patching poetry itself.
