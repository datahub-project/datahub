# datahub-monitors-service

## Local Development

You can deploy the service with locally built docker containers
using `docker/dev.sh` or `docker/dev-without-neo4j.sh`. By default, the containers will be 
deployed in hot reloading mode, which will allow you to edit files locally without needing
to restart the containers.

To deploy the service locally on your own (port 9004):

```sh
./gradlew datahub-monitors-service && cd datahub-monitors-service && source .venv/bin/activate && ./scripts/dev.sh
```

Note that you should stop any running Docker containers for `datahub-monitors-service` before running this, or you'll 
see port conflicts

## Handling `The dependency name for acryl-datahub does not match the actual package's name: <something>`

This is a known issue with poetry (and likely good motivation for moving away from poetry entirely).
I've documented a workaround here https://github.com/python-poetry/poetry/issues/5070.

## Updating lockfiles after a merge

This command might take while - sometimes up to 10 minutes or so.

```sh
poetry lock --no-update
```
