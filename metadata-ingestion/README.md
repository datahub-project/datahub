# Install From Source
## Pre-Requisites
- On MacOS: `brew install librdkafka`
- On Debian/Ubuntu: `sudo apt install librdkafka-dev python3-dev python3-venv`

## Set up python environment (requires python 3.6+)
- python3 -m venv venv
- source venv/bin/activate
- pip install -e .

# Testing
## Deps
```sh
pip install -r test_requirements.txt
```
## Run Unit tests
```sh
pytest tests/unit
```
## Run Integration tests
```sh
pytest tests/integration
```

## Sanity check code before checkin (currently broken)
```sh
flake8 src tests
mypy -p gometa
black --exclude 'gometa/metadata' -S -t py36 src tests
isort --check-only src tests
pytest
```

# Recipes

A recipe is a configuration that tells our ingestion scripts where to pull data from (source) and where to put it (sink).
Here's a simple example that pulls metadata from MSSQL and puts it into datahub.

```yaml
source:
  type: mssql
  mssql:
    username: sa
    password: test!Password
    database: DemoData

sink:
  type: "datahub-rest"
  datahub-rest:
    server: 'http://localhost:8080'
```

Running a recipe is quite easy.

```bash
gometa-ingest -c ./examples/recipes/kafka_to_datahub_rest.yml
```

A number of recipes are included in the recipes directory.

# Using Docker
## Build the image
- source docker/docker_build.sh

## Run an ingestion script (examples/recipes/file_to_file.yml)
We have a simple script provided that supports mounting a local directory for input recipes and an output directory for output data
- source docker/docker_run.sh examples/recipes/file_to_file.yml

