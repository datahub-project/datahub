# Install From Source
## Pre-Requisites
- On MacOS: `brew install librdkafka`
- On Debian/Ubuntu: `sudo apt install librdkafka-dev`

## Set up python environment (requires python 3.7+)
- python3 -m venv venv
- source venv/bin/activate
- pip install -e .

# Run tests
- pip install -r test_requirements.txt
# Run Unit tests
- pytest tests/unit
# Run Integration tests
- pytest tests/integration

# Sanity check code before checkin (currently broken)
- flake8 src test && mypy -p gometa && black --check -l 120 src test && isort --check-only src test && pytest

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

