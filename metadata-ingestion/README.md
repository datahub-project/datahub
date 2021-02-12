# Getting Started

## From source:

### Pre-Requisites
- Python 3.6+
- On MacOS: `brew install librdkafka`
- On Debian/Ubuntu: `sudo apt install librdkafka-dev python3-dev python3-venv`

### Set up python environment
```sh
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

### Usage - from source
```sh
gometa-ingest -c examples/recipes/file_to_file.yml
```

## Using Docker:

### Build the image
```sh
source docker/docker_build.sh
```

### Usage - with Docker
We have a simple script provided that supports mounting a local directory for input recipes and an output directory for output data:
```sh
source docker/docker_run.sh examples/recipes/file_to_file.yml
```

# Recipes

A recipe is a configuration that tells our ingestion scripts where to pull data from (source) and where to put it (sink).
Here's a simple example that pulls metadata from MSSQL and puts it into datahub.

```yaml
# A sample recipe that pulls metadata from MSSQL and puts it into DataHub
# using the Rest API.
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

```sh
gometa-ingest -c ./examples/recipes/mssql_to_datahub.yml
```

A number of recipes are included in the examples/recipes directory.

# Sources
TODO

# Sinks
TODO

# Contributing

Contributions welcome!

## Testing
```sh
# Follow standard install procedure - see above.

# Install requirements.
pip install -r test_requirements.txt

# Run unit tests.
pytest tests/unit

# Run integration tests.
# Note: the integration tests require docker.
pytest tests/integration
```

## Sanity check code before checkin
```sh
flake8 src tests
mypy -p gometa
black --exclude 'gometa/metadata' -S -t py36 src tests
isort src tests
pytest
```
