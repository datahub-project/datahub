# Dev
## Set up dev environment
- On MacOS: `brew install librdkafka`
- On Debian/Ubuntu: `sudo apt install librdkafka-dev`
- python3 -m venv venv
- source venv/bin/activate
- pip install -e .

# Run tests
- pip install -r test_requirements.txt
- pytest

# Sanity check code before checkin (currently broken)
- flake8 src test && mypy -p gometa && black --check -l 120 src test && isort --check-only src test && pytest

# Run recipe
- ./recipes/kafka_to_console.sh

# Using Docker
## Build the image
- docker build . --tag dhub-ingest

## Run the ingestion script (recipes/kafka-to-console.yaml)
docker run --rm --network host dhub-ingest:latest


