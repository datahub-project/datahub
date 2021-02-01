# Dev
## Set up dev environment
- On MacOS: brew install librdkafka
- python3 -m venv venv
- source venv/bin/activate
- pip install -e .

# Run tests
- pip install -r test_requirements.txt
- pytest

# Run recipe
- ./recipes/kafka_to_console.sh

# Using Docker
## Build the image
- docker build . --tag dhub-ingest

## Run the ingestion script (recipes/kafka-to-console.yaml)
docker run --rm --network host dhub-ingest:latest
