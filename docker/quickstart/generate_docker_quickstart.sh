#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

set -euxo pipefail

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python generate_docker_quickstart.py ../docker-compose.yml ../docker-compose.override.yml docker-compose-with-elasticsearch.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-with-neo4j.yml ../docker-compose-with-neo4j.override.yml docker-compose-with-neo4j.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-with-dgraph.yml ../docker-compose-with-dgraph.override.yml docker-compose-with-dgraph.quickstart.yml
python generate_docker_quickstart.py ../monitoring/docker-compose.monitoring.yml docker-compose.monitoring.quickstart.yml
python generate_docker_quickstart.py ../monitoring/docker-compose.monitoring.yml docker-compose.quickstart.monitoring.yml

