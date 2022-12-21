#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

set -euxo pipefail

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python generate_docker_quickstart.py ../docker-compose.yml ../docker-compose.override.yml docker-compose.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-without-neo4j.yml ../docker-compose-without-neo4j.override.yml docker-compose-without-neo4j.quickstart.yml
python generate_docker_quickstart.py ../monitoring/docker-compose.monitoring.yml docker-compose.monitoring.quickstart.yml
python generate_docker_quickstart.py ../docker-compose.consumers.yml docker-compose.consumers.quickstart.yml
python generate_docker_quickstart.py ../docker-compose.consumers-without-neo4j.yml docker-compose.consumers-without-neo4j.quickstart.yml
