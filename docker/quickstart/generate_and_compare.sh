#!/bin/bash

# this scripts checks if docker-compose$flavour.quickstart.yml is up to date for these 'flavours':
FLAVOURS=("-with-elasticsearch" "-with-neo4j" "-with-dgraph" ".monitoring")

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

set -euxo pipefail

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python generate_docker_quickstart.py ../docker-compose.yml ../docker-compose.override.yml temp-with-elasticsearch.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-with-neo4j.yml ../docker-compose-with-neo4j.override.yml temp-with-neo4j.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-with-dgraph.yml ../docker-compose-with-dgraph.override.yml temp-with-dgraph.quickstart.yml

python generate_docker_quickstart.py ../monitoring/docker-compose.monitoring.yml temp.monitoring.quickstart.yml

for flavour in "${FLAVOURS[@]}"
do
  if cmp docker-compose$flavour.quickstart.yml temp$flavour.quickstart.yml; then
    echo "docker-compose$flavour.quickstart.yml is up to date."
  else
    echo "docker-compose$flavour.quickstart.yml is out of date."
    exit 1
  fi
done

