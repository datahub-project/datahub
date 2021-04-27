#!/bin/sh

pip3 install -r requirements.txt
python3 generate_docker_quickstart.py ../docker-compose.yml ../docker-compose.override.yml docker-compose.quickstart.yml

