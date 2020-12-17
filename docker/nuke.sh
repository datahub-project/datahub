#!/bin/sh

# Tear down and clean up all DataHub-related containers, volumes, and network
docker-compose -p datahub down -v
docker-compose rm -f -v
