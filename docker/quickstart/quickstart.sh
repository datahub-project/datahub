#!/bin/bash

export DATA_STORAGE_FOLDER=${DATA_STORAGE_FOLDER:=/tmp/datahub}
mkdir -p ${DATA_STORAGE_FOLDER}

# https://discuss.elastic.co/t/elastic-elasticsearch-docker-not-assigning-permissions-to-data-directory-on-run/65812/4
mkdir -p ${DATA_STORAGE_FOLDER}/elasticsearch
sudo chown 1000:1000 ${DATA_STORAGE_FOLDER}/elasticsearch

docker-compose pull && docker-compose up --build
