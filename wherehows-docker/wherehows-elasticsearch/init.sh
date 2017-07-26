#!/bin/bash

curl -XPUT $ELASTICSEARCH_SERVER_URL/wherehows_v1 --data @/tmp/index_mapping.json