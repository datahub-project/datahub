#!/bin/sh

set -e

function create_index {
  jq -n \
    --slurpfile settings index/$2 \
    --slurpfile mappings index/$3 \
    '.settings=$settings[0] | .mappings.doc=$mappings[0]' > /tmp/data

  curl -XPUT $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/$1 --data @/tmp/data
}

create_index chartdocument chart/settings.json chart/mappings.json
create_index corpuserinfodocument corp-user/settings.json corp-user/mappings.json
create_index dashboarddocument dashboard/settings.json dashboard/mappings.json
create_index datajobdocument datajob/settings.json datajob/mappings.json
create_index dataflowdocument dataflow/settings.json dataflow/mappings.json
create_index dataprocessdocument data-process/settings.json data-process/mappings.json
create_index datasetdocument dataset/settings.json dataset/mappings.json
create_index mlmodeldocument ml-model/settings.json ml-model/mappings.json
