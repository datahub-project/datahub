#!/bin/bash

set -e

: ${DATAHUB_ANALYTICS_ENABLED:=true}
: ${USE_AWS_ELASTICSEARCH:=false}

# protocol: http or https?
if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi

# Elasticsearch URL to be suffixed with a resource address
ELASTICSEARCH_URL="$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT"

# set auth header if none is given
if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  if [[ ! -z $ELASTICSEARCH_USERNAME ]]; then
    # no auth header given, but username is defined -> use it to create the auth header
    AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64 --wrap 0)
    ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
  else
    # no auth header or username given -> use default auth header
    ELASTICSEARCH_AUTH_HEADER="Accept: */*"
  fi
fi

# index prefix used throughout the script
if [[ -z "$INDEX_PREFIX" ]]; then
  PREFIX=''
else
  PREFIX="${INDEX_PREFIX}_"
fi

# path where index definitions are stored
INDEX_DEFINITIONS_ROOT=/index/usage-event


# check Elasticsearch for given index/resource (first argument)
# if it doesn't exist (http code 404), use the given file (second argument) to create it
function create_if_not_exists {
  RESOURCE_ADDRESS="$1"
  RESOURCE_DEFINITION_NAME="$2"

  # query ES to see if the resource already exists
  RESOURCE_STATUS=$(curl -o /dev/null -s -w "%{http_code}\n" --header "$ELASTICSEARCH_AUTH_HEADER" "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS")

  if [ $RESOURCE_STATUS -eq 200 ]; then
    # resource already exists -> nothing to do
    echo -e "\n>>> $RESOURCE_ADDRESS already exists ✓"

  elif [ $RESOURCE_STATUS -eq 404 ]; then
    # resource doesn't exist -> need to create it
    echo -e "\n>>> creating $RESOURCE_ADDRESS ..."
    # use the given path as definition, but first replace all occurences of PREFIX with the actual prefix
    TMP_SOURCE_PATH="/tmp/$RESOURCE_DEFINITION_NAME"
    sed -e "s/PREFIX/$PREFIX/g" "$INDEX_DEFINITIONS_ROOT/$RESOURCE_DEFINITION_NAME" | tee -a "$TMP_SOURCE_PATH"
    curl -s -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS" -H 'Content-Type: application/json' --data "@$TMP_SOURCE_PATH"

  elif [ $RESOURCE_STATUS -eq 401 ] || [ $RESOURCE_STATUS -eq 405 ]; then
    echo -e "\n>>> failed to GET $RESOURCE_ADDRESS ($RESOURCE_STATUS) !"
    echo "... make sure you have correct USE_AWS_ELASTICSEARCH env value set (current=$USE_AWS_ELASTICSEARCH)"
    exit 1

  else
    echo -e "\n>>> unexpected $RESOURCE_ADDRESS status $RESOURCE_STATUS !"
    exit 1
  fi
}

# create indices for ES (non-AWS)
function create_datahub_usage_event_datastream() {
  create_if_not_exists "_ilm/policy/${PREFIX}datahub_usage_event_policy" policy.json
  create_if_not_exists "_index_template/${PREFIX}datahub_usage_event_index_template" index_template.json
}

# create indices for ES OSS (AWS)
function create_datahub_usage_event_aws_elasticsearch() {
  create_if_not_exists "_opendistro/_ism/policies/${PREFIX}datahub_usage_event_policy" aws_es_ism_policy.json
  create_if_not_exists "_template/${PREFIX}datahub_usage_event_index_template" aws_es_index_template.json

  # this fixes the case when datahub_usage_event was created by GMS before datahub_usage_event-000001
  USAGE_EVENT_STATUS=$(curl -o /dev/null -s -w "%{http_code}\n" --header "$ELASTICSEARCH_AUTH_HEADER" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event")
  if [ $USAGE_EVENT_STATUS -eq 200 ]; then
    USAGE_EVENT_DEFINITION=$(curl -s --header "$ELASTICSEARCH_AUTH_HEADER" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event")
    # the definition is expected to contain "datahub_usage_event-000001" string
    if [[ $USAGE_EVENT_DEFINITION != *"datahub_usage_event-000001"* ]]; then
      # ... if it doesn't, we need to drop it
      echo -e "\n>>> deleting invalid datahub_usage_event ..."
      curl -s -XDELETE --header "$ELASTICSEARCH_AUTH_HEADER" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event"
      # ... and then recreate it below
    fi
  fi

  create_if_not_exists "${PREFIX}datahub_usage_event-000001" aws_es_usage_event.json
}

if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  if [[ $USE_AWS_ELASTICSEARCH == false ]]; then
    create_datahub_usage_event_datastream || exit 1
  else
    create_datahub_usage_event_aws_elasticsearch || exit 1
  fi
fi
