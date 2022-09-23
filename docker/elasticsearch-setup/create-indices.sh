#!/bin/bash

set -e

: ${DATAHUB_ANALYTICS_ENABLED:=true}
: ${USE_AWS_ELASTICSEARCH:=false}

if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi
echo -e "Going to use protocol: $ELASTICSEARCH_PROTOCOL"

if [[ ! -z $ELASTICSEARCH_USERNAME ]] && [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64 --wrap 0)
  ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
fi

# Add default header if needed
if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  echo -e "Going to use default elastic headers"
  ELASTICSEARCH_AUTH_HEADER="Accept: */*"
fi

if [[ $ELASTICSEARCH_INSECURE ]]; then
  ELASTICSEARCH_INSECURE="-k "
fi

function create_datahub_usage_event_datastream() {
  if [[ -z "$INDEX_PREFIX" ]]; then
    PREFIX=''
  else
    PREFIX="${INDEX_PREFIX}_"
  fi
  echo -e "Create datahub_usage_event if needed against Elasticsearch at $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT" 
  echo -e "Going to use index prefix:$PREFIX:"
  POLICY_RESPONSE_CODE=$(curl -o /dev/null -s -w "%{http_code}" --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_ilm/policy/${PREFIX}datahub_usage_event_policy")
  echo -e "Policy GET response code is $POLICY_RESPONSE_CODE"
  POLICY_NAME="${PREFIX}datahub_usage_event_policy"
  if [ $POLICY_RESPONSE_CODE -eq 404 ]; then
    echo -e "\ncreating $POLICY_NAME"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/policy.json | tee -a /tmp/policy.json
    curl -s -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_ilm/policy/$POLICY_NAME" --header "Content-Type: application/json" --data "@/tmp/policy.json"
  elif [ $POLICY_RESPONSE_CODE -eq 200 ]; then
    echo -e "\n${POLICY_NAME} exists"
  elif [ $POLICY_RESPONSE_CODE -eq 403 ]; then
    echo -e "Forbidden so exiting"
    exit 1
  else
    echo -e "Got response code $POLICY_RESPONSE_CODE while creating policy so exiting."
    exit 1
  fi
  
  TEMPLATE_RESPONSE_CODE=$(curl -o /dev/null -s -w "%{http_code}" --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_index_template/${PREFIX}datahub_usage_event_index_template")
  echo -e "Template GET response code is $TEMPLATE_RESPONSE_CODE"
  TEMPLATE_NAME="${PREFIX}datahub_usage_event_index_template"
  if [ $TEMPLATE_RESPONSE_CODE -eq 404 ]; then
    echo -e "\ncreating $TEMPLATE_NAME"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/index_template.json | tee -a /tmp/index_template.json
    curl -s -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_index_template/$TEMPLATE_NAME" --header "Content-Type: application/json" --data "@/tmp/index_template.json"
  elif [ $TEMPLATE_RESPONSE_CODE -eq 200 ]; then
    echo -e "\n$TEMPLATE_NAME exists"
  elif [ $TEMPLATE_RESPONSE_CODE -eq 403 ]; then
    echo -e "Forbidden so exiting"
    exit 1
  else
    echo -e "Got response code $TEMPLATE_RESPONSE_CODE while creating template so exiting."
    exit 1
  fi
}

function create_datahub_usage_event_aws_elasticsearch() {
  if [[ -z "$INDEX_PREFIX" ]]; then
    PREFIX=''
  else
    PREFIX="${INDEX_PREFIX}_"
  fi

  if [ $(curl -o /dev/null -s -w "%{http_code}" --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_opendistro/_ism/policies/${PREFIX}datahub_usage_event_policy") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_policy"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/aws_es_ism_policy.json | tee -a /tmp/aws_es_ism_policy.json
    curl -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_opendistro/_ism/policies/${PREFIX}datahub_usage_event_policy" -H 'Content-Type: application/json' --data @/tmp/aws_es_ism_policy.json
  else
    echo -e "\ndatahub_usage_event_policy exists"
  fi
  if [ $(curl -o /dev/null -s -w "%{http_code}" --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_template/${PREFIX}datahub_usage_event_index_template") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_index_template"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/aws_es_index_template.json | tee -a /tmp/aws_es_index_template.json
    curl -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_template/${PREFIX}datahub_usage_event_index_template" -H 'Content-Type: application/json' --data @/tmp/aws_es_index_template.json
    curl -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/${PREFIX}datahub_usage_event-000001"  -H 'Content-Type: application/json' --data "{\"aliases\":{\"${PREFIX}datahub_usage_event\":{\"is_write_index\":true}}}"
  else
    echo -e "\ndatahub_usage_event_index_template exists"
  fi
}

if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  echo -e "\n datahub_analytics_enabled: $DATAHUB_ANALYTICS_ENABLED"
  if [[ $USE_AWS_ELASTICSEARCH == false ]]; then
    create_datahub_usage_event_datastream || exit 1
  else
    create_datahub_usage_event_aws_elasticsearch || exit 1
  fi
else
  echo -e "\ndatahub_analytics_enabled: $DATAHUB_ANALYTICS_ENABLED"
  DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE=$(curl -o /dev/null -s -w "%{http_code}" --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/cat/indices/${PREFIX}datahub_usage_event")
  if [ $DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE -eq 404 ]
  then
    echo -e "\ncreating ${PREFIX}datahub_usage_event"
    curl -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/${PREFIX}datahub_usage_event"
  elif [ $DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE -eq 200 ]; then
    echo -e "\n${PREFIX}datahub_usage_event exists"
  elif [ $DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE -eq 403 ]; then
    echo -e "Forbidden so exiting"
  fi
fi
