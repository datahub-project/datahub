#!/bin/sh

set -e

: ${DATAHUB_ANALYTICS_ENABLED:=true}
: ${USE_AWS_ELASTICSEARCH:=false}

if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi

if [[ -z $ELASTICSEARCH_USERNAME ]]; then
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_HOST
else
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD@$ELASTICSEARCH_HOST
fi

function create_datahub_usage_event_datastream() {
  if [[ -z "$INDEX_PREFIX" ]]; then
    PREFIX=''
  else
    PREFIX="${INDEX_PREFIX}_"
  fi

  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_ilm/policy/${PREFIX}datahub_usage_event_policy") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_policy"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/policy.json | tee -a /tmp/policy.json
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_ilm/policy/${PREFIX}datahub_usage_event_policy" -H 'Content-Type: application/json' --data @/tmp/policy.json
  else
    echo -e "\ndatahub_usage_event_policy exists"
  fi
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_index_template/${PREFIX}datahub_usage_event_index_template") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_index_template"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/index_template.json | tee -a /tmp/index_template.json
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_index_template/${PREFIX}datahub_usage_event_index_template" -H 'Content-Type: application/json' --data @/tmp/index_template.json
  else
    echo -e "\ndatahub_usage_event_index_template exists"
  fi
}

function create_datahub_usage_event_aws_elasticsearch() {
  if [[ -z "$INDEX_PREFIX" ]]; then
    PREFIX=''
  else
    PREFIX="${INDEX_PREFIX}_"
  fi

  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_opendistro/_ism/policies/${PREFIX}datahub_usage_event_policy") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_policy"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/aws_es_ism_policy.json | tee -a /tmp/aws_es_ism_policy.json
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_opendistro/_ism/policies/${PREFIX}datahub_usage_event_policy" -H 'Content-Type: application/json' --data @/tmp/aws_es_ism_policy.json
  else
    echo -e "\ndatahub_usage_event_policy exists"
  fi
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_template/${PREFIX}datahub_usage_event_index_template") -eq 404 ]
  then
    echo -e "\ncreating datahub_usagAe_event_index_template"
    sed -e "s/PREFIX/${PREFIX}/g" /index/usage-event/aws_es_index_template.json | tee -a /tmp/aws_es_index_template.json
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_template/${PREFIX}datahub_usage_event_index_template" -H 'Content-Type: application/json' --data @/tmp/aws_es_index_template.json
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/${PREFIX}datahub_usage_event-000001"  -H 'Content-Type: application/json' --data "{\"aliases\":{\"${PREFIX}datahub_usage_event\":{\"is_write_index\":true}}}"
  else
    echo -e "\ndatahub_usage_event_index_template exists"
  fi
}

if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  if [[ $USE_AWS_ELASTICSEARCH == false ]]; then
    create_datahub_usage_event_datastream || exit 1
  else
    create_datahub_usage_event_aws_elasticsearch || exit 1
  fi
fi

