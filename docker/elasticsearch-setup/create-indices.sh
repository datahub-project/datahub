#!/bin/sh

set -e

: ${DATAHUB_ANALYTICS_ENABLED:=true}
: ${USE_AWS_ELASTICSEARCH:=false}

if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi

if [[ -z $ELASTICSEARCH_MASTER_USERNAME ]]; then
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_HOST
else
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_MASTER_USERNAME:$ELASTICSEARCH_MASTER_PASSWORD@$ELASTICSEARCH_HOST
fi

function create_datahub_usage_event_datastream() {
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_ilm/policy/datahub_usage_event_policy") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_policy"
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_ilm/policy/datahub_usage_event_policy" -H 'Content-Type: application/json' --data @/index/usage-event/policy.json
  else
    echo -e "\ndatahub_usage_event_policy exists"
  fi
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_index_template/datahub_usage_event_index_template") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_index_template"
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_index_template/datahub_usage_event_index_template" -H 'Content-Type: application/json' --data @/index/usage-event/index_template.json
  else
    echo -e "\ndatahub_usage_event_index_template exists"
  fi
}

function create_datahub_usage_event_aws_elasticsearch() {
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_opendistro/_ism/policies/datahub_usage_event_policy") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_policy"
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_opendistro/_ism/policies/datahub_usage_event_policy" -H 'Content-Type: application/json' --data @/index/usage-event/aws_es_ism_policy.json
  else
    echo -e "\ndatahub_usage_event_policy exists"
  fi
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_template/datahub_usage_event_index_template") -eq 404 ]
  then
    echo -e "\ncreating datahub_usage_event_index_template"
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_template/datahub_usage_event_index_template" -H 'Content-Type: application/json' --data @/index/usage-event/aws_es_index_template.json
    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/datahub_usage_event-000001"  -H 'Content-Type: application/json' --data "{\"aliases\":{\"datahub_usage_event\":{\"is_write_index\":true}}}"
  else
    echo -e "\ndatahub_usage_event_index_template exists"
  fi
}

function create_user() {
  ROLE="${INDEX_PREFIX}_access"
  echo -e '\ncreating role' "${ROLE}"
  curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_opendistro/_security/api/roles/${ROLE}" -H 'Content-Type: application/json' \
    -d "{\"index_permissions\":[{\"index_patterns\":[\"${INDEX_PREFIX}_*\"], \"allowed_actions\":[\"indices_all\"]}]}"
  echo -e '\ncreating user' "${ELASTICSEARCH_USERNAME}"
  curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_opendistro/_security/api/internalusers/${ELASTICSEARCH_USERNAME}" -H 'Content-Type: application/json' \
    -d "{\"password\":\"${ELASTICSEARCH_PASSWORD}\", \"opendistro_security_roles\":[\"${ROLE}\"]}"
}

create_user || exit 1
if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  if [[ $USE_AWS_ELASTICSEARCH == false ]]; then
    create_datahub_usage_event_datastream || exit 1
  else
    create_datahub_usage_event_aws_elasticsearch || exit 1
  fi
fi
