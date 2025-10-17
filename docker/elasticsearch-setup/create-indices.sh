#!/bin/bash

set -e

: ${DATAHUB_ANALYTICS_ENABLED:=true}
: ${USE_AWS_ELASTICSEARCH:=false}
: ${ELASTICSEARCH_INSECURE:=false}
: ${DUE_SHARDS:=1}
: ${DUE_REPLICAS:=1}
: ${MAX_RETRIES:=5}
: ${INITIAL_RETRY_DELAY:=2}

# protocol: http or https?
if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi
echo -e "going to use protocol: $ELASTICSEARCH_PROTOCOL"

# Elasticsearch URL to be suffixed with a resource address
ELASTICSEARCH_URL="$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT"

# set auth header if none is given
if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  if [[ ! -z $ELASTICSEARCH_USERNAME ]]; then
    # no auth header given, but username is defined -> use it to create the auth header
    AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64 --wrap 0)
    ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
    echo -e "going to use elastic headers based on username and password"
  else
    # no auth header or username given -> use default auth header
    ELASTICSEARCH_AUTH_HEADER="Accept: */*"
    echo -e "going to use default elastic headers"
  fi
fi

# will be using this for all curl communication with Elasticsearch:
CURL_ARGS=(
  --silent
  --header "$ELASTICSEARCH_AUTH_HEADER"
)
# ... also optionally use --insecure
if [[ $ELASTICSEARCH_INSECURE == true ]]; then
  CURL_ARGS+=(--insecure)
fi

# index prefix used throughout the script
if [[ -z "$INDEX_PREFIX" ]]; then
  PREFIX=''
  echo -e "not using any prefix"
else
  PREFIX="${INDEX_PREFIX}_"
  echo -e "going to use prefix: '$PREFIX'"
fi

# path where index definitions are stored
INDEX_DEFINITIONS_ROOT=/index/usage-event

# Retry function with exponential backoff
function retry_with_backoff {
  local max_attempts=$1
  shift
  local delay=$INITIAL_RETRY_DELAY
  local attempt=1

  while [ $attempt -le $max_attempts ]; do
    if "$@"; then
      return 0
    fi

    if [ $attempt -lt $max_attempts ]; then
      echo -e ">>> Attempt $attempt failed. Retrying in ${delay}s..."
      sleep $delay
      delay=$((delay * 2))
      attempt=$((attempt + 1))
    else
      echo -e ">>> All $max_attempts attempts failed."
      return 1
    fi
  done
}

# check Elasticsearch for given index/resource (first argument)
# if it doesn't exist (http code 404), use the given file (second argument) to create it
function create_if_not_exists {
  RESOURCE_ADDRESS="$1"
  RESOURCE_DEFINITION_NAME="$2"

  # Retry the GET request to check if resource exists
  echo -e "\n>>> Checking if $RESOURCE_ADDRESS exists..."
  retry_with_backoff $MAX_RETRIES curl "${CURL_ARGS[@]}" -o /dev/null -w "%{http_code}" \
    "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS" | grep -qE "^(200|404)$"

  RESOURCE_STATUS=$(curl "${CURL_ARGS[@]}" -o /dev/null -w "%{http_code}\n" "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS")
  echo -e ">>> GET $RESOURCE_ADDRESS response code is $RESOURCE_STATUS"

  if [ $RESOURCE_STATUS -eq 200 ]; then
    # resource already exists -> nothing to do
    echo -e ">>> $RESOURCE_ADDRESS already exists ✓"

  elif [ $RESOURCE_STATUS -eq 404 ]; then
    # resource doesn't exist -> need to create it
    echo -e ">>> creating $RESOURCE_ADDRESS because it doesn't exist ..."
    # use the file at given path as definition, but first replace all occurences of `PREFIX`
    # placeholder within the file with the actual prefix value
    TMP_SOURCE_PATH="/tmp/$RESOURCE_DEFINITION_NAME"
    sed -e "s/PREFIX/$PREFIX/g" "$INDEX_DEFINITIONS_ROOT/$RESOURCE_DEFINITION_NAME" \
       | sed -e "s/DUE_SHARDS/$DUE_SHARDS/g" \
       | sed -e "s/DUE_REPLICAS/$DUE_REPLICAS/g" \
       | tee -a "$TMP_SOURCE_PATH"
    curl "${CURL_ARGS[@]}" -XPUT "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS" -H 'Content-Type: application/json' --data "@$TMP_SOURCE_PATH"

  elif [ $RESOURCE_STATUS -eq 403 ]; then
    # probably authorization fail
    echo -e ">>> forbidden access to $RESOURCE_ADDRESS ! -> exiting"
    exit 1

  else
    # when `USE_AWS_ELASTICSEARCH` was forgotten to be set to `true` when running against AWS ES OSS,
    # this script will use wrong paths (e.g. `_ilm/policy/` instead of AWS-compatible `_plugins/_ism/policies/`)
    # and the ES endpoint will return `401 Unauthorized` or `405 Method Not Allowed`
    # let's use this as chance to point that wrong config might be used!
    if [ $RESOURCE_STATUS -eq 401 ] || [ $RESOURCE_STATUS -eq 405 ]; then
      if [[ $USE_AWS_ELASTICSEARCH == false ]] && [[ $ELASTICSEARCH_URL == *"amazonaws"* ]]; then
        echo "... looks like AWS OpenSearch is used; please set USE_AWS_ELASTICSEARCH env value to true"
      fi
    fi

    echo -e ">>> failed to GET $RESOURCE_ADDRESS ! -> exiting"
    exit 1
  fi
}

# Update ISM policy. Non-fatal if policy cannot be updated.
function update_ism_policy {
  RESOURCE_ADDRESS="$1"
  RESOURCE_DEFINITION_NAME="$2"

  TMP_CURRENT_POLICY_PATH="/tmp/current-$RESOURCE_DEFINITION_NAME"

  # Get existing policy
  RESOURCE_STATUS=$(curl "${CURL_ARGS[@]}" -o $TMP_CURRENT_POLICY_PATH -w "%{http_code}\n" "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS")
  echo -e "\n>>> GET $RESOURCE_ADDRESS response code is $RESOURCE_STATUS"

  if [ $RESOURCE_STATUS -ne 200 ]; then
    echo -e ">>> Could not get ISM policy $RESOURCE_ADDRESS. Ignoring."
    return
  fi

  SEQ_NO=$(cat $TMP_CURRENT_POLICY_PATH | jq -r '._seq_no')
  PRIMARY_TERM=$(cat $TMP_CURRENT_POLICY_PATH | jq -r '._primary_term')

  TMP_NEW_RESPONSE_PATH="/tmp/response-$RESOURCE_DEFINITION_NAME"
  TMP_NEW_POLICY_PATH="/tmp/new-$RESOURCE_DEFINITION_NAME"
  sed -e "s/PREFIX/$PREFIX/g" "$INDEX_DEFINITIONS_ROOT/$RESOURCE_DEFINITION_NAME" \
      | sed -e "s/DUE_SHARDS/$DUE_SHARDS/g" \
      | sed -e "s/DUE_REPLICAS/$DUE_REPLICAS/g" \
      | tee -a "$TMP_NEW_POLICY_PATH"

  # Retry the update with exponential backoff
  retry_with_backoff $MAX_RETRIES curl "${CURL_ARGS[@]}" -XPUT \
    "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS?if_seq_no=$SEQ_NO&if_primary_term=$PRIMARY_TERM" \
    -H 'Content-Type: application/json' -w "%{http_code}" -o $TMP_NEW_RESPONSE_PATH --data "@$TMP_NEW_POLICY_PATH" | \
    grep -qE "^(200|201)$"

  if [ $? -eq 0 ]; then
    echo -e ">>> Successfully updated ISM policy $RESOURCE_ADDRESS"
  else
    echo -e ">>> Failed to update ISM policy $RESOURCE_ADDRESS after $MAX_RETRIES attempts (non-fatal)"
  fi
}

# create indices for ES (non-AWS)
function create_datahub_usage_event_datastream() {
  # non-AWS env requires creation of three resources for Datahub usage events:
  #   1. ILM policy
  create_if_not_exists "_ilm/policy/${PREFIX}datahub_usage_event_policy" policy.json
  #   2. index template
  create_if_not_exists "_index_template/${PREFIX}datahub_usage_event_index_template" index_template.json
  #   3. although indexing request creates the data stream, it's not queryable before creation, causing GMS to throw exceptions
  create_if_not_exists "_data_stream/${PREFIX}datahub_usage_event" "datahub_usage_event"
}

# create indices for ES OSS (AWS)
function create_datahub_usage_event_aws_elasticsearch() {
  # AWS env requires creation of three resources for Datahub usage events:
  #   1. ISM policy
  create_if_not_exists "_plugins/_ism/policies/${PREFIX}datahub_usage_event_policy" aws_es_ism_policy.json

  #   1.1 ISM policy update if it already existed
  if [ $RESOURCE_STATUS -eq 200 ]; then
    update_ism_policy "_plugins/_ism/policies/${PREFIX}datahub_usage_event_policy" aws_es_ism_policy.json
  fi

  #   2. index template
  create_if_not_exists "_template/${PREFIX}datahub_usage_event_index_template" aws_es_index_template.json

  #   3. event index datahub_usage_event-000001
  #     (note that AWS *rollover* indices need to use `^.*-\d+$` naming pattern)
  #     -> https://aws.amazon.com/premiumsupport/knowledge-center/opensearch-failed-rollover-index/
  INDEX_SUFFIX="000001"
  #     ... but first check whether `datahub_usage_event` wasn't already autocreated by GMS before `datahub_usage_event-000001`
  #     (as is common case when this script was initially run without properly setting `USE_AWS_ELASTICSEARCH` to `true`)
  #     -> https://github.com/datahub-project/datahub/issues/5376
  USAGE_EVENT_STATUS=$(curl "${CURL_ARGS[@]}" -o /dev/null -w "%{http_code}\n" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event")
  if [ $USAGE_EVENT_STATUS -eq 200 ]; then
    USAGE_EVENT_DEFINITION=$(curl "${CURL_ARGS[@]}" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event")
    # the definition is expected to contain "datahub_usage_event-000001" string
    if [[ $USAGE_EVENT_DEFINITION != *"datahub_usage_event-"* ]]; then
      # ... if it doesn't, we need to drop it
      echo -e "\n>>> deleting invalid datahub_usage_event ..."
      curl "${CURL_ARGS[@]}" -XDELETE "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event"
      # ... and then recreate it below
    fi
  fi

  #   ... now we are safe to create the index
  create_if_not_exists "${PREFIX}datahub_usage_event-$INDEX_SUFFIX" aws_es_index.json
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
  DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE=$(curl "${CURL_ARGS[@]}" -o /dev/null -w "%{http_code}" "$ELASTICSEARCH_URL/_cat/indices/${PREFIX}datahub_usage_event")
  if [ $DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE -eq 404 ]
  then
    echo -e "\ncreating ${PREFIX}datahub_usage_event"
    curl "${CURL_ARGS[@]}" -XPUT "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event"
  elif [ $DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE -eq 200 ]; then
    echo -e "\n${PREFIX}datahub_usage_event exists"
  elif [ $DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE -eq 403 ]; then
    echo -e "Forbidden so exiting"
  fi
fi
