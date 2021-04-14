#!/bin/sh

set -e

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

function get_index_name() {
  if [[ -z "$INDEX_PREFIX" ]]; then
    echo $1
  else
    echo "${INDEX_PREFIX}_$1"
  fi
}

function generate_index_file() {
  jq -n \
    --slurpfile settings "$1" \
    --slurpfile mappings "$2" \
    '.settings=$settings[0] | .mappings=$mappings[0]' > "$3"
}

function check_reindex() {
    initial_documents=$(curl -XGET "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$1/_count" -H 'Content-Type: application/json' | jq '.count')
    for i in $(seq 30); do
      echo $i
      reindexed_documents=$(curl -XGET "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$2/_count" -H 'Content-Type: application/json' | jq '.count')
      if [[ $reindexed_documents == "$initial_documents" ]]; then
        echo -e "\nPost-reindex document reconcialiation completed. doc_source_index_count: $initial_documents; doc_target_index_count: $reindexed_documents"
        return 0
      else
        sleep 3
      fi
    done

    echo -e "\nPost-reindex document reconcialiation failed. doc_source_index_count: $initial_documents; doc_target_index_count: $reindexed_documents"
    return 1
}

function reindex() {
  source_index=$1
  target_index="$1_$(date +%s)"

  #create target index with latest index config
  curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$target_index" -H 'Content-Type: application/json' --data @/tmp/data

  #reindex the documents in source index to target index.
  # One of the assumption here is that we only add properties to document when index-config is evolved.
  # In case a property is deleted from document, it will still be reindexed in target index as default behaviour and
  # it is not breaking the code. If still needs to be purged from target index, use "removed" property in POST data.
  curl -XPOST "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_reindex?pretty" -H 'Content-Type: application/json' \
    -d "{\"source\":{\"index\":\"$source_index\"},\"dest\":{\"index\":\"$target_index\"}}"

  if check_reindex "$source_index" "$target_index"
  then
    #checking if source index is concrete index or alias
    if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_alias/$source_index") -eq 404 ]
    then
      curl -XDELETE "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$source_index"
    else
      concrete_index_name=$(curl -XGET "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_alias/$source_index" | jq 'keys[]' | head -1 | tr -d \")
      curl -XDELETE "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$concrete_index_name"
    fi

    curl -XPOST "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/_aliases" -H 'Content-Type: application/json' \
      -d "{\"actions\":[{\"remove\":{\"index\":\"*\",\"alias\":\"$source_index\"}},{\"add\":{\"index\":\"$target_index\",\"alias\":\"$source_index\"}}]}"

    echo -e "\nReindexing to $target_index succeded"
    return 0
  else
    curl -XDELETE "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$target_index"
    echo -e "\nReindexing to $target_index failed"
    return 1
  fi
}

function create_index() {
  generate_index_file "index/$2" "index/$3" /tmp/data

  #checking if index(or alias) exists
  if [ $(curl -o /dev/null -s -w "%{http_code}" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$1") -eq 404 ]
  then
    echo -e '\ncreating index' "$1"

    curl -XPUT "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$1" -H 'Content-Type: application/json' --data @/tmp/data
    return 0
  else
    echo -e '\ncomparing with existing version of index' "$1"

    setting_keys_regex=$(jq '.index | keys[]' "index/$2" | xargs | sed 's/ /|/g')
    curl -XGET "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$1/_settings" | \
      jq '.. | .settings? | select(. != null)' | \
      jq --arg KEYS_REGEX "$setting_keys_regex" '.index | with_entries(select(.key | match($KEYS_REGEX))) | {"index":.}' \
      > /tmp/existing_setting

    curl -XGET "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST_URL:$ELASTICSEARCH_PORT/$1/_mapping" | \
      jq '.. | .mappings? | select(. != null)' \
      > /tmp/existing_mapping

    generate_index_file /tmp/existing_setting /tmp/existing_mapping /tmp/existing

    jq -S . /tmp/existing > /tmp/existing_sorted
    jq -S . /tmp/data > /tmp/data_sorted
    if diff /tmp/existing_sorted /tmp/data_sorted
    then
      echo -e "\nno changes to index $1 mappings and settings"
      return 0
    else
      echo -e "\nupdating index" "$1"

      reindex "$1" && return 0 || return 1
    fi
  fi
}

create_index $(get_index_name chartdocument) chart/settings.json chart/mappings.json || exit 1
create_index $(get_index_name corpuserinfodocument) corp-user/settings.json corp-user/mappings.json  || exit 1
create_index $(get_index_name dashboarddocument) dashboard/settings.json dashboard/mappings.json  || exit 1
create_index $(get_index_name datajobdocument) datajob/settings.json datajob/mappings.json || exit 1
create_index $(get_index_name dataflowdocument) dataflow/settings.json dataflow/mappings.json || exit 1
create_index $(get_index_name dataprocessdocument) data-process/settings.json data-process/mappings.json || exit 1
create_index $(get_index_name datasetdocument) dataset/settings.json dataset/mappings.json || exit 1
create_index $(get_index_name mlmodeldocument) ml-model/settings.json ml-model/mappings.json || exit 1
create_index $(get_index_name tagdocument) tags/settings.json tags/mappings.json || exit 1