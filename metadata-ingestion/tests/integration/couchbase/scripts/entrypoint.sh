#!/bin/bash

staticConfigFile=/opt/couchbase/etc/couchbase/static_config
restPortValue=8091

# see https://developer.couchbase.com/documentation/server/current/install/install-ports.html
function overridePort() {
    portName=$1
    portNameUpper=$(echo $portName | awk '{print toupper($0)}')
    portValue=${!portNameUpper}

    # only override port if value available AND not already contained in static_config
    if [ "$portValue" != "" ]; then
        if grep -Fq "{${portName}," ${staticConfigFile}
        then
            echo "Don't override port ${portName} because already available in $staticConfigFile"
        else
            echo "Override port '$portName' with value '$portValue'"
            echo "{$portName, $portValue}." >> ${staticConfigFile}

            if [ ${portName} == "rest_port" ]; then
                restPortValue=${portValue}
            fi
        fi
    fi
}

overridePort "rest_port"
overridePort "mccouch_port"
overridePort "memcached_port"
overridePort "query_port"
overridePort "ssl_query_port"
overridePort "fts_http_port"
overridePort "moxi_port"
overridePort "ssl_rest_port"
overridePort "ssl_capi_port"
overridePort "ssl_proxy_downstream_port"
overridePort "ssl_proxy_upstream_port"

[[ "$1" == "couchbase-server" ]] && {

    if [ "$(whoami)" = "couchbase" ]; then
        # Ensure that /opt/couchbase/var is owned by user 'couchbase' and
        # is writable
        if [ ! -w /opt/couchbase/var -o \
            $(find /opt/couchbase/var -maxdepth 0 -printf '%u') != "couchbase" ]; then
            echo "/opt/couchbase/var is not owned and writable by UID 1000"
            echo "Aborting as Couchbase Server will likely not run"
            exit 1
        fi
    fi
    echo "Starting Couchbase Server -- Web UI available at http://<ip>:$restPortValue"
    echo "and logs available in /opt/couchbase/var/lib/couchbase/logs"
    runsvdir -P /etc/service &
}

max_retries=5
retry_count=0

while [ $retry_count -lt $max_retries ]; do
  curl http://127.0.0.1:8091 > /dev/null 2>&1

  if [ $? -eq 0 ]; then
    echo "Server started"
    break
  else
    echo "Retrying..."
    retry_count=$((retry_count + 1))
    sleep 5
  fi
done

if [ $retry_count -eq $max_retries ]; then
  echo "Wait timeout after $max_retries retries. Exiting."
fi

echo "Configuring Couchbase Server"

/opt/couchbase/bin/couchbase-cli cluster-init -c 10.10.1.2 --cluster-username Administrator --cluster-password password --services data,query,index --cluster-ramsize 1024

sleep 2
echo "Importing Test Data"
/opt/couchbase/bin/couchbase-cli bucket-create -c couchbase://127.0.0.1 --username Administrator --password password --bucket data --bucket-type couchbase --bucket-ramsize 128
/opt/couchbase/bin/couchbase-cli collection-manage -c couchbase://127.0.0.1 --username Administrator --password password --bucket data --create-scope data
/opt/couchbase/bin/couchbase-cli collection-manage -c couchbase://127.0.0.1 --username Administrator --password password --bucket data --create-collection data.customers
sleep 2
/opt/couchbase/bin/cbimport json --format list -c couchbase://127.0.0.1 -u Administrator -p password -b data --scope-collection-exp data.customers -d file:///import.json --generate-key '#UUID#'

echo "The following output is now a tail of couchdb.log:"
tail -f /opt/couchbase/var/lib/couchbase/logs/couchdb.log &
childPID=$!
wait $childPID
