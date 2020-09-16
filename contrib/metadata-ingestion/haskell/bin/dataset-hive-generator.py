#! /usr/bin/env nix-shell                                                                                                                                           
#! nix-shell dataset-hive-generator.py.nix -i python                                                                                                                      

import sys
import time
from pyhive import hive
from TCLIService.ttypes import TOperationState

import simplejson as json

HIVESTORE='localhost'

AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent_v4'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'

def hive_query(query):
    """
    Execute the query to the HiveStore.
    """
    cursor = hive.connect(HIVESTORE).cursor()
    cursor.execute(query, async_=True)
    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            sys.stdout.write(message)
        status = cursor.poll().operationState
    results = cursor.fetchall()
    return results

def build_hive_dataset_mce(dataset_name, schema, metadata):
    """
    Create the MetadataChangeEvent via dataset_name and schema.
    """
    actor, type, created_time, upstreams_dataset, sys_time = "urn:li:corpuser:" + metadata[2][7:], str(metadata[-1][11:-1]), int(metadata[3][12:]), metadata[-28][10:], int(time.time())
    owners = {"owners":[{"owner":actor,"type":"DATAOWNER"}],"lastModified":{"time":sys_time,"actor":actor}}
    upstreams = {"upstreams":[{"auditStamp":{"time":sys_time,"actor":actor},"dataset":"urn:li:dataset:(urn:li:dataPlatform:hive," + upstreams_dataset + ",PROD)","type":"TRANSFORMED"}]}
    elements = {"elements":[{"url":HIVESTORE,"description":"sample doc to describe upstreams","createStamp":{"time":sys_time,"actor":actor}}]}
    schema_name = {"schemaName":dataset_name,"platform":"urn:li:dataPlatform:hive","version":0,"created":{"time":created_time,"actor":actor},
                  "lastModified":{"time":sys_time,"actor":actor},"hash":"","platformSchema":{"com.linkedin.pegasus2avro.schema.OtherSchema": {"rawSchema": schema}},
                   "fields":[{"fieldPath":"","description":{"string":""},"nativeDataType":"string","type":{"type":{"com.linkedin.pegasus2avro.schema.StringType":{}}}}]}

    mce = {"auditHeader": None,
           "proposedSnapshot":{"com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot":
                               {"urn": "urn:li:dataset:(urn:li:dataPlatform:hive,"+ dataset_name +",PROD)"
                               ,"aspects": [
                                   {"com.linkedin.pegasus2avro.common.Ownership": owners}
                                 , {"com.linkedin.pegasus2avro.dataset.UpstreamLineage": upstreams}
                                 , {"com.linkedin.pegasus2avro.common.InstitutionalMemory": elements}
                                 , {"com.linkedin.pegasus2avro.schema.SchemaMetadata": schema_name}
                                 ]}},
           "proposedDelta": None}

    print(json.dumps(mce))

databases = hive_query('show databases')
for database in databases:
    tables = hive_query('show tables in ' + database[0])
    for table in tables:
        dataset_name = database[0] + '.' + table[0]
        description = hive_query('describe extended ' + dataset_name)
        build_hive_dataset_mce(dataset_name, str(description[:-1][:-1]), description[-1][1].split(','))

sys.exit(0)
