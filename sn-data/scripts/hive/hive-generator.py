#! /usr/bin/env nix-shell                                                                                                                                           
#! nix-shell dataset-hive-generator.py.nix -i python                                                                                                                      

import sys
import time
from pyhive import hive
from TCLIService.ttypes import TOperationState
import traceback
import simplejson as json

HIVESTORE='hive.smartnews.internal'

AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent_v4'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'

def hive_query(query, cursor):
    """
    Execute the query to the HiveStore.
    """
    cursor.execute(query, async_=True)
    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            sys.stdout.write(message)
        status = cursor.poll().operationState
    results = cursor.fetchall()
    return results

def build_hive_dataset_mce(dataset_name, actor, created_time, schema, last_time, location, table_type, description = ""):
    """
    Create the MetadataChangeEvent via dataset_name and schema.
    """
    # https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/DatasetAspect.pdl
    # actor, type, created_time, upstreams_dataset, sys_time = "urn:li:corpuser:" + metadata[2][7:], str(metadata[-1][11:-1]), int(metadata[3][12:]), metadata[-28][10:], int(time.time())
    # owners = {"owners":[{"owner":actor,"type":"DATAOWNER"}]}
    # upstreams = {"upstreams":[{"auditStamp":{"time":sys_time,"actor":actor},"dataset":"urn:li:dataset:(urn:li:dataPlatform:hive," + upstreams_dataset + ",PROD)","type":"TRANSFORMED"}]}
    elements = {"elements":[{"url":HIVESTORE,"description":description}]}
    schema_name = {
                    "schemaName":dataset_name,
                    "platform":"urn:li:dataPlatform:hive",
                    "version":0,
                    # "created":{"time":created_time,"actor":actor},
                    "hash":"",
       #             "platformSchema":{"com.linkedin.pegasus2avro.schema.OtherSchema": {"rawSchema": ""}},
                    "fields": schema
                }

    dataset_properties = {
                            "description": dataset_name,
                            "uri": None,
                            "tags": [],
                            "customProperties": {
                                "Owner": actor,
                                "CreateTime": created_time,
                                "LastAccessTime": last_time,
                                "Location": location,
                                "Table Type": table_type,
                            }                       
                        }
    global_tags = {}
    mce = {"auditHeader": None,
           "proposedSnapshot":{"com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot":
                               {"urn": "urn:li:dataset:(urn:li:dataPlatform:hive,"+ dataset_name +",PROD)"
                               ,"aspects": [
#                                   {"com.linkedin.pegasus2avro.common.Ownership": owners}
#                                 , {"com.linkedin.pegasus2avro.dataset.UpstreamLineage": upstreams}
#                                 {"com.linkedin.pegasus2avro.common.InstitutionalMemory": elements}
                                 {"com.linkedin.pegasus2avro.schema.SchemaMetadata": schema_name}
                                 , {"com.linkedin.pegasus2avro.dataset.DatasetProperties": dataset_properties}
#                                 , {"com.linkedin.pegasus2avro.common.GlobalTags": global_tags}
                                 ]}},
           "proposedDelta": None}

    return json.dumps(mce)

# init hive cursor
cursor = hive.connect(HIVESTORE, auth="NOSASL").cursor()
counter = 0
databases = hive_query('show databases', cursor)
result = []
for database in databases:
    if database[0] in ["tmp", "temp"]:
        continue
    try:
        tables = hive_query('show tables in ' + database[0], cursor)
    except:
        print(f"error in process {database[0]}")
        continue

    print(f"In processing {database[0]}")
    for table in tables:
        dataset_name = database[0] + '.' + table[0]
        # if table[0] in ["window", "conf", "date"]: continue
        try:        
            description = hive_query('describe formatted ' + dataset_name, cursor)
        except:
            print(f"this table failed to process {dataset_name}")
            continue
        #description = hive_query('describe formatted analytics.site_articles', cursor)
        # first go through all fields
        # Partition Information, then into partition fields
        # Detailed Table Information then into porperties 
        table_fields, Owner, Des, created_time, last_time, location, table_type = [],  "", "", "", "", "", ""
        mce_fields = []
        status = ""
        for d in description:
            if "#" in d[0]:
                if "col_name" in d[0]: continue
                if "Table" in d[0]:
                    status = "table"
                    continue
                if "Partition" in d[0]:
                    status = "par"
                    continue
                break

            if status == "":
                # in fleld
                name, _type, des = d
                if not name: continue
                table_fields.append(d)
                continue
            if status == "par":
                name, _type, des = d
                if not name: continue
                table_fields.append((name, f"PK {_type}", des))
                continue
            if status == "table":
                name, value, _ = d
                if not name or not value: continue
                # 'Database:           '
                name = name.strip()[:-1]
                value = value.strip()
                if name == "Owner": Owner = value
                if name == "CreateTime": created_time = value
                if name == "Location": location = value
                if name == "Table Type": table_type = value
                if name == "LastAccessTime": last_time = value
        
        for f in table_fields:
            mce_fields.append(
                {
                    "fieldPath":f[0],
                    "description":{"string":f[2]},
                    "nativeDataType":f[1],
                    "type": {
                        "type": {
                            "com.linkedin.pegasus2avro.schema.StringType": {}
                        }
                    },
                }
            )
        mce = None
        try:
            mce = build_hive_dataset_mce(dataset_name, Owner, created_time, mce_fields, last_time, location, table_type)
        except Exception as e:
            print(f"fail to process {dataset_name}")
            print(f"{Owner}, {created_time}, {last_time}, {location}, {table_type}")
            print(mce_fields)
            traceback.print_exc()

        if mce:    
            result.append(mce)

        counter = counter + 1
        print(f"processing counter {counter}")
        if counter % 100 == 0:
            content = "[" + ",".join(result) + "]"
            with open(f'results/result_{counter}.json','w') as f:
                f.write(content)
            result = []


content = "[" + ",".join(result) + "]"
with open(f'results/result_{counter}.json','w') as f:
    f.write(content)

sys.exit(0)

# datafeed.catalog_item_allow_list
