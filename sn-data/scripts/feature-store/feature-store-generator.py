#! /usr/bin/env nix-shell                                                                                                                                           
#! nix-shell dataset-hive-generator.py.nix -i python                                                                                                                      

import sys
import time
import traceback
import simplejson as json

SCHEMAREGISTRY = 'http://localhost:8081'

def build_hive_dataset_mce(dataset_name, description, user_id_type, update_pattern, schema, start_date, retention_days, source_team):
    """
    Create the MetadataChangeEvent via dataset_name and schema.
    """
    # https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/DatasetAspect.pdl
    # actor, type, created_time, upstreams_dataset, sys_time = "urn:li:corpuser:" + metadata[2][7:], str(metadata[-1][11:-1]), int(metadata[3][12:]), metadata[-28][10:], int(time.time())
    # owners = {"owners":[{"owner":actor,"type":"DATAOWNER"}]}
    # upstreams = {"upstreams":[{"auditStamp":{"time":sys_time,"actor":actor},"dataset":"urn:li:dataset:(urn:li:dataPlatform:hive," + upstreams_dataset + ",PROD)","type":"TRANSFORMED"}]}
    # elements = {"elements":[{"url":HIVESTORE,"description":description}]}
    schema_name = {
                    "schemaName":dataset_name,
                    "platform":"urn:li:dataPlatform:feature_store",
                    "version":0,
                    # "created":{"time":created_time,"actor":actor},
                    "hash":"",
       #             "platformSchema":{"com.linkedin.pegasus2avro.schema.OtherSchema": {"rawSchema": ""}},
                    "fields": schema
                }

    dataset_properties = {
                            "description": description,
                            "uri": None,
                            "tags": [],
                            "customProperties": {
                                "User Id Type": user_id_type,
                                "Update Pattern": update_pattern,
                                "Start Date": start_date,
                                "Retention Days": retention_days,
                                "Source Team": source_team,
                            }                       
                        }
    global_tags = {}
    mce = {"auditHeader": None,
           "proposedSnapshot":{"com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot":
                               {"urn": "urn:li:dataset:(urn:li:dataPlatform:feature_store,"+ dataset_name +",PROD)"
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

group_name = ["uuid_batch", "uuid_realtime", "uuid_history", "uuid_ad_uuid_daily_features_d","device_token_location_gps","device_token_location_manual_selection"]
result = []
for group in group_name:
    schema, _property = None, None
    # read from file
    with open("/Users/dingli/code/datahub/contrib/metadata-ingestion/haskell/bin/fs-data") as f:
        schema = f.readlines()
    with open("/Users/dingli/code/datahub/contrib/metadata-ingestion/haskell/bin/fs-schema") as f:
        _property = f.readlines()

    schema_list = []
    for sch in schema:
        n, v, t = sch.split(",")
        if n == group:
            schema_list.append(                {
                    "fieldPath":v,
                    "description":{"string":""},
                    "nativeDataType":t,
                    "type": {
                        "type": {
                            "com.linkedin.pegasus2avro.schema.StringType": {}
                        }
                    },
                }
            )
    _group_name, description, user_id_type, update_pattern, start_date, retention_days, source_team = None, None, None, None, None, None, None
    for p in _property:
        _group_name, description, user_id_type, update_pattern, start_date, retention_days, source_team = p.split(",")
        if _group_name == group:
            break
    # generate mce
    mce = build_hive_dataset_mce(_group_name, description, user_id_type, update_pattern, schema_list, start_date, retention_days, source_team)
    result.append(mce)


content = "[" + ",".join(result) + "]"
with open(f'/Users/dingli/code/datahub/fs_result.json','w') as f:
    f.write(content)