"""Convenience functions for creating MCEs"""

from dataclasses import fields
import time
from typing import List, Optional, Type, TypeVar, Union, Dict

from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
import json
from pathlib import Path
from datahub.metadata.schema_classes import *

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"

T = TypeVar("T")

def get_sys_time() -> int:
    return int(time.time() * 1000)


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"

def make_path(platform: str, name: str, env:str = DEFAULT_FLOW_CLUSTER) -> str:
    return f"/{env}/{platform}/{name}"

def make_platform(platform:str) -> str:
    return f"urn:li:dataPlatform:{platform}"

def make_user_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def make_tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"


def make_data_flow_urn(
    orchestrator: str, flow_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
) -> str:
    return f"urn:li:dataFlow:({orchestrator},{flow_id},{cluster})"


def make_data_job_urn_with_flow(flow_urn: str, job_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def make_data_job_urn(
    orchestrator: str, flow_id: str, job_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
) -> str:
    return make_data_job_urn_with_flow(
        make_data_flow_urn(orchestrator, flow_id, cluster), job_id
    )

def make_institutionalmemory_mce(
    datset_urn: str,
    input_url: List[str],
    input_description: List[str],
    actor: str 
) -> MetadataChangeEventClass:
    """
    returns a list of Documents    
    """
    sys_time = get_sys_time()
    actor = make_user_urn(actor)
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
        urn=datset_urn,
        aspects=[
                InstitutionalMemoryClass(
                    elements=[
                        InstitutionalMemoryMetadataClass(
                            url=url,
                            description=description,
                            createStamp=AuditStampClass(
                                time=sys_time,
                                actor=actor,
                            )
                        )
                        for url, description in zip(input_url, input_description)                        
                    ]
                )                
            ],    
    ))
    return mce

def make_browsepath_mce(
    dataset_urn: str,
    path:List[str],        
) -> MetadataChangeEventClass:
    """
    Creates browsepath for dataset. By default, if not specified, Datahub assigns it to /prod/platform/datasetname    
    """
    sys_time = get_sys_time()
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
        urn=dataset_urn,
        aspects=[
                BrowsePathsClass(
                    paths = path            
                )                
            ],    
    ))
    return mce

# def make_properties_mce(
#     dataset_urn: str,
#     path:List[str],        
# ) -> MetadataChangeEventClass:
#     """
#     Creates browsepath for dataset. By default, if not specified, Datahub assigns it to /prod/platform/datasetname    
#     """
#     sys_time = get_sys_time()
#     mce = MetadataChangeEventClass(
#         proposedSnapshot=DatasetSnapshotClass(
#         urn=dataset_urn,
#         aspects=[
#                 DatasetPropertiesClass(
#                     paths = path            
#                 )                
#             ],    
#     ))
#     return mce

def make_lineage_mce(
    upstream_urns: List[str],
    downstream_urn: str,
    actor: str,
    lineage_type: str = Union[DatasetLineageTypeClass.TRANSFORMED, DatasetLineageTypeClass.COPY, DatasetLineageTypeClass.VIEW] 
) -> MetadataChangeEventClass:
    """
    Specifies Upstream Datasets relative to this dataset. Downstream is always referring to current dataset
    urns should be created using make_dataset_urn
    lineage have to be one of the 3
    """
    sys_time = get_sys_time()
    actor = actor
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=downstream_urn,
            aspects=[
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            auditStamp=AuditStampClass(
                                time=sys_time,
                                actor=actor,
                            ),
                            dataset=upstream_urn,
                            type=lineage_type,
                        )
                        for upstream_urn in upstream_urns
                    ]
                )
            ],
        )
    )
    return mce

def make_dataset_description_mce(
    dataset_name: str,
    description: str,
    externalUrl: str = None, 
    tags: List[str] = [],
    customProperties: Optional[Dict[str, str]]=None
) -> MetadataChangeEventClass:
    """
    Tags and externalUrl doesnt seem to have any impact on UI. 
    """
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name,
            aspects=[
                DatasetPropertiesClass(
                    description=description,
                    externalUrl = externalUrl,
                    customProperties = customProperties
#                    tags = [make_tag_urn(tag) for tag in tags]
                )
            ],
        )
    )
def make_schema_mce(
    dataset_urn: str,
    platformName:str,
    actor : str,
    fields: List[Dict[str, str]],
    primaryKeys : List[str] = None,  
    foreignKeysSpecs: List[str] = None,  
) -> MetadataChangeEventClass:
    sys_time = get_sys_time()
    
    for item in fields:
        item["field_type"] = {"bool":  BooleanTypeClass(), 
                        "fixed": FixedTypeClass(), 
                        "string":StringTypeClass(),
                        "byte":  BytesTypeClass(),
                        "num":   NumberTypeClass(),
                        "date":  DateTypeClass(),
                        "time":  TimeTypeClass(),
                        "enum":  EnumTypeClass(),
                        "null":  NullTypeClass(),
                        "map":   MapTypeClass(),
                        "array": ArrayTypeClass(),
                        "union": UnionTypeClass(),
                        "record":RecordTypeClass()
                        }.get(item["field_type"])           
        
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
        urn=dataset_urn,
        aspects=[
            SchemaMetadataClass(
                schemaName = "OtherSchema",
                platform = platformName,
                version = 0,
                created = AuditStampClass(
                                time=sys_time,
                                actor=actor),
                lastModified = AuditStampClass(
                                time=sys_time,
                                actor=actor),
                hash ="",
                platformSchema = OtherSchemaClass(rawSchema=""),                
                fields = [SchemaFieldClass(fieldPath=item["fieldPath"], 
                                            type=SchemaFieldDataTypeClass(type=item["field_type"]), 
                                            nativeDataType=item.get("nativeType",""),
                                            description=item.get("field_description",""),
                                            nullable=item.get('nullable', None)) for item in fields],
                primaryKeys = primaryKeys, #no visual impact in UI
                foreignKeysSpecs = None,                 
            )
        ],    
    ))
    return mce

def make_ownership_mce(
    actor: str,
    dataset_urn:str
    ) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[
                OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=actor,
                            type=OwnershipTypeClass.DATAOWNER,
                        )                        
                    ],
                    lastModified=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor=make_user_urn(actor),
                    ),
                )
            ],
        )
    )

def generate_json_output(mces: List[MetadataChangeEventClass], file_loc:str)->None:
    """
    Generates the json MCE files that can be ingested via CLI. For debugging
    """    
    path = Path(file_loc)
    mce_objs = [item.to_obj() for item in mces]
    # work_unit = MetadataWorkUnit(f"myfile:0", mce)
    # envelope = RecordEnvelope(work_unit.mce, {"workunit_id": work_unit.id,})
    # record = envelope.record
    with open(path, 'w') as f:
        json.dump(mce_objs, f, indent=4)

def delete_mce(
    dataset_name:str,
    ) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name,
            aspects=[
                StatusClass(
                    removed = True
                )
            ]
        )
    ) 

def recover_mce(
    dataset_name:str,
    ) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name,
            aspects=[
                StatusClass(
                    removed = False
                )
            ]
        )
    ) 