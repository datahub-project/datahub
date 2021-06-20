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
    actor = make_user_urn(actor)
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




def get_aspect_if_available(
    mce: MetadataChangeEventClass, type: Type[T]
) -> Optional[T]:
    all_aspects = mce.proposedSnapshot.aspects
    aspects: List[T] = [aspect for aspect in all_aspects if isinstance(aspect, type)]

    if len(aspects) > 1:
        raise ValueError(f"MCE contains multiple aspects of type {type}: {aspects}")
    if aspects:
        return aspects[0]
    return None


def get_or_add_aspect(mce: MetadataChangeEventClass, default: T) -> T:
    existing = get_aspect_if_available(mce, type(default))
    if existing is not None:
        return existing
    mce.proposedSnapshot.aspects.append(default)
    return default


def make_dataset_description_mce(
    dataset_name: str,
    description: str,
    externalUrl: str = None, 
    tags: List[str] = []
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
                    tags = [make_tag_urn(tag) for tag in tags]
                )
            ],
        )
    )
def make_schema_mce(
    datset_urn: str,
    platformName:str,
    actor : str,
    fields: List[Dict[str, str]],
    primaryKeys : List[str] = None,  
    foreignKeysSpecs: List[str] = None,  
) -> MetadataChangeEventClass:
    sys_time = get_sys_time()
    actor = make_user_urn(actor)
    for item in fields:
        item["type"] = {"bool":  BooleanTypeClass(), 
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
                        }.get(item["type"])           
        
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
        urn=datset_urn,
        aspects=[
            SchemaMetadataClass(
                schemaName = "OtherSchema",
                platform = make_platform(platformName),
                version = 0,
                created = AuditStampClass(
                                time=sys_time,
                                actor=actor,),
                lastModified = AuditStampClass(
                                time=sys_time,
                                actor=actor,),
                hash ="",
                platformSchema = OtherSchemaClass(rawSchema=""),                
                fields = [SchemaFieldClass(fieldPath=item["fieldPath"], 
                                            type=SchemaFieldDataTypeClass(type=item["type"]), 
                                            nativeDataType=item.get("nativeType",""),
                                            description=item.get("description",""),
                                            nullable=item.get('nullable', None)) for item in fields],
                primaryKeys = primaryKeys,
                foreignKeysSpecs = None,                 
            )
        ],    
    ))
    return mce

def generate_json_output(mce: MetadataChangeEventClass, file_loc:str)->None:
    path = Path(file_loc)
    work_unit = MetadataWorkUnit(f"myfile:0", mce)
    envelope = RecordEnvelope(work_unit.mce, {"workunit_id": work_unit.id,})
    record = envelope.record
    with open(path, 'w') as f:
        json.dump(record.to_obj, f, indent=4)

def make_ownership_mce(
    owner: str,
    dataset_urn:str
    ) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[
                OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=make_user_urn(owner),
                            type=OwnershipTypeClass.DATAOWNER,
                        )                        
                    ],
                    lastModified=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor=make_user_urn(owner),
                    ),
                )
            ],
        )
    )