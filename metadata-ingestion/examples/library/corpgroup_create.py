# metadata-ingestion/examples/library/corpgroup_create.py
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    OriginClass,
    OriginTypeClass,
    StatusClass,
)
from datahub.utilities.urns.corp_group_urn import CorpGroupUrn

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

group_urn = CorpGroupUrn("data-engineering")

group_info = CorpGroupInfoClass(
    displayName="Data Engineering",
    description="The data engineering team builds and maintains data pipelines and infrastructure",
    email="data-eng@example.com",
    slack="data-engineering",
    admins=[],
    members=[],
    groups=[],
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=str(group_urn),
    aspect=group_info,
)
emitter.emit(metadata_event)

status_aspect = StatusClass(removed=False)
metadata_event = MetadataChangeProposalWrapper(
    entityUrn=str(group_urn),
    aspect=status_aspect,
)
emitter.emit(metadata_event)

origin_aspect = OriginClass(type=OriginTypeClass.NATIVE)
metadata_event = MetadataChangeProposalWrapper(
    entityUrn=str(group_urn),
    aspect=origin_aspect,
)
emitter.emit(metadata_event)

print(f"Created group: {group_urn}")
