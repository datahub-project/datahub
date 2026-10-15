# metadata-ingestion/examples/library/data_platform_create.py
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
)

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

platform_urn = "urn:li:dataPlatform:customdb"

platform_info = DataPlatformInfoClass(
    name="customdb",
    displayName="Custom Database Platform",
    type="RELATIONAL_DB",
    datasetNameDelimiter=".",
    logoUrl="https://company.com/logos/customdb.png",
)

event = MetadataChangeProposalWrapper(
    entityUrn=platform_urn,
    aspect=platform_info,
)

emitter.emit(event)
print(f"Created data platform {platform_urn}")
