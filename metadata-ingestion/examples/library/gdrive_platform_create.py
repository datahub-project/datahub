# metadata-ingestion/examples/library/gdrive_platform_create.py
import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    DataPlatformKeyClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

platform_urn = "urn:li:dataPlatform:gdrive"

# Create Google Drive platform info
platform_info = DataPlatformInfoClass(
    name="gdrive",
    displayName="Google Drive",
    type="FILE_SYSTEM",
    datasetNameDelimiter="/",
    logoUrl="https://developers.google.com/drive/images/drive_icon.png",
)

# Create platform info event
platform_info_event = MetadataChangeProposalWrapper(
    entityUrn=platform_urn,
    aspectName="dataPlatformInfo",
    aspect=platform_info,
)

# Create platform key
platform_key = DataPlatformKeyClass(platformName="gdrive")

platform_key_event = MetadataChangeProposalWrapper(
    entityUrn=platform_urn,
    aspectName="dataPlatformKey",
    aspect=platform_key,
)

# Emit both aspects
emitter.emit(platform_info_event)
emitter.emit(platform_key_event)

log.info(f"Created Google Drive platform {platform_urn}")
log.info("Platform will appear in DataHub UI for dataset filtering and browsing")
