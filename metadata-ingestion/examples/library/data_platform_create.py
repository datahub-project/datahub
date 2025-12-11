# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/data_platform_create.py
import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
log.info(f"Created data platform {platform_urn}")
