# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging
import os
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BusinessAttributeInfoClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

business_attribute_urn = "urn:li:businessAttribute:customer_id"

business_attribute_info = BusinessAttributeInfoClass(
    fieldPath="customer_id",
    name="Customer ID",
    description="Primary customer identifier field. This attribute should be applied to fields that contain the main customer identifier, typically an integer or long type.",
    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
    created=AuditStampClass(
        time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
    ),
    lastModified=AuditStampClass(
        time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
    ),
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=business_attribute_urn,
    aspect=business_attribute_info,
)

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
rest_emitter.emit(event)
log.info(f"Created business attribute {business_attribute_urn}")
