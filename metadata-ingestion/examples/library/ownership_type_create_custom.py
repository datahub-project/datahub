# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os
import time

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnershipTypeInfoClass,
    OwnershipTypeKeyClass,
)

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

ownership_type_id = "data_quality_lead"
ownership_type_urn = f"urn:li:ownershipType:{ownership_type_id}"

current_timestamp = int(time.time() * 1000)
actor_urn = make_user_urn("datahub")

# Emit the key aspect
ownership_type_key = OwnershipTypeKeyClass(id=ownership_type_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=ownership_type_urn,
        aspect=ownership_type_key,
    )
)

# Emit the info aspect
ownership_type_info = OwnershipTypeInfoClass(
    name="Data Quality Lead",
    description="Responsible for ensuring data quality standards and monitoring data quality metrics",
    created=AuditStampClass(time=current_timestamp, actor=actor_urn),
    lastModified=AuditStampClass(time=current_timestamp, actor=actor_urn),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=ownership_type_urn,
        aspect=ownership_type_info,
    )
)

print(f"Created custom ownership type: {ownership_type_urn}")
