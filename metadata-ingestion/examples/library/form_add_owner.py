# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpUserUrn, FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Form URN to add owner to
form_urn = FormUrn("metadata_initiative_2024")

# Create ownership aspect
ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=str(CorpUserUrn("governance_team")),
            type=OwnershipTypeClass.TECHNICAL_OWNER,
        )
    ],
    lastModified=AuditStampClass(
        time=0, actor="urn:li:corpuser:datahub", impersonator=None
    ),
)

# Create and emit metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=ownership,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added owner to form {form_urn}")
