# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import json

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
)

dq_aspect = {
    "rules": [
        {
            "field": "my_event_data",
            "isFieldLevel": False,
            "type": "isNull",
            "checkDefinition": "n/a",
            "url": "https://github.com/datahub-project/datahub/blob/master/checks/nonNull.sql",
        },
        {
            "field": "timestamp",
            "isFieldLevel": True,
            "type": "increasing",
            "checkDefinition": "n/a",
            "url": "https://github.com/datahub-project/datahub/blob/master/checks/increasing.sql",
        },
    ]
}

emitter: DatahubRestEmitter = DatahubRestEmitter(gms_server="http://localhost:8080")

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
mcp_raw: MetadataChangeProposalClass = MetadataChangeProposalClass(
    entityUrn=dataset_urn,
    changeType=ChangeTypeClass.UPSERT,
    aspectName="customDataQualityRules",
    aspect=GenericAspectClass(
        contentType="application/json",
        value=json.dumps(dq_aspect).encode("utf-8"),
    ),
)

try:
    emitter.emit(mcp_raw)
    print("Successfully wrote to DataHub")
except Exception as e:
    print("Failed to write to DataHub")
    raise e
