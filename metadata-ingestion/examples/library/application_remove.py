#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationsClass


def remove_application_aspect():
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")
    dataset_urn = make_dataset_urn("snowflake", "database.schema.table", "PROD")

    applications_aspect = ApplicationsClass(applications=[])

    emitter.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=applications_aspect)
    )

    print("Successfully removed application")


if __name__ == "__main__":
    remove_application_aspect()
