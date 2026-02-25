#!/usr/bin/env python3

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
