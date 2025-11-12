#!/usr/bin/env python3
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationsClass


# Utility function for creating application URNs (not yet in SDK)
def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def add_application_aspect():
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")
    dataset_urn = make_dataset_urn("snowflake", "database.schema.table", "PROD")

    application_urn = make_application_urn("my_application")
    applications_aspect = ApplicationsClass(applications=[application_urn])

    emitter.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=applications_aspect)
    )

    print(f"Successfully added application: {application_urn}")


if __name__ == "__main__":
    add_application_aspect()
