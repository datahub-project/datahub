# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/application_add_assets.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationsClass


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def make_dataset_urn(platform: str, name: str, env: str) -> str:
    """Create a DataHub dataset URN."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

application_urn = make_application_urn("customer-analytics-service")

dataset_urns = [
    make_dataset_urn("snowflake", "prod.marketing.customer_events", "PROD"),
    make_dataset_urn("snowflake", "prod.marketing.customer_profiles", "PROD"),
    make_dataset_urn("kafka", "customer-events-stream", "PROD"),
]

for dataset_urn in dataset_urns:
    applications_aspect = ApplicationsClass(applications=[application_urn])

    metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=applications_aspect,
    )
    emitter.emit(metadata_event)

    print(f"Associated {dataset_urn} with application {application_urn}")

print(f"\nSuccessfully associated {len(dataset_urns)} assets with application")
