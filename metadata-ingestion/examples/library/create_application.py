#!/usr/bin/env python3

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ApplicationPropertiesClass,
    ApplicationsClass,
    DomainsClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)


# Utility function for creating application URNs (not yet in SDK)
def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def create_banking_application():
    """Complete example of creating an application and associating entities."""

    # Initialize emitter
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")

    try:
        # Create application
        application_urn = make_application_urn("banking-app")

        # 1. Application Properties
        application_properties = ApplicationPropertiesClass(
            name="Banking Application",
            description="Core banking application handling customer accounts and transactions.",
        )

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=application_urn, aspect=application_properties
            )
        )

        # 2. Ownership
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=make_user_urn("john.smith@company.com"),
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                ),
                OwnerClass(
                    owner=make_user_urn("jane.doe@company.com"),
                    type=OwnershipTypeClass.BUSINESS_OWNER,
                ),
            ]
        )
        emitter.emit(
            MetadataChangeProposalWrapper(entityUrn=application_urn, aspect=ownership)
        )

        # 3. Tags and Domain
        tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Production")])
        emitter.emit(
            MetadataChangeProposalWrapper(entityUrn=application_urn, aspect=tags)
        )

        domains = DomainsClass(domains=["urn:li:domain:finance-domain"])
        emitter.emit(
            MetadataChangeProposalWrapper(entityUrn=application_urn, aspect=domains)
        )

        # 4. Associate datasets
        datasets = ["customer_accounts", "transaction_history", "account_balances"]

        for dataset_name in datasets:
            dataset_urn = make_dataset_urn("hive", dataset_name, "PROD")
            applications_aspect = ApplicationsClass(applications=[application_urn])

            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=applications_aspect
                )
            )

        print(f"Successfully created application: {application_urn}")
        print(f"Associated {len(datasets)} datasets with the application")

    finally:
        emitter.close()


if __name__ == "__main__":
    create_banking_application()
