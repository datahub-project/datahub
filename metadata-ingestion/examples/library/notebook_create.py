# metadata-ingestion/examples/library/notebook_create.py
import os
import time
from typing import Dict, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    NotebookInfoClass,
)


def create_notebook_metadata(
    notebook_urn: str,
    title: str,
    description: str,
    external_url: str,
    custom_properties: Optional[Dict[str, str]] = None,
    actor: str = "urn:li:corpuser:data_scientist",
    timestamp_millis: Optional[int] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create metadata for a notebook entity.

    Args:
        notebook_urn: URN of the notebook
        title: Title of the notebook
        description: Description of the notebook
        external_url: URL to access the notebook
        custom_properties: Optional dictionary of custom properties
        actor: URN of the actor creating the notebook
        timestamp_millis: Optional timestamp in milliseconds (defaults to current time)

    Returns:
        MetadataChangeProposalWrapper containing the notebook metadata
    """
    timestamp_millis = timestamp_millis or int(time.time() * 1000)

    audit_stamp = AuditStampClass(time=timestamp_millis, actor=actor)

    notebook_info = NotebookInfoClass(
        title=title,
        description=description,
        externalUrl=external_url,
        customProperties=custom_properties or {},
        changeAuditStamps=ChangeAuditStampsClass(
            created=audit_stamp,
            lastModified=audit_stamp,
        ),
    )

    return MetadataChangeProposalWrapper(
        entityUrn=notebook_urn,
        aspect=notebook_info,
    )


def main() -> None:
    """
    Main function to create a notebook example.

    Environment Variables:
        DATAHUB_GMS_URL: DataHub GMS server URL (default: http://localhost:8080)
        DATAHUB_GMS_TOKEN: DataHub access token (if authentication is required)
    """
    gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    token = os.getenv("DATAHUB_GMS_TOKEN")
    emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

    notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

    event = create_notebook_metadata(
        notebook_urn=notebook_urn,
        title="Customer Segmentation Analysis 2024",
        description="Comprehensive analysis of customer segments including RFM analysis, cohort analysis, and predictive scoring for marketing campaigns",
        external_url="https://querybook.company.com/notebook/customer_analysis_2024",
        custom_properties={
            "workspace": "analytics",
            "team": "growth",
            "last_run": "2024-01-15T10:30:00Z",
        },
    )

    emitter.emit(event)
    print(f"Created notebook {notebook_urn}")


if __name__ == "__main__":
    main()
