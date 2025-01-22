import time
from typing import List

import pytest

from datahub.emitter.mce_builder import make_dashboard_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata._schema_classes import MetadataChangeProposalClass
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    DashboardInfoClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.restli.restli_test import MetadataChangeProposalInvalidWrapper

generated_urns: List[str] = []


@pytest.fixture(scope="module")
def ingest_cleanup_data(auth_session, graph_client, request):
    yield


def _create_valid_dashboard_mcps() -> List[MetadataChangeProposalClass]:
    mcps = []
    num_valid_mcp = 5

    audit_stamp = pre_json_transform(
        ChangeAuditStampsClass(
            created=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub",
            )
        ).to_obj()
    )

    valid_dashboard_info = DashboardInfoClass(
        title="Dummy Title For Testing",
        description="Dummy Description For Testing",
        lastModified=audit_stamp,
    )

    for i in range(num_valid_mcp):
        mcp_valid = MetadataChangeProposalWrapper(
            entityUrn=make_dashboard_urn(
                platform="looker", name=f"dummy-test-invalid-{i}"
            ),
            aspectName="dashboardInfo",
            aspect=valid_dashboard_info,
        )
        mcps.append(mcp_valid.make_mcp())

    return mcps


def _create_invalid_dashboard_mcp() -> MetadataChangeProposalClass:
    audit_stamp = pre_json_transform(
        ChangeAuditStampsClass(
            created=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub",
            )
        ).to_obj()
    )

    invalid_dashboard_info = {
        "title": "Dummy Title For Testing",
        "description": "Dummy Description For Testing",
        "lastModified": audit_stamp,
        "notValidField": "invalid field value",
    }

    mcp_invalid = MetadataChangeProposalInvalidWrapper(
        entityUrn=make_dashboard_urn(platform="looker", name="dummy-test-valid"),
        aspectName="dashboardInfo",
        aspect=invalid_dashboard_info,
    )
    return mcp_invalid.make_mcp()


def test_restli_batch_ingestion_sync(graph_client):
    # Positive Test (all valid MetadataChangeProposal)
    mcps = _create_valid_dashboard_mcps()
    ret = graph_client.emit_mcps(mcps, async_flag=False)
    assert ret >= 0

    # Negative Test (contains invalid MetadataChangeProposal)
    invalid_mcp = _create_invalid_dashboard_mcp()
    mcps.append(invalid_mcp)
    ret = graph_client.emit_mcps(mcps, async_flag=False)
    assert ret >= 0

    # Expected that invalid field of MetadataChangeProposal is ignored,
    # Rest Fields are persistd into DB
    aspect = graph_client.get_aspect(
        entity_urn=invalid_mcp.entityUrn, aspect_type=DashboardInfoClass
    )

    assert aspect is not None
    assert isinstance(aspect, DashboardInfoClass)
    assert aspect.title == "Dummy Title For Testing"
    assert aspect.description == "Dummy Description For Testing"
    assert aspect.lastModified is not None


def test_restli_batch_ingestion_async(graph_client):
    # Positive Test (all valid MetadataChangeProposal)
    mcps = _create_valid_dashboard_mcps()
    ret = graph_client.emit_mcps(mcps, async_flag=True)
    assert ret >= 0

    # Negative Test (contains invalid MetadataChangeProposal)
    invalid_mcp = _create_invalid_dashboard_mcp()
    mcps.append(invalid_mcp)
    ret = graph_client.emit_mcps(mcps, async_flag=True)
    assert ret >= 0

    # Expected that invalid field of MetadataChangeProposal is ignored,
    # Rest Fields are persistd into DB
    wait_for_writes_to_sync()
    aspect = graph_client.get_aspect(
        entity_urn=invalid_mcp.entityUrn, aspect_type=DashboardInfoClass
    )

    assert aspect is not None
    assert isinstance(aspect, DashboardInfoClass)
    assert aspect.title == "Dummy Title For Testing"
    assert aspect.description == "Dummy Description For Testing"
    assert aspect.lastModified is not None
