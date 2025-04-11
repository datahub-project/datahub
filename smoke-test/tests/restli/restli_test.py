import dataclasses
import json
import time
from typing import List

import pytest

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mce_builder import make_dashboard_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    DashboardInfoClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
)
from datahub.utilities.urns.urn import guess_entity_type
from tests.utils import delete_urns

generated_urns: List[str] = []


@dataclasses.dataclass
class MetadataChangeProposalInvalidWrapper(MetadataChangeProposalWrapper):
    @staticmethod
    def _make_generic_aspect(dict) -> GenericAspectClass:
        serialized = json.dumps(pre_json_transform(dict))
        return GenericAspectClass(
            value=serialized.encode(),
            contentType=JSON_CONTENT_TYPE,
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __post_init__(self) -> None:
        if self.entityUrn:
            self.entityType = guess_entity_type(self.entityUrn)

    def make_mcp(self) -> MetadataChangeProposalClass:
        serializedAspect = None
        if self.aspect is not None:
            serializedAspect = (
                MetadataChangeProposalInvalidWrapper._make_generic_aspect(self.aspect)
            )

        mcp = self._make_mcp_without_aspects()
        mcp.aspect = serializedAspect
        return mcp


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    yield
    delete_urns(graph_client, generated_urns)


def test_gms_ignore_unknown_dashboard_info(graph_client):
    dashboard_urn = make_dashboard_urn(platform="looker", name="test-ignore-unknown")
    generated_urns.extend([dashboard_urn])

    audit_stamp = pre_json_transform(
        ChangeAuditStampsClass(
            created=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub",
            )
        ).to_obj()
    )

    invalid_dashboard_info = {
        "title": "Ignore Unknown Title",
        "description": "Ignore Unknown Description",
        "lastModified": audit_stamp,
        "notAValidField": "invalid field value",
    }
    mcpw = MetadataChangeProposalInvalidWrapper(
        entityUrn=dashboard_urn,
        aspectName="dashboardInfo",
        aspect=invalid_dashboard_info,
    )

    mcp = mcpw.make_mcp()
    assert "notAValidField" in str(mcp)
    assert "invalid field value" in str(mcp)

    graph_client.emit_mcp(mcpw, async_flag=False)

    dashboard_info = graph_client.get_aspect(
        entity_urn=dashboard_urn,
        aspect_type=DashboardInfoClass,
    )

    assert dashboard_info
    assert dashboard_info.title == invalid_dashboard_info["title"]
    assert dashboard_info.description == invalid_dashboard_info["description"]
