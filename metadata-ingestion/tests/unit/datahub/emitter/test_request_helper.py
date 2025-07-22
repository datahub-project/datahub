import json

import pytest

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.request_helper import (
    OpenApiRequest,
)
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChangeTypeClass,
    ChartInfoClass,
    GenericAspectClass,
    SystemMetadataClass,
)
from datahub.specific.chart import ChartPatchBuilder

GMS_SERVER = "http://localhost:8080"
CHART_INFO = ChartInfoClass(
    title="Test Chart",
    description="Test Description",
    lastModified=ChangeAuditStampsClass(
        created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub")
    ),
)


def test_from_mcp_none_no_aspect():
    """Test that from_mcp returns None when aspect is missing"""
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspectName="chartInfo",
        changeType=ChangeTypeClass.UPSERT,
    )

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER)
    assert request is None


def test_from_mcp_upsert():
    """Test creating an OpenApiRequest from an UPSERT MCP"""
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspect=CHART_INFO,
    )

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert request is not None
    assert request.method == "post"
    assert request.url == f"{GMS_SERVER}/openapi/v3/entity/chart?async=false"
    assert len(request.payload) == 1
    assert request.payload[0]["urn"] == "urn:li:chart:(test,test)"
    assert "chartInfo" in request.payload[0]
    assert request.payload[0]["chartInfo"]["value"]["title"] == "Test Chart"
    assert request.payload[0]["chartInfo"]["value"]["description"] == "Test Description"
    assert request.payload[0]["chartInfo"]["systemMetadata"] is None


def test_from_mcp_upsert_with_system_metadata():
    """Test creating an OpenApiRequest from an UPSERT MCP with system metadata"""
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspect=CHART_INFO,
        systemMetadata=SystemMetadataClass(runId="test-run-id"),
    )

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert request is not None
    assert request.method == "post"
    assert request.url == f"{GMS_SERVER}/openapi/v3/entity/chart?async=false"
    assert len(request.payload) == 1
    assert "chartInfo" in request.payload[0]
    assert request.payload[0]["chartInfo"]["systemMetadata"]["runId"] == "test-run-id"


def test_from_mcp_upsert_without_wrapper():
    """Test creating an OpenApiRequest from an UPSERT MCP without wrapper"""
    mcp_wrapper = MetadataChangeProposal(
        entityType="chart",
        entityUrn="urn:li:chart:(test,test)",
        aspectName="chartInfo",
        aspect=GenericAspectClass(
            value=json.dumps(pre_json_transform(CHART_INFO.to_obj())).encode(),
            contentType=JSON_CONTENT_TYPE,
        ),
        changeType=ChangeTypeClass.UPSERT,
    )

    request = OpenApiRequest.from_mcp(mcp_wrapper, GMS_SERVER)

    assert request is not None
    assert request.method == "post"
    assert request.url == f"{GMS_SERVER}/openapi/v3/entity/chart?async=false"
    assert len(request.payload) == 1
    assert request.payload[0]["urn"] == "urn:li:chart:(test,test)"
    assert "chartInfo" in request.payload[0]
    assert request.payload[0]["chartInfo"]["value"]["title"] == "Test Chart"
    assert request.payload[0]["chartInfo"]["value"]["description"] == "Test Description"


def test_from_mcp_delete():
    """Test creating an OpenApiRequest from a DELETE MCP"""
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspectName="chartInfo",
        changeType=ChangeTypeClass.DELETE,
        aspect=None,
    )

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert request is not None
    assert request.method == "delete"
    assert (
        request.url == f"{GMS_SERVER}/openapi/v3/entity/chart/urn:li:chart:(test,test)"
    )
    assert len(request.payload) == 0


def test_from_mcp_patch():
    """Test creating an OpenApiRequest from a PATCH MCP"""
    patch_data = [{"op": "add", "path": "/title", "value": "Updated Title"}]
    mcp = next(
        iter(
            ChartPatchBuilder("urn:li:chart:(test,test)")
            .set_title("Updated Title")
            .build()
        )
    )

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert request is not None
    assert request.method == "patch"
    assert request.url == f"{GMS_SERVER}/openapi/v3/entity/chart?async=false"
    assert len(request.payload) == 1
    assert request.payload[0]["urn"] == "urn:li:chart:(test,test)"
    assert "chartInfo" in request.payload[0]
    assert request.payload[0]["chartInfo"]["value"]["patch"] == patch_data


def test_patch_unsupported_operation():
    """Test that PATCH with non-JSON_PATCH_CONTENT_TYPE raises NotImplementedError"""
    mcp = next(
        iter(
            ChartPatchBuilder("urn:li:chart:(test,test)")
            .set_title("Updated Title")
            .build()
        )
    )
    if mcp.aspect:
        mcp.aspect.contentType = "application/json"  # Not JSON_PATCH_CONTENT_TYPE

    with pytest.raises(NotImplementedError) as excinfo:
        OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert "only supports context type application/json-patch+json" in str(
        excinfo.value
    )


def test_upsert_incompatible_content_type():
    """Test that UPSERT with JSON_PATCH_CONTENT_TYPE raises NotImplementedError"""
    mcp = next(
        iter(
            ChartPatchBuilder("urn:li:chart:(test,test)")
            .set_title("Updated Title")
            .build()
        )
    )
    mcp.changeType = ChangeTypeClass.UPSERT

    with pytest.raises(NotImplementedError) as excinfo:
        OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert "does not support patch" in str(excinfo.value)


def test_from_mcp_async_flag():
    """Test creating an OpenApiRequest with async/sync"""
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspect=CHART_INFO,
    )

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER, async_flag=True)

    assert request is not None
    assert "async=true" in request.url

    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER, async_flag=False)

    assert request is not None
    assert "async=false" in request.url


def test_from_mcp_search_sync_flag():
    """Test that search_sync_flag adds the appropriate header to the request only when async_flag is False"""
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspect=CHART_INFO,
    )

    # Test with default values (async_flag=False, search_sync_flag=False)
    request = OpenApiRequest.from_mcp(mcp, GMS_SERVER)

    assert request is not None
    assert "async=false" in request.url
    assert request.payload[0]["chartInfo"]["headers"] == {}

    # Test with search_sync_flag=True and async_flag=False - should add header
    request = OpenApiRequest.from_mcp(
        mcp, GMS_SERVER, async_flag=False, search_sync_flag=True
    )

    assert request is not None
    assert "async=false" in request.url
    assert request.payload[0]["chartInfo"]["headers"] == {
        "X-DataHub-Sync-Index-Update": "true"
    }

    # Test with async_flag=True and search_sync_flag=True - should NOT add header
    request = OpenApiRequest.from_mcp(
        mcp, GMS_SERVER, async_flag=True, search_sync_flag=True
    )

    assert request is not None
    assert "async=true" in request.url
    # Header should NOT be present when async_flag is True
    assert request.payload[0]["chartInfo"]["headers"] == {}

    # Test with async_flag=True and search_sync_flag=False - should NOT add header
    request = OpenApiRequest.from_mcp(
        mcp, GMS_SERVER, async_flag=True, search_sync_flag=False
    )

    assert request is not None
    assert "async=true" in request.url
    assert request.payload[0]["chartInfo"]["headers"] == {}

    # Test with patch operation - header should be added when not async
    patch_mcp = next(
        iter(
            ChartPatchBuilder("urn:li:chart:(test,test)")
            .set_title("Updated Title")
            .build()
        )
    )

    request = OpenApiRequest.from_mcp(
        patch_mcp, GMS_SERVER, async_flag=False, search_sync_flag=True
    )

    assert request is not None
    assert request.method == "patch"
    assert "async=false" in request.url
    assert request.payload[0]["chartInfo"]["headers"] == {
        "X-DataHub-Sync-Index-Update": "true"
    }

    # Test with patch operation - header should NOT be added when async
    request = OpenApiRequest.from_mcp(
        patch_mcp, GMS_SERVER, async_flag=True, search_sync_flag=True
    )

    assert request is not None
    assert request.method == "patch"
    assert "async=true" in request.url
    assert request.payload[0]["chartInfo"]["headers"] == {}

    # Test with delete operation (headers don't apply)
    delete_mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:chart:(test,test)",
        aspectName="chartInfo",
        changeType=ChangeTypeClass.DELETE,
        aspect=None,
    )

    request = OpenApiRequest.from_mcp(
        delete_mcp, GMS_SERVER, async_flag=False, search_sync_flag=True
    )

    assert request is not None
    assert request.method == "delete"
    # For DELETE, there's no payload so no headers
    assert len(request.payload) == 0
