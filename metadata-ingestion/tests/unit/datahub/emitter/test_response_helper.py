import json
from typing import Any
from unittest.mock import Mock

from requests import Response

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.response_helper import (
    extract_trace_data,
    extract_trace_data_from_mcps,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal


def create_response(status_code: int, headers: dict, json_data: Any) -> Response:
    """Helper function to create a mock response with specific data"""
    response = Mock(spec=Response)
    response.status_code = status_code
    response.headers = headers
    response.json = Mock(return_value=json_data)
    return response


def create_mcp(entity_urn: str, aspect_name: str) -> Mock:
    """Helper function to create a mock MCP"""
    mcp = Mock(spec=MetadataChangeProposal)
    mcp.entityUrn = entity_urn
    mcp.aspectName = aspect_name
    return mcp


def test_invalid_status_code():
    """Test that non-200 status codes return None"""
    response = create_response(
        status_code=404, headers={"traceparent": "test-trace-id"}, json_data=[]
    )
    result = extract_trace_data(response)
    assert result is None


def test_missing_trace_header():
    """Test that missing trace header returns None"""
    response = create_response(status_code=200, headers={}, json_data=[])
    result = extract_trace_data(response)
    assert result is None


def test_invalid_json_format():
    """Test that non-list JSON returns None"""
    response = create_response(
        status_code=200,
        headers={"traceparent": "test-trace-id"},
        json_data={"key": "value"},  # Dictionary instead of list
    )
    result = extract_trace_data(response)
    assert result is None


def test_json_decode_error():
    """Test handling of JSON decode error"""
    response = Mock(spec=Response)
    response.status_code = 200
    response.headers = {"traceparent": "test-trace-id"}
    response.json.side_effect = json.JSONDecodeError("Test error", "", 0)

    result = extract_trace_data(response)
    assert result is None


def test_successful_extraction_all_aspects():
    """Test successful extraction with no specific aspects"""
    test_data = [
        {
            "urn": "test:1",
            "datasetProperties": {"name": "foobar"},
            "status": {"removed": False},
        },
        {"urn": "test:2", "datasetProperties": {"name": "foobar"}, "status": None},
    ]

    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=test_data
    )

    result = extract_trace_data(response)

    assert result is not None
    assert result.trace_id == "test-trace-id"
    assert "test:1" in result.data
    assert len(result.data["test:1"]) == 2  # All fields except 'urn' and None values
    assert "datasetProperties" in result.data["test:1"]
    assert "status" in result.data["test:1"]
    assert "test:2" in result.data
    assert len(result.data["test:2"]) == 1  # Excluding None value


def test_successful_extraction_specific_aspects():
    """Test successful extraction with specific aspects"""
    test_data = [
        {
            "urn": "test:1",
            "datasetProperties": {"name": "foobar"},
            "status": {"removed": False},
        }
    ]

    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=test_data
    )

    result = extract_trace_data(response, aspects_to_trace=["notpresent", "status"])

    assert result is not None
    assert result.trace_id == "test-trace-id"
    assert "test:1" in result.data
    assert len(result.data["test:1"]) == 1
    assert "status" in result.data["test:1"]


def test_missing_urn():
    """Test handling of items without URN"""
    test_data = [
        {"datasetProperties": {"name": "foobar"}, "status": {"removed": False}},
        {
            "urn": "test:2",
            "datasetProperties": {"name": "foobar"},
            "status": {"removed": False},
        },
    ]

    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=test_data
    )

    result = extract_trace_data(response)
    assert result is not None
    assert len(result.data) == 1  # Only one item should be processed
    assert "test:2" in result.data


def test_custom_trace_header():
    """Test using a custom trace header"""
    response = create_response(
        status_code=200,
        headers={"custom-trace": "test-trace-id"},
        json_data=[{"urn": "test:1", "status": {"removed": False}}],
    )

    result = extract_trace_data(response, trace_header="custom-trace")

    assert result is not None
    assert result.trace_id == "test-trace-id"
    assert "test:1" in result.data
    assert len(result.data["test:1"]) == 1


def test_mcps_invalid_status_code():
    """Test that non-200 status codes return None for MCPs"""
    response = create_response(
        status_code=404, headers={"traceparent": "test-trace-id"}, json_data=[]
    )
    mcps = [create_mcp("urn:test:1", "testAspect")]
    result = extract_trace_data_from_mcps(response, mcps)
    assert result is None


def test_mcps_missing_trace_header():
    """Test that missing trace header returns None for MCPs"""
    response = create_response(status_code=200, headers={}, json_data=[])
    mcps = [create_mcp("urn:test:1", "testAspect")]
    result = extract_trace_data_from_mcps(response, mcps)
    assert result is None


def test_successful_mcp_extraction():
    """Test successful extraction from MCPs"""
    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=[]
    )

    mcps = [
        create_mcp("urn:test:1", "datasetProperties"),
        create_mcp("urn:test:1", "status"),
        create_mcp("urn:test:2", "datasetProperties"),
    ]

    result = extract_trace_data_from_mcps(response, mcps)

    assert result is not None
    assert result.trace_id == "test-trace-id"
    assert "urn:test:1" in result.data
    assert len(result.data["urn:test:1"]) == 2
    assert "datasetProperties" in result.data["urn:test:1"]
    assert "status" in result.data["urn:test:1"]
    assert "urn:test:2" in result.data
    assert len(result.data["urn:test:2"]) == 1


def test_mcps_with_aspect_filter():
    """Test MCP extraction with specific aspects filter"""
    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=[]
    )

    mcps = [
        create_mcp("urn:test:1", "datasetProperties"),
        create_mcp("urn:test:1", "status"),
        create_mcp("urn:test:2", "datasetProperties"),
    ]

    result = extract_trace_data_from_mcps(response, mcps, aspects_to_trace=["status"])

    assert result is not None
    assert "urn:test:1" in result.data
    assert len(result.data["urn:test:1"]) == 1
    assert "status" in result.data["urn:test:1"]
    assert "urn:test:2" not in result.data


def test_mcps_missing_attributes():
    """Test handling of MCPs with missing attributes"""
    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=[]
    )

    # Create malformed MCPs
    bad_mcp1 = Mock(spec=MetadataChangeProposal)
    delattr(bad_mcp1, "entityUrn")  # Explicitly remove the attribute
    delattr(bad_mcp1, "aspectName")

    bad_mcp2 = Mock(spec=MetadataChangeProposal)
    bad_mcp2.entityUrn = "urn:test:1"
    delattr(bad_mcp2, "aspectName")  # Only remove aspectName

    good_mcp = create_mcp("urn:test:2", "status")

    mcps = [bad_mcp1, bad_mcp2, good_mcp]

    result = extract_trace_data_from_mcps(response, mcps)

    assert result is not None
    assert len(result.data) == 1  # Only the good MCP should be included
    assert "urn:test:2" in result.data
    assert result.data["urn:test:2"] == ["status"]


def test_mcps_with_wrapper():
    """Test handling of MetadataChangeProposalWrapper objects"""
    response = create_response(
        status_code=200, headers={"traceparent": "test-trace-id"}, json_data=[]
    )

    # Create a wrapped MCP
    mcp = create_mcp("urn:test:1", "testAspect")
    wrapper = Mock(spec=MetadataChangeProposalWrapper)
    wrapper.entityUrn = mcp.entityUrn
    wrapper.aspectName = mcp.aspectName

    result = extract_trace_data_from_mcps(response, [wrapper])

    assert result is not None
    assert "urn:test:1" in result.data
    assert "testAspect" in result.data["urn:test:1"]
