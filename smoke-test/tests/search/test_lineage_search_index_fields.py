import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

import pytest
import tenacity

from tests.utils import delete_urns_from_file, ingest_file_via_rest

logger = logging.getLogger(__name__)

# Test constants
UPSTREAM_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,upstream_table,PROD)"
DOWNSTREAM_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hive,downstream_table,PROD)"
)
DATASET_WITHOUT_LINEAGE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hive,no_lineage_table,PROD)"
)


def create_upstream_dataset_mcp_data() -> Dict[str, Any]:
    """Create the MCP data for the upstream dataset."""
    return {
        "entityType": "dataset",
        "entityUrn": UPSTREAM_DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "datasetProperties",
        "aspect": {
            "json": {
                "name": "upstream_table",
                "description": "Upstream dataset for lineage testing",
                "customProperties": {"platform": "hive", "env": "PROD"},
            }
        },
    }


def create_downstream_dataset_with_lineage_mcp_data() -> Dict[str, Any]:
    """Create the MCP data for the downstream dataset with upstream lineage."""
    return {
        "entityType": "dataset",
        "entityUrn": DOWNSTREAM_DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "datasetProperties",
        "aspect": {
            "json": {
                "name": "downstream_table",
                "description": "Downstream dataset with lineage",
                "customProperties": {"platform": "hive", "env": "PROD"},
            }
        },
    }


def create_upstream_lineage_mcp_data() -> Dict[str, Any]:
    """Create the MCP data for upstream lineage aspect."""
    return {
        "entityType": "dataset",
        "entityUrn": DOWNSTREAM_DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "upstreamLineage",
        "aspect": {
            "json": {
                "upstreams": [
                    {
                        "dataset": UPSTREAM_DATASET_URN,
                        "type": "TRANSFORMED",
                        "auditStamp": {
                            "time": 1640995200000,
                            "actor": "urn:li:corpuser:datahub",
                        },
                    }
                ]
            }
        },
    }


def create_dataset_without_lineage_mcp_data() -> Dict[str, Any]:
    """Create the MCP data for a dataset without lineage."""
    return {
        "entityType": "dataset",
        "entityUrn": DATASET_WITHOUT_LINEAGE_URN,
        "changeType": "UPSERT",
        "aspectName": "datasetProperties",
        "aspect": {
            "json": {
                "name": "no_lineage_table",
                "description": "Dataset without lineage",
                "customProperties": {"platform": "hive", "env": "PROD"},
            }
        },
    }


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=10),
    retry=tenacity.retry_if_exception_type(AssertionError),
    reraise=True,
)
def verify_search_index_fields_via_openapi(
    auth_session,
    dataset_urn: str,
    expected_has_upstreams: bool,
    expected_has_fine_grained_upstreams: bool,
    expected_fine_grained_upstreams: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Verify search index fields using the OpenAPI endpoint."""
    logger.info(
        f"Checking search index fields for {dataset_urn} via OpenAPI endpoint..."
    )

    try:
        # Use the OpenAPI endpoint to get raw Elasticsearch document
        openapi_url = (
            f"{auth_session.gms_url()}/openapi/operations/elasticSearch/entity/raw"
        )
        response = auth_session.post(
            openapi_url,
            json=[dataset_urn],
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

        raw_documents = response.json()
        logger.info(f"Raw documents response: {raw_documents}")

        if dataset_urn not in raw_documents:
            raise AssertionError(f"Dataset {dataset_urn} not found in raw documents")

        document = raw_documents[dataset_urn]
        logger.info(f"Raw document for {dataset_urn}: {document}")

        # Check hasUpstreams field
        has_upstreams = document.get("hasUpstreams", False)
        logger.info(f"hasUpstreams field: {has_upstreams}")
        assert has_upstreams == expected_has_upstreams, (
            f"Expected hasUpstreams={expected_has_upstreams}, got {has_upstreams}"
        )

        # Check hasFineGrainedUpstreams field
        has_fine_grained_upstreams = document.get("hasFineGrainedUpstreams", False)
        logger.info(f"hasFineGrainedUpstreams field: {has_fine_grained_upstreams}")
        assert has_fine_grained_upstreams == expected_has_fine_grained_upstreams, (
            f"Expected hasFineGrainedUpstreams={expected_has_fine_grained_upstreams}, got {has_fine_grained_upstreams}"
        )

        # Check fineGrainedUpstreams field if expected
        if expected_fine_grained_upstreams is not None:
            fine_grained_upstreams = document.get("fineGrainedUpstreams", [])
            logger.info(f"fineGrainedUpstreams field: {fine_grained_upstreams}")
            assert set(fine_grained_upstreams) == set(
                expected_fine_grained_upstreams
            ), (
                f"Expected fineGrainedUpstreams={expected_fine_grained_upstreams}, got {fine_grained_upstreams}"
            )

        logger.info(f"Search index field verification successful for {dataset_urn}")
        return {
            "urn": dataset_urn,
            "hasUpstreams": has_upstreams,
            "hasFineGrainedUpstreams": has_fine_grained_upstreams,
            "fineGrainedUpstreams": document.get("fineGrainedUpstreams", []),
        }

    except Exception as e:
        logger.error(f"Could not verify search index fields via OpenAPI: {e}")
        raise


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Fixture to ingest test data and clean up after tests."""
    # Create temporary file for MCP data
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        mcp_data = [
            create_upstream_dataset_mcp_data(),
            create_downstream_dataset_with_lineage_mcp_data(),
            create_upstream_lineage_mcp_data(),
            create_dataset_without_lineage_mcp_data(),
        ]
        json.dump(mcp_data, f, indent=2)
        temp_file_path = f.name

    try:
        logger.info("Ingesting lineage test data")
        ingest_file_via_rest(auth_session, temp_file_path)
        yield
        logger.info("Removing lineage test data")
        delete_urns_from_file(graph_client, temp_file_path)
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def test_lineage_search_index_fields_with_lineage(auth_session):
    """
    Test that verifies search index fields are correctly populated for a dataset with lineage.
    """
    # Verify that the downstream dataset has the correct search index fields
    verify_search_index_fields_via_openapi(
        auth_session,
        dataset_urn=DOWNSTREAM_DATASET_URN,
        expected_has_upstreams=True,
        expected_has_fine_grained_upstreams=False,
        expected_fine_grained_upstreams=[],  # No fine-grained lineage in this test
    )


def test_lineage_search_index_fields_without_lineage(auth_session):
    """
    Test that verifies search index fields are correctly populated for a dataset without lineage.
    """
    # Verify that the dataset without lineage has the correct search index fields
    verify_search_index_fields_via_openapi(
        auth_session,
        dataset_urn=DATASET_WITHOUT_LINEAGE_URN,
        expected_has_upstreams=False,
        expected_has_fine_grained_upstreams=False,
        expected_fine_grained_upstreams=[],
    )


def test_upstream_dataset_search_index_fields(auth_session):
    """
    Test that verifies search index fields for the upstream dataset (should not have upstreams).
    """
    # Verify that the upstream dataset has the correct search index fields
    verify_search_index_fields_via_openapi(
        auth_session,
        dataset_urn=UPSTREAM_DATASET_URN,
        expected_has_upstreams=False,
        expected_has_fine_grained_upstreams=False,
        expected_fine_grained_upstreams=[],
    )
