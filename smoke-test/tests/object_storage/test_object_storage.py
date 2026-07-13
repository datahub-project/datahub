"""
Smoke tests for the unified object storage client (replaces legacy datahub.s3 / S3Util).

Quickstart uses file:// local storage, which does not support presigned URLs. These tests
verify real runtime behavior against a live GMS instance.
"""

import logging
import uuid
from typing import Any, Generator

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass
from tests.utils import execute_graphql, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

_OBJECT_STORAGE_URI_KEY = "datahub.objectStorage.uri"

_APP_CONFIG_QUERY = """
query {
  appConfig {
    featureFlags {
      documentationFileUploadV1
    }
  }
}
"""

_GET_PRESIGNED_UPLOAD_URL_QUERY = """
query getPresignedUploadUrl($input: GetPresignedUploadUrlInput!) {
  getPresignedUploadUrl(input: $input) {
    url
    fileId
  }
}
"""


def _fetch_simple_properties(auth_session: Any) -> dict[str, str]:
    response = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v1/system-info/properties/simple"
    )
    response.raise_for_status()
    properties = response.json()
    assert isinstance(properties, dict)
    return properties


def _object_storage_uri(auth_session: Any) -> str:
    return _fetch_simple_properties(auth_session).get(_OBJECT_STORAGE_URI_KEY, "")


@pytest.fixture(scope="module")
def documentation_dataset_urn(graph_client) -> Generator[str, None, None]:
    """Dataset used as the assetUrn for documentation upload authorization checks."""
    urn = make_dataset_urn(
        platform="snowflake",
        name=f"object_storage_smoke.{uuid.uuid4()}",
        env="PROD",
    )
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(name="object-storage-smoke-test"),
        )
    )
    wait_for_writes_to_sync()
    yield urn
    graph_client.hard_delete_entity(urn)


def test_object_storage_config_exposed_in_system_info(auth_session: Any) -> None:
    uri = _object_storage_uri(auth_session)
    assert uri, f"Expected {_OBJECT_STORAGE_URI_KEY} in system-info simple properties"
    assert uri.startswith("file://"), (
        f"Expected local file storage in quickstart, got: {uri}"
    )
    logger.info("Object storage URI: %s", uri)


def test_documentation_file_upload_gated_without_presigned_urls(
    auth_session: Any,
) -> None:
    res = execute_graphql(auth_session, _APP_CONFIG_QUERY)
    enabled = res["data"]["appConfig"]["featureFlags"]["documentationFileUploadV1"]
    assert enabled is False


def test_get_presigned_upload_url_rejects_local_provider(
    auth_session: Any, documentation_dataset_urn: str
) -> None:
    variables = {
        "input": {
            "fileName": "smoke-test.txt",
            "scenario": "ASSET_DOCUMENTATION",
            "assetUrn": documentation_dataset_urn,
            "contentType": "text/plain",
        }
    }
    res = execute_graphql(
        auth_session, _GET_PRESIGNED_UPLOAD_URL_QUERY, variables, expect_errors=True
    )

    # Non-nullable return type nulls entire data when the resolver throws.
    assert res.get("data") is None
    errors = res.get("errors")
    assert errors, "Expected GraphQL errors for unsupported presigned upload provider"
    error_text = str(errors).lower()
    assert "presigned" in error_text or "not supported" in error_text, (
        f"Unexpected GraphQL error: {errors}"
    )
