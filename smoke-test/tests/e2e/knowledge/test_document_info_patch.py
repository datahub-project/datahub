import logging

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DocumentInfoClass, DocumentStateClass
from datahub.sdk.document import Document
from datahub.utilities.server_config_util import ServiceFeature
from tests.e2e.knowledge.document_helpers import (
    create_unique_dataset,
    delete_unique_dataset,
    execute_graphql,
    unique_id,
)
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.domain(Domain.AI), pytest.mark.p0]

UPDATE_RELATED_ENTITIES = """
    mutation UpdateRelated($input: UpdateDocumentRelatedEntitiesInput!) {
      updateDocumentRelatedEntities(input: $input)
    }
"""


def _ingest(graph_client: DataHubGraph, document_id: str, text: str) -> None:
    """Emit a Document the way document sources do when the server supports patches."""
    doc = Document.create_external_document(
        id=document_id,
        title=f"Patched {document_id}",
        platform="urn:li:dataPlatform:notion",
        external_url=f"https://example.com/{document_id}",
        external_id=document_id,
        text=text,
        status=DocumentStateClass.PUBLISHED,
    )
    for workunit in doc.as_workunits(patch_document_info=True):
        graph_client.emit(workunit.metadata)


def _document_info(graph_client: DataHubGraph, document_urn: str) -> DocumentInfoClass:
    info = graph_client.get_aspect(document_urn, DocumentInfoClass)
    assert info is not None
    return info


def test_reingesting_a_document_keeps_user_added_related_assets(
    auth_session, graph_client: DataHubGraph
) -> None:
    assert graph_client.server_config.supports_feature(
        ServiceFeature.DOCUMENT_INFO_PATCH
    )
    document_id = unique_id("smoke-doc-patch")
    document_urn = f"urn:li:document:{document_id}"
    dataset_urn = create_unique_dataset(graph_client, "smoke-doc-patch-asset")
    try:
        # The first patch creates the Document from the server-side defaults.
        _ingest(graph_client, document_id, text="first version")
        created = _document_info(graph_client, document_urn)
        assert created.contents.text == "first version"
        assert created.status.state == DocumentStateClass.PUBLISHED

        result = execute_graphql(
            auth_session,
            UPDATE_RELATED_ENTITIES,
            {"input": {"urn": document_urn, "relatedAssets": [dataset_urn]}},
        )
        assert result["data"]["updateDocumentRelatedEntities"] is True

        _ingest(graph_client, document_id, text="second version")

        updated = _document_info(graph_client, document_urn)
        assert updated.contents.text == "second version"
        assert [asset.asset for asset in updated.relatedAssets or []] == [dataset_urn]
    finally:
        try:
            graph_client.hard_delete_entity(document_urn)
            delete_unique_dataset(graph_client, dataset_urn)
        except Exception:
            logger.warning("cleanup failed for %s", document_urn, exc_info=True)
