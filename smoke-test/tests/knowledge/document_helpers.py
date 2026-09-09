import uuid

import tenacity

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass
from tests.utils import get_sleep_info, unique_dataset_urn, wait_for_writes_to_sync

_RETRY_SLEEP, _RETRY_TIMES = get_sleep_info()


def execute_graphql(
    auth_session,
    query: str,
    variables: dict | None = None,
    no_sync_wait: bool = False,
) -> dict:
    """Execute a GraphQL query against the frontend API.

    no_sync_wait=True uses auth_session.raw_post to skip TestSessionWrapper's
    automatic wait_for_writes_to_sync() call. Use for all-but-the-last call in
    a batch of writes where only the state after the whole batch matters.
    """
    payload = {"query": query, "variables": variables or {}}
    if no_sync_wait:
        response = auth_session.raw_post(
            f"{auth_session.frontend_url()}/api/graphql", json=payload
        )
    else:
        response = auth_session.post(
            f"{auth_session.frontend_url()}/api/graphql", json=payload
        )
    response.raise_for_status()
    return response.json()


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def create_unique_dataset(graph_client, name_prefix: str) -> str:
    """Emit a run-unique dataset so relatedDocuments tests do not share SampleKafkaDataset."""
    dataset_urn = unique_dataset_urn(name_prefix)
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(name=name_prefix),
        )
    )
    wait_for_writes_to_sync()
    return dataset_urn


def delete_unique_dataset(graph_client, dataset_urn: str) -> None:
    graph_client.hard_delete_entity(dataset_urn)


class DocumentNotIndexedYet(Exception):
    """The document is not visible in the graph index yet -- worth retrying."""


@tenacity.retry(
    stop=tenacity.stop_after_attempt(_RETRY_TIMES),
    wait=tenacity.wait_fixed(_RETRY_SLEEP),
    retry=tenacity.retry_if_exception_type(DocumentNotIndexedYet),
    reraise=True,
)
def fetch_related_documents(auth_session, dataset_urn: str, expected_doc_urn: str):
    """Read an entity's relatedDocuments, retrying until the document shows up."""
    related_query = """
        query GetDatasetDocs($urn: String!, $input: RelatedDocumentsInput!) {
          dataset(urn: $urn) {
            relatedDocuments(input: $input) {
              total
              documents {
                urn
                info { title }
                settings { showInGlobalContext }
              }
            }
          }
        }
    """
    related_vars = {
        "urn": dataset_urn,
        "input": {"start": 0, "count": 100},
    }
    related_res = execute_graphql(auth_session, related_query, related_vars)
    assert "errors" not in related_res, f"GraphQL errors: {related_res.get('errors')}"

    dataset = related_res["data"]["dataset"]
    if dataset is None or dataset.get("relatedDocuments") is None:
        raise DocumentNotIndexedYet(
            f"Dataset {dataset_urn} not yet available for relatedDocuments lookup"
        )

    related_docs = dataset["relatedDocuments"]
    found_urns = [doc["urn"] for doc in related_docs["documents"] or []]
    if expected_doc_urn not in found_urns:
        raise DocumentNotIndexedYet(
            f"Context-only document {expected_doc_urn} SHOULD appear in relatedDocuments. "
            f"Found: {found_urns}."
        )
    return related_docs
