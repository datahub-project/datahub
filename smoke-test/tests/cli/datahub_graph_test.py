from typing import Optional

import pytest
import tenacity

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import KafkaSchemaClass, SchemaMetadataClass
from tests.utils import delete_urns_from_file, get_sleep_info, ingest_file_via_rest

sleep_sec, sleep_times = get_sleep_info()


graph = "test_resources/graph_data.json"
graph_2 = "test_resources/graph_dataDiff.json"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(graph_client, auth_session, request):
    print("removing graph test data")
    delete_urns_from_file(graph_client, "tests/cli/graph_data.json")
    print("ingesting graph test data")
    ingest_file_via_rest(auth_session, "tests/cli/graph_data.json")
    yield
    print("removing graph test data")
    delete_urns_from_file(graph_client, "tests/cli/graph_data.json")


def test_get_aspect_v2(graph_client, ingest_cleanup_data):
    urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-rollback,PROD)"
    schema_metadata: Optional[SchemaMetadataClass] = graph_client.get_aspect_v2(
        urn, aspect="schemaMetadata", aspect_type=SchemaMetadataClass
    )

    assert schema_metadata is not None
    assert schema_metadata.platform == "urn:li:dataPlatform:kafka"
    assert isinstance(schema_metadata.platformSchema, KafkaSchemaClass)
    k_schema: KafkaSchemaClass = schema_metadata.platformSchema
    assert (
        k_schema.documentSchema
        == '{"type":"record","name":"SampleKafkaSchema","namespace":"com.linkedin.dataset","doc":"Sample Kafka dataset","fields":[{"name":"field_foo","type":["string"]},{"name":"field_bar","type":["boolean"]}]}'
    )


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_dataset_present_correctly(auth_session, graph_client: DataHubGraph):
    urn = "urn:li:dataset:(urn:li:dataPlatform:graph,graph-test,PROD)"
    json = {
        "query": """query getDataset($urn: String!) {\n
                dataset(urn: $urn) {\n
                    urn\n
                    name\n
                    description\n
                    platform {\n
                        urn\n
                    }\n
                    schemaMetadata {\n
                        name\n
                        version\n
                        createdAt\n
                    }\n
                    outgoing: relationships(\n
                      input: { types: ["SchemaFieldTaggedWith"], direction: OUTGOING, start: 0, count: 10000 }\n
                    ) {\n
                            start\n
                            count\n
                            total\n
                            relationships {\n
                                type\n
                                direction\n
                                entity {\n
                                    urn\n
                                    type\n
                                }\n
                            }\n
                    }\n
                }\n
            }""",
        "variables": {"urn": urn},
    }
    res_data = graph_client._post_generic(f"{auth_session.gms_url()}/api/graphql", json)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    assert len(res_data["data"]["dataset"]["outgoing"]["relationships"]) == 3


def test_graph_relationships(graph_client, auth_session):
    delete_urns_from_file(graph_client, graph)
    delete_urns_from_file(graph_client, graph_2)
    ingest_file_via_rest(auth_session, graph)
    ingest_file_via_rest(auth_session, graph_2)
    _ensure_dataset_present_correctly(auth_session, graph_client)
