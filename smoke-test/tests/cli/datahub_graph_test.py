from typing import Optional

import pytest
import tenacity

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    KafkaSchemaClass,
    OwnershipClass,
    SchemaMetadataClass,
    SystemMetadataClass,
)
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


def test_get_entities_v3(graph_client, ingest_cleanup_data):
    ownership_aspect_name = "ownership"
    dataset_properties_aspect_name = "datasetProperties"
    urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-rollback,PROD)"
    entities = graph_client.get_entities(
        entity_name="dataset",
        urns=[urn],
        aspects=[ownership_aspect_name, dataset_properties_aspect_name],
    )

    assert entities
    assert len(entities) == 1 and urn in entities
    assert (
        len(entities[urn]) == 2
        and ownership_aspect_name in entities[urn]
        and dataset_properties_aspect_name in entities[urn]
        and isinstance(entities[urn][ownership_aspect_name][0], OwnershipClass)
        and isinstance(
            entities[urn][dataset_properties_aspect_name][0], DatasetPropertiesClass
        )
        and entities[urn][ownership_aspect_name][1] is None
        and entities[urn][dataset_properties_aspect_name][1] is None
    )
    assert {
        owner.owner for owner in entities[urn][ownership_aspect_name][0].owners
    } == {
        "urn:li:corpuser:datahub",
        "urn:li:corpuser:jdoe",
    }
    assert not entities[urn][dataset_properties_aspect_name][0].description
    assert entities[urn][dataset_properties_aspect_name][0].customProperties == {
        "prop1": "fakeprop",
        "prop2": "pikachu",
    }

    # Test with system metadata
    entities_with_metadata = graph_client.get_entities(
        entity_name="dataset",
        urns=[urn],
        aspects=[ownership_aspect_name],
        with_system_metadata=True,
    )

    assert entities_with_metadata
    assert len(entities_with_metadata) == 1 and urn in entities_with_metadata
    assert (
        ownership_aspect_name in entities_with_metadata[urn]
        and entities_with_metadata[urn][ownership_aspect_name][0]
        and isinstance(
            entities_with_metadata[urn][ownership_aspect_name][0], OwnershipClass
        )
        and entities_with_metadata[urn][ownership_aspect_name][1]
        and isinstance(
            entities_with_metadata[urn][ownership_aspect_name][1], SystemMetadataClass
        )
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
                      input: { types: ["SchemaFieldTaggedWith"], direction: OUTGOING, start: 0, count: 2000 }\n
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
