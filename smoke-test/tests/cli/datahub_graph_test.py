import pytest
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import KafkaSchemaClass, SchemaMetadataClass
from tests.utils import delete_urns_from_file, ingest_file_via_rest, get_gms_url



@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(request):
    print("ingesting graph test data")
    ingest_file_via_rest("tests/cli/graph_data.json")
    yield
    print("removing graph test data")
    delete_urns_from_file("tests/cli/graph_data.json")


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_get_aspect_v2(frontend_session, ingest_cleanup_data):
    graph: DataHubGraph = DataHubGraph(DatahubClientConfig(server=get_gms_url()))
    urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-rollback,PROD)"
    schema_metadata: SchemaMetadataClass = graph.get_aspect_v2(
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
