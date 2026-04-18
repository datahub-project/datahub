import pytest

from datahub.metadata.schema_classes import DatasetPropertiesClass
from tests.test_helpers.graph_helpers import MockDataHubGraph


@pytest.fixture
def mock_datahub_graph(pytestconfig):
    graph = MockDataHubGraph()
    demo_data_path = pytestconfig.rootpath / "examples" / "demo_data" / "demo_data.json"
    graph.import_file(demo_data_path)
    return graph


def test_get_entities(mock_datahub_graph):
    aspect_name = "datasetProperties"
    urn_1 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.hospital_beds,PROD)"
    urn_2 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_geotab_mobility_impact.city_congestion,PROD)"

    entities = mock_datahub_graph.get_entities(
        entity_name="dataset",
        urns=[
            urn_1,  # misses datasetProperties aspect
            urn_2,  # has datasetProperties aspect
        ],
        aspects=[aspect_name],
    )

    # Verify results
    assert entities
    assert (
        urn_1 in entities
        and aspect_name not in entities[urn_1]
        and entities[urn_1] == {}
    )
    assert (
        urn_2 in entities
        and aspect_name in entities[urn_2]
        and isinstance(entities[urn_2][aspect_name][0], DatasetPropertiesClass)
        and entities[urn_2][aspect_name][1] is None
    )

    # Test system_metadata is always None regardless of flag
    entities_with_metadata = mock_datahub_graph.get_entities(
        entity_name="dataset",
        urns=[urn_2],
        aspects=[aspect_name],
        with_system_metadata=True,
    )
    assert (
        urn_2 in entities_with_metadata
        and aspect_name in entities_with_metadata[urn_2]
        and isinstance(
            entities_with_metadata[urn_2][aspect_name][0], DatasetPropertiesClass
        )
        and entities_with_metadata[urn_2][aspect_name][1] is None
    )
