import unittest
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.neo4j.neo4j_source import (
    Neo4jConfig,
    Neo4jSource,
    Neo4jSourceReport,
    StatefulStaleMetadataRemovalConfig,
)


@pytest.fixture
def tracking_uri(tmp_path: Path) -> str:
    return "neo4j+ssc://host:7687"


@pytest.fixture
def mock_data():
    """Test data fixture"""
    return [
        {
            "key": "Node_1",
            "value": {
                "count": 433026,
                "relationships": {
                    "RELATIONSHIP_1": {
                        "count": 1,
                        "properties": {
                            "Relationship1_Property1": {
                                "existence": False,
                                "type": "STRING",
                                "indexed": False,
                                "array": False,
                            }
                        },
                        "direction": "in",
                        "labels": ["Node_2"],
                    }
                },
                "RELATIONSHIP_2": {
                    "count": 2,
                    "properties": {
                        "Relationship2_Property1": {
                            "existence": False,
                            "type": "STRING",
                            "indexed": False,
                            "array": False,
                        }
                    },
                    "direction": "in",
                    "labels": ["Node_3"],
                },
                "type": "node",
                "properties": {
                    "Node1_Property1": {
                        "existence": False,
                        "type": "DATE",
                        "indexed": False,
                        "unique": False,
                    },
                    "Node1_Property2": {
                        "existence": False,
                        "type": "STRING",
                        "indexed": False,
                        "unique": False,
                    },
                    "Node1_Property3": {
                        "existence": False,
                        "type": "STRING",
                        "indexed": False,
                        "unique": False,
                    },
                },
                "labels": [],
            },
        },
        {
            "key": "Node_2",
            "value": {
                "count": 3,
                "relationships": {
                    "RELATIONSHIP_1": {
                        "count": 1,
                        "properties": {
                            "Relationship1_Property1": {
                                "existence": False,
                                "type": "STRING",
                                "indexed": False,
                                "array": False,
                            }
                        },
                        "direction": "out",
                        "labels": ["Node_2"],
                    }
                },
                "type": "node",
                "properties": {
                    "Node2_Property1": {
                        "existence": False,
                        "type": "DATE",
                        "indexed": False,
                        "unique": False,
                    },
                    "Node2_Property2": {
                        "existence": False,
                        "type": "STRING",
                        "indexed": False,
                        "unique": False,
                    },
                    "Node2_Property3": {
                        "existence": False,
                        "type": "STRING",
                        "indexed": False,
                        "unique": False,
                    },
                },
                "labels": [],
            },
        },
        {
            "key": "RELATIONSHIP_1",
            "value": {
                "count": 4,
                "type": "relationship",
                "properties": {
                    "Relationship1_Property1": {
                        "existence": False,
                        "type": "STRING",
                        "indexed": False,
                        "array": False,
                    }
                },
            },
        },
    ]


@pytest.fixture
def mock_neo4j_session():
    """Create a mock Neo4j session"""
    mock_driver = mock.MagicMock()
    mock_session = mock.MagicMock()
    mock_result = mock.MagicMock()

    mock_session.__enter__.return_value = mock_session
    mock_session.run.return_value = mock_result
    mock_driver.session.return_value = mock_session

    return mock_driver, mock_session, mock_result


@pytest.fixture
def mock_datahub_graph():
    """Create a mock DataHub graph"""
    mock_graph = mock.MagicMock(spec=DataHubGraph)
    mock_graph.get_aspect.return_value = None
    mock_graph.get_config.return_value = {
        "statefulIngestionCapable": True,
        "supportedFeatures": [
            "DATAHUB_UPGRADE_HISTORY",
            "DOMAINS",
            "GLOSSARY_TERMS",
            "LINEAGE_RELATIONSHIPS",
        ],
    }
    return mock_graph


@pytest.fixture
def basic_source(tracking_uri: str) -> Neo4jSource:
    return Neo4jSource(
        ctx=PipelineContext(run_id="neo4j-test"),
        config=Neo4jConfig(
            uri=tracking_uri, env="PROD", username="test", password="test"
        ),
    )


@pytest.fixture
def source_with_platform_instance(tracking_uri: str) -> Neo4jSource:
    return Neo4jSource(
        ctx=PipelineContext(run_id="neo4j-test"),
        config=Neo4jConfig(
            uri=tracking_uri,
            env="PROD",
            username="test",
            password="test",
            platform_instance="test-instance",
        ),
    )


@pytest.fixture
def source_with_stateful_ingestion(
    tracking_uri: str, mock_datahub_graph: mock.MagicMock
) -> Neo4jSource:
    stateful_config = StatefulStaleMetadataRemovalConfig(
        enabled=True,
    )

    return Neo4jSource(
        ctx=PipelineContext(
            run_id="neo4j-test", pipeline_name="test-pipeline", graph=mock_datahub_graph
        ),
        config=Neo4jConfig(
            uri=tracking_uri,
            env="PROD",
            username="test",
            password="test",
            platform_instance="test-instance",
            stateful_ingestion=stateful_config,
        ),
    )


@pytest.fixture
def source(basic_source: Neo4jSource) -> Neo4jSource:
    """Default source for backward compatibility with existing tests"""
    return basic_source


def test_source_initialization(source):
    """Test source initialization with config"""
    assert source.platform == "neo4j"
    assert source.config.env == "PROD"
    assert source.config.username == "test"
    assert source.config.password == "test"


def test_process_nodes(source, mock_data):
    """Test node processing"""
    df = source.process_nodes(data=mock_data)
    assert isinstance(df, pd.DataFrame)
    assert "key" in df.columns
    assert "obj_type" in df.columns
    assert "property_data_types" in df.columns
    assert "description" in df.columns
    assert len(df) == 2  # Should have 2 nodes in test data


def test_process_relationships(source, mock_data):
    """Test relationship processing"""
    node_df = source.process_nodes(data=mock_data)
    df = source.process_relationships(data=mock_data, node_df=node_df)
    assert isinstance(df, pd.DataFrame)
    assert "key" in df.columns
    assert "obj_type" in df.columns
    assert "property_data_types" in df.columns
    assert "description" in df.columns
    assert len(df) == 1  # Should have 1 relationship in test data


def test_get_obj_type(source, mock_data):
    """Test object type determination"""
    results = mock_data
    assert source.get_obj_type(results[0]["value"]) == "node"
    assert source.get_obj_type(results[1]["value"]) == "node"
    assert source.get_obj_type(results[2]["value"]) == "relationship"


def test_get_node_description(source, mock_data):
    """Test node description generation"""
    results = mock_data
    df = source.process_nodes(data=mock_data)
    assert (
        source.get_node_description(results[0], df)
        == "(Node_1)<-[RELATIONSHIP_1]-(Node_2)"
    )
    assert (
        source.get_node_description(results[1], df)
        == "(Node_2)-[RELATIONSHIP_1]->(Node_2)"
    )


def test_get_rel_descriptions(source, mock_data):
    """Test relationship description generation"""
    results = mock_data
    node_df = source.process_nodes(data=mock_data)
    rel_description = source.get_rel_descriptions(results[2], node_df)
    assert "(Node_1)-[RELATIONSHIP_1]->(Node_2)" in rel_description


def test_get_property_data_types(source, mock_data):
    """Test property data type extraction"""
    results = mock_data
    assert source.get_property_data_types(results[0]["value"]["properties"]) == [
        {"Node1_Property1": "DATE"},
        {"Node1_Property2": "STRING"},
        {"Node1_Property3": "STRING"},
    ]
    assert source.get_property_data_types(results[1]["value"]["properties"]) == [
        {"Node2_Property1": "DATE"},
        {"Node2_Property2": "STRING"},
        {"Node2_Property3": "STRING"},
    ]
    assert source.get_property_data_types(results[2]["value"]["properties"]) == [
        {"Relationship1_Property1": "STRING"}
    ]


def test_get_properties(source, mock_data):
    """Test property extraction"""
    results = mock_data
    assert list(source.get_properties(results[0]["value"]).keys()) == [
        "Node1_Property1",
        "Node1_Property2",
        "Node1_Property3",
    ]
    assert list(source.get_properties(results[1]["value"]).keys()) == [
        "Node2_Property1",
        "Node2_Property2",
        "Node2_Property3",
    ]
    assert list(source.get_properties(results[2]["value"]).keys()) == [
        "Relationship1_Property1"
    ]


def test_get_relationships(source, mock_data):
    """Test relationship extraction"""
    results = mock_data
    assert list(source.get_relationships(results[0]["value"]).keys()) == [
        "RELATIONSHIP_1"
    ]
    assert source.get_relationships(results[2]["value"]) is None


def test_get_field_type(source):
    """Test field type mapping"""
    assert source.get_field_type("string").type.__class__.__name__ == "StringTypeClass"
    assert source.get_field_type("integer").type.__class__.__name__ == "NumberTypeClass"
    assert source.get_field_type("date").type.__class__.__name__ == "DateTypeClass"
    assert source.get_field_type("unknown").type.__class__.__name__ == "NullTypeClass"


def test_get_schema_field_class(source):
    """Test schema field class generation"""
    field = source.get_schema_field_class("test_field", "string", obj_type="node")
    assert field.fieldPath == "test_field"
    assert field.nativeDataType == "string"
    assert field.type.type.__class__.__name__ == "StringTypeClass"

    # Test relationship field conversion
    field = source.get_schema_field_class("test_field", "relationship", obj_type="node")
    assert field.nativeDataType == "node"


def test_get_workunits_processor(source):
    """Test workunit processor initialization"""
    processors = source.get_workunit_processors()
    assert len(processors) > 0


def test_report_generation(source):
    """Test report generation"""
    report = source.get_report()
    assert report is not None
    assert isinstance(report, Neo4jSourceReport)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("string", "StringTypeClass"),
        ("integer", "NumberTypeClass"),
        ("float", "NumberTypeClass"),
        ("date", "DateTypeClass"),
        ("boolean", "BooleanTypeClass"),
        ("list", "UnionTypeClass"),
        ("unknown_type", "NullTypeClass"),
    ],
)
def test_type_mapping(source, test_input, expected):
    """Test type mapping for different input types"""
    field_type = source.get_field_type(test_input)
    assert field_type.type.__class__.__name__ == expected


def test_platform_instance_config(source_with_platform_instance):
    """Test platform instance configuration"""
    assert source_with_platform_instance.platform_instance == "test-instance"
    assert source_with_platform_instance.config.platform_instance == "test-instance"


def test_stateful_ingestion_config(source_with_stateful_ingestion):
    """Test stateful ingestion configuration"""
    config = source_with_stateful_ingestion.config.stateful_ingestion
    assert config is not None
    assert config.enabled is True
    assert config.state_provider.type == "datahub"

    # Test pipeline context configuration
    assert source_with_stateful_ingestion.ctx.pipeline_name == "test-pipeline"

    # Verify DataHub graph configuration
    assert source_with_stateful_ingestion.ctx.graph is not None


def test_neo4j_session_handling(source, mock_neo4j_session):
    """Test proper Neo4j session handling"""
    mock_driver, mock_session, mock_result = mock_neo4j_session

    # Configure mock result
    def mock_data_method():
        return []

    mock_result.data = mock_data_method

    # Use the mock with a context manager
    with mock.patch("neo4j.GraphDatabase.driver", return_value=mock_driver):
        source.get_neo4j_metadata(
            "CALL apoc.meta.schema() YIELD value UNWIND keys(value) AS key RETURN key, value[key] AS value;"
        )

        # Verify proper session handling
        assert mock_session.__enter__.called, "Session context manager entry not called"
        assert mock_session.__exit__.called, "Session context manager exit not called"
        assert mock_driver.session.called, "Driver session not called"


def test_workunit_with_platform_instance(
    source_with_platform_instance, mock_data, mock_neo4j_session
):
    """Test that workunits include platform instance information"""
    mock_driver, mock_session, mock_result = mock_neo4j_session

    def mock_data_method():
        return mock_data

    mock_result.data = mock_data_method

    with mock.patch("neo4j.GraphDatabase.driver", return_value=mock_driver):
        df = source_with_platform_instance.get_neo4j_metadata(
            "CALL apoc.meta.schema() YIELD value UNWIND keys(value) AS key RETURN key, value[key] AS value;"
        )

        # Verify session was used as context manager
        assert mock_session.__enter__.called
        assert mock_session.__exit__.called

        if df is not None:
            for workunit in source_with_platform_instance.get_workunits_internal():
                assert "test-instance" in workunit.id


def test_default_values():
    """Test default configuration values"""
    config = Neo4jConfig(
        uri="neo4j://localhost:7687", username="neo4j", password="password", env="PROD"
    )
    assert config.platform == "neo4j"
    assert config.platform_instance is None
    assert config.stateful_ingestion is None


if __name__ == "__main__":
    unittest.main()
