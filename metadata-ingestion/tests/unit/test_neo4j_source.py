import unittest
from pathlib import Path

import pandas as pd
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.neo4j.neo4j_source import Neo4jConfig, Neo4jSource


@pytest.fixture
def tracking_uri(tmp_path: Path) -> str:
    # return str(tmp_path / "neo4j")
    return "neo4j+ssc://host:7687"


@pytest.fixture
def source(tracking_uri: str) -> Neo4jSource:
    return Neo4jSource(
        ctx=PipelineContext(run_id="neo4j-test"),
        config=Neo4jConfig(
            uri=tracking_uri, env="Prod", username="test", password="test"
        ),
    )


def data():
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


def test_process_nodes(source):
    df = source.process_nodes(data=data())
    assert type(df) is pd.DataFrame


def test_process_relationships(source):
    df = source.process_relationships(
        data=data(), node_df=source.process_nodes(data=data())
    )
    assert type(df) is pd.DataFrame


def test_get_obj_type(source):
    results = data()
    assert source.get_obj_type(results[0]["value"]) == "node"
    assert source.get_obj_type(results[1]["value"]) == "node"
    assert source.get_obj_type(results[2]["value"]) == "relationship"


def test_get_node_description(source):
    results = data()
    df = source.process_nodes(data=data())
    assert (
        source.get_node_description(results[0], df)
        == "(Node_1)<-[RELATIONSHIP_1]-(Node_2)"
    )
    assert (
        source.get_node_description(results[1], df)
        == "(Node_2)-[RELATIONSHIP_1]->(Node_2)"
    )


def test_get_property_data_types(source):
    results = data()
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


def test_get_properties(source):
    results = data()
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


def test_get_relationships(source):
    results = data()
    record = list(
        results[0]["value"]["relationships"].keys()
    )  # Get the first key from the dict_keys
    assert record == ["RELATIONSHIP_1"]


if __name__ == "__main__":
    unittest.main()
