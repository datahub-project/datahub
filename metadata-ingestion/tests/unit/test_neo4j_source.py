import unittest

import pandas as pd

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.neo4j.neo4j_source import Neo4jConfig, Neo4jSource


class TestNeo4j(unittest.TestCase):
    def setUp(self):
        self.neo = Neo4jSource(Neo4jConfig(), PipelineContext(run_id="test"))
        self.record_1 = {
            "count": 1,
            "labels": [],
            "properties": {
                "id": {
                    "unique": True,
                    "indexed": True,
                    "type": "STRING",
                    "existence": False,
                },
            },
            "type": "node",
            "relationships": {
                "RELATIONSHIP_1": {
                    "count": 0,
                    "direction": "out",
                    "labels": ["Label_1"],
                    "properties": {},
                }
            },
        }
        self.record_2 = {
            "count": 2,
            "labels": [],
            "properties": {
                "id": {
                    "unique": True,
                    "indexed": True,
                    "type": "STRING",
                    "existence": False,
                },
                "amount": {
                    "unique": True,
                    "indexed": True,
                    "type": "INTEGER",
                    "existence": False,
                },
            },
            "type": "node",
            "relationships": {
                "RELATIONSHIP_1": {
                    "count": 0,
                    "direction": "out",
                    "labels": ["Label_1"],
                    "properties": {},
                },
                "RELATIONSHIP_2": {
                    "count": 1,
                    "direction": "in",
                    "labels": ["Label_1", "Label_2"],
                    "properties": {},
                },
            },
        }
        self.record_3 = {"count": 3, "properties": {}, "type": "relationship"}
        self.record_4 = {
            "RELATIONSHIP_2": {
                "count": 4,
                "properties": {},
                "type": "relationship",
                "relationships": {
                    "RELATIONSHIP_1": {
                        "count": 0,
                        "direction": "out",
                        "labels": ["Label_1"],
                        "properties": {},
                    },
                    "RELATIONSHIP_2": {
                        "count": 1,
                        "direction": "in",
                        "labels": ["Label_1", "Label_2"],
                        "properties": {},
                    },
                },
            }
        }

    def create_df(self):
        data = {
            "key": ["item1", "item2", "item3", "RELATIONSHIP_2"],
            "value": [
                self.record_1,
                self.record_2,
                self.record_3,
                self.record_4,
            ],
        }
        df = pd.DataFrame(data)
        return df


    def test_get_obj_type(self):
        assert self.neo.get_obj_type(self.record_1) == "node"
        assert self.neo.get_obj_type(self.record_2) == "node"
        assert self.neo.get_obj_type(self.record_3) == "relationship"

    def test_get_relationships(self):
        assert self.neo.get_relationships(self.record_1, self.create_df()) == {
            "RELATIONSHIP_1": {
                "count": 0,
                "direction": "out",
                "labels": ["Label_1"],
                "properties": {},
            }
        }
        assert self.neo.get_relationships(self.record_2, self.create_df()) == {
            "RELATIONSHIP_1": {
                "count": 0,
                "direction": "out",
                "labels": ["Label_1"],
                "properties": {},
            },
            "RELATIONSHIP_2": {
                "count": 1,
                "direction": "in",
                "labels": ["Label_1", "Label_2"],
                "properties": {},
            },
        }
        assert self.neo.get_relationships(self.record_3, self.create_df()) is None

    def test_get_property_data_types(self):
        record_1 = self.record_1.get("properties", None)
        record_2 = self.record_2.get("properties", None)
        assert self.neo.get_property_data_types(record_1) == [{"id": "STRING"}]
        assert self.neo.get_property_data_types(record_2) == [
            {"id": "STRING"},
            {"amount": "INTEGER"},
        ]

    def test_get_properties(self):
        assert self.neo.get_properties(self.record_1) == {
            "id": {
                "unique": True,
                "indexed": True,
                "type": "STRING",
                "existence": False,
            },
        }
        assert self.neo.get_properties(self.record_2) == self.record_2.get(
            "properties", None
        )


if __name__ == "__main__":
    unittest.main()
