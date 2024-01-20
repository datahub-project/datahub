import json
import logging
import re
from typing import Any, Dict, List, Tuple

import pytest

from datahub.ingestion.source.cassandra import CassandraToSchemaFieldConverter
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

logger = logging.getLogger(__name__)


def assert_field_paths_are_unique(fields: List[SchemaField]) -> None:
    fields_paths = [f.fieldPath for f in fields if re.match(".*[^]]$", f.fieldPath)]

    if fields_paths:
        assert len(fields_paths) == len(set(fields_paths))


def assert_field_paths_match(
    fields: List[SchemaField], expected_field_paths: List[str]
) -> None:
    logger.debug('FieldPaths=\n"' + '",\n"'.join(f.fieldPath for f in fields) + '"')
    assert len(fields) == len(expected_field_paths)
    for f, efp in zip(fields, expected_field_paths):
        assert f.fieldPath == efp
    assert_field_paths_are_unique(fields)


# TODO: cover one for every item on https://cassandra.apache.org/doc/stable/cassandra/cql/types.html (version 4.1)
schema_test_cases: Dict[str, Tuple[str, List[str]]] = {
    "all_types_on_4.1": (
        """{
            "column_infos": [
                {"keyspace_name": "playground", "table_name": "people", "column_name": "birthday", "clustering_order": "none", "column_name_bytes": null, "kind": "regular", "position": -1, "type": "timestamp"},
                {"keyspace_name": "playground", "table_name": "people", "column_name": "email", "clustering_order": "none", "column_name_bytes": null, "kind": "partition_key", "position": 0, "type": "text"},
                {"keyspace_name": "playground", "table_name": "people", "column_name": "name", "clustering_order": "none", "column_name_bytes": null, "kind": "regular", "position": -1, "type": "text"}
            ]
        }""",
        [
            "[version=2.0].[type=timestamp].birthday",
            "[version=2.0].[type=text].email",
            "[version=2.0].[type=text].name",
        ],
    )
}


@pytest.mark.parametrize(
    "schema, expected_field_paths",
    schema_test_cases.values(),
    ids=schema_test_cases.keys(),
)
def test_cassandra_schema_conversion(
    schema: str, expected_field_paths: List[str]
) -> None:
    schema_dict: Dict[str, List[Any]] = json.loads(schema)
    column_infos: List[dict[str, Any]] = schema_dict["column_infos"]
    actual_fields = list(
        CassandraToSchemaFieldConverter.get_schema_fields(column_infos)
    )
    assert_field_paths_match(actual_fields, expected_field_paths)


def test_no_properties_in_mappings_schema() -> None:
    fields = list(CassandraToSchemaFieldConverter.get_schema_fields([]))
    assert fields == []
