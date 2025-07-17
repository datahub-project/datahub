import json
import logging
import re
from typing import Any, Dict, List, Tuple

# Unit tests for CassandraAPI SSL Configuration
from unittest.mock import ANY, MagicMock, patch

import pytest

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.cassandra.cassandra import CassandraToSchemaFieldConverter
from datahub.ingestion.source.cassandra.cassandra_api import (
    CassandraAPI,
    CassandraColumn,
)
from datahub.ingestion.source.cassandra.cassandra_config import (
    CassandraSourceConfig,
)
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
            "birthday",
            "email",
            "name",
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
    column_infos: List = schema_dict["column_infos"]

    column_list: List[CassandraColumn] = [
        CassandraColumn(
            keyspace_name=row["keyspace_name"],
            table_name=row["table_name"],
            column_name=row["column_name"],
            clustering_order=row["clustering_order"],
            kind=row["kind"],
            position=row["position"],
            type=row["type"],
        )
        for row in column_infos
    ]
    actual_fields = list(CassandraToSchemaFieldConverter.get_schema_fields(column_list))
    assert_field_paths_match(actual_fields, expected_field_paths)


def test_no_properties_in_mappings_schema() -> None:
    fields = list(CassandraToSchemaFieldConverter.get_schema_fields([]))
    assert fields == []


def _get_base_config_dict() -> dict:
    return {
        "contact_point": "localhost",
        "port": 9042,
    }


def test_authenticate_no_ssl():
    config_dict = _get_base_config_dict()
    config = CassandraSourceConfig.parse_obj(config_dict)
    report = MagicMock(spec=SourceReport)
    api = CassandraAPI(config, report)

    with patch(
        "datahub.ingestion.source.cassandra.cassandra_api.Cluster"
    ) as mock_cluster:
        mock_cluster.return_value.connect.return_value = MagicMock()
        assert api.authenticate()
        mock_cluster.assert_called_once()
        assert mock_cluster.call_args[1].get("ssl_context") is None
        report.failure.assert_not_called()


def test_authenticate_ssl_ca_certs():
    config_dict = _get_base_config_dict()
    config_dict["ssl_ca_certs"] = "ca.pem"
    config = CassandraSourceConfig.parse_obj(config_dict)
    report = MagicMock(spec=SourceReport)
    api = CassandraAPI(config, report)

    with (
        patch(
            "datahub.ingestion.source.cassandra.cassandra_api.Cluster"
        ) as mock_cluster,
        patch(
            "datahub.ingestion.source.cassandra.cassandra_api.ssl.SSLContext"
        ) as mock_ssl_context,
    ):
        mock_ssl_instance = MagicMock()
        mock_ssl_context.return_value = mock_ssl_instance
        mock_cluster.return_value.connect.return_value = MagicMock()

        assert api.authenticate()

        mock_ssl_context.assert_called_once_with(ANY)  # ssl.PROTOCOL_TLS_CLIENT
        mock_ssl_instance.load_verify_locations.assert_called_once_with("ca.pem")
        mock_ssl_instance.load_cert_chain.assert_not_called()

        mock_cluster.assert_called_once()
        assert mock_cluster.call_args[1].get("ssl_context") == mock_ssl_instance
        report.failure.assert_not_called()


def test_authenticate_ssl_all_certs():
    config_dict = _get_base_config_dict()
    config_dict["ssl_ca_certs"] = "ca.pem"
    config_dict["ssl_certfile"] = "client.crt"
    config_dict["ssl_keyfile"] = "client.key"
    config = CassandraSourceConfig.parse_obj(config_dict)
    report = MagicMock(spec=SourceReport)
    api = CassandraAPI(config, report)

    with (
        patch(
            "datahub.ingestion.source.cassandra.cassandra_api.Cluster"
        ) as mock_cluster,
        patch(
            "datahub.ingestion.source.cassandra.cassandra_api.ssl.SSLContext"
        ) as mock_ssl_context,
    ):
        mock_ssl_instance = MagicMock()
        mock_ssl_context.return_value = mock_ssl_instance
        mock_cluster.return_value.connect.return_value = MagicMock()

        assert api.authenticate()

        mock_ssl_context.assert_called_once_with(ANY)  # ssl.PROTOCOL_TLS_CLIENT
        mock_ssl_instance.load_verify_locations.assert_called_once_with("ca.pem")
        mock_ssl_instance.load_cert_chain.assert_called_once_with(
            certfile="client.crt", keyfile="client.key"
        )

        mock_cluster.assert_called_once()
        assert mock_cluster.call_args[1].get("ssl_context") == mock_ssl_instance
        report.failure.assert_not_called()
