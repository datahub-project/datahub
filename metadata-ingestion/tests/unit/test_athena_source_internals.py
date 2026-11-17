"""AthenaSource and CustomAthenaRestDialect behaviour around the SQLAlchemy /
PyAthena boundary: engine wiring, view DDL reads, container keys, partition
discovery from CREATE TABLE statements and the probe's failing report."""

from datetime import datetime
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pyathena.error
import pytest
from pyathena.model import AthenaTableMetadata
from sqlalchemy import exc

from datahub.ingestion.agent.sql_passthrough import QueryBudget
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.athena import (
    AthenaConfig,
    AthenaProbeReadFailed,
    AthenaSource,
    CustomAthenaRestDialect,
    Partitionitem,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.metadata.schema_classes import ContainerClass, ContainerPropertiesClass
from tests.unit.conftest import as_mock, aspects_of_type, single_aspect


def _config(**overrides: object) -> AthenaConfig:
    base: Dict[str, object] = {
        "aws_region": "us-east-1",
        "work_group": "primary",
        "query_result_location": "s3://bucket/results/",
    }
    base.update(overrides)
    return AthenaConfig.model_validate(base)


def _source(**overrides: object) -> AthenaSource:
    return AthenaSource(config=_config(**overrides), ctx=PipelineContext(run_id="t"))


def _table_metadata(partition_keys: List[str]) -> AthenaTableMetadata:
    return AthenaTableMetadata(
        response={
            "TableMetadata": {
                "Name": "t",
                "TableType": "EXTERNAL_TABLE",
                "CreateTime": datetime(2024, 1, 1),
                "PartitionKeys": [
                    {"Name": k, "Type": "string", "Comment": ""} for k in partition_keys
                ],
                "Parameters": {"location": "s3://bucket/t/"},
            }
        }
    )


# --------------------------------------------------------------------------- #
# Dialect
# --------------------------------------------------------------------------- #


def test_get_view_definition_joins_show_create_view_output() -> None:
    dialect = CustomAthenaRestDialect()
    connection = MagicMock()
    connection.scalars.return_value = iter(["CREATE VIEW v AS", "SELECT 1"])

    ddl = dialect.get_view_definition(connection, "v", schema="s")

    assert ddl == "CREATE VIEW v AS\nSELECT 1"
    query = str(connection.scalars.call_args[0][0])
    assert '"s"."v"' in query


def test_get_view_definition_defaults_schema_from_connection() -> None:
    dialect = CustomAthenaRestDialect()
    connection = MagicMock()
    connection.connection.schema_name = "default_db"
    connection.scalars.return_value = iter(["x"])

    dialect.get_view_definition(connection, "v")

    assert '"default_db"."v"' in str(connection.scalars.call_args[0][0])


def test_get_view_definition_maps_operational_error_to_no_such_table() -> None:
    dialect = CustomAthenaRestDialect()
    connection = MagicMock()
    connection.scalars.side_effect = exc.OperationalError(
        "SHOW CREATE VIEW", {}, Exception("not a view")
    )
    with pytest.raises(exc.NoSuchTableError):
        dialect.get_view_definition(connection, "t", schema="s")


def test_a_view_athena_refuses_to_describe_is_missing_not_a_driver_error() -> None:
    """Athena refuses SHOW CREATE VIEW for views it did not create. SQLAlchemy
    wraps the driver's error through the dialect's DBAPI; a swapped-in dialect
    without one raised InvalidRequestError and every such view was dropped."""
    inspectors = iter(_source().get_inspectors())
    inspector = next(inspectors)
    with (
        patch.object(
            CustomAthenaRestDialect,
            "do_execute",
            side_effect=pyathena.error.OperationalError("view not created by Athena"),
        ),
        pytest.raises(exc.NoSuchTableError),
    ):
        inspector.get_view_definition("v", schema="s")
    # Leaving the generator closes the connection it holds open.
    for _ in inspectors:
        pass


def test_get_column_type_rejects_unsupported_definitions() -> None:
    with pytest.raises(RuntimeError):
        CustomAthenaRestDialect()._get_column_type(42)  # type: ignore[arg-type]


def test_get_column_type_unknown_pandas_dtype_falls_back_to_base_dialect() -> None:
    # Only the dtypes we know are mapped; anything else goes through PyAthena,
    # which yields NullType rather than raising.
    with pytest.warns(exc.SAWarning):
        result = CustomAthenaRestDialect()._get_column_type("complex128dtype")
    assert type(result).__name__ == "NullType"


def test_probe_dialect_turns_boto3_fallback_failure_into_an_error() -> None:
    # Ingestion records a warning and carries on with an empty schema; the
    # probe must instead fail loudly, so "could not read" never reads as
    # "no tables".
    config = _config()
    engine = MagicMock()
    config._use_ingestions_dialect(engine)
    dialect = engine.dialect
    assert isinstance(dialect, CustomAthenaRestDialect)

    raw_conn = MagicMock()
    raw_conn.catalog_name = "s3tablescatalog/my-bucket"
    raw_conn.schema_name = "ns"
    raw_conn.client.get_paginator.side_effect = RuntimeError("AccessDenied")
    connection = MagicMock()
    connection.connection = raw_conn
    with (
        patch.object(dialect, "_get_tables", return_value=[]),
        pytest.raises(AthenaProbeReadFailed, match="s3tablescatalog/my-bucket"),
    ):
        dialect.get_table_names(connection, schema="ns")


def test_probe_engine_settings_install_the_ingestion_dialect() -> None:
    settings = _config().probe_engine_settings(QueryBudget())
    engine = MagicMock()
    assert settings.prepare is not None
    settings.prepare(engine)
    assert isinstance(engine.dialect, CustomAthenaRestDialect)


# --------------------------------------------------------------------------- #
# Source wiring
# --------------------------------------------------------------------------- #


def test_create_builds_source_from_dict() -> None:
    source = AthenaSource.create(
        {
            "aws_region": "us-east-1",
            "work_group": "primary",
            "query_result_location": "s3://bucket/results/",
        },
        PipelineContext(run_id="t"),
    )
    assert source.config.aws_region == "us-east-1"


def test_get_inspectors_installs_custom_dialect_with_report() -> None:
    source = _source()
    engine = MagicMock()
    inspector = MagicMock()
    with (
        patch(
            "datahub.ingestion.source.sql.athena.create_engine", return_value=engine
        ) as create_engine,
        patch("datahub.ingestion.source.sql.athena.inspect", return_value=inspector),
    ):
        inspectors = list(source.get_inspectors())

    assert inspectors == [inspector]
    assert "awsathena+rest://" in create_engine.call_args[0][0]
    assert isinstance(engine.dialect, CustomAthenaRestDialect)
    assert engine.dialect._report is source.report


def test_catalog_type_requires_initialised_cursor() -> None:
    with pytest.raises(RuntimeError):
        _ = _source()._catalog_type


def test_get_db_schema_treats_first_component_as_schema() -> None:
    assert _source().get_db_schema("my_db.my_view") == (None, "my_db")


def test_get_table_properties_opens_cursor_from_inspector_on_first_use() -> None:
    """The PyAthena cursor is created lazily from the first inspector, and the
    partition keys of its table metadata become the ``partition_keys`` property."""
    source = _source()
    inspector = MagicMock()
    cursor = inspector.engine.raw_connection.return_value.cursor.return_value
    cursor.get_table_metadata.return_value = _table_metadata(["dt"])

    _, props, _ = source.get_table_properties(inspector, "s", "t")

    assert source.cursor is cursor
    assert '"name": "dt"' in props["partition_keys"]


def test_schema_names_filtered_to_configured_database() -> None:
    inspector = MagicMock()
    inspector.get_schema_names.return_value = ["a", "b", "c"]
    assert _source(database="b").get_schema_names(inspector) == ["b"]
    assert _source().get_schema_names(inspector) == ["a", "b", "c"]


# --------------------------------------------------------------------------- #
# Containers: Athena schemas are DataHub databases
# --------------------------------------------------------------------------- #


def test_no_database_level_container_is_emitted() -> None:
    assert list(_source().gen_database_containers("db")) == []


def test_schema_container_is_emitted_as_database_container() -> None:
    """Athena has no level above the Glue database, which SQLAlchemy calls a
    schema: it is emitted as a top-level database container."""
    source = _source(platform_instance="athena_prod")
    workunits = list(source.gen_schema_containers(schema="sales", database="sales"))
    assert single_aspect(workunits, ContainerPropertiesClass).name == "sales"
    # Databases have no parent container.
    assert not aspects_of_type(workunits, ContainerClass)


def test_database_container_key_falls_back_to_schema_when_db_unset() -> None:
    source = _source()
    assert source.get_database_container_key(
        "", "sales"
    ) == source.get_database_container_key("sales", "ignored")


def test_add_table_to_schema_container_parents_dataset_to_database() -> None:
    source = _source()
    workunits = list(
        source.add_table_to_schema_container(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:athena,sales.t,PROD)",
            db_name="sales",
            schema="sales",
        )
    )
    assert (
        single_aspect(workunits, ContainerClass).container
        == source.get_database_container_key("sales", "sales").as_urn()
    )


# --------------------------------------------------------------------------- #
# Partitions
# --------------------------------------------------------------------------- #


def test_get_partitions_returns_none_before_cursor_exists() -> None:
    assert _source().get_partitions(MagicMock(), "s", "t") is None


def test_get_partitions_returns_empty_list_for_unpartitioned_table() -> None:
    source = _source()
    cursor = MagicMock()
    cursor.get_table_metadata.return_value = _table_metadata([])
    source.cursor = cursor
    assert source.get_partitions(MagicMock(), "s", "t") == []


def _create_statement_cursor(lines: List[str]) -> MagicMock:
    cursor = MagicMock()
    cursor.execute.return_value.fetchall.return_value = [(line,) for line in lines]
    cursor.get_table_metadata.return_value = _table_metadata(["fallback"])
    return cursor


def test_partitions_from_create_table_statement() -> None:
    """With extract_partitions_using_create_statements, Iceberg transform
    partitions (day(ts), bucket(16, id)) resolve to their source columns."""
    source = _source(extract_partitions_using_create_statements=True)
    source.cursor = _create_statement_cursor(
        [
            "CREATE TABLE s.t (",
            "  id bigint,",
            "  ts timestamp",
            ")",
            "PARTITIONED BY (day(ts), bucket(16, id))",
            "LOCATION 's3://bucket/t/'",
        ]
    )
    assert source.get_partitions(MagicMock(), "s", "t") == ["ts", "id"]
    executed = as_mock(source.cursor).execute.call_args[0][0]
    assert executed == "SHOW CREATE TABLE `s`.`t`"


def test_partitions_fall_back_to_metadata_when_statement_is_unparseable() -> None:
    source = _source(extract_partitions_using_create_statements=True)
    source.cursor = _create_statement_cursor(["CREATE TABLE s.t ( broken"])
    assert source.get_partitions(MagicMock(), "s", "t") == ["fallback"]


def test_partitions_fall_back_to_metadata_when_show_create_fails() -> None:
    source = _source(extract_partitions_using_create_statements=True)
    cursor = _create_statement_cursor([])
    cursor.execute.side_effect = RuntimeError("views have no CREATE TABLE")
    source.cursor = cursor
    assert source.get_partitions(MagicMock(), "s", "t") == ["fallback"]


# --------------------------------------------------------------------------- #
# Partition profiling query
# --------------------------------------------------------------------------- #


def test_partition_profiler_query_filters_on_cached_max_partition() -> None:
    """The profiling query targets the newest partition found earlier, with
    values quoted for SQL (the single quote in ``us'east`` is doubled)."""
    source = _source(profiling={"enabled": True, "partition_profiling_enabled": True})
    source.table_partition_cache = {
        "s": {
            "t": Partitionitem(
                partitions=["dt", "region"],
                max_partition={"dt": "2024-01-01", "region": "us'east"},
            )
        }
    }
    label, query = source.generate_partition_profiler_query("s", "t", None)
    assert label == "{'dt': '2024-01-01', 'region': \"us'east\"}"
    assert query == (
        'SELECT * FROM "s"."t" WHERE CAST(dt as VARCHAR) = \'2024-01-01\' '
        "AND CAST(region as VARCHAR) = 'us''east'"
    )


def test_partition_profiler_query_absent_without_cache_or_feature() -> None:
    enabled = _source(profiling={"enabled": True, "partition_profiling_enabled": True})
    assert enabled.generate_partition_profiler_query("s", "t", None) == (None, None)

    disabled = _source(
        profiling={"enabled": True, "partition_profiling_enabled": False}
    )
    disabled.table_partition_cache = {
        "s": {"t": Partitionitem(partitions=["dt"], max_partition={"dt": "x"})}
    }
    assert disabled.generate_partition_profiler_query("s", "t", None) == (None, None)


def test_close_releases_cursor() -> None:
    source = _source()
    cursor = MagicMock()
    source.cursor = cursor
    source.close()
    cursor.close.assert_called_once()
    assert isinstance(source.report, SQLSourceReport)
