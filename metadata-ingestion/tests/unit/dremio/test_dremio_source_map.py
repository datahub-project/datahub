from types import SimpleNamespace
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.dremio.dremio_config import (
    DremioSourceMapping,
    DremioSourceTypeOverride,
)
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_entities import DremioSourceContainer
from datahub.ingestion.source.dremio.dremio_source import (
    DremioSource,
    DremioSourceMapEntry,
    build_dremio_source_map,
)


def test_build_source_map_simple():
    """Test basic source mapping functionality with simple configuration"""
    config_mapping: List[DremioSourceMapping] = [
        DremioSourceMapping(source_name="source1", platform="S3", env="PROD"),
        DremioSourceMapping(source_name="source2", platform="redshift", env="DEV"),
    ]
    sources: List[DremioSourceContainer] = [
        DremioSourceContainer(
            container_name="source1",
            location_id="xxx",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="S3",
            root_path="/",
            database_name=None,
        ),
        DremioSourceContainer(
            container_name="source2",
            location_id="yyy",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="REDSHIFT",
            root_path="/",
            database_name="redshiftdb",
        ),
    ]
    source_map = build_dremio_source_map(sources, config_mapping)

    assert source_map == {
        "source1": DremioSourceMapEntry(
            source_name="source1",
            platform="S3",
            env="PROD",
            dremio_source_category="file_object_storage",
            root_path="/",
            database_name="",
        ),
        "source2": DremioSourceMapEntry(
            source_name="source2",
            platform="redshift",
            env="DEV",
            dremio_source_category="database",
            root_path="/",
            database_name="redshiftdb",
        ),
    }


def test_build_source_map_same_platform_multiple_sources():
    """Test source mapping with multiple sources using the same platform and complex scenarios"""
    config_mapping: List[DremioSourceMapping] = [
        DremioSourceMapping(source_name="source1", platform="S3", env="PROD"),
        DremioSourceMapping(source_name="source2", platform="redshift", env="DEV"),
        DremioSourceMapping(source_name="source2", platform="redshift", env="PROD"),
    ]
    sources: List[DremioSourceContainer] = [
        DremioSourceContainer(
            container_name="source1",
            location_id="xxx",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="S3",
            root_path="/",
            database_name=None,
        ),
        DremioSourceContainer(
            container_name="source2",
            location_id="yyy",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="REDSHIFT",
            root_path="/",
            database_name="redshiftdb",
        ),
        DremioSourceContainer(
            container_name="source3",
            location_id="tt",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="REDSHIFT",
            root_path="/",
            database_name="redshiftproddb",
        ),
        DremioSourceContainer(
            container_name="Source4",
            location_id="zz",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="NEWSOURCE",
            root_path="/",
            database_name="somedb",
        ),
    ]
    source_map = build_dremio_source_map(sources, config_mapping)

    assert source_map == {
        "source1": DremioSourceMapEntry(
            source_name="source1",
            platform="S3",
            env="PROD",
            dremio_source_category="file_object_storage",
            root_path="/",
            database_name="",
        ),
        "source2": DremioSourceMapEntry(
            source_name="source2",
            platform="redshift",
            env="DEV",
            dremio_source_category="database",
            root_path="/",
            database_name="redshiftdb",
        ),
        "source3": DremioSourceMapEntry(
            source_name="source3",
            platform="redshift",
            env=None,
            dremio_source_category="database",
            root_path="/",
            database_name="redshiftproddb",
        ),
        "source4": DremioSourceMapEntry(
            source_name="Source4",
            platform="newsource",
            env=None,
            dremio_source_category="unknown",
            root_path="/",
            database_name="somedb",
        ),
    }


def test_build_source_map_honors_recipe_overrides_end_to_end():
    # Recipe override -> mapper -> source_map: pin end-to-end propagation.
    overrides = {
        "MYNEWCONNECTOR": DremioSourceTypeOverride(
            platform="myplatform", category="database"
        ),
        "MYSTORE": DremioSourceTypeOverride(
            platform="mystore", category="file_object_storage"
        ),
    }
    mapper = DremioToDataHubSourceTypeMapping(extra_mappings=overrides)

    sources: List[DremioSourceContainer] = [
        DremioSourceContainer(
            container_name="custom_db",
            location_id="a",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="MYNEWCONNECTOR",
            root_path="/",
            database_name="custom",
        ),
        DremioSourceContainer(
            container_name="custom_lake",
            location_id="b",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="MYSTORE",
            root_path="/lake",
            database_name=None,
        ),
    ]

    source_map = build_dremio_source_map(
        sources, source_mappings_config=[], source_type_mapper=mapper
    )

    assert source_map == {
        "custom_db": DremioSourceMapEntry(
            source_name="custom_db",
            platform="myplatform",
            env=None,
            dremio_source_category="database",
            root_path="/",
            database_name="custom",
        ),
        "custom_lake": DremioSourceMapEntry(
            source_name="custom_lake",
            platform="mystore",
            env=None,
            dremio_source_category="file_object_storage",
            root_path="/lake",
            database_name="",
        ),
    }


def test_build_source_map_default_mapper_marks_unknown_arp_types():
    # Negative control: unknown type falls through with category='unknown'.
    sources: List[DremioSourceContainer] = [
        DremioSourceContainer(
            container_name="custom_db",
            location_id="a",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="MYNEWCONNECTOR",
            root_path="/",
            database_name="custom",
        ),
    ]
    source_map = build_dremio_source_map(sources, source_mappings_config=[])
    assert source_map["custom_db"].dremio_source_category == "unknown"
    assert source_map["custom_db"].platform == "mynewconnector"


def _map_to_urn(
    dremio_source_type: str,
    database_name: Optional[str],
    path: List[str],
    dataset: str,
    root_path: str = "/",
) -> Optional[str]:
    source = DremioSourceContainer(
        container_name=path[0],
        location_id="xxx",
        path=[],
        api_operations=MagicMock(),  # type:ignore
        dremio_source_type=dremio_source_type,
        root_path=root_path,
        database_name=database_name,
    )
    stub = SimpleNamespace(
        source_map=build_dremio_source_map([source], []),
        config=SimpleNamespace(env="DEV"),
    )
    return DremioSource._map_dremio_dataset_to_urn(
        stub,  # type: ignore[arg-type]
        dremio_source=path[0],
        dremio_path=path,
        dremio_dataset=dataset,
    )


@pytest.mark.parametrize(
    "database_name",
    [
        pytest.param("my_db", id="database_configured"),
        pytest.param(None, id="no_database_configured"),
    ],
)
@pytest.mark.parametrize(
    "path,expected_name",
    [
        pytest.param(
            ["MY_SQLSERVER", "MY_DB", "dbo"],
            "MY_DB.dbo.my_Table",
            id="configured_database",
        ),
        pytest.param(
            ["MY_SQLSERVER", "other_db", "dbo"],
            "other_db.dbo.my_Table",
            id="other_database",
        ),
    ],
)
def test_map_urn_does_not_prepend_database_already_in_path(
    database_name: Optional[str], path: List[str], expected_name: str
) -> None:
    # SQL Server lists every database as a folder under the source, whether or
    # not the Dremio source config names one.
    assert (
        _map_to_urn("MSSQL", database_name, path, "my_Table")
        == f"urn:li:dataset:(urn:li:dataPlatform:mssql,{expected_name},DEV)"
    )


def test_map_urn_prepends_database_missing_from_path() -> None:
    # Postgres connects to one database and lists only its schemas.
    assert (
        _map_to_urn("POSTGRES", "my_db", ["my_postgres", "public"], "my_table")
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.public.my_table,DEV)"
    )


@pytest.mark.parametrize(
    "source_type,platform,path,dataset,expected_name",
    [
        pytest.param(
            "SNOWFLAKE",
            "snowflake",
            ["my_snowflake", "MY_DB", "PUBLIC"],
            "My_Table",
            "my_db.public.my_table",
            id="snowflake_lowercases",
        ),
        pytest.param(
            "ORACLE",
            "oracle",
            ["my_oracle", "MY_SCHEMA"],
            "MY_TABLE",
            "my_schema.my_table",
            id="oracle_lowercases_uppercase_identifiers",
        ),
        pytest.param(
            "ORACLE",
            "oracle",
            ["my_oracle", "MY_SCHEMA"],
            "My_Table",
            "my_schema.My_Table",
            id="oracle_keeps_quoted_mixed_case",
        ),
        pytest.param(
            "MYSQL",
            "mysql",
            ["my_mysql", "myDb"],
            "My_Table",
            "myDb.My_Table",
            id="mysql_keeps_case",
        ),
    ],
)
def test_map_urn_matches_target_connector_default_casing(
    source_type: str, platform: str, path: List[str], dataset: str, expected_name: str
) -> None:
    assert (
        _map_to_urn(source_type, None, path, dataset)
        == f"urn:li:dataset:(urn:li:dataPlatform:{platform},{expected_name},DEV)"
    )


def test_map_urn_file_object_storage_unchanged() -> None:
    assert (
        _map_to_urn("S3", None, ["my_s3", "My_Folder"], "data.parquet", "/my_bucket")
        == "urn:li:dataset:(urn:li:dataPlatform:s3,my_bucket/My_Folder/data.parquet,DEV)"
    )
