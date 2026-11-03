from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dremio.dremio_config import (
    DremioSourceConfig,
    DremioSourceMapping,
    DremioSourceTypeOverride,
)
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_entities import DremioSourceContainer
from datahub.ingestion.source.dremio.dremio_models import DremioDatasetType
from datahub.ingestion.source.dremio.dremio_source import (
    DremioSource,
    DremioSourceMapEntry,
    build_dremio_source_map,
)
from datahub.metadata.schema_classes import UpstreamLineageClass


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


def _source_map(
    dremio_source_type: str,
    database_name: Optional[str],
    source_name: str,
    root_path: str = "/",
    mappings: Optional[List[DremioSourceMapping]] = None,
    mapper: Optional[DremioToDataHubSourceTypeMapping] = None,
) -> Dict[str, DremioSourceMapEntry]:
    source = DremioSourceContainer(
        container_name=source_name,
        location_id="xxx",
        path=[],
        api_operations=MagicMock(),  # type:ignore
        dremio_source_type=dremio_source_type,
        root_path=root_path,
        database_name=database_name,
    )
    return build_dremio_source_map([source], mappings or [], mapper)


def _map_to_urn(
    dremio_source_type: str,
    database_name: Optional[str],
    path: List[str],
    dataset: str,
    root_path: str = "/",
    mappings: Optional[List[DremioSourceMapping]] = None,
    mapper: Optional[DremioToDataHubSourceTypeMapping] = None,
) -> Optional[str]:
    stub = SimpleNamespace(
        source_map=_source_map(
            dremio_source_type, database_name, path[0], root_path, mappings, mapper
        ),
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
    "source_type,platform,path,expected_name",
    [
        pytest.param(
            "MSSQL",
            "mssql",
            ["MY_SRC", "MY_DB", "dbo"],
            "MY_DB.dbo.my_Table",
            id="mssql_configured_database",
        ),
        pytest.param(
            "MSSQL",
            "mssql",
            ["MY_SRC", "other_db", "dbo"],
            "other_db.dbo.my_Table",
            id="mssql_other_database",
        ),
        pytest.param(
            "MYSQL",
            "mysql",
            ["MY_SRC", "my_db"],
            "my_db.my_Table",
            id="mysql_configured_database",
        ),
        pytest.param(
            "MYSQL",
            "mysql",
            ["MY_SRC", "other_db"],
            "other_db.my_Table",
            id="mysql_other_database",
        ),
        pytest.param(
            "SNOWFLAKE",
            "snowflake",
            ["MY_SRC", "MY_DB", "PUBLIC"],
            "my_db.public.my_table",
            id="snowflake_configured_database",
        ),
        pytest.param(
            "SNOWFLAKE",
            "snowflake",
            ["MY_SRC", "OTHER_DB", "PUBLIC"],
            "other_db.public.my_table",
            id="snowflake_other_database",
        ),
    ],
)
def test_map_urn_does_not_prepend_database_already_in_path(
    database_name: Optional[str],
    source_type: str,
    platform: str,
    path: List[str],
    expected_name: str,
) -> None:
    # These sources list every database as a folder under the source, whether
    # or not the Dremio source config names one.
    assert (
        _map_to_urn(source_type, database_name, path, "my_Table")
        == f"urn:li:dataset:(urn:li:dataPlatform:{platform},{expected_name},DEV)"
    )


@pytest.mark.parametrize(
    "source_type,platform",
    [
        pytest.param("POSTGRES", "postgres", id="postgres"),
        pytest.param("REDSHIFT", "redshift", id="redshift"),
        pytest.param("SYNAPSE", "mssql", id="synapse"),
    ],
)
def test_map_urn_prepends_database_missing_from_path(
    source_type: str, platform: str
) -> None:
    # These sources connect to the one configured database and list only its
    # schemas.
    assert (
        _map_to_urn(source_type, "my_db", ["my_src", "my_schema"], "my_table")
        == f"urn:li:dataset:(urn:li:dataPlatform:{platform},my_db.my_schema.my_table,DEV)"
    )


@pytest.mark.parametrize(
    "path,expected_name",
    [
        pytest.param(
            ["my_src", "MY_DB", "my_schema"],
            "MY_DB.my_schema.my_table",
            id="path_starts_with_database",
        ),
        pytest.param(
            ["my_src", "my_schema"],
            "my_db.my_schema.my_table",
            id="path_without_database",
        ),
    ],
)
def test_map_urn_custom_source_type_skips_database_already_in_path(
    path: List[str], expected_name: str
) -> None:
    mapper = DremioToDataHubSourceTypeMapping(
        extra_mappings={
            "MYARPDB": DremioSourceTypeOverride(
                platform="myplatform", category="database"
            )
        }
    )
    assert (
        _map_to_urn("MYARPDB", "my_db", path, "my_table", mapper=mapper)
        == f"urn:li:dataset:(urn:li:dataPlatform:myplatform,{expected_name},DEV)"
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


@pytest.mark.parametrize(
    "source_type,platform,convert,expected_name",
    [
        pytest.param(
            "MSSQL", "mssql", True, "my_db.dbo.my_table", id="mssql_force_lowercase"
        ),
        pytest.param(
            "SNOWFLAKE",
            "snowflake",
            False,
            "MY_DB.dbo.My_Table",
            id="snowflake_keep_case",
        ),
    ],
)
def test_map_urn_convert_urns_to_lowercase_override(
    source_type: str, platform: str, convert: bool, expected_name: str
) -> None:
    mappings = [
        DremioSourceMapping(
            source_name="my_src",
            platform=platform,
            env="DEV",
            convert_urns_to_lowercase=convert,
        )
    ]
    assert (
        _map_to_urn(
            source_type,
            "my_db",
            ["my_src", "MY_DB", "dbo"],
            "My_Table",
            mappings=mappings,
        )
        == f"urn:li:dataset:(urn:li:dataPlatform:{platform},{expected_name},DEV)"
    )


def test_map_urn_source_mapping_platform_instance_and_env() -> None:
    mappings = [
        DremioSourceMapping(
            source_name="MY_SRC",
            platform="Snowflake",
            platform_instance="my_instance",
            env="PROD",
        )
    ]
    assert (
        _map_to_urn(
            "SNOWFLAKE",
            "my_db",
            ["my_src", "MY_DB", "PUBLIC"],
            "My_Table",
            mappings=mappings,
        )
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_instance.my_db.public.my_table,PROD)"
    )


def test_map_urn_file_object_storage_unchanged() -> None:
    assert (
        _map_to_urn("S3", None, ["my_s3", "My_Folder"], "data.parquet", "/my_bucket")
        == "urn:li:dataset:(urn:li:dataPlatform:s3,my_bucket/My_Folder/data.parquet,DEV)"
    )


def _dremio_source(
    monkeypatch: pytest.MonkeyPatch,
    source_mappings: Optional[List[DremioSourceMapping]] = None,
) -> DremioSource:
    mock_session = MagicMock()
    monkeypatch.setattr("requests.Session", MagicMock(return_value=mock_session))
    mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
    mock_session.post.return_value.status_code = 200
    ctx = MagicMock(run_id="test-run-id", graph=None)
    return DremioSource(
        DremioSourceConfig(
            hostname="test-host",
            port=9047,
            username="u",
            password="p",
            env="DEV",
            source_mappings=source_mappings,
        ),
        ctx,
    )


def test_process_dataset_emits_mapped_copy_upstream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _dremio_source(monkeypatch)
    source.source_map = _source_map("MSSQL", "my_db", "MY_SRC")
    source.dremio_aspects = MagicMock()
    source.dremio_aspects.populate_dataset_mcp.return_value = []
    source.sql_parsing_aggregator = MagicMock()
    dataset = MagicMock(
        path=["MY_SRC", "MY_DB", "dbo"],
        resource_name="my_Table",
        dataset_type=DremioDatasetType.TABLE,
    )

    workunits = list(source.process_dataset(dataset))

    expected = "urn:li:dataset:(urn:li:dataPlatform:mssql,MY_DB.dbo.my_Table,DEV)"
    lineage = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]
    assert [u.dataset for la in lineage for u in la.upstreams] == [expected]
    source.sql_parsing_aggregator.add_known_lineage_mapping.assert_called_once()
    kwargs = source.sql_parsing_aggregator.add_known_lineage_mapping.call_args.kwargs
    assert kwargs["upstream_urn"] == expected
    assert kwargs["downstream_urn"] == (
        "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.my_src.my_db.dbo.my_table,DEV)"
    )
    assert source.report.source_mapping_sample_upstream_urns == {"my_src": expected}


def _warning_contexts(source: DremioSource, message_prefix: str) -> List[str]:
    return [
        context
        for w in source.report.warnings
        if w.message and w.message.startswith(message_prefix)
        for context in w.context
    ]


def test_platform_instance_warning_only_for_listed_mappings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _dremio_source(
        monkeypatch,
        [DremioSourceMapping(source_name="Listed_Src", platform="mssql")],
    )
    source._validate_source_mappings(
        {
            **_source_map(
                "MSSQL",
                None,
                "Listed_Src",
                mappings=[
                    DremioSourceMapping(source_name="Listed_Src", platform="mssql")
                ],
            ),
            **_source_map("POSTGRES", "my_db", "auto_src"),
        }
    )
    assert _warning_contexts(source, "Cross-platform source mapping") == [
        "source=listed_src, platform=mssql"
    ]


def test_unknown_source_category_warns_once_per_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _dremio_source(monkeypatch)
    source._validate_source_mappings(
        {
            **_source_map("MYARPDB", None, "custom_src"),
            **_source_map("POSTGRES", "my_db", "pg_src"),
        }
    )
    assert _warning_contexts(source, "Dremio source type has no built-in") == [
        "source=custom_src, platform=myarpdb"
    ]


@pytest.mark.parametrize(
    "source_type,platform,path,dataset,expected_name",
    [
        pytest.param(
            "ORACLE",
            "oracle",
            ["my_oracle", "MY_SCHEMA"],
            "ORDER",
            "my_schema.ORDER",
            id="oracle_keeps_reserved_word",
        ),
        pytest.param(
            "ORACLE",
            "oracle",
            ["my_oracle", "MY_SCHEMA"],
            "MY#TABLE",
            "my_schema.MY#TABLE",
            id="oracle_keeps_name_needing_quotes",
        ),
        pytest.param(
            "SAPHANA",
            "hana",
            ["my_hana", "MY_SCHEMA"],
            "MY_TABLE",
            "my_schema.my_table",
            id="hana_lowercases_uppercase_identifiers",
        ),
        pytest.param(
            "SAPHANA",
            "hana",
            ["my_hana", "MY_SCHEMA"],
            "My_Table",
            "my_schema.My_Table",
            id="hana_keeps_quoted_mixed_case",
        ),
    ],
)
def test_map_urn_normalizes_like_sqlalchemy_dialects(
    source_type: str, platform: str, path: List[str], dataset: str, expected_name: str
) -> None:
    assert (
        _map_to_urn(source_type, None, path, dataset)
        == f"urn:li:dataset:(urn:li:dataPlatform:{platform},{expected_name},DEV)"
    )


@pytest.mark.parametrize(
    "root_path,path,expected_name",
    [
        pytest.param(
            "/",
            ["my_s3", "my_bucket", "My_Folder"],
            "my_bucket/My_Folder/data.parquet",
            id="root_path_slash_adds_nothing",
        ),
        pytest.param(
            "/My_Bucket/My_Prefix",
            ["my_s3", "My_Folder"],
            "My_Bucket/My_Prefix/My_Folder/data.parquet",
            id="root_path_keeps_case",
        ),
        pytest.param(
            "/",
            ["my_s3"],
            "data.parquet",
            id="dataset_directly_under_source",
        ),
    ],
)
def test_map_urn_file_object_storage_keeps_case(
    root_path: str, path: List[str], expected_name: str
) -> None:
    assert (
        _map_to_urn("S3", None, path, "data.parquet", root_path)
        == f"urn:li:dataset:(urn:li:dataPlatform:s3,{expected_name},DEV)"
    )


def test_map_urn_db2_uppercases_database() -> None:
    # The Db2 connector uppercases the database and keeps the catalog's case
    # for schema and table.
    assert (
        _map_to_urn("DB2", "my_db", ["my_db2", "MY_SCHEMA"], "MY_TABLE")
        == "urn:li:dataset:(urn:li:dataPlatform:db2,MY_DB.MY_SCHEMA.MY_TABLE,DEV)"
    )


def test_map_urn_dremio_to_dremio_uses_dremio_naming() -> None:
    mappings = [
        DremioSourceMapping(
            source_name="remote_dremio",
            platform="dremio",
            platform_instance="remote",
            env="DEV",
        )
    ]
    assert (
        _map_to_urn(
            "DREMIOTODREMIO",
            None,
            ["remote_dremio", "My_Space", "My_Folder"],
            "My_View",
            mappings=mappings,
        )
        == "urn:li:dataset:(urn:li:dataPlatform:dremio,remote.dremio.my_space.my_folder.my_view,DEV)"
    )


def test_mapping_without_ingested_source_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # schema_pattern ^MY_SRC\.MY_DB.* filters out the MY_SRC container itself,
    # so build_dremio_source_map never sees it.
    source = _dremio_source(
        monkeypatch,
        [
            DremioSourceMapping(source_name="MY_SRC", platform="snowflake"),
            DremioSourceMapping(
                source_name="other_src", platform="mssql", platform_instance="i"
            ),
        ],
    )
    source._validate_source_mappings(
        _source_map(
            "MSSQL",
            None,
            "other_src",
            mappings=[
                DremioSourceMapping(
                    source_name="other_src", platform="mssql", platform_instance="i"
                )
            ],
        )
    )
    assert _warning_contexts(source, "source_mappings entry matches no") == [
        "source=my_src"
    ]
