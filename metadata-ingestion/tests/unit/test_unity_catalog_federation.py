from typing import List, Optional, Set
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogType,
    ConnectionInfo,
    ConnectionType,
)
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity import federation as fed
from datahub.ingestion.source.unity.config import (
    UnityCatalogSourceConfig,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Catalog, Metastore, Schema, Table
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    UpstreamLineageClass,
)


def _metastore() -> Metastore:
    return Metastore(
        id="ms",
        name="ms",
        comment=None,
        global_metastore_id=None,
        metastore_id=None,
        owner=None,
        region=None,
        cloud=None,
    )


def test_catalog_carries_federation_fields():
    catalog = Catalog(
        id="c",
        name="c",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn",
        options={"database": "my_db"},
    )
    assert catalog.connection_name == "pg_conn"
    assert catalog.options == {"database": "my_db"}
    assert catalog.is_foreign_catalog is True


def test_managed_catalog_is_not_foreign():
    catalog = Catalog(
        id="c",
        name="c",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.MANAGED_CATALOG,
    )
    assert catalog.is_foreign_catalog is False
    assert catalog.connection_name is None


def _proxy(workspace_client: MagicMock) -> UnityCatalogApiProxy:
    workspace_client.config.warehouse_id = "wh"
    return UnityCatalogApiProxy(
        workspace_client=workspace_client, report=UnityCatalogReport()
    )


def test_connections_returns_dict_keyed_by_name():
    wc = MagicMock(spec=WorkspaceClient)
    wc.connections.list.return_value = [
        ConnectionInfo(name="pg_conn", connection_type=ConnectionType.POSTGRESQL),
        ConnectionInfo(name="ss_conn", connection_type=ConnectionType.SQLSERVER),
    ]
    proxy = _proxy(wc)
    result = proxy.connections()
    assert set(result) == {"pg_conn", "ss_conn"}
    assert result["ss_conn"].connection_type == ConnectionType.SQLSERVER
    # cached: second call does not re-list
    proxy.connections()
    wc.connections.list.assert_called_once()


def test_connections_returns_empty_on_error():
    wc = MagicMock(spec=WorkspaceClient)
    wc.connections.list.side_effect = PermissionError("no access")
    proxy = _proxy(wc)
    assert proxy.connections() == {}
    assert proxy.report.num_federation_connections_list_failed == 1


_BASE = {"workspace_url": "https://x.cloud.databricks.com", "token": "t"}


def test_federation_connection_detail_override():
    cfg = UnityCatalogSourceConfig.model_validate(
        {
            **_BASE,
            "include_federation_lineage": True,
            "federation_connection_details": {
                "pg_conn": {
                    "platform": "postgres",
                    "platform_instance": "prod-pg",
                    "env": "PROD",
                    "database": "my_db",
                }
            },
        }
    )
    assert cfg.include_federation_lineage is True
    detail = cfg.federation_connection_details["pg_conn"]
    assert detail.platform == "postgres"
    assert detail.platform_instance == "prod-pg"
    assert detail.database == "my_db"
    assert detail.convert_urns_to_lowercase is True


def test_federation_connection_detail_env_normalized_and_validated():
    # The env override is upper-cased so the external URN byte-matches the source.
    cfg = UnityCatalogSourceConfig.model_validate(
        {
            **_BASE,
            "federation_connection_details": {"c": {"env": "dev"}},
        }
    )
    assert cfg.federation_connection_details["c"].env == "DEV"

    with pytest.raises(ValidationError):
        UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "federation_connection_details": {"c": {"env": "NOT_AN_ENV"}},
            }
        )


def test_resolve_three_tier_uses_database_option():
    target = fed.resolve_federation_target(
        ConnectionType.POSTGRESQL,
        options={"database": "my_db"},
        override_platform=None,
        override_database=None,
    )
    assert target is not None
    assert target.platform == "postgres"
    assert target.remote_database == "my_db"
    assert fed.external_dataset_name(target, "my_schema", "t") == "my_db.my_schema.t"


def test_resolve_two_tier_has_no_database():
    target = fed.resolve_federation_target(
        ConnectionType.MYSQL,
        options=None,
        override_platform=None,
        override_database=None,
    )
    assert target is not None
    assert target.platform == "mysql"
    assert target.remote_database is None
    assert fed.external_dataset_name(target, "my_schema", "t") == "my_schema.t"


def test_resolve_bigquery_uses_data_project_id():
    target = fed.resolve_federation_target(
        ConnectionType.BIGQUERY,
        options={"dataProjectId": "proj"},
        override_platform=None,
        override_database=None,
    )
    assert target is not None
    assert target.platform == "bigquery"
    assert fed.external_dataset_name(target, "ds", "t") == "proj.ds.t"


def test_resolve_databricks_to_databricks_uses_catalog():
    target = fed.resolve_federation_target(
        ConnectionType.DATABRICKS,
        options={"catalog": "remote_cat"},
        override_platform=None,
        override_database=None,
    )
    assert target is not None
    assert target.platform == "databricks"
    assert fed.external_dataset_name(target, "s", "t") == "remote_cat.s.t"


def test_override_wins_over_autodetect():
    target = fed.resolve_federation_target(
        ConnectionType.POSTGRESQL,
        options={"database": "auto_db"},
        override_platform="mssql",
        override_database="override_db",
    )
    assert target is not None
    assert target.platform == "mssql"
    assert target.remote_database == "override_db"


def test_three_tier_missing_database_returns_none():
    # three-tier connector but options lack the key and no override -> cannot resolve
    assert (
        fed.resolve_federation_target(
            ConnectionType.POSTGRESQL,
            options={},
            override_platform=None,
            override_database=None,
        )
        is None
    )


def test_unmapped_connection_type_returns_none():
    assert (
        fed.resolve_federation_target(
            ConnectionType.UNKNOWN_CONNECTION_TYPE,
            options=None,
            override_platform=None,
            override_database=None,
        )
        is None
    )


def test_override_platform_without_connection_type():
    # connections API unavailable (connection_type None) but user supplied platform+db
    target = fed.resolve_federation_target(
        None, options=None, override_platform="mssql", override_database="my_db"
    )
    assert target is not None
    assert target.platform == "mssql"
    assert target.remote_database == "my_db"


def test_none_connection_type_three_tier_platform_without_database_is_unresolvable():
    # connections API unavailable (connection_type None) and the overridden platform
    # is three-tier (mssql) with no database override: emitting schema.table would
    # drop the database segment and dangle, so resolution must fail (not guess).
    assert (
        fed.resolve_federation_target(
            None, options=None, override_platform="mssql", override_database=None
        )
        is None
    )


def test_none_connection_type_two_tier_platform_is_two_tier():
    # A genuinely two-tier overridden platform (mysql) with no connection type and no
    # database is safe to emit as schema.table.
    target = fed.resolve_federation_target(
        None, options=None, override_platform="mysql", override_database=None
    )
    assert target is not None
    assert target.platform == "mysql"
    assert target.remote_database is None


def test_connection_type_map_platform_and_tier_consistent():
    # Exhaustively pin every mapped connection type's platform and two/three-tier
    # classification so adding/editing a ConnectionType forces this test to be updated.
    two_tier = {
        ConnectionType.MYSQL,
        ConnectionType.GLUE,
        ConnectionType.HIVE_METASTORE,
    }
    for connection_type, mapping in fed.CONNECTION_TYPE_MAP.items():
        options = (
            {mapping.database_option_key: "db"} if mapping.database_option_key else None
        )
        target = fed.resolve_federation_target(
            connection_type,
            options=options,
            override_platform=None,
            override_database=None,
        )
        assert target is not None
        assert target.platform == mapping.platform
        if connection_type in two_tier:
            assert mapping.database_option_key is None
            assert target.remote_database is None
        else:
            assert mapping.database_option_key is not None
            assert target.remote_database == "db"
        assert mapping.platform in fed.KNOWN_FEDERATION_PLATFORMS


def test_resolve_all_none_cannot_resolve():
    # No connection type, no options, no overrides at all: the true cannot-resolve path.
    assert (
        fed.resolve_federation_target(
            None, options=None, override_platform=None, override_database=None
        )
        is None
    )


def test_property_definition_mcps_target_container_and_platform_allowed_values():
    mcps = fed.federation_property_definition_mcps("databricks.federation")
    assert len(mcps) == 4
    by_qn = {
        aspect.qualifiedName: aspect
        for m in mcps
        if isinstance(aspect := m.aspect, StructuredPropertyDefinitionClass)
    }
    platform_def = by_qn["databricks.federation.platform"]
    assert isinstance(platform_def, StructuredPropertyDefinitionClass)
    assert platform_def.entityTypes == ["urn:li:entityType:datahub.container"]
    assert platform_def.valueType == "urn:li:dataType:datahub.string"
    allowed = {av.value for av in (platform_def.allowedValues or [])}
    assert "mssql" in allowed and "postgres" in allowed
    # non-enumerated property has no allowedValues
    assert by_qn["databricks.federation.connection"].allowedValues is None


def test_property_definition_mcps_use_create_if_absent():
    mcps = fed.federation_property_definition_mcps("databricks.federation")
    mcp = mcps[0]
    assert mcp.changeType == ChangeTypeClass.CREATE
    assert mcp.headers == {"If-None-Match": "*"}


def _foreign_catalog():
    ms = Metastore(
        id="ms",
        name="ms",
        comment=None,
        global_metastore_id=None,
        metastore_id=None,
        owner=None,
        region=None,
        cloud=None,
    )
    return Catalog(
        id="c",
        name="my_catalog",
        metastore=ms,
        comment=None,
        owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn",
        options={"database": "my_db"},
    )


def _make_source():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {**_BASE, "include_metastore": False}
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    return src


def test_foreign_catalog_container_has_structured_properties():
    src = _make_source()
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.POSTGRESQL
        )
    }
    wus = list(src.gen_catalog_containers(_foreign_catalog()))
    sp_aspects = [
        wu.get_aspect_of_type(StructuredPropertiesClass)
        for wu in wus
        if wu.get_aspect_of_type(StructuredPropertiesClass) is not None
    ]
    assert len(sp_aspects) == 1
    assigned = {p.propertyUrn: p.values[0] for p in sp_aspects[0].properties}
    assert (
        assigned["urn:li:structuredProperty:databricks.federation.platform"]
        == "postgres"
    )
    assert (
        assigned["urn:li:structuredProperty:databricks.federation.remote_database"]
        == "my_db"
    )


def test_property_definitions_emitted_once_when_enabled():
    src = _make_source()
    src.unity_catalog_api_proxy._connections_cache = {}
    wus = list(src._gen_federation_property_definition_workunits())
    qns = {
        wu.get_aspect_of_type(StructuredPropertyDefinitionClass).qualifiedName
        for wu in wus
    }
    assert qns == {
        "databricks.federation.catalog_type",
        "databricks.federation.platform",
        "databricks.federation.connection",
        "databricks.federation.remote_database",
    }


def test_no_property_definitions_when_disabled():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {**_BASE, "emit_federation_structured_properties": False}
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    assert list(src._gen_federation_property_definition_workunits()) == []


def _managed_catalog() -> Catalog:
    return Catalog(
        id="c",
        name="my_managed_catalog",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.MANAGED_CATALOG,
    )


def _definition_qns(wus: List[MetadataWorkUnit]) -> Set[Optional[str]]:
    return {
        aspect.qualifiedName
        for wu in wus
        if isinstance(
            aspect := wu.get_aspect_of_type(StructuredPropertyDefinitionClass),
            StructuredPropertyDefinitionClass,
        )
    }


def test_property_definitions_emitted_lazily_from_gen_catalog_containers():
    src = _make_source()
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.POSTGRESQL
        )
    }
    assert src._federation_defs_emitted is False

    # First foreign catalog: definitions are emitted exactly once.
    first_wus = list(src.gen_catalog_containers(_foreign_catalog()))
    assert _definition_qns(first_wus) == {
        "databricks.federation.catalog_type",
        "databricks.federation.platform",
        "databricks.federation.connection",
        "databricks.federation.remote_database",
    }
    assert src._federation_defs_emitted is True

    # A second foreign catalog in the same source instance: no more definitions.
    second_wus = list(src.gen_catalog_containers(_foreign_catalog()))
    assert _definition_qns(second_wus) == set()


def test_property_definitions_registered_via_graph_when_available():
    # With a graph (e.g. a datahub-rest sink), definitions must be committed
    # synchronously BEFORE the structuredProperties assignment (GMS validates the
    # assignment against an existing definition). They are emitted via the graph,
    # not yielded into the workunit stream.
    src = _make_source()
    graph = MagicMock()
    src.ctx.graph = graph
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.POSTGRESQL
        )
    }

    wus = list(src.gen_catalog_containers(_foreign_catalog()))

    # 4 definitions committed synchronously via the graph, none in the stream.
    assert graph.emit_mcp.call_count == 4
    assert _definition_qns(wus) == set()
    committed = {
        call.args[0].aspect.qualifiedName for call in graph.emit_mcp.call_args_list
    }
    assert committed == {
        "databricks.federation.catalog_type",
        "databricks.federation.platform",
        "databricks.federation.connection",
        "databricks.federation.remote_database",
    }


def test_property_definitions_not_emitted_for_managed_catalog():
    src = _make_source()
    wus = list(src.gen_catalog_containers(_managed_catalog()))
    assert _definition_qns(wus) == set()
    assert src._federation_defs_emitted is False


def _foreign_table(catalog: Catalog) -> Table:
    schema = Schema(
        id="c.my_schema", name="my_schema", catalog=catalog, comment=None, owner=None
    )
    return Table(
        id="c.my_schema.t",
        name="t",
        comment=None,
        schema=schema,
        columns=[],
        storage_location=None,
        data_source_format=None,
        table_type=None,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=None,
        updated_by=None,
        table_id=None,
        view_definition=None,
        properties={},
    )


def _source_with_link(include_lineage: bool = True) -> UnityCatalogSource:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "include_metastore": False,
                "include_federation_lineage": include_lineage,
                "federation_connection_details": {
                    "pg_conn": {"platform_instance": "prod-pg"}
                },
            }
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.POSTGRESQL
        )
    }
    return src


def test_federation_lineage_mode_emits_upstream():
    src = _source_with_link()
    catalog = _foreign_catalog()
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.t,PROD)"
    )
    wus = list(src._gen_federation_link(dataset_urn, _foreign_table(catalog), catalog))
    up = [
        aspect
        for wu in wus
        if isinstance(
            aspect := wu.get_aspect_of_type(UpstreamLineageClass), UpstreamLineageClass
        )
    ]
    assert up and up[0].upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)"
    )
    assert src.report.num_federation_links_emitted == 1


def test_federation_link_none_emits_nothing():
    src = _source_with_link(include_lineage=False)
    catalog = _foreign_catalog()
    assert list(src._gen_federation_link("x", _foreign_table(catalog), catalog)) == []


def test_federation_link_skipped_for_managed_catalog():
    # The COPY edge must never land on a regular (non-foreign) managed table.
    src = _source_with_link()
    catalog = _foreign_catalog()
    catalog.type = CatalogType.MANAGED_CATALOG
    catalog.connection_name = None
    assert list(src._gen_federation_link("x", _foreign_table(catalog), catalog)) == []
    assert src.report.num_federation_links_emitted == 0


def test_federation_resolution_distinguishes_catalogs_sharing_a_connection():
    # Two foreign catalogs on the same connection but different remote databases must
    # resolve to different external URNs — the resolution cache is keyed by catalog,
    # not by connection.
    src = _source_with_link()
    cat_a = _foreign_catalog()  # name="my_catalog", options database=my_db
    cat_b = Catalog(
        id="c2",
        name="other_catalog",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn",
        options={"database": "other_db"},
    )
    urn_a = src._external_dataset_urn(cat_a, "my_schema", "t")
    urn_b = src._external_dataset_urn(cat_b, "my_schema", "t")
    assert urn_a == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)"
    )
    assert urn_b == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.other_db.my_schema.t,PROD)"
    )


def test_federation_link_lowercase_applied():
    src = _source_with_link()  # external URN is lower-cased by default
    catalog = Catalog(
        id="c",
        name="My_Catalog",
        metastore=Metastore(
            id="ms",
            name="ms",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        ),
        comment=None,
        owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn",
        options={"database": "My_DB"},
    )
    schema = Schema(
        id="c.My_Schema", name="My_Schema", catalog=catalog, comment=None, owner=None
    )
    table = Table(
        id="c.My_Schema.T",
        name="T",
        comment=None,
        schema=schema,
        columns=[],
        storage_location=None,
        data_source_format=None,
        table_type=None,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=None,
        updated_by=None,
        table_id=None,
        view_definition=None,
        properties={},
    )
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.t,PROD)"
    )
    wus = list(src._gen_federation_link(dataset_urn, table, catalog))
    up = [
        aspect
        for wu in wus
        if isinstance(
            aspect := wu.get_aspect_of_type(UpstreamLineageClass), UpstreamLineageClass
        )
    ]
    assert up[0].upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)"
    )


def test_unresolved_target_warns_once_and_caches():
    src = _source_with_link()
    # No override in federation_connection_details for this connection, and the
    # connections API doesn't know about it either -> cannot resolve.
    src.unity_catalog_api_proxy._connections_cache = {}
    catalog = Catalog(
        id="c",
        name="unresolvable_catalog",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="unknown_conn",
        options={},
    )
    table = _foreign_table(catalog)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,c.my_schema.t,PROD)"

    wus = list(src._gen_federation_link(dataset_urn, table, catalog))
    assert wus == []
    assert src.report.num_federation_targets_unresolved == 1
    assert any(
        w.title == "Could not resolve Lakehouse Federation target"
        for w in src.report.warnings
    )

    # A second table in the same catalog must not double-count or re-warn.
    more_wus = list(src._gen_federation_link(dataset_urn, table, catalog))
    assert more_wus == []
    assert src.report.num_federation_targets_unresolved == 1


def test_per_connection_lowercase_false_preserves_case():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "include_metastore": False,
                "include_federation_lineage": True,
                "federation_connection_details": {
                    "pg_conn": {
                        "platform_instance": "prod-pg",
                        # opt out of the default lower-casing for this connection
                        "convert_urns_to_lowercase": False,
                    }
                },
            }
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.POSTGRESQL
        )
    }
    catalog = Catalog(
        id="c",
        name="My_Catalog",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn",
        options={"database": "My_DB"},
    )
    schema = Schema(
        id="c.My_Schema", name="My_Schema", catalog=catalog, comment=None, owner=None
    )
    table = Table(
        id="c.My_Schema.T",
        name="T",
        comment=None,
        schema=schema,
        columns=[],
        storage_location=None,
        data_source_format=None,
        table_type=None,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=None,
        updated_by=None,
        table_id=None,
        view_definition=None,
        properties={},
    )
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.t,PROD)"
    )
    wus = list(src._gen_federation_link(dataset_urn, table, catalog))
    up = [
        aspect
        for wu in wus
        if isinstance(
            aspect := wu.get_aspect_of_type(UpstreamLineageClass), UpstreamLineageClass
        )
    ]
    # convert_urns_to_lowercase=False on the connection preserves the mixed case of
    # the external URN, to match an external source ingested case-sensitively.
    assert up[0].upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.My_DB.My_Schema.T,PROD)"
    )


def _snowflake_conn_source() -> UnityCatalogSource:
    """Source whose proxy resolves pg_conn -> a SNOWFLAKE connection."""
    src = _make_source()
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.SNOWFLAKE
        )
    }
    return src


def _external_schema(fields: List[SchemaFieldClass]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="ext",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=MySqlDDLClass(tableSchema=""),
        fields=fields,
    )


def _field(path: str) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="NUMBER",
    )


def _seed_external_schema(
    src: UnityCatalogSource,
    catalog: Catalog,
    schema_name: str,
    table_name: str,
    schema_info: dict,
) -> str:
    """Seed a bulk-initialized SchemaResolver under the catalog's external schema
    scope so backfill / column-lineage resolve the external schema in-memory, and
    return the external URN. Mirrors what SchemaResolverProvider does at runtime (one
    scroll per external database)."""
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    urn = src._external_dataset_urn(catalog, schema_name, table_name)
    key = src._external_platform_key(catalog)
    assert urn is not None and key is not None
    resolver = SchemaResolver(
        platform=key.platform,
        platform_instance=key.platform_instance,
        env=key.env,
        graph=None,
    )
    resolver.add_raw_schema_info(urn, schema_info)
    src._external_schema_resolvers[key] = resolver
    return urn


def test_resolve_external_schema_fields_backfills_from_external():
    src = _snowflake_conn_source()
    catalog = _foreign_catalog()  # connection pg_conn, options {"database": "my_db"}
    table = _foreign_table(catalog)  # schema my_schema, table t, no columns
    _seed_external_schema(
        src,
        catalog,
        "my_schema",
        "t",
        {"invoice_id": "NUMBER", "total_amount": "DECIMAL"},
    )

    fields = src._resolve_external_schema_fields(table)

    assert fields is not None
    assert [f.fieldPath for f in fields] == ["invoice_id", "total_amount"]
    # native type is carried from the external source's schema
    assert fields[1].nativeDataType == "DECIMAL"


def test_resolve_external_schema_fields_none_for_managed_catalog():
    src = _snowflake_conn_source()
    src.ctx.graph = MagicMock()
    managed = Catalog(
        id="c",
        name="c",
        metastore=_metastore(),
        comment=None,
        owner=None,
        type=CatalogType.MANAGED_CATALOG,
    )
    assert src._resolve_external_schema_fields(_foreign_table(managed)) is None


def test_resolve_external_schema_fields_none_without_graph():
    src = _snowflake_conn_source()  # ctx.graph is None by default
    assert (
        src._resolve_external_schema_fields(_foreign_table(_foreign_catalog())) is None
    )


def test_resolve_external_schema_fields_none_when_disabled():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "include_metastore": False,
                "include_federation_column_backfill": False,
            }
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    src.ctx.graph = MagicMock()
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.SNOWFLAKE
        )
    }
    assert (
        src._resolve_external_schema_fields(_foreign_table(_foreign_catalog())) is None
    )


def test_schema_metadata_backfilled_for_foreign_table_without_columns():
    src = _snowflake_conn_source()
    catalog = _foreign_catalog()
    table = _foreign_table(catalog)  # no UC columns
    _seed_external_schema(
        src,
        catalog,
        "my_schema",
        "t",
        {"invoice_id": "NUMBER", "total_amount": "NUMBER"},
    )
    schema_metadata, _ = src._create_schema_metadata_aspect(table)
    assert [f.fieldPath for f in schema_metadata.fields] == [
        "invoice_id",
        "total_amount",
    ]
    assert src.report.num_federation_columns_backfilled == 1


def test_schema_metadata_prefers_uc_columns_when_present():
    from datahub.ingestion.source.unity.proxy_types import Column

    src = _snowflake_conn_source()
    catalog = _foreign_catalog()
    # even with an external schema available, UC's own columns take precedence
    _seed_external_schema(src, catalog, "my_schema", "t", {"should_not_be_used": "INT"})
    schema = Schema(
        id="c.my_schema", name="my_schema", catalog=catalog, comment=None, owner=None
    )
    col = Column(
        id="c.my_schema.t.uc_col",
        name="uc_col",
        type_text="int",
        type_name=None,
        type_precision=None,
        type_scale=None,
        position=0,
        nullable=True,
        comment=None,
    )
    table = Table(
        id="c.my_schema.t",
        name="t",
        comment=None,
        schema=schema,
        columns=[col],
        storage_location=None,
        data_source_format=None,
        table_type=None,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=None,
        updated_by=None,
        table_id=None,
        view_definition=None,
        properties={},
    )
    schema_metadata, _ = src._create_schema_metadata_aspect(table)
    assert [f.fieldPath for f in schema_metadata.fields] == ["uc_col"]
    assert src.report.num_federation_columns_backfilled == 0


def test_backfilled_fields_carry_structure_only():
    # Backfill builds columns from the external source's field path + native type;
    # governance (tags / glossary terms) is never copied onto the mirror.
    src = _snowflake_conn_source()
    catalog = _foreign_catalog()
    _seed_external_schema(src, catalog, "my_schema", "t", {"invoice_id": "NUMBER"})

    fields = src._resolve_external_schema_fields(_foreign_table(catalog))

    assert fields is not None
    assert fields[0].fieldPath == "invoice_id"
    assert fields[0].nativeDataType == "NUMBER"
    assert fields[0].globalTags is None
    assert fields[0].glossaryTerms is None


def test_identity_column_lineage_matches_case_insensitively():
    from datahub.metadata.schema_classes import FineGrainedLineageClass

    dbx = "urn:li:dataset:(urn:li:dataPlatform:databricks,c.s.t,PROD)"
    ext = "urn:li:dataset:(urn:li:dataPlatform:snowflake,demo_db.s.t,PROD)"
    cll = fed.identity_column_lineage(
        dataset_urn=dbx,
        external_urn=ext,
        downstream_field_paths=["CUSTOMER_ID", "NAME", "ONLY_IN_DBX"],
        upstream_field_paths=["customer_id", "name", "only_in_ext"],
    )
    assert all(isinstance(x, FineGrainedLineageClass) for x in cll)
    # 2 matched (case-insensitive); unmatched dbx column dropped
    pairs = [(x.upstreams, x.downstreams) for x in cll]
    assert pairs == [
        (
            ["urn:li:schemaField:(%s,customer_id)" % ext],
            ["urn:li:schemaField:(%s,CUSTOMER_ID)" % dbx],
        ),
        (
            ["urn:li:schemaField:(%s,name)" % ext],
            ["urn:li:schemaField:(%s,NAME)" % dbx],
        ),
    ]


def test_identity_column_lineage_empty_when_no_overlap():
    assert (
        fed.identity_column_lineage(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:databricks,c.s.t,PROD)",
            external_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,d.s.t,PROD)",
            downstream_field_paths=["a"],
            upstream_field_paths=["x", "y"],
        )
        == []
    )


def _lineage_cll_source():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "include_metastore": False,
                "include_federation_lineage": True,
                "include_column_lineage": True,
            }
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.SNOWFLAKE
        )
    }
    return src


def test_federation_lineage_emits_identity_column_lineage():
    src = _lineage_cll_source()
    catalog = _foreign_catalog()
    table = _foreign_table(catalog)
    # external snowflake schema has lowercase field paths
    _seed_external_schema(
        src, catalog, "my_schema", "t", {"customer_id": "NUMBER", "name": "TEXT"}
    )

    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,"
        "datahub_snowflake.my_schema.t,PROD)"
    )
    # the databricks-side schema has uppercase field paths
    dbx_schema = _external_schema([_field("CUSTOMER_ID"), _field("NAME")])

    wus = list(src._gen_federation_link(dataset_urn, table, catalog, dbx_schema))
    up = [
        agg
        for wu in wus
        if isinstance(
            agg := wu.get_aspect_of_type(UpstreamLineageClass), UpstreamLineageClass
        )
    ]
    assert up, "expected an UpstreamLineage workunit"
    fgl = up[0].fineGrainedLineages
    assert fgl is not None and len(fgl) == 2  # CUSTOMER_ID<-customer_id, NAME<-name


def test_federation_lineage_no_column_lineage_when_flag_off():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "include_metastore": False,
                "include_federation_lineage": True,
                "include_column_lineage": False,
            }
        )
        src = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)
    src.unity_catalog_api_proxy._connections_cache = {
        "pg_conn": ConnectionInfo(
            name="pg_conn", connection_type=ConnectionType.SNOWFLAKE
        )
    }
    src.ctx.graph = MagicMock()
    catalog = _foreign_catalog()
    dbx_schema = _external_schema([_field("CUSTOMER_ID")])
    wus = list(
        src._gen_federation_link(
            "urn:li:dataset:(urn:li:dataPlatform:databricks,datahub_snowflake.my_schema.t,PROD)",
            _foreign_table(catalog),
            catalog,
            dbx_schema,
        )
    )
    up = [
        agg
        for wu in wus
        if isinstance(
            agg := wu.get_aspect_of_type(UpstreamLineageClass), UpstreamLineageClass
        )
    ]
    assert up and not up[0].fineGrainedLineages


def test_external_schema_fetch_scoped_to_remote_database():
    # The bulk fetch must be scoped to the mirrored remote database, not the whole
    # external platform — otherwise federating a few tables from a huge source pulls
    # the entire platform's schemas.
    src = _snowflake_conn_source()
    provider = MagicMock()
    src._schema_resolver_provider = provider
    key = src._external_platform_key(_foreign_catalog())
    assert key is not None and key.remote_database == "my_db"

    src._external_schema_resolver(key)

    provider.get.assert_called_once_with(
        platform="snowflake",
        platform_instance=None,
        env="PROD",
        query="my_db",
    )


def test_federation_cll_skipped_counted_when_external_schema_missing():
    # UC supplies columns but the external schema can't be resolved (no graph here):
    # the COPY link is still emitted, column-level lineage is skipped and counted.
    src = _lineage_cll_source()
    catalog = _foreign_catalog()
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,"
        "datahub_snowflake.my_schema.t,PROD)"
    )
    dbx_schema = _external_schema([_field("CUSTOMER_ID")])
    wus = list(
        src._gen_federation_link(
            dataset_urn, _foreign_table(catalog), catalog, dbx_schema
        )
    )
    up = [
        agg
        for wu in wus
        if isinstance(
            agg := wu.get_aspect_of_type(UpstreamLineageClass), UpstreamLineageClass
        )
    ]
    assert up and not up[0].fineGrainedLineages
    assert src.report.num_federation_cll_skipped == 1
