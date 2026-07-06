from unittest.mock import MagicMock

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogType,
    ConnectionInfo,
    ConnectionType,
)

from datahub.ingestion.source.unity import federation as fed
from datahub.ingestion.source.unity.config import (
    FederationLinkType,
    UnityCatalogSourceConfig,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Catalog, Metastore
from datahub.ingestion.source.unity.report import UnityCatalogReport


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


def test_federation_config_defaults():
    cfg = UnityCatalogSourceConfig.model_validate(_BASE)
    assert cfg.federation_link_type == FederationLinkType.SIBLINGS
    assert cfg.emit_federation_structured_properties is True
    assert cfg.federation_structured_property_namespace == "databricks.federation"
    assert cfg.federation_connection_details == {}


def test_federation_connection_detail_override():
    cfg = UnityCatalogSourceConfig.model_validate(
        {
            **_BASE,
            "federation_link_type": "lineage",
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
    assert cfg.federation_link_type == FederationLinkType.LINEAGE
    detail = cfg.federation_connection_details["pg_conn"]
    assert detail.platform == "postgres"
    assert detail.platform_instance == "prod-pg"
    assert detail.database == "my_db"
    assert detail.convert_urns_to_lowercase is None


def test_resolve_three_tier_uses_database_option():
    target = fed.resolve_federation_target(
        ConnectionType.POSTGRESQL, {"database": "my_db"}, None, None
    )
    assert target is not None
    assert target.platform == "postgres"
    assert target.remote_database == "my_db"
    assert fed.external_dataset_name(target, "my_schema", "t") == "my_db.my_schema.t"


def test_resolve_two_tier_has_no_database():
    target = fed.resolve_federation_target(ConnectionType.MYSQL, None, None, None)
    assert target is not None
    assert target.platform == "mysql"
    assert target.remote_database is None
    assert fed.external_dataset_name(target, "my_schema", "t") == "my_schema.t"


def test_resolve_bigquery_uses_data_project_id():
    target = fed.resolve_federation_target(
        ConnectionType.BIGQUERY, {"dataProjectId": "proj"}, None, None
    )
    assert target.platform == "bigquery"
    assert fed.external_dataset_name(target, "ds", "t") == "proj.ds.t"


def test_resolve_databricks_to_databricks_uses_catalog():
    target = fed.resolve_federation_target(
        ConnectionType.DATABRICKS, {"catalog": "remote_cat"}, None, None
    )
    assert target.platform == "databricks"
    assert fed.external_dataset_name(target, "s", "t") == "remote_cat.s.t"


def test_override_wins_over_autodetect():
    target = fed.resolve_federation_target(
        ConnectionType.POSTGRESQL, {"database": "auto_db"}, "mssql", "override_db"
    )
    assert target.platform == "mssql"
    assert target.remote_database == "override_db"


def test_three_tier_missing_database_returns_none():
    # three-tier connector but options lack the key and no override -> cannot resolve
    assert (
        fed.resolve_federation_target(ConnectionType.POSTGRESQL, {}, None, None) is None
    )


def test_unmapped_connection_type_returns_none():
    assert (
        fed.resolve_federation_target(
            ConnectionType.UNKNOWN_CONNECTION_TYPE, None, None, None
        )
        is None
    )


def test_override_platform_without_connection_type():
    # connections API unavailable (connection_type None) but user supplied platform+db
    target = fed.resolve_federation_target(None, None, "mssql", "my_db")
    assert target.platform == "mssql"
    assert target.remote_database == "my_db"


def test_none_connection_type_without_database_is_two_tier():
    target = fed.resolve_federation_target(None, None, "mssql", None)
    assert target is not None
    assert target.platform == "mssql"
    assert target.remote_database is None


def test_structured_property_urns():

    urns = fed.structured_property_urns("databricks.federation")
    assert (
        urns["platform"] == "urn:li:structuredProperty:databricks.federation.platform"
    )
    assert set(urns) == {"catalog_type", "platform", "connection", "remote_database"}


def test_property_definition_mcps_target_container_and_platform_allowed_values():
    from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass

    mcps = fed.federation_property_definition_mcps("databricks.federation")
    assert len(mcps) == 4
    by_qn = {m.aspect.qualifiedName: m.aspect for m in mcps}
    platform_def = by_qn["databricks.federation.platform"]
    assert isinstance(platform_def, StructuredPropertyDefinitionClass)
    assert platform_def.entityTypes == ["urn:li:entityType:datahub.container"]
    assert platform_def.valueType == "urn:li:dataType:datahub.string"
    allowed = {av.value for av in (platform_def.allowedValues or [])}
    assert "mssql" in allowed and "postgres" in allowed
    # non-enumerated property has no allowedValues
    assert by_qn["databricks.federation.connection"].allowedValues is None
