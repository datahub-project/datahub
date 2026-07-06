from unittest.mock import MagicMock

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogType,
    ConnectionInfo,
    ConnectionType,
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
