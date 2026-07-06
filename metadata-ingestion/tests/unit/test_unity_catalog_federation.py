from databricks.sdk.service.catalog import CatalogType

from datahub.ingestion.source.unity.proxy_types import Catalog, Metastore


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
