from types import SimpleNamespace

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.bigid.bigid_probe import list_bigid_children
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


class _Connection:
    def __init__(self, name: str):
        self.name = name


class _CatalogObject:
    def __init__(self, source: str, fully_qualified_name: str):
        self.source = source
        self.fully_qualified_name = fully_qualified_name


class _BigIDClient:
    def __init__(self, connections, catalog_objects):
        self._connections = connections
        self._catalog_objects = catalog_objects
        self.closed = False

    def get_connections(self):
        return self._connections

    def get_catalog_objects(self):
        return iter(self._catalog_objects)

    def close(self):
        self.closed = True


def _config():
    client = _BigIDClient(
        connections=[_Connection("snowflake_prod"), _Connection("mysql_dev")],
        catalog_objects=[
            _CatalogObject("snowflake_prod", "snowflake_prod.orders"),
            _CatalogObject("snowflake_prod", "snowflake_prod.tmp_scratch"),
            _CatalogObject("mysql_dev", "mysql_dev.other_table"),
        ],
    )
    return SimpleNamespace(
        get_client=lambda: client,
        connection_pattern=AllowDenyPattern(allow=[".*"], deny=["^mysql_.*"]),
        dataset_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_bigid_lists_connections_with_pattern_verdict():
    result = list_bigid_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["snowflake_prod"].kind == DatasetSubTypes.CONNECTION
    assert by_name["snowflake_prod"].pattern_field == "connection_pattern"
    assert by_name["snowflake_prod"].included is True
    # Reuses the connector's own connection_pattern deny for the verdict.
    assert by_name["mysql_dev"].included is False
    assert by_name["mysql_dev"].excluded_by == "connection_pattern"


def test_bigid_lists_catalog_objects_reusing_dataset_pattern():
    result = list_bigid_children(_config(), ["snowflake_prod"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].fqn == "snowflake_prod.orders"
    assert by_name["orders"].included is True
    # Reuses the connector's own dataset_pattern deny for the verdict.
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "dataset_pattern"
    # Catalog objects belonging to other connections are excluded from this level.
    assert "other_table" not in by_name
