from typing import Any, Callable

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.iceberg.iceberg_probe import list_iceberg_children


class _Catalog:
    def __init__(self, namespaces, tables):
        self._namespaces = namespaces
        self._tables = tables

    def list_namespaces(self):
        return self._namespaces

    def list_tables(self, namespace):
        return self._tables.get(namespace, [])


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for namespace_pattern/table_pattern, which the probe now
# resolves by convention rather than declaring explicitly.
class _Config(ConfigModel):
    get_catalog: Callable[[], Any]
    namespace_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def _config():
    return _Config(
        get_catalog=lambda: _Catalog(
            [("analytics",), ("staging",)],
            {"analytics": [("analytics", "orders"), ("analytics", "tmp_scratch")]},
        ),
        namespace_pattern=AllowDenyPattern(allow=[".*"]),
        table_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_iceberg_lists_namespaces_with_pattern_verdict():
    result = list_iceberg_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["analytics"].kind == DatasetContainerSubTypes.NAMESPACE
    assert by_name["analytics"].pattern_field == "namespace_pattern"
    assert by_name["analytics"].included is True


def test_iceberg_lists_tables_reusing_table_pattern():
    result = list_iceberg_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    # The connector's own table_pattern deny (^tmp_) is reused for the verdict.
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"
