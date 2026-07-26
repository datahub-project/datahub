from types import SimpleNamespace

from datahub.configuration.common import AllowDenyPattern
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


def _config():
    return SimpleNamespace(
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


def test_iceberg_table_pattern_matches_the_fully_qualified_name():
    # iceberg.py's _process_dataset matches table_pattern against the dotted
    # "<namespace>.<table>" identifier (e.g. "analytics.tmp_scratch"), not the
    # bare table name — so a deny anchored to the bare name ("^tmp_.*") never
    # matches and does NOT exclude the table; this is what real ingestion does
    # too.
    result = list_iceberg_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    assert by_name["tmp_scratch"].included is True

    # A deny anchored to the fully qualified name does exclude it.
    fqn_config = _config()
    fqn_config.table_pattern = AllowDenyPattern(
        allow=[".*"], deny=[r"^analytics\.tmp_scratch$"]
    )
    result = list_iceberg_children(fqn_config, ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"
    assert by_name["orders"].included is True


def test_iceberg_nested_namespace_target_matches_ingestions_dotted_identifier():
    # Regression guard for the nested-namespace claim in iceberg_probe.py: the
    # probe's parent_path element is already a dotted namespace string (see
    # _namespaces), while ingestion's Identifier is a flat tuple of every
    # namespace segment plus the table. Both must dot-join to the same string.
    from datahub.ingestion.agent.probe import ClassifyContext
    from datahub.ingestion.source.iceberg.iceberg import dataset_name

    probe_target = dataset_name(list(("a.b",)) + ["orders"])
    ingestion_target = dataset_name(("a", "b", "orders"))
    assert probe_target == ingestion_target == "a.b.orders"

    ctx = ClassifyContext(
        config=SimpleNamespace(table_pattern=AllowDenyPattern.allow_all()),
        name="orders",
        fqn="a.b.orders",
        pattern_field="table_pattern",
        parent_path=("a.b",),
    )
    assert dataset_name(list(ctx.parent_path) + [ctx.name]) == ingestion_target
