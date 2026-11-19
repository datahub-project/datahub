from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import patch

import pytest
from databricks.sdk.service.catalog import TableType

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.metric_view_semantic_model import (
    parse_metric_view_expr,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Column,
    Schema,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.source import UnityCatalogSource

pytestmark = pytest.mark.skipif(
    not hasattr(TableType, "METRIC_VIEW"),
    reason="Installed databricks-sdk lacks TableType.METRIC_VIEW",
)

_MODEL = "urn:li:semanticModel:(urn:li:dataPlatform:databricks,main.sales,orders_mv)"


def _metric(name: str, path: str = "main.sales.orders_mv") -> str:
    return f"urn:li:metric:(urn:li:dataPlatform:databricks,{path},{name})"


def _smd(alias: str) -> str:
    return (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,"
        f"main.sales.orders_mv.{alias},PROD)"
    )


def _physical(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:databricks,main.sales.{table},PROD)"


@pytest.fixture(autouse=True)
def _mock_workspace_client() -> Iterator[None]:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        yield


def _schema() -> Schema:
    catalog = Catalog(
        id="main", name="main", metastore=None, comment=None, owner=None, type=None
    )
    return Schema(
        id="main.sales", name="sales", catalog=catalog, comment=None, owner=None
    )


def _table(
    yaml_body: Optional[str],
    *,
    name: str = "orders_mv",
    table_type: TableType = TableType.METRIC_VIEW,
    columns: Optional[List[Column]] = None,
) -> Table:
    return Table(
        id=f"main.sales.{name}",
        name=name,
        comment="UC comment",
        schema=_schema(),
        columns=columns or [],
        storage_location=None,
        data_source_format=None,
        table_type=table_type,
        owner=None,
        generation=None,
        created_at=datetime(2024, 1, 1),
        created_by=None,
        updated_at=datetime(2024, 1, 2),
        updated_by=None,
        table_id=f"{name}-id",
        view_definition=yaml_body,
        properties={},
    )


def _source(**extra: Any) -> UnityCatalogSource:
    config = UnityCatalogSourceConfig.model_validate(
        {
            "token": "t",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_metric_views": True,
            "include_tags": False,
            **extra,
        }
    )
    source = UnityCatalogSource.create(config, PipelineContext(run_id="t"))
    source._resolve_semantic_model_emission()
    return source


def _aspects(source: UnityCatalogSource, yaml_body: str) -> Dict[str, Dict[str, Any]]:
    """aspect name -> {urn: aspect} for a metric view mapped in SM mode."""
    wus = source._gen_metric_view_semantic_model(_table(yaml_body), _schema())
    assert wus is not None
    out: Dict[str, Dict[str, Any]] = {}
    for wu in wus:
        mcp = wu.metadata
        out.setdefault(mcp.aspectName, {})[mcp.entityUrn] = mcp.aspect  # type: ignore
    return out


_SIMPLE = """
version: 1.1
source: main.sales.orders
filter: "o_orderstatus = 'F'"
joins:
  - name: customer
    source: main.sales.customer
    on: o_custkey = c_custkey
dimensions:
  - name: order_date
    expr: o_orderdate
    synonyms: [date, day]
  - name: customer_name
    expr: customer.c_name
measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    display_name: Total revenue
    synonyms: [revenue]
  - name: order_count
    expr: COUNT(1)
  - name: aov
    expr: MEASURE(total_revenue) / MEASURE(order_count)
  - name: revenue_or_zero
    expr: COALESCE(MEASURE(total_revenue), MAX(o_totalprice))
  - name: broken_ratio
    expr: MEASURE(total_revenue) / MEASURE(no_such_measure)
"""


class TestGate:
    def test_default_without_graph_stays_legacy(self) -> None:
        source = _source()
        assert source.report.semantic_model_emission_effective is False
        assert not source._is_semantic_model_metric_view(_table(_SIMPLE))

    def test_explicit_true_enables(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        assert source.report.semantic_model_emission_effective is True
        assert source._is_semantic_model_metric_view(_table(_SIMPLE))

    def test_explicit_false_forces_legacy(self) -> None:
        source = _source(emit_semantic_model_entities=False)
        assert source.report.semantic_model_emission_effective is False

    def test_not_resolved_without_metric_views(self) -> None:
        source = _source(include_metric_views=False, emit_semantic_model_entities=True)
        assert source.report.semantic_model_emission_effective is None
        assert not source._is_semantic_model_metric_view(_table(_SIMPLE))


class TestMigration:
    """ING-2944: in SM mode the legacy `Metric View` dataset is not emitted."""

    def _process(self, source: UnityCatalogSource, table: Table) -> List[Any]:
        with patch.object(
            source.unity_catalog_api_proxy, "tables", return_value=[table]
        ):
            return [wu.metadata for wu in source.process_tables(_schema())]

    def test_semantic_model_mode_replaces_dataset(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        table = _table(_SIMPLE)
        mcps = self._process(source, table)
        urns = {m.entityUrn for m in mcps}
        assert source.gen_dataset_urn(table.ref) not in urns
        assert _MODEL in urns
        # Out of table_refs, so usage/operations never write to the old URN.
        assert table.ref not in source.table_refs
        assert source.report.metric_views.processed_entities

    def test_legacy_mode_keeps_dataset(self) -> None:
        source = _source(emit_semantic_model_entities=False)
        table = _table(_SIMPLE)
        mcps = self._process(source, table)
        urns = {m.entityUrn for m in mcps}
        assert source.gen_dataset_urn(table.ref) in urns
        assert _MODEL not in urns
        assert table.ref in source.table_refs

    def test_unreadable_yaml_falls_back_to_legacy_dataset(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        table = _table("source: [unterminated")
        mcps = self._process(source, table)
        assert source.gen_dataset_urn(table.ref) in {m.entityUrn for m in mcps}
        assert table.ref in source.table_refs
        assert source.report.num_metric_view_semantic_model_fallbacks == 1

    def test_regular_tables_unaffected(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        table = _table(None, name="orders", table_type=TableType.MANAGED)
        mcps = self._process(source, table)
        assert source.gen_dataset_urn(table.ref) in {m.entityUrn for m in mcps}


class TestMapping:
    def test_simple_metric_view(self) -> None:
        aspects = _aspects(_source(emit_semantic_model_entities=True), _SIMPLE)
        info = aspects["semanticModelInfo"][_MODEL]
        assert info.name == "orders_mv"
        assert info.description == "UC comment"
        assert info.nativeDefinition == _SIMPLE
        assert set(aspects["metricInfo"]) == {
            _metric(n)
            for n in (
                "total_revenue",
                "order_count",
                "aov",
                "revenue_or_zero",
                "broken_ratio",
            )
        }
        revenue = aspects["metricInfo"][_metric("total_revenue")]
        assert revenue.name == "Total revenue"
        assert revenue.expression.dialects[0].dialect == "DATABRICKS"
        assert revenue.expression.dialects[0].expression == "SUM(o_totalprice)"
        assert aspects["aiContext"][_metric("total_revenue")].synonyms == ["revenue"]
        annotation = aspects["semanticFieldAnnotation"][
            f"urn:li:schemaField:({_smd('source')},total_revenue)"
        ]
        assert annotation.type == "MEASURE"
        assert annotation.aggregationFunction == "SUM"
        field_ai = aspects["aiContext"][
            f"urn:li:schemaField:({_smd('source')},order_date)"
        ]
        assert field_ai.synonyms == ["date", "day"]

    def test_unqualified_on_predicate_is_parent_equals_join(self) -> None:
        aspects = _aspects(_source(emit_semantic_model_entities=True), _SIMPLE)
        (rel,) = aspects["semanticModelInfo"][_MODEL].relationships
        assert (rel.from_, rel.fromColumns, rel.to, rel.toColumns) == (
            "source",
            ["o_custkey"],
            "customer",
            ["c_custkey"],
        )
        assert rel.cardinality == "N_ONE"

    def test_using_join_and_nested_join(self) -> None:
        body = """
source: main.sales.orders
joins:
  - name: customer
    source: main.sales.customer
    using: [custkey]
    joins:
      - name: nation
        source: sales.nation
        on: nation.n_nationkey = customer.c_nationkey
dimensions:
  - name: nation_name
    expr: customer.nation.n_name
measures:
  - name: n
    expr: COUNT(1)
"""
        aspects = _aspects(_source(emit_semantic_model_entities=True), body)
        rels = {
            (r.from_, r.to): (r.fromColumns, r.toColumns)
            for r in aspects["semanticModelInfo"][_MODEL].relationships
        }
        # The reversed predicate is normalised to parent -> join.
        assert rels == {
            ("source", "customer"): (["custkey"], ["custkey"]),
            ("customer", "customer.nation"): (["c_nationkey"], ["n_nationkey"]),
        }
        lineage = aspects["upstreamLineage"][_smd("customer.nation")]
        # 2-part source resolves against the metric view's catalog.
        assert lineage.upstreams[0].dataset == _physical("nation")
        assert lineage.fineGrainedLineages[0].upstreams == [
            f"urn:li:schemaField:({_physical('nation')},n_name)"
        ]

    def test_composable_measures(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        aspects = _aspects(source, _SIMPLE)
        rels = aspects["metricRelationships"]
        assert [e.destinationUrn for e in rels[_metric("aov")].derivedFrom] == [
            _metric("order_count"),
            _metric("total_revenue"),
        ]
        # A measure composed only of MEASURE() refs reads no dataset itself.
        aov = aspects["metricUpstreams"][_metric("aov")]
        assert aov.datasetUpstreams == [] and aov.fieldUpstreams == []
        # Mixed: derivedFrom plus its own column.
        mixed = aspects["metricUpstreams"][_metric("revenue_or_zero")]
        assert [e.destinationUrn for e in mixed.datasetUpstreams] == [_smd("source")]
        assert [
            e.destinationUrn for e in rels[_metric("revenue_or_zero")].derivedFrom
        ] == [_metric("total_revenue")]
        # Unresolved refs are reported, resolved ones still linked.
        assert [
            e.destinationUrn for e in rels[_metric("broken_ratio")].derivedFrom
        ] == [_metric("total_revenue")]
        assert source.report.num_metric_view_unresolved_measure_refs == 1
        assert rels[_metric("total_revenue")].derivedFrom == []

    def test_window_measure(self) -> None:
        body = """
source: main.sales.orders
dimensions:
  - name: order_date
    expr: o_orderdate
measures:
  - name: rolling
    expr: COUNT(DISTINCT o_orderkey)
    window:
      - order: order_date
        range: trailing 7 day
        semiadditive: last
"""
        aspects = _aspects(_source(emit_semantic_model_entities=True), body)
        expr = aspects["metricInfo"][_metric("rolling")].expression.dialects[0]
        assert expr.expression == (
            "COUNT(DISTINCT o_orderkey) -- window: order=order_date, "
            "range=trailing 7 day, semiadditive=last"
        )
        props = aspects["datasetProperties"][_smd("source")].customProperties
        assert props["metric_view.field.rolling.window.range"] == "trailing 7 day"
        annotation = aspects["semanticFieldAnnotation"][
            f"urn:li:schemaField:({_smd('source')},rolling)"
        ]
        assert annotation.aggregationFunction == "COUNT_DISTINCT"

    def test_materialization_and_filter_extensions(self) -> None:
        body = """
version: 1.1
comment: From YAML
source: main.sales.orders
filter: amount > 0
materialization:
  schedule: every 6 hours
  mode: relaxed
dimensions:
  - name: d
    expr: o_d
    format:
      type: date
      date_format: year_month_day
measures:
  - name: m
    expr: SUM(amount)
"""
        aspects = _aspects(_source(emit_semantic_model_entities=True), body)
        assert aspects["semanticModelInfo"][_MODEL].description == "From YAML"
        props = aspects["datasetProperties"][_smd("source")].customProperties
        assert props["metric_view.filter"] == "amount > 0"
        assert props["metric_view.spec_version"] == "1.1"
        assert props["metric_view.materialization.schedule"] == "every 6 hours"
        assert props["metric_view.materialization.mode"] == "relaxed"
        assert props["metric_view.field.d.format.type"] == "date"

    def test_platform_instance_in_urns(self) -> None:
        source = _source(emit_semantic_model_entities=True, platform_instance="ws1")
        aspects = _aspects(source, _SIMPLE)
        model = (
            "urn:li:semanticModel:(urn:li:dataPlatform:databricks,"
            "ws1.main.sales,orders_mv)"
        )
        assert model in aspects["semanticModelInfo"]
        assert _metric("aov", "ws1.main.sales.orders_mv") in aspects["metricInfo"]
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,"
            "ws1.main.sales.orders_mv.source,PROD)"
        ) in aspects["upstreamLineage"]

    def test_subquery_source_uses_rest_lineage(self) -> None:
        body = """
source: SELECT * FROM main.sales.orders WHERE x > 1
dimensions:
  - name: d
    expr: x
measures:
  - name: m
    expr: COUNT(1)
"""
        source = _source(emit_semantic_model_entities=True)
        with patch.object(
            source, "_metric_view_rest_upstreams", return_value=[_physical("orders")]
        ):
            aspects = _aspects(source, body)
        upstreams = aspects["upstreamLineage"][_smd("source")].upstreams
        assert [u.dataset for u in upstreams] == [_physical("orders")]


def test_nested_joins_with_the_same_name_stay_apart() -> None:
    body = """
source: main.sales.orders
joins:
  - name: buyer
    source: main.sales.customer
    on: source.buyer_id = buyer.id
    joins:
      - name: country
        source: main.sales.country
        on: buyer.country_id = country.id
  - name: seller
    source: main.sales.customer
    on: source.seller_id = seller.id
    joins:
      - name: country
        source: main.sales.country_v2
        on: seller.country_id = country.id
dimensions:
  - name: buyer_country
    expr: buyer.country.name
  - name: seller_country
    expr: seller.country.name
measures:
  - name: n
    expr: COUNT(1)
"""
    aspects = _aspects(_source(emit_semantic_model_entities=True), body)
    lineage = aspects["upstreamLineage"]
    assert lineage[_smd("buyer.country")].fineGrainedLineages[0].upstreams == [
        f"urn:li:schemaField:({_physical('country')},name)"
    ]
    assert lineage[_smd("seller.country")].fineGrainedLineages[0].upstreams == [
        f"urn:li:schemaField:({_physical('country_v2')},name)"
    ]
    rels = {
        (r.from_, r.to): (r.fromColumns, r.toColumns)
        for r in aspects["semanticModelInfo"][_MODEL].relationships
    }
    assert rels[("seller", "seller.country")] == (["country_id"], ["id"])


def test_shared_expr_parser_resolves_source_alias() -> None:
    ref = TableReference(metastore=None, catalog="c", schema="s", table="t")
    parsed = parse_metric_view_expr(
        "SUM(source.a) + MEASURE(x) + b", {"": ref, "source": ref}
    )
    assert {(c.alias, c.column) for c in parsed.columns} == {
        ("source", "a"),
        ("", "b"),
    }
    assert parsed.aggregation_function is None
    assert parse_metric_view_expr("SUM(", {}).error is not None
