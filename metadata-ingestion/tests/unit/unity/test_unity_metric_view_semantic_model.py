import json
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableType

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.semantic_model_gate import (
    ResolvedEmitDecision,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.metric_view_semantic_model import (
    MetricViewReportBuffer,
    MetricViewSemanticModelInput,
    parse_metric_view_expr,
)
from datahub.ingestion.source.unity.proxy import (
    TableInfoWithGeneration,
    UnityCatalogApiProxy,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Column,
    Metastore,
    Schema,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    UpstreamLineageClass,
)

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


def _unresolved_source(**extra: Any) -> UnityCatalogSource:
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
    return UnityCatalogSource.create(config, PipelineContext(run_id="t"))


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


def _gen(source: UnityCatalogSource, table: Table) -> Optional[List[Any]]:
    spec = source._load_metric_view_spec(table)
    assert spec is not None
    return source._gen_metric_view_semantic_model(table, _schema(), spec)


def _aspects(source: UnityCatalogSource, yaml_body: str) -> Dict[str, Dict[str, Any]]:
    """aspect name -> {urn: aspect} for a metric view mapped in SM mode."""
    wus = _gen(source, _table(yaml_body))
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


def _metric_view_column(
    name: str,
    metadata: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    raw_type_json: Optional[str] = None,
) -> Column:
    type_json = raw_type_json
    if type_json is None and metadata is not None:
        type_json = json.dumps({"name": name, "type": "double", "metadata": metadata})
    return Column(
        id=f"main.sales.orders_mv.{name}",
        name=name,
        comment=comment,
        type_text="double",
        type_name=None,
        type_precision=None,
        type_scale=None,
        position=None,
        nullable=True,
        type_json=type_json,
    )


_NORMALIZED = """
version: 1.1
source: main.sales.orders
dimensions:
  - name: region
    expr: region_cd
measures:
  - name: revenue
    expr: SUM(amount)
  - name: margin
    expr: SUM(margin)
    display_name: From YAML
"""


class TestColumnMetadataMerge:
    """Unity Catalog strips field metadata from the YAML and keeps it on columns."""

    def _spec(self, source: UnityCatalogSource, table: Table) -> Dict[str, Any]:
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        return {e["name"]: e for e in spec["dimensions"] + spec["measures"]}

    def test_column_metadata_fills_what_the_yaml_lacks(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        table = _table(
            _NORMALIZED,
            columns=[
                _metric_view_column(
                    "region",
                    {
                        "metric_view.type": "dimension",
                        "semantic_metadata": json.dumps(
                            {"display_name": "Region", "synonyms": ["territory"]}
                        ),
                    },
                    comment="Sales region.",
                ),
                _metric_view_column(
                    "revenue",
                    {
                        "comment": "Booked revenue.",
                        "semantic_metadata": json.dumps(
                            {
                                "display_name": "Revenue",
                                "format": {"currency": {"currency_code": "EUR"}},
                            }
                        ),
                        "metric_view.window": [
                            {"order": "region", "range": "current", "offset": ""}
                        ],
                    },
                ),
                _metric_view_column(
                    "margin",
                    {"semantic_metadata": json.dumps({"display_name": "From column"})},
                ),
            ],
        )
        table.properties["metric_view.where"] = "amount > 0"
        entries = self._spec(source, table)
        assert entries["region"]["comment"] == "Sales region."
        assert entries["region"]["display_name"] == "Region"
        assert entries["region"]["synonyms"] == ["territory"]
        assert entries["revenue"]["comment"] == "Booked revenue."
        assert entries["revenue"]["format"] == {
            "type": "currency",
            "currency_code": "EUR",
        }
        assert entries["revenue"]["window"][0]["order"] == "region"
        # A value in the YAML wins over the column.
        assert entries["margin"]["display_name"] == "From YAML"
        spec = source._load_metric_view_spec(table)
        assert spec is not None and spec["filter"] == "amount > 0"

        aspects = _aspects_for_table(source, table)
        assert aspects["metricInfo"][_metric("revenue")].name == "Revenue"
        assert aspects["aiContext"][
            f"urn:li:schemaField:({_smd('source')},region)"
        ].synonyms == ["territory"]
        props = aspects["datasetProperties"][_smd("source")].customProperties
        assert props["metric_view.field.revenue.format.currency_code"] == "EUR"
        assert props["metric_view.filter"] == "amount > 0"

    def test_bad_json_is_counted_not_raised(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        table = _table(
            _NORMALIZED,
            columns=[
                _metric_view_column("region", raw_type_json="{not json"),
                _metric_view_column("revenue", {"semantic_metadata": "{also not json"}),
                _metric_view_column("margin", {"semantic_metadata": "[1, 2]"}),
            ],
        )
        entries = self._spec(source, table)
        assert "display_name" not in entries["region"]
        assert source.report.num_metric_view_column_metadata_invalid == 3
        assert _aspects_for_table(source, table)["metricInfo"]

    def test_columns_without_type_json_change_nothing(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        table = _table(_NORMALIZED, columns=[_metric_view_column("region")])
        assert "display_name" not in self._spec(source, table)["region"]
        assert source.report.num_metric_view_column_metadata_invalid == 0


def _aspects_for_table(
    source: UnityCatalogSource, table: Table
) -> Dict[str, Dict[str, Any]]:
    wus = _gen(source, table)
    assert wus is not None
    out: Dict[str, Dict[str, Any]] = {}
    for wu in wus:
        mcp = wu.metadata
        out.setdefault(mcp.aspectName, {})[mcp.entityUrn] = mcp.aspect  # type: ignore
    return out


@pytest.mark.parametrize(
    "table_type, keeps_type_json",
    [(TableType.METRIC_VIEW, True), (TableType.MANAGED, False)],
)
def test_proxy_keeps_type_json_for_metric_views_only(
    table_type: TableType, keeps_type_json: bool
) -> None:
    proxy = UnityCatalogApiProxy(
        MagicMock(), report=UnityCatalogReport(), hive_metastore_proxy=None
    )
    type_json = '{"name": "c", "type": "double", "metadata": {"comment": "x"}}'
    info = TableInfoWithGeneration(
        name="orders_mv",
        catalog_name="main",
        schema_name="sales",
        table_type=table_type,
        columns=[ColumnInfo(name="c", type_text="double", type_json=type_json)],
    )
    table = proxy._create_table(_schema(), info)
    assert table is not None
    (column,) = table.columns
    assert column.type_json == (type_json if keeps_type_json else None)


_FIELDS_SPEC = """
version: 1.1
source: main.sales.orders
joins:
  - name: customer
    source: main.sales.customer
    on: source.custkey = customer.custkey
    rely:
      at_most_one_match: true
  - name: line
    source: main.sales.lineitem
    on: source.orderkey = line.orderkey
    cardinality: one_to_many
  - name: odd
    source: main.sales.odd
    on: source.k = odd.k
    cardinality: many_to_many
fields:
  - name: region
    expr: region_cd
    display_name: Region
  - name: customer_name
    expr: customer.c_name
dimensions:
  - name: order_date
    expr: o_orderdate
  - name: region
    expr: other_region_cd
measures:
  - name: revenue
    expr: SUM(amount)
"""


class TestFieldsKeyAndJoins:
    def test_fields_and_dimensions_both_read_on_semantic_model_path(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        aspects = _aspects(source, _FIELDS_SPEC)
        annotations = aspects["semanticFieldAnnotation"]
        dims = {
            urn.rsplit(",", 1)[-1].rstrip(")")
            for urn, a in annotations.items()
            if a.type == "DIMENSION"
            and a.expression.dialects[0].expression
            in ("region_cd", "customer.c_name", "o_orderdate")
        }
        assert dims == {"region", "customer_name", "order_date"}
        # `fields` wins a duplicate name; the other definition is reported.
        region = annotations[f"urn:li:schemaField:({_smd('source')},region)"]
        assert region.expression.dialects[0].expression == "region_cd"
        assert any(
            "both fields and dimensions" in (w.title or "")
            for w in source.report.warnings
        )
        props = aspects["datasetProperties"][_smd("source")].customProperties
        assert props["metric_view.field.region.display_name"] == "Region"

    def test_fields_key_on_legacy_path(self) -> None:
        source = _source(emit_semantic_model_entities=False)
        table = _table(_FIELDS_SPEC)
        by_name = {
            wu.metadata.aspectName: wu.metadata.aspect
            for wu in source.process_table(table, _schema())
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        }
        props = by_name["datasetProperties"]
        assert isinstance(props, DatasetPropertiesClass)
        assert props.customProperties["metric_view.field.region.display_name"] == (
            "Region"
        )
        assert (
            props.customProperties["metric_view.join.customer.rely.at_most_one_match"]
            == "true"
        )
        upstreams = by_name["upstreamLineage"]
        assert isinstance(upstreams, UpstreamLineageClass)
        downstream_fields = {
            f.downstreams[0].rsplit(",", 1)[-1].rstrip(")")
            for f in upstreams.fineGrainedLineages or []
            if f.downstreams
        }
        assert {"region", "customer_name", "order_date"} <= downstream_fields

    def test_join_cardinality_and_rely(self) -> None:
        source = _source(emit_semantic_model_entities=True)
        aspects = _aspects(source, _FIELDS_SPEC)
        cardinality = {
            r.to: r.cardinality
            for r in aspects["semanticModelInfo"][_MODEL].relationships
        }
        assert cardinality == {
            "customer": "N_ONE",  # default
            "line": "ONE_N",
            "odd": None,  # unrecognized: reported, not guessed
        }
        assert any(
            "Unrecognized metric view join cardinality" in (w.title or "")
            for w in source.report.warnings
        )
        props = aspects["datasetProperties"][_smd("source")].customProperties
        assert props["metric_view.join.customer.rely.at_most_one_match"] == "true"
        assert props["metric_view.join.line.cardinality"] == "one_to_many"


def _decision(enabled: bool, **flags: bool) -> ResolvedEmitDecision:
    return ResolvedEmitDecision(
        enabled=enabled,
        reason="test",
        is_saas=True,
        version="v2.0.0",
        metrics_enabled=None,
        entity_types_capable=False,
        **flags,
    )


def _process(source: UnityCatalogSource, table: Table) -> List[Any]:
    with patch.object(source.unity_catalog_api_proxy, "tables", return_value=[table]):
        return [wu.metadata for wu in source.process_tables(_schema())]


@pytest.mark.parametrize(
    "recipe, decision, title",
    [
        (
            None,
            _decision(False, version_unparseable=True),
            "Could not parse DataHub server version",
        ),
        (
            None,
            _decision(False, metrics_probe_failed=True),
            "Could not verify Metrics kill-switch",
        ),
        (True, _decision(False), "emit_semantic_model_entities forced off"),
    ],
)
def test_emission_vetoes_warn_and_keep_the_legacy_dataset(
    recipe: Optional[bool], decision: ResolvedEmitDecision, title: str
) -> None:
    source = _unresolved_source(emit_semantic_model_entities=recipe)
    with patch(
        "datahub.ingestion.source.unity.source.resolve_emit_semantic_model_entities",
        autospec=True,
        return_value=decision,
    ):
        source._resolve_semantic_model_emission()
    assert source.report.semantic_model_emission_effective is False
    assert source.report.semantic_model_emission_reason == "test"
    assert source.report.semantic_model_emission_is_saas is True
    assert title in {w.title for w in source.report.warnings}
    table = _table(_SIMPLE)
    urns = {m.entityUrn for m in _process(source, table)}
    assert source.gen_dataset_urn(table.ref) in urns
    assert _MODEL not in urns


# One field whose display name is truncated, one unknown qualifier, and one
# column whose type_json is not JSON: each is counted once, whichever path
# finally emits the view.
_COUNTED = """
source: main.sales.orders
dimensions:
  - name: region
    expr: bogus.region_cd
    display_name: "{long_name}"
measures:
  - name: revenue
    expr: SUM(amount)
""".replace("{long_name}", "x" * 300)


def _counted_table() -> Table:
    return _table(
        _COUNTED,
        columns=[_metric_view_column("revenue", raw_type_json="{not json")],
    )


def _counters(report: UnityCatalogReport) -> Dict[str, int]:
    return {
        "truncated": report.num_metric_view_display_name_truncated,
        "unresolved": report.num_metric_view_unresolved_qualifiers,
        "invalid": report.num_metric_view_column_metadata_invalid,
    }


def test_mapping_failure_falls_back_without_double_counting() -> None:
    legacy = _source(emit_semantic_model_entities=False)
    _process(legacy, _counted_table())

    source = _source(emit_semantic_model_entities=True)

    def fail_midway(
        inp: MetricViewSemanticModelInput, buffer: MetricViewReportBuffer
    ) -> List[Any]:
        buffer.count("num_metric_view_unresolved_qualifiers", 7)
        buffer.warning(message="discarded", context="x")
        raise RuntimeError("mapper blew up")

    table = _counted_table()
    with patch.object(
        source.metric_view_semantic_model_mapper,
        "gen_workunits",
        side_effect=fail_midway,
    ):
        urns = {m.entityUrn for m in _process(source, table)}

    assert source.gen_dataset_urn(table.ref) in urns
    assert _MODEL not in urns
    assert source.report.num_metric_view_semantic_model_fallbacks == 1
    # Exactly what the legacy path alone counts: the semantic-model attempt's
    # counters and warnings were discarded, and the spec was loaded once.
    assert (
        _counters(source.report)
        == _counters(legacy.report)
        == {
            "truncated": 1,
            "unresolved": 1,
            "invalid": 1,
        }
    )
    assert "discarded" not in {w.message for w in source.report.warnings}


def test_semantic_model_path_counts_once() -> None:
    source = _source(emit_semantic_model_entities=True)
    _process(source, _counted_table())
    assert source.report.num_metric_view_semantic_model_fallbacks == 0
    assert _counters(source.report) == {
        "truncated": 1,
        "unresolved": 1,
        "invalid": 1,
    }


def test_synonym_and_display_name_limits_shared_by_both_paths() -> None:
    synonyms = ", ".join(f"s{i}" for i in range(12))
    body = f"""
source: main.sales.orders
measures:
  - name: revenue
    expr: SUM(amount)
    display_name: "{"d" * 300}"
    synonyms: [{synonyms}]
"""
    aspects = _aspects(_source(emit_semantic_model_entities=True), body)
    metric = aspects["metricInfo"][_metric("revenue")]
    assert len(metric.name) == 255
    assert aspects["aiContext"][_metric("revenue")].synonyms == [
        f"s{i}" for i in range(10)
    ]


def test_include_metastore_urns_and_two_part_source() -> None:
    metastore = Metastore(
        id="ms",
        name="ms",
        comment=None,
        global_metastore_id=None,
        metastore_id=None,
        owner=None,
        region=None,
        cloud=None,
    )
    catalog = Catalog(
        id="main", name="main", metastore=metastore, comment=None, owner=None, type=None
    )
    schema = Schema(
        id="main.sales", name="sales", catalog=catalog, comment=None, owner=None
    )
    table = _table(
        """
source: sales.orders
dimensions:
  - name: d
    expr: o_d
measures:
  - name: m
    expr: SUM(amount)
"""
    )
    table.schema = schema
    table.ref = TableReference.create(table)
    source = _source(
        emit_semantic_model_entities=True, include_metastore=True, workspace_name="ws"
    )
    spec = source._load_metric_view_spec(table)
    assert spec is not None
    wus = source._gen_metric_view_semantic_model(table, schema, spec)
    assert wus is not None
    by_aspect: Dict[str, Dict[str, Any]] = {}
    for wu in wus:
        mcp = wu.metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        by_aspect.setdefault(mcp.aspectName or "", {})[mcp.entityUrn or ""] = mcp.aspect

    model = "urn:li:semanticModel:(urn:li:dataPlatform:databricks,ws.ms.main.sales,orders_mv)"
    assert set(by_aspect["semanticModelInfo"]) == {model}
    assert (
        "urn:li:metric:(urn:li:dataPlatform:databricks,ws.ms.main.sales.orders_mv,m)"
        in by_aspect["metricInfo"]
    )
    smd = (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,"
        "ws.ms.main.sales.orders_mv.source,PROD)"
    )
    # The 2-part source resolves against the view's catalog, and the upstream is
    # the URN the physical table gets in the same run.
    physical = source.gen_dataset_urn(
        TableReference(metastore="ms", catalog="main", schema="sales", table="orders")
    )
    assert physical == (
        "urn:li:dataset:(urn:li:dataPlatform:databricks,ws.ms.main.sales.orders,PROD)"
    )
    assert [u.dataset for u in by_aspect["upstreamLineage"][smd].upstreams] == [
        physical
    ]
    browse = by_aspect["browsePathsV2"][model].path
    assert browse[0].id == "ws"
    assert browse[1].urn == source.gen_metastore_key(metastore).as_urn()
    assert len(browse) == 4
