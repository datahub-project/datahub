import json
from typing import Dict, Iterable, List, Optional, Tuple
from unittest.mock import Mock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.genie import GenieSpaceExtractor
from datahub.ingestion.source.unity.genie_client import GenieSpace, GenieSpaceClient
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Column,
    Metastore,
    Schema,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
)

VIEW = "c.s.revenue"


@pytest.fixture(autouse=True)
def _skip_without_metric_view_enum() -> None:
    from databricks.sdk.service.catalog import TableType

    if not hasattr(TableType, "METRIC_VIEW"):
        pytest.skip("Installed databricks-sdk lacks TableType.METRIC_VIEW")


@pytest.fixture
def source() -> Iterable[UnityCatalogSource]:
    yield from _source(emit_semantic_model_entities=True)


def _source(**config: object) -> Iterable[UnityCatalogSource]:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        instance = UnityCatalogSource(
            ctx=PipelineContext(run_id="test-run"),
            config=UnityCatalogSourceConfig.model_validate(
                {
                    "token": "t",
                    "workspace_url": "https://test.databricks.com",
                    "include_hive_metastore": False,
                    "include_tags": False,
                    "include_ownership": False,
                    "include_metric_views": True,
                    **config,
                }
            ),
        )
        instance._resolve_semantic_model_emission()
        yield instance
        instance.close()


def _metric_view(yaml_body: str, columns: Tuple[str, ...] = ()) -> Tuple[Table, Schema]:
    from databricks.sdk.service.catalog import TableType

    metastore = Metastore(
        id=None,
        name="metastore",
        comment=None,
        global_metastore_id=None,
        metastore_id=None,
        owner=None,
        region=None,
        cloud=None,
    )
    catalog = Catalog(
        id="c", name="c", metastore=metastore, comment=None, owner=None, type=None
    )
    schema = Schema(id="c.s", name="s", catalog=catalog, comment=None, owner=None)
    table = Table(
        id=VIEW,
        name="revenue",
        comment=None,
        schema=schema,
        columns=[
            Column(
                id=f"{VIEW}.{name}",
                name=name,
                type_text="date" if name.endswith("_date") else "string",
                type_name=None,
                type_precision=None,
                type_scale=None,
                position=None,
                nullable=True,
                comment=None,
            )
            for name in columns
        ],
        storage_location=None,
        data_source_format=None,
        table_type=TableType.METRIC_VIEW,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=None,
        updated_by=None,
        table_id=None,
        view_definition=yaml_body,
        properties={},
    )
    return table, schema


def _emit(source: UnityCatalogSource, yaml_body: str) -> List[MetadataWorkUnit]:
    table, schema = _metric_view(yaml_body, columns=("order_date", "region"))
    proxy = Mock()
    proxy.tables.return_value = [table]
    source.unity_catalog_api_proxy = proxy
    return list(source.process_tables(schema))


def _aspects(workunits: List[MetadataWorkUnit], aspect_type: type) -> Dict[str, object]:
    found: Dict[str, object] = {}
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, aspect_type
        ):
            found[str(wu.metadata.entityUrn)] = wu.metadata.aspect
    return found


def _dataset(alias: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:databricks,{VIEW}.{alias},PROD)"


def _metric(name: str) -> str:
    return f"urn:li:metric:(urn:li:dataPlatform:databricks,{VIEW},{name})"


STAR_SCHEMA = """
version: 1.1
source: c.s.orders
filter: status = 'F'
joins:
  - name: customer
    source: c.s.customers
    on: source.customer_id = customer.id
    joins:
      - name: nation
        source: c.s.nations
        using: [nation_key]
dimensions:
  - name: order_date
    expr: order_date
  - name: region
    expr: customer.region
  - name: label
    expr: concat(source.code, nation.name)
measures:
  - name: revenue
    expr: SUM(amount)
  - name: rows
    expr: COUNT(1)
  - name: per_customer
    expr: MEASURE(revenue) / COUNT(DISTINCT customer.id)
"""


def test_metric_view_replaces_dataset_with_semantic_model(
    source: UnityCatalogSource,
) -> None:
    workunits = _emit(source, STAR_SCHEMA)

    subtypes = _aspects(workunits, SubTypesClass)
    assert not any(
        isinstance(aspect, SubTypesClass) and "Metric View" in aspect.typeNames
        for aspect in subtypes.values()
    )
    assert len(_aspects(workunits, SemanticModelInfoClass)) == 1
    assert set(_aspects(workunits, SemanticModelPropertiesClass)) == {
        _dataset("source"),
        _dataset("customer"),
        _dataset("nation"),
    }
    # Usage and profiling would otherwise recreate the dataset this mode drops.
    assert not source.table_refs
    assert source.report.metric_views.processed_entities


def test_dimensions_land_on_the_table_they_read(source: UnityCatalogSource) -> None:
    workunits = _emit(source, STAR_SCHEMA)
    lineage = _aspects(workunits, UpstreamLineageClass)

    def downstream_fields(alias: str) -> List[str]:
        aspect = lineage[_dataset(alias)]
        assert isinstance(aspect, UpstreamLineageClass)
        return sorted(
            downstream.split(",")[-1].rstrip(")")
            for fgl in aspect.fineGrainedLineages or []
            for downstream in fgl.downstreams or []
        )

    # `label` reads two tables, so it stays at the `source` grain.
    assert "label" in downstream_fields("source")
    assert "order_date" in downstream_fields("source")
    assert "region" in downstream_fields("customer")
    assert source.report.num_metric_view_dimensions_on_source_multi_alias == 1


def test_joins_become_relationships_with_key_fields(
    source: UnityCatalogSource,
) -> None:
    workunits = _emit(source, STAR_SCHEMA)
    (model_info,) = _aspects(workunits, SemanticModelInfoClass).values()
    assert isinstance(model_info, SemanticModelInfoClass)
    relationships = {
        (r.from_, tuple(r.fromColumns or []), r.to, tuple(r.toColumns or []))
        for r in model_info.relationships or []
    }
    assert relationships == {
        ("source", ("customer_id",), "customer", ("id",)),
        ("customer", ("nation_key",), "nation", ("nation_key",)),
    }


def test_unqualified_join_condition_is_not_guessed(
    source: UnityCatalogSource,
) -> None:
    workunits = _emit(
        source,
        """
version: 1.1
source: c.s.orders
joins:
  - name: customer
    source: c.s.customers
    on: customer_id = id
measures:
  - name: rows
    expr: COUNT(1)
""",
    )
    (model_info,) = _aspects(workunits, SemanticModelInfoClass).values()
    assert isinstance(model_info, SemanticModelInfoClass)
    assert not model_info.relationships
    assert source.report.num_metric_view_relationships_skipped == 1


def test_measures_become_metrics_with_lineage(source: UnityCatalogSource) -> None:
    workunits = _emit(source, STAR_SCHEMA)
    upstreams = _aspects(workunits, MetricUpstreamsClass)
    relationships = _aspects(workunits, MetricRelationshipsClass)

    def upstream_urns(name: str) -> List[str]:
        aspect = upstreams[_metric(name)]
        assert isinstance(aspect, MetricUpstreamsClass)
        return [edge.destinationUrn for edge in aspect.datasetUpstreams or []]

    def derived_from(name: str) -> List[str]:
        aspect = relationships[_metric(name)]
        assert isinstance(aspect, MetricRelationshipsClass)
        return [edge.destinationUrn for edge in aspect.derivedFrom or []]

    assert upstream_urns("revenue") == [_dataset("source")]
    # COUNT(1) reads no column but still counts `source` rows.
    assert upstream_urns("rows") == [_dataset("source")]
    assert upstream_urns("per_customer") == [_dataset("customer")]
    assert derived_from("per_customer") == [_metric("revenue")]
    assert derived_from("revenue") == []


def test_view_filter_is_recorded_without_rewriting_the_expression(
    source: UnityCatalogSource,
) -> None:
    from datahub.metadata.schema_classes import MetricInfoClass

    workunits = _emit(source, STAR_SCHEMA)
    info = _aspects(workunits, MetricInfoClass)[_metric("revenue")]
    assert isinstance(info, MetricInfoClass)
    assert info.expression is not None
    (dialect,) = info.expression.dialects
    assert dialect.dialect == "DATABRICKS"
    assert dialect.expression == "SUM(amount) /* filter: status = 'F' */"


def test_unreadable_yaml_falls_back_to_dataset(source: UnityCatalogSource) -> None:
    workunits = _emit(source, 'version: 1.1\nsource: "unterminated\n')
    assert not _aspects(workunits, SemanticModelInfoClass)
    assert any(
        isinstance(aspect, SubTypesClass) and "Metric View" in aspect.typeNames
        for aspect in _aspects(workunits, SubTypesClass).values()
    )
    assert source.report.num_metric_views_semantic_model_fallback == 1
    assert source.report.num_metric_views_yaml_parse_failures == 1


@pytest.mark.parametrize(
    "config, enabled",
    [
        ({"emit_semantic_model_entities": True}, True),
        ({"emit_semantic_model_entities": False}, False),
        # No graph to follow, so the default stays on datasets.
        ({}, False),
        (
            {"emit_semantic_model_entities": True, "include_metric_views": False},
            False,
        ),
    ],
)
def test_emission_switch(config: Dict[str, object], enabled: bool) -> None:
    for instance in _source(**config):
        assert (instance._metric_view_semantic_model_mapper is not None) is enabled


def _genie_space(identifier: str, *, metric_view: bool) -> GenieSpace:
    return GenieSpace(
        space_id="space-1",
        title="Sales",
        description=None,
        serialized_space=json.dumps(
            {
                "version": 2,
                "data_sources": {
                    "metric_views" if metric_view else "tables": [
                        {"identifier": identifier}
                    ]
                },
            }
        ),
    )


@pytest.mark.parametrize("metric_view", [True, False])
def test_genie_points_metric_views_at_logical_datasets(
    source: UnityCatalogSource, metric_view: bool
) -> None:
    logical = [_dataset("source"), _dataset("customer")]

    def metric_view_urns(ref: TableReference) -> Optional[List[str]]:
        return logical if ref.qualified_table_name == VIEW else None

    client = Mock(spec=GenieSpaceClient)
    space = _genie_space(VIEW, metric_view=metric_view)
    client.list_spaces.return_value = [space]
    client.get_space.return_value = space
    workunits = list(
        GenieSpaceExtractor(
            config=source.config,
            client=client,
            report=source.report,
            table_urn_builder=source.gen_dataset_urn,
            platform_instance=None,
            metastore=None,
            preserve_urn=lambda urn: None,
            preserve_all_urns=lambda: None,
            metric_view_urns_builder=metric_view_urns,
        ).get_workunits()
    )
    (lineage,) = _aspects(workunits, UpstreamLineageClass).values()
    assert isinstance(lineage, UpstreamLineageClass)
    upstreams = sorted(u.dataset for u in lineage.upstreams)
    if metric_view:
        assert upstreams == sorted(logical)
    else:
        # A plain table with the same name keeps its own dataset URN.
        assert upstreams == [
            f"urn:li:dataset:(urn:li:dataPlatform:databricks,{VIEW},PROD)"
        ]
