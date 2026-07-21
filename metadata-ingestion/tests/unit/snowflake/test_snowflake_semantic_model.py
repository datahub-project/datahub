import datetime
from typing import Dict, List, Optional, Type, TypeVar

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeSemanticView,
    SnowflakeSemanticViewRelationship,
    SnowflakeTag,
)
from datahub.ingestion.source.snowflake.snowflake_semantic_model import (
    SnowflakeSemanticModelMapper,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DimensionClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    ModelDatasetClass,
    SemanticFieldClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    StatusClass,
    StructuredPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
)

_DB = "TEST_DB"
_SCHEMA = "PUBLIC"


def _col(
    name: str,
    data_type: str,
    subtype: SemanticViewColumnSubtype,
    table_name: Optional[str] = None,
    comment: Optional[str] = None,
    synonyms: Optional[List[str]] = None,
    expression: Optional[str] = None,
) -> SemanticViewColumnMetadata:
    return SemanticViewColumnMetadata(
        name=name,
        data_type=data_type,
        comment=comment,
        subtype=subtype,
        table_name=table_name,
        synonyms=synonyms or [],
        expression=expression,
    )


def _make_semantic_view(
    column_occurrences: Dict[str, List[SemanticViewColumnMetadata]],
    logical_to_physical_table: Optional[Dict[str, tuple]] = None,
    resolved_upstream_urns: Optional[List[str]] = None,
    primary_key_columns: Optional[set] = None,
    tags: Optional[list] = None,
    column_tags: Optional[dict] = None,
    relationships: Optional[List[SnowflakeSemanticViewRelationship]] = None,
) -> SnowflakeSemanticView:
    return SnowflakeSemanticView(
        name="Sales_Analytics",
        created=datetime.datetime(2024, 1, 1),
        comment="Sales semantic view",
        view_definition="CREATE SEMANTIC VIEW Sales_Analytics AS ...",
        last_altered=datetime.datetime(2024, 2, 1),
        column_occurrences=column_occurrences,
        logical_to_physical_table=logical_to_physical_table
        or {
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
            "CUSTOMERS": (_DB, _SCHEMA, "CUSTOMERS"),
        },
        resolved_upstream_urns=resolved_upstream_urns or [],
        primary_key_columns=primary_key_columns or set(),
        tags=tags,
        column_tags=column_tags or {},
        relationships=relationships or [],
    )


def _make_mapper(
    convert_urns_to_lowercase: bool = True,
    platform_instance: Optional[str] = None,
    include_view_definitions: bool = True,
    extract_tags_as_structured_properties: bool = False,
) -> SnowflakeSemanticModelMapper:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
            "convert_urns_to_lowercase": convert_urns_to_lowercase,
            "platform_instance": platform_instance,
            "include_view_definitions": include_view_definitions,
            "extract_tags_as_structured_properties": extract_tags_as_structured_properties,
        }
    )
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    )
    return SnowflakeSemanticModelMapper(
        config=config, report=report, identifiers=identifiers
    )


_AspectT = TypeVar("_AspectT")


def _aspects_for(
    workunits: List[MetadataWorkUnit], entity_urn: str, aspect_type: Type[_AspectT]
) -> List[_AspectT]:
    results: List[_AspectT] = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        if wu.metadata.entityUrn == entity_urn and isinstance(
            wu.metadata.aspect, aspect_type
        ):
            results.append(wu.metadata.aspect)
    return results


def _fields_by_path(dataset: ModelDatasetClass) -> Dict[str, SemanticFieldClass]:
    assert dataset.fields is not None
    return {f.schemaField.fieldPath: f for f in dataset.fields}


def test_urn_builders_default_lowercase():
    mapper = _make_mapper()
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        "Sales_Analytics", _SCHEMA, _DB
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "Total_Revenue", "Sales_Analytics", _SCHEMA, _DB
    )
    assert model_urn == (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"
    )
    assert metric_urn == (
        "urn:li:metric:(urn:li:dataPlatform:snowflake,"
        "test_db.public.sales_analytics,total_revenue)"
    )


def test_urn_builders_platform_instance_and_no_lowercase():
    mapper = _make_mapper(
        convert_urns_to_lowercase=False, platform_instance="my_instance"
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        "Sales_Analytics", _SCHEMA, _DB
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "Total_Revenue", "Sales_Analytics", _SCHEMA, _DB
    )
    assert model_urn == (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,"
        f"my_instance.{_DB}.{_SCHEMA},Sales_Analytics)"
    )
    assert metric_urn == (
        "urn:li:metric:(urn:li:dataPlatform:snowflake,"
        f"my_instance.{_DB}.{_SCHEMA}.Sales_Analytics,Total_Revenue)"
    )


def test_semantic_model_info_datasets_and_field_grouping():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "CUSTOMER_ID": [
                _col(
                    "Customer_Id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="CUSTOMERS",
                    comment="Customer ID",
                )
            ],
            "ORDER_DATE": [
                _col(
                    "Order_Date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
            "ORDER_TOTAL": [
                _col(
                    "Order_Total",
                    "NUMBER(10,2)",
                    SemanticViewColumnSubtype.FACT,
                    table_name="ORDERS",
                    expression="ORDER_TOTAL",
                )
            ],
            "TOTAL_REVENUE": [
                _col(
                    "Total_Revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.order_total)",
                )
            ],
        },
        primary_key_columns={"CUSTOMER_ID"},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    infos = _aspects_for(workunits, model_urn, SemanticModelInfoClass)
    assert len(infos) == 1
    info = infos[0]
    assert info.name == "Sales_Analytics"
    assert info.description == "Sales semantic view"

    datasets_by_name = {d.name: d for d in info.datasets}
    assert set(datasets_by_name) == {"ORDERS", "CUSTOMERS"}

    orders_fields = _fields_by_path(datasets_by_name["ORDERS"])
    assert set(orders_fields) == {"order_date", "order_total"}
    assert orders_fields["order_date"].type == SemanticFieldTypeClass.DIMENSION
    assert orders_fields["order_total"].type == SemanticFieldTypeClass.MEASURE

    customers_fields = _fields_by_path(datasets_by_name["CUSTOMERS"])
    assert set(customers_fields) == {"customer_id"}
    assert customers_fields["customer_id"].schemaField.isPartOfKey is True

    # Metrics are not fields on any dataset.
    for fields in (orders_fields, customers_fields):
        assert "total_revenue" not in fields


def test_dimension_is_time_for_date_type_and_measure_for_fact():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
            "CUSTOMER_NAME": [
                _col(
                    "customer_name",
                    "VARCHAR(100)",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
            "ORDER_TOTAL": [
                _col(
                    "order_total",
                    "NUMBER",
                    SemanticViewColumnSubtype.FACT,
                    table_name="ORDERS",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    fields_by_path = _fields_by_path(info.datasets[0])

    assert fields_by_path["order_date"].dimension == DimensionClass(isTime=True)
    assert fields_by_path["customer_name"].dimension == DimensionClass(isTime=False)
    # FACT columns are MEASUREs and never carry a dimension aspect.
    assert fields_by_path["order_total"].type == SemanticFieldTypeClass.MEASURE
    assert fields_by_path["order_total"].dimension is None


def test_unplaced_column_triggers_warning():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORPHAN_COL": [
                _col(
                    "orphan_col",
                    "VARCHAR",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="UNKNOWN_TABLE",
                )
            ],
        },
    )

    list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    messages = [w.title for w in mapper.report.warnings]
    assert any("without a logical table" in (m or "") for m in messages)


def test_metric_entities_emitted_with_derived_from_relationships():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    comment="Sum of order totals",
                    synonyms=["revenue"],
                    expression="SUM(orders.order_total)",
                )
            ],
            "ORDER_COUNT": [
                _col(
                    "order_count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT(orders.order_id)",
                )
            ],
            "AVG_ORDER_VALUE": [
                _col(
                    "avg_order_value",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="total_revenue / order_count",
                )
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    revenue_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )
    count_urn = mapper.identifiers.gen_metric_urn(
        "order_count", semantic_view.name, _SCHEMA, _DB
    )
    avg_urn = mapper.identifiers.gen_metric_urn(
        "avg_order_value", semantic_view.name, _SCHEMA, _DB
    )

    for urn in (revenue_urn, count_urn, avg_urn):
        statuses = _aspects_for(workunits, urn, StatusClass)
        assert len(statuses) == 1 and statuses[0].removed is False

    revenue_info = _aspects_for(workunits, revenue_urn, MetricInfoClass)[0]
    assert revenue_info.name == "total_revenue"
    assert revenue_info.semanticModel == model_urn
    assert revenue_info.aiContext is not None
    assert revenue_info.aiContext.synonyms == ["revenue"]

    # Metrics that don't reference other metrics emit no relationships aspect.
    assert not _aspects_for(workunits, revenue_urn, MetricRelationshipsClass)
    assert not _aspects_for(workunits, count_urn, MetricRelationshipsClass)

    avg_relationships = _aspects_for(workunits, avg_urn, MetricRelationshipsClass)
    assert len(avg_relationships) == 1
    derived_urns = [d.destinationUrn for d in avg_relationships[0].derivedFrom]
    # Sorted alphabetically by metric name, self excluded.
    assert derived_urns == [count_urn, revenue_urn]


def test_derived_from_preserves_case_when_lowercasing_disabled():
    # Regression test: the destination URN for a metric-to-metric derivation must
    # match the referenced metric's own URN exactly, including case, when
    # convert_urns_to_lowercase is disabled.
    mapper = _make_mapper(convert_urns_to_lowercase=False)
    semantic_view = _make_semantic_view(
        # column_occurrences is always keyed by the uppercased column name
        # (see SnowflakeDataDictionary._process_column_occurrences); only the
        # occurrence's own `name` field preserves the original case.
        column_occurrences={
            "ORDER_COUNT": [
                _col(
                    "Order_Count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT(orders.order_id)",
                )
            ],
            "AVG_ORDER_VALUE": [
                _col(
                    "Avg_Order_Value",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="Order_Count * 2",
                )
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    count_urn = mapper.identifiers.gen_metric_urn(
        "Order_Count", semantic_view.name, _SCHEMA, _DB
    )
    avg_urn = mapper.identifiers.gen_metric_urn(
        "Avg_Order_Value", semantic_view.name, _SCHEMA, _DB
    )

    # The metric's own entity was in fact emitted under this exact URN.
    assert _aspects_for(workunits, count_urn, MetricInfoClass)

    relationships = _aspects_for(workunits, avg_urn, MetricRelationshipsClass)[0]
    assert [d.destinationUrn for d in relationships.derivedFrom] == [count_urn]


def test_fine_grained_lineage_split_between_model_and_metric():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.order_total)",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
        resolved_upstream_urns=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
        ],
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    orders_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
    )
    order_date_source_urn = make_schema_field_urn(orders_dataset_urn, "order_date")
    order_total_source_urn = make_schema_field_urn(orders_dataset_urn, "order_total")

    dimension_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[order_date_source_urn],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(model_urn, "order_date")],
    )
    metric_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[order_total_source_urn],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(model_urn, "total_revenue")],
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[dimension_fgl, metric_fgl],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    upstream_lineages = _aspects_for(workunits, model_urn, UpstreamLineageClass)
    assert len(upstream_lineages) == 1
    upstream_lineage = upstream_lineages[0]
    assert [u.dataset for u in upstream_lineage.upstreams] == [orders_dataset_urn]
    assert upstream_lineage.fineGrainedLineages == [dimension_fgl]

    metric_upstreams = _aspects_for(workunits, metric_urn, MetricUpstreamsClass)[0]
    assert metric_upstreams.fieldUpstreams is not None
    assert metric_upstreams.datasetUpstreams is not None
    assert [e.destinationUrn for e in metric_upstreams.fieldUpstreams] == [
        order_total_source_urn
    ]
    assert [e.destinationUrn for e in metric_upstreams.datasetUpstreams] == [
        orders_dataset_urn
    ]


def test_split_lineages_by_metric_handles_multi_downstream_without_crashing():
    """_split_lineages_by_metric (via _downstream_field_name) assumes each
    FineGrainedLineageClass has exactly one downstream - true for every current
    producer of semantic view FGLs. If that assumption is ever violated, routing
    must fall back to the first downstream rather than crash."""
    mapper = _make_mapper()
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        "Sales_Analytics", _SCHEMA, _DB
    )

    multi_downstream_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=["urn:li:schemaField:(some,upstream)"],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[
            make_schema_field_urn(model_urn, "total_revenue"),
            make_schema_field_urn(model_urn, "order_date"),
        ],
    )

    model_lineages, metric_lineages = mapper._split_lineages_by_metric(
        [multi_downstream_fgl],
        metric_names_upper={"TOTAL_REVENUE"},
        shadowed_metric_names=set(),
    )

    # Routed using only the first downstream (TOTAL_REVENUE), matching a metric.
    assert model_lineages == []
    assert metric_lineages == {"TOTAL_REVENUE": [multi_downstream_fgl]}


def test_no_upstream_lineage_emitted_when_no_resolved_upstreams():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "COL1": [
                _col(
                    "col1",
                    "VARCHAR",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
        resolved_upstream_urns=[],
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    assert not _aspects_for(workunits, model_urn, UpstreamLineageClass)


def test_subtypes_and_status_always_emitted():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(column_occurrences={})
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    subtypes = _aspects_for(workunits, model_urn, SubTypesClass)
    assert len(subtypes) == 1
    assert subtypes[0].typeNames == ["Semantic View"]

    statuses = _aspects_for(workunits, model_urn, StatusClass)
    assert len(statuses) == 1 and statuses[0].removed is False


def test_native_definition_gated_by_include_view_definitions():
    semantic_view = _make_semantic_view(column_occurrences={})

    mapper_with_defs = _make_mapper(include_view_definitions=True)
    model_urn = mapper_with_defs.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    workunits = list(
        mapper_with_defs.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    assert info.nativeDefinition == semantic_view.view_definition

    mapper_without_defs = _make_mapper(include_view_definitions=False)
    workunits = list(
        mapper_without_defs.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    assert info.nativeDefinition is None


def test_view_level_tags_emitted_as_global_tags():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={},
        tags=[SnowflakeTag(database=_DB, schema=_SCHEMA, name="PII", value="true")],
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    tags = _aspects_for(workunits, model_urn, GlobalTagsClass)
    assert len(tags) == 1
    assert len(tags[0].tags) == 1
    assert "true" in tags[0].tags[0].tag


def test_derived_from_ignores_qualified_column_matching_metric_name():
    # A fact reference that is qualified by its logical table (ORDERS.AMOUNT) must
    # not be mistaken for a reference to a metric named AMOUNT: in Snowflake
    # semantic view expressions, metric-to-metric references are always bare
    # (unqualified) names.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "AMOUNT": [
                _col(
                    "amount",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.amount)",
                )
            ],
            "DOUBLE_AMOUNT": [
                _col(
                    "double_amount",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="orders.amount * 2",
                )
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    double_amount_urn = mapper.identifiers.gen_metric_urn(
        "double_amount", semantic_view.name, _SCHEMA, _DB
    )

    assert not _aspects_for(workunits, double_amount_urn, MetricRelationshipsClass)


def test_derived_from_ignores_metric_name_inside_string_literal():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_COUNT": [
                _col(
                    "order_count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT(orders.order_id)",
                )
            ],
            "LABEL_METRIC": [
                _col(
                    "label_metric",
                    "VARCHAR",
                    SemanticViewColumnSubtype.METRIC,
                    # 'order_count' appears here as a string literal, not an
                    # identifier reference to the ORDER_COUNT metric.
                    expression="'order_count'",
                )
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    label_metric_urn = mapper.identifiers.gen_metric_urn(
        "label_metric", semantic_view.name, _SCHEMA, _DB
    )

    assert not _aspects_for(workunits, label_metric_urn, MetricRelationshipsClass)


def test_derived_from_unparseable_expression_yields_no_edges():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_COUNT": [
                _col(
                    "order_count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT(orders.order_id)",
                )
            ],
            "BROKEN_METRIC": [
                _col(
                    "broken_metric",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="not a valid ((( sql",
                )
            ],
        },
    )

    # Must not raise despite the unparseable expression.
    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    broken_metric_urn = mapper.identifiers.gen_metric_urn(
        "broken_metric", semantic_view.name, _SCHEMA, _DB
    )

    assert not _aspects_for(workunits, broken_metric_urn, MetricRelationshipsClass)
    # The parse failure must be diagnosable via the report counter, not silent.
    assert mapper.report.num_semantic_view_metric_expr_parse_failures == 1


def test_metric_column_tags_emitted_as_global_tags():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.order_total)",
                )
            ],
        },
        column_tags={
            "total_revenue": [
                SnowflakeTag(database=_DB, schema=_SCHEMA, name="PII", value="true")
            ]
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    tags = _aspects_for(workunits, metric_urn, GlobalTagsClass)
    assert len(tags) == 1
    assert len(tags[0].tags) == 1
    assert "pii" in tags[0].tags[0].tag


def test_metric_column_tags_emitted_as_structured_properties():
    mapper = _make_mapper(extract_tags_as_structured_properties=True)
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.order_total)",
                )
            ],
        },
        column_tags={
            "total_revenue": [
                SnowflakeTag(database=_DB, schema=_SCHEMA, name="PII", value="true")
            ]
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    structured_props = _aspects_for(workunits, metric_urn, StructuredPropertiesClass)
    assert len(structured_props) == 1
    assert len(structured_props[0].properties) == 1

    # No GlobalTags should be emitted for the metric in structured-property mode.
    assert not _aspects_for(workunits, metric_urn, GlobalTagsClass)


def test_dimension_field_tags_emitted_as_structured_properties_in_sp_mode():
    # In SP mode, DIMENSION/FACT field tags cannot ride on the SchemaField aspect,
    # so they are emitted as schemaField-level structured properties instead.
    mapper = _make_mapper(extract_tags_as_structured_properties=True)
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
        column_tags={
            "order_date": [
                SnowflakeTag(database=_DB, schema=_SCHEMA, name="PII", value="true")
            ]
        },
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    field_urn = make_schema_field_urn(
        model_urn, mapper.identifiers.snowflake_identifier("ORDER_DATE")
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    field_props = _aspects_for(workunits, field_urn, StructuredPropertiesClass)
    assert len(field_props) == 1
    assert len(field_props[0].properties) == 1

    # The field's globalTags must not carry the tag in SP mode.
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    field = _fields_by_path(info.datasets[0])[
        mapper.identifiers.snowflake_identifier("ORDER_DATE")
    ]
    assert field.schemaField.globalTags is None


def test_metrics_not_reported_as_unplaced_when_no_logical_tables():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.order_total)",
                )
            ],
        },
        logical_to_physical_table={},
    )

    list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    messages = [w.title for w in mapper.report.warnings]
    assert not any("without a logical table" in (m or "") for m in messages)


def test_semantic_field_path_matches_fine_grained_lineage_anchor_when_no_lowercasing():
    # Regression test: snowflake_schema_gen.py anchors the fine-grained-lineage
    # downstream field on `snowflake_identifier(col_name_upper)` (the uppercased
    # column_occurrences key). The mapper's SemanticField.fieldPath must use the
    # same casing so the two match under convert_urns_to_lowercase=False.
    mapper = _make_mapper(convert_urns_to_lowercase=False)
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "Order_Date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    fields_by_path = _fields_by_path(info.datasets[0])
    field_path = next(iter(fields_by_path))

    lineage_anchor_urn = make_schema_field_urn(
        model_urn, mapper.identifiers.snowflake_identifier("ORDER_DATE")
    )
    semantic_field_urn = make_schema_field_urn(model_urn, field_path)
    assert semantic_field_urn == lineage_anchor_urn


def test_derived_from_omits_metric_name_shadowed_by_a_column():
    # A bare name referenced in a metric expression that is BOTH a metric and a
    # dimension/fact column of the same view is ambiguous. Since derivedFrom is
    # isLineage:true, the ambiguous edge is omitted; an unambiguous metric-only
    # reference is still emitted.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            # REVENUE exists both as a fact column and as a metric of the view.
            "REVENUE": [
                _col(
                    "revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.FACT,
                    table_name="ORDERS",
                ),
                _col(
                    "revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.amount)",
                ),
            ],
            # ORDER_COUNT is unambiguously a metric only.
            "ORDER_COUNT": [
                _col(
                    "order_count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT(orders.order_id)",
                )
            ],
            "MARGIN": [
                _col(
                    "margin",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="revenue / order_count",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    margin_urn = mapper.identifiers.gen_metric_urn(
        "margin", semantic_view.name, _SCHEMA, _DB
    )
    revenue_urn = mapper.identifiers.gen_metric_urn(
        "revenue", semantic_view.name, _SCHEMA, _DB
    )
    count_urn = mapper.identifiers.gen_metric_urn(
        "order_count", semantic_view.name, _SCHEMA, _DB
    )

    relationships = _aspects_for(workunits, margin_urn, MetricRelationshipsClass)[0]
    derived_urns = [d.destinationUrn for d in relationships.derivedFrom]
    # REVENUE is shadowed by the fact column and omitted; ORDER_COUNT remains.
    assert count_urn in derived_urns
    assert revenue_urn not in derived_urns


def test_shadowed_metric_name_fine_grained_lineage_stays_on_model():
    # REVENUE is both a FACT column and a METRIC of the same view. The FGL for
    # the FACT column's own downstream field must not be misrouted into the
    # metric's metricUpstreams just because the (shadowed) name also happens to
    # be a metric - it must stay on the model's upstreamLineage.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "REVENUE": [
                _col(
                    "revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.FACT,
                    table_name="ORDERS",
                ),
                _col(
                    "revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.amount)",
                ),
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
        resolved_upstream_urns=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
        ],
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    orders_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
    )
    revenue_source_urn = make_schema_field_urn(orders_dataset_urn, "amount")
    revenue_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[revenue_source_urn],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(model_urn, "revenue")],
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[revenue_fgl],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "revenue", semantic_view.name, _SCHEMA, _DB
    )

    upstream_lineage = _aspects_for(workunits, model_urn, UpstreamLineageClass)[0]
    assert upstream_lineage.fineGrainedLineages == [revenue_fgl]

    assert not _aspects_for(workunits, metric_urn, MetricUpstreamsClass)


def test_metric_conflicting_expressions_across_logical_tables_warns():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="SUM(orders.amount)",
                ),
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="RETURNS",
                    expression="SUM(returns.amount)",
                ),
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    # Deterministic selection: the occurrence with the lexicographically smallest
    # table_name ("ORDERS" < "RETURNS") wins, regardless of declaration order.
    info = _aspects_for(workunits, metric_urn, MetricInfoClass)[0]
    assert info.expression is not None
    assert info.expression.dialects[0].expression == "SUM(orders.amount)"

    messages = [w.title for w in mapper.report.warnings]
    assert any("conflicting expressions" in (m or "") for m in messages)


def test_metric_conflicting_expressions_selection_is_order_independent():
    # Same conflict as above but with occurrences declared in the opposite order -
    # the canonical selection must depend only on table_name, not on which
    # occurrence Snowflake happened to return first.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="RETURNS",
                    expression="SUM(returns.amount)",
                ),
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="SUM(orders.amount)",
                ),
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    info = _aspects_for(workunits, metric_urn, MetricInfoClass)[0]
    assert info.expression is not None
    assert info.expression.dialects[0].expression == "SUM(orders.amount)"

    messages = [w.title for w in mapper.report.warnings]
    assert any("conflicting expressions" in (m or "") for m in messages)


def test_column_defined_on_multiple_logical_tables_warns_field_path_collision():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "STATUS": [
                _col(
                    "status",
                    "VARCHAR",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                ),
                _col(
                    "status",
                    "VARCHAR",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="CUSTOMERS",
                ),
            ],
        },
    )

    list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    messages = [w.title for w in mapper.report.warnings]
    assert any("multiple logical tables" in (m or "") for m in messages)


def test_repeated_information_schema_row_does_not_duplicate_field():
    # A repeated INFORMATION_SCHEMA row for the same column on the same logical
    # table must not produce two SemanticFields with the same fieldPath (the
    # legacy dataset-mode path merges duplicates the same way).
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                ),
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                ),
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    assert info.datasets is not None
    orders_dataset = next(d for d in info.datasets if d.name == "ORDERS")
    assert orders_dataset.fields is not None
    assert len(orders_dataset.fields) == 1


def test_metric_expression_omitted_when_declared_without_expression():
    # MetricInfo.expression is optional in the PDL - don't fabricate a value
    # from the metric's own name when Snowflake reports no expression for it.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "MYSTERY_METRIC": [
                _col(
                    "mystery_metric",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression=None,
                )
            ],
        },
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "mystery_metric", semantic_view.name, _SCHEMA, _DB
    )

    info = _aspects_for(workunits, metric_urn, MetricInfoClass)[0]
    assert info.expression is None


def test_semantic_field_expression_falls_back_to_column_name_when_missing():
    # SemanticField.expression is a required PDL field, so the fabricated
    # fallback to the column's own name must remain for this path only.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                    expression=None,
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    field = _fields_by_path(info.datasets[0])["order_date"]
    assert field.expression.dialects[0].expression == "order_date"


def test_relationships_populated_with_datasets_matching_model_dataset_names():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "CUSTOMER_ID": [
                _col(
                    "customer_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="CUSTOMERS",
                )
            ],
            "ORDER_ID": [
                _col(
                    "order_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                )
            ],
        },
        relationships=[
            SnowflakeSemanticViewRelationship(
                name="orders_to_customers",
                from_table="orders",
                from_columns=["customer_id"],
                to_table="customers",
                to_columns=["customer_id"],
            ),
        ],
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]

    assert info.relationships is not None
    assert len(info.relationships) == 1
    relationship = info.relationships[0]
    assert relationship.name == "orders_to_customers"
    # from/to must be normalized to match the ModelDataset.name values (the
    # uppercased logical-table keys) so relationship references resolve.
    dataset_names = {d.name for d in info.datasets}
    assert relationship.from_ in dataset_names
    assert relationship.to in dataset_names
    assert relationship.from_ == "ORDERS"
    assert relationship.to == "CUSTOMERS"
    assert relationship.fromColumns == ["customer_id"]
    assert relationship.toColumns == ["customer_id"]


def test_relationships_omitted_when_none_defined():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(column_occurrences={})
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    assert info.relationships is None


def test_join_key_column_collision_does_not_warn():
    # ORDER_ID is a join key (declared as a relationship's from/to column) that is
    # legitimately defined on both logical tables - this must not trigger the
    # field-path-collision warning, unlike a genuine unrelated same-name collision.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_ID": [
                _col(
                    "order_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                ),
                _col(
                    "order_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="CUSTOMERS",
                ),
            ],
        },
        relationships=[
            SnowflakeSemanticViewRelationship(
                name="orders_to_customers",
                from_table="orders",
                from_columns=["order_id"],
                to_table="customers",
                to_columns=["order_id"],
            ),
        ],
    )

    list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    messages = [w.title for w in mapper.report.warnings]
    assert not any("multiple logical tables" in (m or "") for m in messages)


def test_primary_key_column_collision_does_not_warn():
    # Same as above, but suppressed via primary_key_columns rather than a
    # relationship - the fallback the task calls out for views with no
    # relationships metadata.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ID": [
                _col(
                    "id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                ),
                _col(
                    "id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="CUSTOMERS",
                ),
            ],
        },
        primary_key_columns={"ID"},
    )

    list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    messages = [w.title for w in mapper.report.warnings]
    assert not any("multiple logical tables" in (m or "") for m in messages)
