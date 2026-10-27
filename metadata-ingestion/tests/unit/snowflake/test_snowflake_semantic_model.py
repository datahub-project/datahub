import datetime
from typing import Dict, List, Optional, Type, TypeVar

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
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
    snowflake_identity_key,
)
from datahub.metadata.schema_classes import (
    AiContextClass,
    DatasetPropertiesClass,
    DimensionClass,
    ERModelRelationshipCardinalityClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
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
    preserve: bool = False,
) -> SemanticViewColumnMetadata:
    return SemanticViewColumnMetadata(
        name=name,
        identity_key=snowflake_identity_key(name, preserve_column_case=preserve),
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
    primary_key_columns_by_table: Optional[Dict[str, set]] = None,
    unique_key_column_sets_by_table: Optional[Dict[str, list]] = None,
    tags: Optional[list] = None,
    column_tags: Optional[dict] = None,
    relationships: Optional[List[SnowflakeSemanticViewRelationship]] = None,
    column_synonyms: Optional[Dict[str, List[str]]] = None,
    table_synonyms: Optional[Dict[str, List[str]]] = None,
) -> SnowflakeSemanticView:
    pk_by_table = primary_key_columns_by_table or {}
    # Mirror real ingestion: the flat set is the union across logical tables.
    flat_pk = set(primary_key_columns or set())
    for cols in pk_by_table.values():
        flat_pk |= cols
    # Distinguish an explicit empty {} (no logical tables) from None (use default);
    # `x or default` would silently swap {} for the default and never exercise the
    # no-logical-tables path.
    if logical_to_physical_table is None:
        logical_to_physical_table = {
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
            "CUSTOMERS": (_DB, _SCHEMA, "CUSTOMERS"),
        }
    return SnowflakeSemanticView(
        name="Sales_Analytics",
        created=datetime.datetime(2024, 1, 1),
        comment="Sales semantic view",
        view_definition="CREATE SEMANTIC VIEW Sales_Analytics AS ...",
        last_altered=datetime.datetime(2024, 2, 1),
        column_occurrences=column_occurrences,
        logical_to_physical_table=logical_to_physical_table,
        resolved_upstream_urns=resolved_upstream_urns or [],
        primary_key_columns=flat_pk,
        primary_key_columns_by_table=pk_by_table,
        unique_key_column_sets_by_table=unique_key_column_sets_by_table or {},
        tags=tags,
        column_tags=column_tags or {},
        relationships=relationships or [],
        column_synonyms=column_synonyms or {},
        table_synonyms=table_synonyms or {},
    )


def _make_mapper(
    convert_urns_to_lowercase: bool = True,
    platform_instance: Optional[str] = None,
    include_view_definitions: bool = True,
    extract_tags_as_structured_properties: bool = False,
    # Pinned rather than inherited: these tests assert exact field paths, so they
    # must not follow the ambient default that the flag-on sweep flips.
    preserve_column_case: bool = False,
) -> SnowflakeSemanticModelMapper:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
            "convert_urns_to_lowercase": convert_urns_to_lowercase,
            "preserve_column_case": preserve_column_case,
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


def _logical_dataset_urn(
    mapper: SnowflakeSemanticModelMapper, logical_table: str
) -> str:
    return mapper.identifiers.gen_semantic_model_dataset_urn(
        "Sales_Analytics", logical_table, _SCHEMA, _DB
    )


def _annotation_for(
    workunits: List[MetadataWorkUnit], dataset_urn: str, field_path: str
) -> Optional[SemanticFieldAnnotationClass]:
    field_urn = make_schema_field_urn(dataset_urn, field_path)
    annotations = _aspects_for(workunits, field_urn, SemanticFieldAnnotationClass)
    return annotations[0] if annotations else None


def _schema_fields_by_path(
    workunits: List[MetadataWorkUnit], dataset_urn: str
) -> Dict[str, SchemaFieldClass]:
    schemas = _aspects_for(workunits, dataset_urn, SchemaMetadataClass)
    assert len(schemas) == 1
    return {f.fieldPath: f for f in schemas[0].fields}


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


def test_logical_dataset_urn_shape_default_lowercase():
    mapper = _make_mapper()
    urn = mapper.identifiers.gen_semantic_model_dataset_urn(
        "Sales_Analytics", "ORDERS", _SCHEMA, _DB
    )
    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "test_db.public.sales_analytics.orders,PROD)"
    )


def test_logical_dataset_urn_shape_with_platform_instance_and_no_lowercase():
    mapper = _make_mapper(
        convert_urns_to_lowercase=False, platform_instance="my_instance"
    )
    urn = mapper.identifiers.gen_semantic_model_dataset_urn(
        "Sales_Analytics", "ORDERS", _SCHEMA, _DB
    )
    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        f"my_instance.{_DB}.{_SCHEMA}.Sales_Analytics.ORDERS,PROD)"
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
        primary_key_columns_by_table={"CUSTOMERS": {"CUSTOMER_ID"}},
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
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    customers_urn = _logical_dataset_urn(mapper, "CUSTOMERS")

    info = _aspects_for(workunits, model_urn, SemanticModelInfoClass)[0]
    assert info.name == "Sales_Analytics"
    assert info.description == "Sales semantic view"
    # Membership is member-side only (semanticModelProperties / metricInfo).

    # Each logical dataset is a dataset entity with the SEMANTIC_MODEL_DATASET
    # subtype and a semanticModelProperties back-reference to the model.
    for dataset_urn in (orders_urn, customers_urn):
        subtypes = _aspects_for(workunits, dataset_urn, SubTypesClass)
        assert len(subtypes) == 1
        assert subtypes[0].typeNames == [DatasetSubTypes.SEMANTIC_MODEL_DATASET]
        props = _aspects_for(workunits, dataset_urn, SemanticModelPropertiesClass)
        assert len(props) == 1
        assert props[0].semanticModel == model_urn

    # The alias must match the uppercased logical-table key so relationship
    # from/to references resolve.
    orders_props = _aspects_for(workunits, orders_urn, SemanticModelPropertiesClass)[0]
    customers_props = _aspects_for(
        workunits, customers_urn, SemanticModelPropertiesClass
    )[0]
    assert orders_props.alias == "ORDERS"
    assert customers_props.alias == "CUSTOMERS"

    orders_fields = _schema_fields_by_path(workunits, orders_urn)
    assert set(orders_fields) == {"order_date", "order_total"}
    customers_fields = _schema_fields_by_path(workunits, customers_urn)
    assert set(customers_fields) == {"customer_id"}
    assert customers_fields["customer_id"].isPartOfKey is True

    # Per-field semantic metadata lives on semanticFieldAnnotation aspects
    # anchored on each logical dataset's schemaField URN.
    order_date_ann = _annotation_for(workunits, orders_urn, "order_date")
    assert order_date_ann is not None
    assert order_date_ann.type == SemanticFieldTypeClass.DIMENSION
    order_total_ann = _annotation_for(workunits, orders_urn, "order_total")
    assert order_total_ann is not None
    assert order_total_ann.type == SemanticFieldTypeClass.MEASURE
    customer_id_ann = _annotation_for(workunits, customers_urn, "customer_id")
    assert customer_id_ann is not None
    assert customer_id_ann.type == SemanticFieldTypeClass.DIMENSION
    # Metrics are not fields on any logical dataset and have no annotation.
    for dataset_urn in (orders_urn, customers_urn):
        assert "total_revenue" not in _schema_fields_by_path(workunits, dataset_urn)
        assert _annotation_for(workunits, dataset_urn, "total_revenue") is None


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
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")

    order_date_ann = _annotation_for(workunits, orders_urn, "order_date")
    assert order_date_ann is not None
    customer_name_ann = _annotation_for(workunits, orders_urn, "customer_name")
    assert customer_name_ann is not None
    order_total_ann = _annotation_for(workunits, orders_urn, "order_total")
    assert order_total_ann is not None

    assert order_date_ann.dimension == DimensionClass(isTime=True)
    assert customer_name_ann.dimension == DimensionClass(isTime=False)
    # FACT columns are MEASUREs and never carry a dimension aspect.
    assert order_total_ann.type == SemanticFieldTypeClass.MEASURE
    assert order_total_ann.dimension is None


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
    # MetricInfo no longer carries a nested aiContext; synonyms are not
    # emitted in this PR (first-class aiContext is a fast-follow).

    # Metrics that don't reference other metrics still emit metricRelationships,
    # with an empty derivedFrom and no parentMetric, so hasParentMetric is indexed
    # as false and they appear as root metrics in the /metrics sidebar.
    for urn in (revenue_urn, count_urn):
        relationships = _aspects_for(workunits, urn, MetricRelationshipsClass)
        assert len(relationships) == 1
        assert relationships[0].derivedFrom == []
        assert relationships[0].parentMetric is None

    # View-scoped metrics with qualified table refs get Metric → SMD lineage.
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    for urn in (revenue_urn, count_urn):
        upstreams = _aspects_for(workunits, urn, MetricUpstreamsClass)
        assert len(upstreams) == 1
        assert upstreams[0].datasetUpstreams is not None
        assert [e.destinationUrn for e in upstreams[0].datasetUpstreams] == [orders_urn]
    # Derived metric with only metric-to-metric refs has empty datasetUpstreams
    # (still emitted so re-ingestion clears any stale server-side edges).
    avg_upstreams = _aspects_for(workunits, avg_urn, MetricUpstreamsClass)
    assert len(avg_upstreams) == 1
    assert avg_upstreams[0].datasetUpstreams == []

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
        # column_occurrences is keyed by the column's stored name (see
        # SnowflakeDataDictionary._process_column_occurrences).
        column_occurrences={
            "Order_Count": [
                _col(
                    "Order_Count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT(orders.order_id)",
                )
            ],
            "Avg_Order_Value": [
                _col(
                    "Avg_Order_Value",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    # Quoted, because that is the only way Snowflake can reference
                    # a metric stored mixed-case -- an unquoted `Order_Count` folds
                    # to ORDER_COUNT and is rejected at CREATE SEMANTIC VIEW.
                    # SEMANTIC_METRICS.EXPRESSION returns the DDL text verbatim,
                    # so the quotes reach the connector.
                    expression='"Order_Count" * 2',
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


def test_quoted_and_unquoted_refs_resolve_to_different_members_of_a_case_pair():
    # Snowflake folds an unquoted reference up and takes a quoted one as written,
    # so with casing preserved the two spellings select DIFFERENT metrics. This
    # fixture is a transcript of a real semantic view -- verified on Snowflake,
    # where `orders."Order_Count" * 2` returned 4 (COUNT * 2) and
    # `orders.Order_Count * 3` returned 90 (SUM * 3), i.e. distinct targets.
    # Both members of the pair are referenced; exercising only one would pass
    # even if resolution ignored the quoting entirely.
    mapper = _make_mapper(preserve_column_case=True)
    semantic_view = _make_semantic_view(
        column_occurrences={
            "Order_Count": [
                _col(
                    "Order_Count",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="COUNT(orders.order_id)",
                    preserve=True,
                )
            ],
            "ORDER_COUNT": [
                _col(
                    "ORDER_COUNT",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="SUM(orders.amt)",
                    preserve=True,
                )
            ],
            "FROM_QUOTED": [
                _col(
                    "FROM_QUOTED",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression='orders."Order_Count" * 2',
                    preserve=True,
                )
            ],
            "FROM_UNQUOTED": [
                _col(
                    "FROM_UNQUOTED",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="orders.Order_Count * 3",
                    preserve=True,
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS_TBL")},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    def urn_of(metric_name: str) -> str:
        return mapper.identifiers.gen_metric_urn(
            metric_name, semantic_view.name, _SCHEMA, _DB, logical_table="ORDERS"
        )

    def derived_from(metric_name: str) -> List[str]:
        rels = _aspects_for(workunits, urn_of(metric_name), MetricRelationshipsClass)
        return [d.destinationUrn for d in rels[0].derivedFrom]

    assert derived_from("FROM_QUOTED") == [urn_of("Order_Count")]
    assert derived_from("FROM_UNQUOTED") == [urn_of("ORDER_COUNT")]


def test_view_scoped_metric_qualified_by_mixed_case_metric_ref_does_not_emit_smd_upstream():
    # With preserve_column_case, table_bound_metrics is keyed by the stored
    # spelling. A quoted metric-to-metric ref must hit that skip check; folding
    # both halves to upper would miss and fall through to a bogus Metric → SMD
    # edge for ORDERS.
    mapper = _make_mapper(preserve_column_case=True)
    semantic_view = _make_semantic_view(
        column_occurrences={
            "Total_Amount": [
                _col(
                    "Total_Amount",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="SUM(orders.amt)",
                    preserve=True,
                )
            ],
            "DOUBLE_TOTAL": [
                _col(
                    "DOUBLE_TOTAL",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression='ORDERS."Total_Amount" * 2',
                    preserve=True,
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS_TBL")},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    total_urn = mapper.identifiers.gen_metric_urn(
        "Total_Amount",
        semantic_view.name,
        _SCHEMA,
        _DB,
        logical_table="ORDERS",
    )
    derived_urn = mapper.identifiers.gen_metric_urn(
        "DOUBLE_TOTAL", semantic_view.name, _SCHEMA, _DB
    )

    relationships = _aspects_for(workunits, derived_urn, MetricRelationshipsClass)[0]
    assert [d.destinationUrn for d in relationships.derivedFrom] == [total_urn]

    upstreams = _aspects_for(workunits, derived_urn, MetricUpstreamsClass)
    assert len(upstreams) == 1
    assert upstreams[0].datasetUpstreams == []


def test_view_scoped_metric_qualified_by_quoted_table_emits_smd_upstream():
    # Quoted logical table "Orders" is stored with that spelling in
    # logical_dataset_urns. Looking up ORDERS (unconditional upper) would miss
    # and drop a real Metric → SMD edge for a fact/dimension column ref.
    mapper = _make_mapper(preserve_column_case=True)
    semantic_view = _make_semantic_view(
        column_occurrences={
            "amount": [
                _col(
                    "amount",
                    "NUMBER",
                    SemanticViewColumnSubtype.FACT,
                    table_name="Orders",
                    preserve=True,
                )
            ],
            "AMOUNT_PLUS_ONE": [
                _col(
                    "AMOUNT_PLUS_ONE",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression='"Orders".amount + 1',
                    preserve=True,
                )
            ],
        },
        logical_to_physical_table={"Orders": (_DB, _SCHEMA, "ORDERS_TBL")},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    orders_urn = _logical_dataset_urn(mapper, "Orders")
    derived_urn = mapper.identifiers.gen_metric_urn(
        "AMOUNT_PLUS_ONE", semantic_view.name, _SCHEMA, _DB
    )

    upstreams = _aspects_for(workunits, derived_urn, MetricUpstreamsClass)
    assert len(upstreams) == 1
    assert upstreams[0].datasetUpstreams is not None
    assert [e.destinationUrn for e in upstreams[0].datasetUpstreams] == [orders_urn]


def test_case_only_metric_pair_stays_one_metric_without_preserve_column_case():
    # preserve_column_case off means case-only spellings are the same metric, and
    # that must hold however convert_urns_to_lowercase is set. When both are off
    # the two folds the mapper used to key on both reduce to identity, so the pair
    # split into two metric entities -- one of them net-new output nobody asked for.
    for convert_urns_to_lowercase in (True, False):
        mapper = _make_mapper(convert_urns_to_lowercase=convert_urns_to_lowercase)
        semantic_view = _make_semantic_view(
            # One bucket holding both spellings, as _process_column_occurrences
            # builds it when the bucket is not split by case.
            column_occurrences={
                "Rev": [
                    _col(
                        "Rev",
                        "NUMBER",
                        SemanticViewColumnSubtype.METRIC,
                        table_name="ORDERS",
                        expression="SUM(a)",
                    ),
                    _col(
                        "REV",
                        "NUMBER",
                        SemanticViewColumnSubtype.METRIC,
                        table_name="ORDERS",
                        expression="SUM(b)",
                    ),
                ],
            },
            logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS_TBL")},
        )
        workunits = list(
            mapper.gen_workunits(
                semantic_view=semantic_view,
                schema_name=_SCHEMA,
                db_name=_DB,
                fine_grained_lineages=[],
            )
        )
        metric_urns = {
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, MetricInfoClass)
        }
        assert len(metric_urns) == 1, (
            f"convert_urns_to_lowercase={convert_urns_to_lowercase}: {metric_urns}"
        )


def test_fine_grained_lineage_split_between_logical_dataset_and_metric():
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
                    table_name="ORDERS",
                    expression="SUM(orders.order_total)",
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
        resolved_upstream_urns=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
        ],
    )
    orders_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
    )
    orders_logical_urn = _logical_dataset_urn(mapper, "ORDERS")
    order_date_source_urn = make_schema_field_urn(orders_dataset_urn, "order_date")
    order_total_source_urn = make_schema_field_urn(orders_dataset_urn, "order_total")

    dimension_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[order_date_source_urn],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(orders_logical_urn, "order_date")],
    )
    metric_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[order_total_source_urn],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(orders_logical_urn, "total_revenue")],
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
        "total_revenue",
        semantic_view.name,
        _SCHEMA,
        _DB,
        logical_table="ORDERS",
    )
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )

    # The dimension FGL is re-homed onto the logical dataset's upstreamLineage;
    # the metric FGL is dropped (metric → SMD lineage is on metricUpstreams).
    logical_upstream_lineages = _aspects_for(
        workunits, orders_logical_urn, UpstreamLineageClass
    )
    assert len(logical_upstream_lineages) == 1
    upstream_lineage = logical_upstream_lineages[0]
    # Table-level lineage: the physical base table for this logical table.
    assert [u.dataset for u in upstream_lineage.upstreams] == [orders_dataset_urn]
    assert upstream_lineage.fineGrainedLineages == [dimension_fgl]

    # The model carries no upstreamLineage (it is a container, not a lineage hop).
    assert not _aspects_for(workunits, model_urn, UpstreamLineageClass)
    # Table-bound metric has Metric → SMD lineage via metricUpstreams.
    metric_upstreams = _aspects_for(workunits, metric_urn, MetricUpstreamsClass)
    assert len(metric_upstreams) == 1
    assert metric_upstreams[0].datasetUpstreams is not None
    assert [e.destinationUrn for e in metric_upstreams[0].datasetUpstreams] == [
        orders_logical_urn
    ]


def test_lineage_routing_scoped_by_table_for_shared_metric_fact_name():
    # REVENUE is a FACT on ORDERS (emitted as a schemaField) and a METRIC on
    # RETURNS (emitted as a metric entity, NOT a field). Routing must keep the
    # ORDERS fact FGL and drop the RETURNS metric FGL - dropping by bare name would
    # either keep the RETURNS edge (dangling to a field RETURNS never emits) or
    # drop the valid ORDERS edge, since the name is shared across tables.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "REVENUE": [
                _col("revenue", "NUMBER", SemanticViewColumnSubtype.FACT, "ORDERS"),
                _col(
                    "revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="RETURNS",
                    expression="SUM(returns.amount)",
                ),
            ],
            "RETURN_ID": [
                _col(
                    "return_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "RETURNS",
                ),
            ],
        },
        logical_to_physical_table={
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
            "RETURNS": (_DB, _SCHEMA, "RETURNS"),
        },
    )
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    returns_urn = _logical_dataset_urn(mapper, "RETURNS")
    src = (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "test_db.public.base,PROD),amount)"
    )
    orders_fact_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[src],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(orders_urn, "revenue")],
    )
    returns_metric_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[src],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(returns_urn, "revenue")],
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[orders_fact_fgl, returns_metric_fgl],
        )
    )

    # ORDERS keeps its fact FGL, and the field exists on ORDERS.
    orders_lineage = _aspects_for(workunits, orders_urn, UpstreamLineageClass)
    assert len(orders_lineage) == 1
    assert orders_lineage[0].fineGrainedLineages == [orders_fact_fgl]
    assert "revenue" in _schema_fields_by_path(workunits, orders_urn)

    # RETURNS: revenue is a metric, so it is not a schemaField and its FGL is
    # dropped (no dangling reference).
    assert "revenue" not in _schema_fields_by_path(workunits, returns_urn)
    returns_lineage = _aspects_for(workunits, returns_urn, UpstreamLineageClass)
    returns_fgls = returns_lineage[0].fineGrainedLineages if returns_lineage else []
    assert not returns_fgls


def test_route_lineages_handles_multi_downstream_without_crashing():
    """_route_lineages (via _downstream_field_name) assumes each
    FineGrainedLineageClass has exactly one downstream - true for every current
    producer of semantic view FGLs. If that assumption is ever violated, routing
    must fall back to the first downstream rather than crash."""
    mapper = _make_mapper()
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        "Sales_Analytics", _SCHEMA, _DB
    )
    # Both downstreams anchor on the model URN, but route differently: the first
    # is a view-scoped metric (dropped silently, lineage flows via derivedFrom),
    # the second a dimension with no logical table (dropped with a warning). So
    # the absence of a warning is what proves the first downstream was used.
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col("total_revenue", "NUMBER", SemanticViewColumnSubtype.METRIC)
            ],
            "ORDER_DATE": [
                _col("order_date", "DATE", SemanticViewColumnSubtype.DIMENSION)
            ],
        },
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

    by_dataset = mapper._route_lineages(
        [multi_downstream_fgl],
        logical_dataset_urns={},
        model_urn=model_urn,
        semantic_view=semantic_view,
    )

    assert by_dataset == {}
    assert not mapper.report.warnings


def test_logical_dataset_upstream_lineage_uses_base_table_even_without_resolved_upstreams():
    # In the new model each logical dataset's table-level upstream comes from
    # logical_to_physical_table (the per-logical-table base table), not the
    # view-level resolved_upstream_urns list. So even with an empty
    # resolved_upstream_urns the logical dataset still gets an upstreamLineage
    # edge to its physical base table.
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
    orders_logical_urn = _logical_dataset_urn(mapper, "ORDERS")
    base_table_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    # The model carries no upstreamLineage in the new model.
    assert not _aspects_for(workunits, model_urn, UpstreamLineageClass)
    # The logical dataset carries the base-table upstream edge.
    upstream_lineages = _aspects_for(
        workunits, orders_logical_urn, UpstreamLineageClass
    )
    assert len(upstream_lineages) == 1
    assert [u.dataset for u in upstream_lineages[0].upstreams] == [base_table_urn]


def test_subtypes_and_status_always_emitted():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(column_occurrences={})
    model_urn = mapper.identifiers.gen_semantic_model_urn(
        semantic_view.name, _SCHEMA, _DB
    )
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    customers_urn = _logical_dataset_urn(mapper, "CUSTOMERS")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    # The semanticModel entity carries Status but no SubTypes (SEMANTIC_VIEW was
    # a dataset subtype that does not belong on a semanticModel).
    statuses = _aspects_for(workunits, model_urn, StatusClass)
    assert len(statuses) == 1 and statuses[0].removed is False
    assert not _aspects_for(workunits, model_urn, SubTypesClass)

    # Each logical dataset carries the SEMANTIC_MODEL_DATASET subtype + Status.
    for dataset_urn in (orders_urn, customers_urn):
        subtypes = _aspects_for(workunits, dataset_urn, SubTypesClass)
        assert len(subtypes) == 1
        assert subtypes[0].typeNames == [DatasetSubTypes.SEMANTIC_MODEL_DATASET]
        ds_statuses = _aspects_for(workunits, dataset_urn, StatusClass)
        assert len(ds_statuses) == 1 and ds_statuses[0].removed is False


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

    # The qualified fact reference yields no derived edge, but the aspect is still
    # emitted with an empty derivedFrom.
    relationships = _aspects_for(workunits, double_amount_urn, MetricRelationshipsClass)
    assert len(relationships) == 1
    assert relationships[0].derivedFrom == []


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

    # The metric name inside the string literal is not a reference, so no derived
    # edge is produced, but the aspect is still emitted with an empty derivedFrom.
    relationships = _aspects_for(workunits, label_metric_urn, MetricRelationshipsClass)
    assert len(relationships) == 1
    assert relationships[0].derivedFrom == []


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

    # An unparseable expression yields no derived edges, but the aspect is still
    # emitted with an empty derivedFrom.
    relationships = _aspects_for(workunits, broken_metric_urn, MetricRelationshipsClass)
    assert len(relationships) == 1
    assert relationships[0].derivedFrom == []
    # The parse failure must be diagnosable via the report counter, not silent.
    assert mapper.report.num_semantic_view_metric_expr_parse_failures == 1


def test_derived_from_tokenizer_failure_yields_no_edges():
    # An unterminated string literal makes sqlglot raise TokenError during
    # tokenization (a SqlglotError that is NOT a ParseError). It must be caught
    # too, so one bad metric can't abort emission of the rest of the view.
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
            "UNCLOSED_QUOTE_METRIC": [
                _col(
                    "unclosed_quote_metric",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="COUNT('unclosed",
                )
            ],
        },
    )

    # Must not raise despite the tokenizer failure.
    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    broken_metric_urn = mapper.identifiers.gen_metric_urn(
        "unclosed_quote_metric", semantic_view.name, _SCHEMA, _DB
    )
    relationships = _aspects_for(workunits, broken_metric_urn, MetricRelationshipsClass)
    assert len(relationships) == 1
    assert relationships[0].derivedFrom == []
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
    # In SP mode, DIMENSION/FACT field tags cannot ride on the SchemaField
    # aspect, so they are emitted as schemaField-level structured properties
    # anchored on the logical dataset's schemaField URN.
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
    orders_logical_urn = _logical_dataset_urn(mapper, "ORDERS")
    field_urn = make_schema_field_urn(
        orders_logical_urn,
        mapper.identifiers.snowflake_identifier("ORDER_DATE"),
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
    fields = _schema_fields_by_path(workunits, orders_logical_urn)
    assert (
        fields[mapper.identifiers.snowflake_identifier("ORDER_DATE")].globalTags is None
    )


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

    # With no logical tables, the view exposes only a view-scoped metric.
    assert semantic_view.logical_to_physical_table == {}
    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    # The metric is still emitted (path actually exercised)...
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )
    assert len(_aspects_for(workunits, metric_urn, MetricInfoClass)) == 1
    # ...and it is NOT reported as an unplaced column (metrics are emitted as
    # metric entities, not logical-dataset fields).
    messages = [w.title for w in mapper.report.warnings]
    assert not any("without a logical table" in (m or "") for m in messages)


def test_semantic_field_path_matches_fine_grained_lineage_anchor_when_no_lowercasing():
    # Regression test: snowflake_schema_gen.py anchors the fine-grained-lineage
    # downstream field on the logical dataset URN, resolving the uppercased
    # column_occurrences key back to the column's stored name. The mapper's
    # schemaMetadata fieldPath must resolve to the same name, or lineage points at
    # a field that does not exist. Asserted under convert_urns_to_lowercase=False,
    # where the casing is visible rather than flattened away.
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
    orders_logical_urn = _logical_dataset_urn(mapper, "ORDERS")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    fields_by_path = _schema_fields_by_path(workunits, orders_logical_urn)
    field_path = next(iter(fields_by_path))

    lineage_anchor_urn = make_schema_field_urn(
        orders_logical_urn,
        mapper.identifiers.logical_dataset_field_path("ORDER_DATE"),
    )
    semantic_field_urn = make_schema_field_urn(orders_logical_urn, field_path)
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


def test_shadowed_metric_name_fine_grained_lineage_lands_on_logical_dataset():
    # REVENUE is both a FACT column and a METRIC of the same view. The FGL for
    # the FACT column's own downstream field must not be dropped as a metric
    # just because the (shadowed) name also happens to be a metric - it stays on
    # the FACT column's logical dataset's upstreamLineage.
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
    orders_logical_urn = _logical_dataset_urn(mapper, "ORDERS")
    orders_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
    )
    revenue_source_urn = make_schema_field_urn(orders_dataset_urn, "amount")
    revenue_fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[revenue_source_urn],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(orders_logical_urn, "revenue")],
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

    upstream_lineage = _aspects_for(
        workunits, orders_logical_urn, UpstreamLineageClass
    )[0]
    assert upstream_lineage.fineGrainedLineages == [revenue_fgl]

    # View-scoped metric with qualified `orders.amount` gets Metric → ORDERS SMD.
    metric_upstreams = _aspects_for(workunits, metric_urn, MetricUpstreamsClass)
    assert len(metric_upstreams) == 1
    assert metric_upstreams[0].datasetUpstreams is not None
    assert [e.destinationUrn for e in metric_upstreams[0].datasetUpstreams] == [
        orders_logical_urn
    ]


def test_same_named_metrics_on_different_tables_emit_distinct_entities():
    # Snowflake allows the same metric name on different logical tables; they are
    # distinct, table-qualified metrics and must NOT collapse into one entity or
    # be treated as a conflict.
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
    orders_metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB, logical_table="ORDERS"
    )
    returns_metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB, logical_table="RETURNS"
    )

    assert orders_metric_urn != returns_metric_urn
    orders_info = _aspects_for(workunits, orders_metric_urn, MetricInfoClass)
    returns_info = _aspects_for(workunits, returns_metric_urn, MetricInfoClass)
    assert len(orders_info) == 1
    assert len(returns_info) == 1
    # Each metric keeps its own expression, not a sibling table's.
    assert orders_info[0].expression is not None
    assert returns_info[0].expression is not None
    assert orders_info[0].expression.dialects[0].expression == "SUM(orders.amount)"
    assert returns_info[0].expression.dialects[0].expression == "SUM(returns.amount)"

    # Distinct metrics on different tables are not a conflict.
    messages = [w.title for w in mapper.report.warnings]
    assert not any("conflicting expressions" in (m or "") for m in messages)


def test_duplicate_metric_on_same_table_with_conflicting_expressions_warns():
    # A genuine anomaly: the same metric declared twice on the SAME logical table
    # with different expressions. One is kept deterministically; a warning fires.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="SUM(orders.net)",
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
        "total_revenue", semantic_view.name, _SCHEMA, _DB, logical_table="ORDERS"
    )
    infos = _aspects_for(workunits, metric_urn, MetricInfoClass)
    assert len(infos) == 1
    # Lexicographically smallest expression wins deterministically.
    assert infos[0].expression is not None
    assert infos[0].expression.dialects[0].expression == "SUM(orders.amount)"

    messages = [w.title for w in mapper.report.warnings]
    assert any("conflicting expressions" in (m or "") for m in messages)


def test_derived_metric_resolves_table_qualified_references():
    # Real Snowflake derived metrics reference table-bound metrics by their logical
    # table (ORDERS.GROSS_REVENUE + TRANSACTIONS.NET_PAYMENT). Those qualified refs
    # must resolve to the table-scoped metric URNs, not be dropped as columns.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "GROSS_REVENUE": [
                _col(
                    "gross_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    expression="SUM(orders.amount)",
                )
            ],
            "NET_PAYMENT": [
                _col(
                    "net_payment",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    table_name="TRANSACTIONS",
                    expression="SUM(transactions.paid)",
                )
            ],
            "TOTAL_ORDER_REVENUE": [
                _col(
                    "total_order_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    # View-scoped derived metric: no logical table.
                    expression="ORDERS.GROSS_REVENUE + TRANSACTIONS.NET_PAYMENT",
                )
            ],
        },
        logical_to_physical_table={
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
            "TRANSACTIONS": (_DB, _SCHEMA, "TRANSACTIONS"),
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
    total_urn = mapper.identifiers.gen_metric_urn(
        "total_order_revenue", semantic_view.name, _SCHEMA, _DB
    )
    gross_urn = mapper.identifiers.gen_metric_urn(
        "gross_revenue", semantic_view.name, _SCHEMA, _DB, logical_table="ORDERS"
    )
    net_urn = mapper.identifiers.gen_metric_urn(
        "net_payment", semantic_view.name, _SCHEMA, _DB, logical_table="TRANSACTIONS"
    )

    relationships = _aspects_for(workunits, total_urn, MetricRelationshipsClass)
    assert len(relationships) == 1
    derived_urns = sorted(d.destinationUrn for d in relationships[0].derivedFrom)
    assert derived_urns == sorted([gross_urn, net_urn])

    # Qualified TABLE.METRIC refs must not also become direct Metric → SMD edges;
    # lineage reaches SMDs transitively via derivedFrom.
    total_upstreams = _aspects_for(workunits, total_urn, MetricUpstreamsClass)
    assert len(total_upstreams) == 1
    assert total_upstreams[0].datasetUpstreams == []


def test_primary_key_does_not_leak_across_same_named_columns():
    # A PK on ORDERS.ID must not mark CUSTOMERS.ID as part of the key.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ID": [
                _col("id", "NUMBER", SemanticViewColumnSubtype.DIMENSION, "ORDERS"),
                _col("id", "NUMBER", SemanticViewColumnSubtype.DIMENSION, "CUSTOMERS"),
            ],
        },
        logical_to_physical_table={
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
            "CUSTOMERS": (_DB, _SCHEMA, "CUSTOMERS"),
        },
        primary_key_columns_by_table={"ORDERS": {"ID"}},
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    customers_urn = _logical_dataset_urn(mapper, "CUSTOMERS")
    assert _schema_fields_by_path(workunits, orders_urn)["id"].isPartOfKey is True
    assert _schema_fields_by_path(workunits, customers_urn)["id"].isPartOfKey is False


def test_relationship_cardinality_inferred_from_primary_key():
    # Snowflake infers 1:1 when the from-side join columns are that table's primary
    # key, else many-to-one.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "CUSTOMER_ID": [
                _col(
                    "customer_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "PROFILES",
                ),
                _col(
                    "customer_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "ORDERS",
                ),
                _col(
                    "customer_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "CUSTOMERS",
                ),
            ],
        },
        logical_to_physical_table={
            "PROFILES": (_DB, _SCHEMA, "PROFILES"),
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
            "CUSTOMERS": (_DB, _SCHEMA, "CUSTOMERS"),
        },
        # PROFILES.customer_id is a PK (1:1 to CUSTOMERS); ORDERS.customer_id is not.
        primary_key_columns_by_table={
            "PROFILES": {"CUSTOMER_ID"},
            "CUSTOMERS": {"CUSTOMER_ID"},
        },
        relationships=[
            SnowflakeSemanticViewRelationship(
                name="profile_to_customer",
                from_table="PROFILES",
                from_columns=["CUSTOMER_ID"],
                to_table="CUSTOMERS",
                to_columns=["CUSTOMER_ID"],
            ),
            SnowflakeSemanticViewRelationship(
                name="order_to_customer",
                from_table="ORDERS",
                from_columns=["CUSTOMER_ID"],
                to_table="CUSTOMERS",
                to_columns=["CUSTOMER_ID"],
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
    by_name = {r.name: r for r in info.relationships}
    assert (
        by_name["profile_to_customer"].cardinality
        == ERModelRelationshipCardinalityClass.ONE_ONE
    )
    assert (
        by_name["order_to_customer"].cardinality
        == ERModelRelationshipCardinalityClass.N_ONE
    )


def test_relationship_cardinality_requires_full_composite_primary_key():
    # Joining on only part of a composite primary key does not uniquely identify a
    # row, so the relationship is many-to-one. Only a join on the complete key is
    # one-to-one.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "PART_KEY": [
                _col(
                    "part_key",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "LINEITEMS",
                ),
                _col(
                    "part_key",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "PARTSUPP",
                ),
            ],
            "SUPP_KEY": [
                _col(
                    "supp_key",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "LINEITEMS",
                ),
                _col(
                    "supp_key",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "PARTSUPP",
                ),
            ],
        },
        logical_to_physical_table={
            "LINEITEMS": (_DB, _SCHEMA, "LINEITEMS"),
            "PARTSUPP": (_DB, _SCHEMA, "PARTSUPP"),
        },
        primary_key_columns_by_table={"LINEITEMS": {"PART_KEY", "SUPP_KEY"}},
        relationships=[
            SnowflakeSemanticViewRelationship(
                name="partial_key_join",
                from_table="LINEITEMS",
                from_columns=["PART_KEY"],  # subset of the composite PK
                to_table="PARTSUPP",
                to_columns=["PART_KEY"],
            ),
            SnowflakeSemanticViewRelationship(
                name="full_key_join",
                from_table="LINEITEMS",
                from_columns=["PART_KEY", "SUPP_KEY"],  # complete composite PK
                to_table="PARTSUPP",
                to_columns=["PART_KEY", "SUPP_KEY"],
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
    by_name = {r.name: r for r in info.relationships}
    assert (
        by_name["partial_key_join"].cardinality
        == ERModelRelationshipCardinalityClass.N_ONE
    )
    assert (
        by_name["full_key_join"].cardinality
        == ERModelRelationshipCardinalityClass.ONE_ONE
    )


def test_relationship_cardinality_from_declared_unique_key():
    # Snowflake infers one-to-one when the join columns are a declared UNIQUE key,
    # not only the primary key. A subset of that unique key is still many-to-one.
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_ID": [
                _col(
                    "order_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "TRANSACTIONS",
                ),
                _col(
                    "order_id", "NUMBER", SemanticViewColumnSubtype.DIMENSION, "ORDERS"
                ),
            ],
            "TRANSACTION_ID": [
                _col(
                    "transaction_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "TRANSACTIONS",
                ),
                _col(
                    "transaction_id",
                    "NUMBER",
                    SemanticViewColumnSubtype.DIMENSION,
                    "ORDERS",
                ),
            ],
        },
        logical_to_physical_table={
            "TRANSACTIONS": (_DB, _SCHEMA, "TRANSACTIONS"),
            "ORDERS": (_DB, _SCHEMA, "ORDERS"),
        },
        # PK is TRANSACTION_ID; the unique key is the composite (ORDER_ID, TRANSACTION_ID).
        primary_key_columns_by_table={"TRANSACTIONS": {"TRANSACTION_ID"}},
        unique_key_column_sets_by_table={
            "TRANSACTIONS": [{"ORDER_ID", "TRANSACTION_ID"}]
        },
        relationships=[
            SnowflakeSemanticViewRelationship(
                name="unique_key_join",
                from_table="TRANSACTIONS",
                from_columns=["ORDER_ID", "TRANSACTION_ID"],  # the full unique key
                to_table="ORDERS",
                to_columns=["ORDER_ID", "TRANSACTION_ID"],
            ),
            SnowflakeSemanticViewRelationship(
                name="partial_unique_key_join",
                from_table="TRANSACTIONS",
                from_columns=["ORDER_ID"],  # subset of the unique key
                to_table="ORDERS",
                to_columns=["ORDER_ID"],
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
    by_name = {r.name: r for r in info.relationships}
    assert (
        by_name["unique_key_join"].cardinality
        == ERModelRelationshipCardinalityClass.ONE_ONE
    )
    assert (
        by_name["partial_unique_key_join"].cardinality
        == ERModelRelationshipCardinalityClass.N_ONE
    )


def test_column_defined_on_multiple_logical_tables_emits_per_dataset_field():
    # In the new model each logical table has its own schemaField URN, so a
    # column on multiple logical tables no longer collides - it appears on each
    # logical dataset's schemaMetadata, and no warning is raised.
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
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    customers_urn = _logical_dataset_urn(mapper, "CUSTOMERS")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    assert "status" in _schema_fields_by_path(workunits, orders_urn)
    assert "status" in _schema_fields_by_path(workunits, customers_urn)
    messages = [w.title for w in mapper.report.warnings]
    assert not any("multiple logical tables" in (m or "") for m in messages)


def test_repeated_information_schema_row_does_not_duplicate_field():
    # A repeated INFORMATION_SCHEMA row for the same column on the same logical
    # table must not produce two schemaFields with the same fieldPath on that
    # logical dataset's schemaMetadata.
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
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    # Count against the RAW fields list, not a fieldPath-keyed dict (which would
    # collapse duplicates before counting and hide the regression this guards).
    schemas = _aspects_for(workunits, orders_urn, SchemaMetadataClass)
    assert len(schemas) == 1
    order_date_fields = [f for f in schemas[0].fields if f.fieldPath == "order_date"]
    assert len(order_date_fields) == 1
    # And exactly one semanticFieldAnnotation for it.
    assert (
        len(
            _aspects_for(
                workunits,
                make_schema_field_urn(orders_urn, "order_date"),
                SemanticFieldAnnotationClass,
            )
        )
        == 1
    )


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


def test_semantic_field_expression_falls_back_to_qualified_column_ref_when_missing():
    # SemanticFieldAnnotation.expression is a required PDL field, so when
    # Snowflake reports no expression for a dimension/fact the mapper
    # synthesizes a trivial qualified column reference
    # (<logical_table_alias>.<col_name>) rather than dropping the annotation.
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
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )
    annotation = _annotation_for(workunits, orders_urn, "order_date")
    assert annotation is not None
    assert annotation.expression.dialects[0].expression == "ORDERS.order_date"


def test_relationships_populated_with_aliases_matching_logical_dataset_aliases():
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
                from_table="ORDERS",
                from_columns=["customer_id"],
                to_table="CUSTOMERS",
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
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    customers_urn = _logical_dataset_urn(mapper, "CUSTOMERS")

    assert info.relationships is not None
    assert len(info.relationships) == 1
    relationship = info.relationships[0]
    assert relationship.name == "orders_to_customers"
    # from/to must be normalized to match each logical dataset's
    # semanticModelProperties.alias (the uppercased logical-table keys) so
    # relationship references resolve.
    aliases = {
        _aspects_for(workunits, urn, SemanticModelPropertiesClass)[0].alias: urn
        for urn in (orders_urn, customers_urn)
        if _aspects_for(workunits, urn, SemanticModelPropertiesClass)
    }
    assert relationship.from_ in aliases
    assert relationship.to in aliases
    assert relationship.from_ == "ORDERS"
    assert relationship.to == "CUSTOMERS"
    assert relationship.fromColumns == ["customer_id"]
    assert relationship.toColumns == ["customer_id"]
    # FK joins are many-to-one from the referencing side to the referenced side.
    assert relationship.cardinality == ERModelRelationshipCardinalityClass.N_ONE


def test_relationship_join_columns_normalized_to_match_field_paths():
    # Snowflake reports join columns in their own casing; the emitted join keys
    # must go through the same normalization as schemaField paths
    # (snowflake_identifier(name.upper())) or they never resolve against the
    # lowercased field paths under convert_urns_to_lowercase=True.
    mapper = _make_mapper(convert_urns_to_lowercase=True)
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
                from_table="ORDERS",
                # Uppercase as Snowflake returns them.
                from_columns=["ORDER_ID"],
                to_table="CUSTOMERS",
                to_columns=["CUSTOMER_ID"],
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
    relationship = info.relationships[0]

    # Lowercased to match the lowercased field paths, not left uppercase.
    assert relationship.fromColumns == ["order_id"]
    assert relationship.toColumns == ["customer_id"]

    # Each join key must actually exist as a field path on its logical dataset.
    alias_to_urn = {
        props.alias: urn
        for urn in (_logical_dataset_urn(mapper, lt) for lt in ("ORDERS", "CUSTOMERS"))
        for props in _aspects_for(workunits, urn, SemanticModelPropertiesClass)[:1]
    }
    from_fields = _schema_fields_by_path(workunits, alias_to_urn[relationship.from_])
    to_fields = _schema_fields_by_path(workunits, alias_to_urn[relationship.to])
    assert all(col in from_fields for col in relationship.fromColumns)
    assert all(col in to_fields for col in relationship.toColumns)


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


def test_join_key_and_primary_key_columns_on_multiple_tables_do_not_warn():
    # A join-key or primary-key column legitimately defined on multiple logical
    # tables is the normal case in the new model (each logical dataset gets its
    # own schemaField), so no warning is raised.
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
                from_table="ORDERS",
                from_columns=["order_id"],
                to_table="CUSTOMERS",
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


def test_column_synonyms_emitted_as_ai_context_on_schema_field():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "ORDER_DATE": [
                _col(
                    "order_date",
                    "DATE",
                    SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                    synonyms=["date of order", "order day"],
                )
            ],
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
    )
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    field_urn = make_schema_field_urn(orders_urn, "order_date")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    ai_contexts = _aspects_for(workunits, field_urn, AiContextClass)
    assert len(ai_contexts) == 1
    assert ai_contexts[0].synonyms == ["date of order", "order day"]


def test_metric_synonyms_emitted_as_ai_context_on_metric_entity():
    mapper = _make_mapper()
    semantic_view = _make_semantic_view(
        column_occurrences={
            "TOTAL_REVENUE": [
                _col(
                    "total_revenue",
                    "NUMBER",
                    SemanticViewColumnSubtype.METRIC,
                    expression="SUM(orders.order_total)",
                    synonyms=["revenue", "sales total"],
                )
            ],
        },
    )
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    ai_contexts = _aspects_for(workunits, metric_urn, AiContextClass)
    assert len(ai_contexts) == 1
    assert ai_contexts[0].synonyms == ["revenue", "sales total"]


def test_table_synonyms_emitted_as_dataset_properties_on_logical_dataset():
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
        },
        logical_to_physical_table={"ORDERS": (_DB, _SCHEMA, "ORDERS")},
        table_synonyms={"ORDERS": ["sales_orders", "orders_table"]},
    )
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    props = _aspects_for(workunits, orders_urn, DatasetPropertiesClass)
    assert len(props) == 1
    assert props[0].customProperties == {
        "TABLE_SYNONYM_ORDERS": "sales_orders, orders_table"
    }


def test_no_ai_context_emitted_when_synonyms_absent():
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
        # No column_synonyms, no table_synonyms.
    )
    orders_urn = _logical_dataset_urn(mapper, "ORDERS")
    metric_urn = mapper.identifiers.gen_metric_urn(
        "total_revenue", semantic_view.name, _SCHEMA, _DB
    )

    workunits = list(
        mapper.gen_workunits(
            semantic_view=semantic_view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )

    # No aiContext on the schemaField, the metric, or the logical dataset
    # (aiContext isn't registered on dataset anyway).
    assert not _aspects_for(
        workunits, make_schema_field_urn(orders_urn, "order_date"), AiContextClass
    )
    assert not _aspects_for(workunits, metric_urn, AiContextClass)
    assert not _aspects_for(workunits, orders_urn, AiContextClass)
    # And no datasetProperties MCP is emitted when there are no table synonyms.
    assert not _aspects_for(workunits, orders_urn, DatasetPropertiesClass)
