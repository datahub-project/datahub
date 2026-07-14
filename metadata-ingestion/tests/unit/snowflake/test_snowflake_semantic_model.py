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
    )


def _make_mapper(
    convert_urns_to_lowercase: bool = True,
    platform_instance: Optional[str] = None,
    include_view_definitions: bool = True,
) -> SnowflakeSemanticModelMapper:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
            "convert_urns_to_lowercase": convert_urns_to_lowercase,
            "platform_instance": platform_instance,
            "include_view_definitions": include_view_definitions,
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
    from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag

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
