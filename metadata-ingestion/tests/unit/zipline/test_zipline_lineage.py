from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from datahub.ingestion.source.zipline.config import (
    ZiplineConfig,
    ZiplinePlatformDetail,
)
from datahub.ingestion.source.zipline.lineage import (
    SourceResolver,
    StagingQueryLineageExtractor,
    build_group_by_column_lineage,
    build_join_column_lineage,
    strip_sql_templates,
)
from datahub.ingestion.source.zipline.models import (
    Aggregation,
    EntitySource,
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    MetaData,
    Source,
    Window,
)
from datahub.ingestion.source.zipline.report import ZiplineSourceReport


def _resolver(**overrides: Any) -> SourceResolver:
    config = ZiplineConfig(
        path="/tmp/x",
        source_platform_map={"warehouse": "snowflake", "data": "hive"},
        default_source_platform="hive",
        stream_platform="kafka",
        **overrides,
    )
    return SourceResolver(config, ZiplineSourceReport())


def test_resolve_table_urn_uses_namespace_mapping():
    resolver = _resolver()
    assert resolver.resolve_table_urn("warehouse.accounts") == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.accounts,PROD)"
    )


def test_resolve_table_urn_namespace_match_is_case_insensitive():
    # The map key is capitalized but the table's namespace is lower-case; they
    # must still match so mixed-case Chronon tables resolve to the right platform.
    config = ZiplineConfig(
        path="/tmp/x",
        source_platform_map={"Warehouse": "snowflake"},
        default_source_platform="hive",
    )
    resolver = SourceResolver(config, ZiplineSourceReport())
    assert resolver.resolve_table_urn("warehouse.accounts") == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.accounts,PROD)"
    )
    assert not resolver.report.warnings


def test_resolve_table_urn_lowercases_when_enabled():
    config = ZiplineConfig(
        path="/tmp/x",
        source_platform_map={"warehouse": "snowflake"},
        default_source_platform="hive",
        convert_urns_to_lowercase=True,
    )
    resolver = SourceResolver(config, ZiplineSourceReport())
    assert resolver.resolve_table_urn("Warehouse.Accounts") == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.accounts,PROD)"
    )


def test_resolve_table_urn_falls_back_to_default_and_warns_once():
    resolver = _resolver()
    urn = resolver.resolve_table_urn("legacy.audit_log")
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:hive,legacy.audit_log,PROD)"
    # Namespace not in the map is surfaced to the operator, not silently dropped.
    assert "legacy" in resolver.report.unmapped_source_namespaces
    assert len(resolver.report.warnings) == 1

    # Seeing the same namespace again must not add a second warning.
    resolver.resolve_table_urn("legacy.other_table")
    assert len(resolver.report.warnings) == 1


def _extractor() -> StagingQueryLineageExtractor:
    resolver = _resolver()
    return StagingQueryLineageExtractor(
        resolver.config, resolver.report, graph=None, source_resolver=resolver
    )


def test_staging_query_reresolves_in_tables_through_platform_map():
    # sqlglot attributes every derived table to default_source_platform (hive);
    # re-resolving through the map re-maps warehouse.* to snowflake so lineage
    # actually stitches to the native connector.
    extractor = _extractor()
    result = SimpleNamespace(
        debug_info=None,
        column_lineage=None,
        in_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.accounts,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,data.raw_purchases,PROD)",
        ],
    )
    with patch(
        "datahub.ingestion.source.zipline.lineage.create_lineage_from_sql_statements",
        return_value=result,
    ):
        lineage = extractor.extract(
            "SELECT 1", output_table=None, default_namespace=None, name="team.sq.v1"
        )

    assert lineage.input_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:hive,data.raw_purchases,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.accounts,PROD)",
    ]
    assert extractor.report.sql_lineage_parsed == 1


def test_staging_query_sql_result_error_warns():
    # sqlglot's common failure mode is a result object carrying an error rather
    # than a raised exception — this must surface, not be swallowed at debug.
    extractor = _extractor()
    result = SimpleNamespace(
        debug_info=SimpleNamespace(error="could not parse"),
        column_lineage=None,
        in_tables=[],
    )
    with patch(
        "datahub.ingestion.source.zipline.lineage.create_lineage_from_sql_statements",
        return_value=result,
    ):
        lineage = extractor.extract(
            "SELECT 1", output_table=None, default_namespace=None, name="team.sq.v1"
        )

    assert lineage.input_urns == []
    assert extractor.report.sql_lineage_failures == 1
    assert extractor.report.sql_lineage_parsed == 0
    assert len(extractor.report.warnings) == 1


def test_staging_query_sql_exception_warns():
    extractor = _extractor()
    with patch(
        "datahub.ingestion.source.zipline.lineage.create_lineage_from_sql_statements",
        side_effect=RuntimeError("boom"),
    ):
        lineage = extractor.extract(
            "SELECT 1", output_table=None, default_namespace=None, name="team.sq.v1"
        )

    assert lineage.input_urns == []
    assert extractor.report.sql_lineage_failures == 1
    assert len(extractor.report.warnings) == 1


def test_resolve_event_source_table_and_topic():
    resolver = _resolver()
    source = Source(
        events=EventSource(table="data.purchases", topic="events.purchases")
    )
    assert resolver.resolve_source_urns(source) == [
        "urn:li:dataset:(urn:li:dataPlatform:hive,data.purchases,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,events.purchases,PROD)",
    ]


def test_resolve_entity_source_snapshot_and_mutation_topic():
    resolver = _resolver()
    source = Source(
        entities=EntitySource(
            snapshotTable="warehouse.accounts",
            mutationTopic="events.account_updates",
        )
    )
    assert resolver.resolve_source_urns(source) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.accounts,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,events.account_updates,PROD)",
    ]


def test_resolve_table_urn_three_tier_prepends_default_db():
    # A two-part Chronon name on a three-tier platform must gain the database
    # so the URN matches the native Snowflake connector's `db.schema.table`.
    config = ZiplineConfig(
        path="/tmp/x",
        source_platform_map={
            "analytics": ZiplinePlatformDetail(platform="snowflake", default_db="prod")
        },
    )
    resolver = SourceResolver(config, ZiplineSourceReport())
    assert resolver.resolve_table_urn("analytics.events") == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.events,PROD)"
    )


def test_resolve_field_urn_lowercases_column_per_namespace():
    config = ZiplineConfig(
        path="/tmp/x",
        source_platform_map={
            "warehouse": ZiplinePlatformDetail(
                platform="snowflake", convert_urns_to_lowercase=True
            )
        },
    )
    resolver = SourceResolver(config, ZiplineSourceReport())
    field = resolver.resolve_field_urn("Warehouse.Accounts", "Amount")
    assert "warehouse.accounts" in field
    assert field.endswith(",amount)")


def test_build_group_by_column_lineage_maps_input_to_features():
    resolver = _resolver()
    group_by = GroupBy(
        metaData=MetaData(name="team.purchases", outputNamespace="data"),
        sources=[Source(events=EventSource(table="data.events"))],
        aggregations=[
            Aggregation(
                input_column="amount",
                operation=7,
                windows=[Window(length=7, time_unit=1)],
            )
        ],
    )
    features = group_by.aggregations[0].output_column_names()
    assert features
    output_table = group_by.meta_data.output_table_name()
    assert output_table is not None

    fine_grained = build_group_by_column_lineage(group_by, resolver)
    upstream = resolver.resolve_field_urn("data.events", "amount")
    assert len(fine_grained) == len(features)
    for edge, feature in zip(fine_grained, features, strict=True):
        assert edge.upstreams == [upstream]
        assert edge.downstreams == [resolver.resolve_field_urn(output_table, feature)]


def test_build_join_column_lineage_prefixes_feature_columns():
    resolver = _resolver()
    group_by = GroupBy(
        metaData=MetaData(name="team.purchases", outputNamespace="data"),
        aggregations=[Aggregation(input_column="amount", operation=7)],
    )
    join = Join(
        metaData=MetaData(name="team.checkout", outputNamespace="data"),
        joinParts=[JoinPart(group_by=group_by, prefix="p")],
    )
    features = group_by.feature_names()
    assert features
    group_by_table = group_by.meta_data.output_table_name()
    join_table = join.meta_data.output_table_name()
    assert group_by_table is not None and join_table is not None

    fine_grained = build_join_column_lineage(join, resolver, {})
    assert len(fine_grained) == len(features)
    for edge, feature in zip(fine_grained, features, strict=True):
        assert edge.upstreams == [resolver.resolve_field_urn(group_by_table, feature)]
        assert edge.downstreams == [
            resolver.resolve_field_urn(join_table, f"p_{feature}")
        ]


def test_staging_query_emits_column_lineage():
    extractor = _extractor()
    upstream_ref = SimpleNamespace(
        table="urn:li:dataset:(urn:li:dataPlatform:hive,data.raw,PROD)", column="amount"
    )
    entry = SimpleNamespace(
        downstream=SimpleNamespace(column="total"), upstreams=[upstream_ref]
    )
    result = SimpleNamespace(
        debug_info=None,
        in_tables=["urn:li:dataset:(urn:li:dataPlatform:hive,data.raw,PROD)"],
        column_lineage=[entry],
    )
    with patch(
        "datahub.ingestion.source.zipline.lineage.create_lineage_from_sql_statements",
        return_value=result,
    ):
        lineage = extractor.extract(
            "SELECT SUM(amount) AS total FROM data.raw",
            output_table="data.summary",
            default_namespace="data",
            name="team.sq.v1",
        )

    assert len(lineage.fine_grained_lineages) == 1
    edge = lineage.fine_grained_lineages[0]
    assert edge.downstreams == [
        extractor.source_resolver.resolve_field_urn("data.summary", "total")
    ]
    assert edge.upstreams == [
        extractor.source_resolver.resolve_field_urn("data.raw", "amount")
    ]


def test_staging_query_bare_select_produces_column_lineage():
    # A StagingQuery is a bare SELECT with no target; without the CTAS wrap the
    # statement-level parser yields no column lineage. This exercises real sqlglot
    # (not a mock) and confirms upstreams re-resolve through the platform map.
    extractor = _extractor()
    lineage = extractor.extract(
        query="SELECT user_id, amount FROM warehouse.accounts",
        output_table="data.summary",
        default_namespace="data",
        name="team.sq.v1",
    )
    assert lineage.input_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.accounts,PROD)"
    ]
    downstreams = {
        d for edge in lineage.fine_grained_lineages for d in (edge.downstreams or [])
    }
    assert downstreams == {
        extractor.source_resolver.resolve_field_urn("data.summary", "user_id"),
        extractor.source_resolver.resolve_field_urn("data.summary", "amount"),
    }


def test_strip_sql_templates_preserves_surrounding_quotes():
    # A quoted Jinja macro must remain a single valid string literal after
    # stripping, otherwise the SQL fails to parse.
    query = "SELECT * FROM t WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'"
    cleaned = strip_sql_templates(query)
    assert "{{" not in cleaned
    assert "''" not in cleaned
    assert cleaned.count("'__zipline_template__'") == 2
