from typing import Any

from datahub.ingestion.source.zipline.config import ZiplineConfig
from datahub.ingestion.source.zipline.lineage import (
    SourceResolver,
    strip_sql_templates,
)
from datahub.ingestion.source.zipline.models import EntitySource, EventSource, Source
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


def test_resolve_table_urn_falls_back_to_default_and_reports_unmapped():
    resolver = _resolver()
    urn = resolver.resolve_table_urn("legacy.audit_log")
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:hive,legacy.audit_log,PROD)"
    # Namespace not in the map is surfaced to the operator, not silently dropped.
    assert "legacy" in resolver.report.unmapped_source_namespaces


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


def test_strip_sql_templates_preserves_surrounding_quotes():
    # A quoted Jinja macro must remain a single valid string literal after
    # stripping, otherwise the SQL fails to parse.
    query = "SELECT * FROM t WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'"
    cleaned = strip_sql_templates(query)
    assert "{{" not in cleaned
    assert "''" not in cleaned
    assert cleaned.count("'__zipline_template__'") == 2
