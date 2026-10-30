"""Snowsight URL construction and dataset-type filtering in snowflake_utils."""

from typing import List

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeFilterConfig
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowsightUrlBuilder,
    _cleanup_qualified_name,
    is_snowflake_default_schema,
)
from tests.unit.snowflake.conftest import log_keys


def test_snowsight_base_url_uses_the_china_domain() -> None:
    """The public and override forms are covered by test_snowflake_source.py."""
    china = SnowsightUrlBuilder(
        "locator", "aws_cn_north_1", snowflake_domain="snowflakecomputing.cn"
    )
    assert china.snowsight_base_url.startswith("https://app.snowflake.cn/")


def test_semantic_view_urls_use_their_own_path_segment() -> None:
    builder = SnowsightUrlBuilder("locator", "aws_us_west_2")

    semantic_url = builder.get_external_url_for_table(
        "SV", "S", "DB", SnowflakeObjectDomain.SEMANTIC_VIEW
    )

    assert semantic_url is not None and "/semantic-view/SV/" in semantic_url


def _filter(**overrides: object) -> SnowflakeFilter:
    config = SnowflakeFilterConfig.model_validate(
        {
            "stream_pattern": {"deny": [".*\\.old_.*"]},
            "semantic_view_pattern": {"deny": [".*\\.draft_.*"]},
            "view_pattern": {"deny": [".*\\.hidden_.*"]},
            **overrides,
        }
    )
    return SnowflakeFilter(filter_config=config, structured_reporter=SourceReport())


def test_dataset_pattern_is_chosen_by_object_kind() -> None:
    filters = _filter()

    assert filters.is_dataset_pattern_allowed("db.s.events", "stream")
    assert not filters.is_dataset_pattern_allowed("db.s.old_events", "stream")
    assert filters.is_dataset_pattern_allowed("db.s.model", "semantic view")
    assert not filters.is_dataset_pattern_allowed("db.s.draft_model", "semantic view")
    assert not filters.is_dataset_pattern_allowed("db.s.hidden_v", "materialized view")
    # Kinds the connector does not ingest are rejected outright; a missing kind
    # or name cannot be filtered and is let through.
    assert not filters.is_dataset_pattern_allowed("db.s.stage1", "stage")
    assert filters.is_dataset_pattern_allowed(None, "table")
    assert filters.is_dataset_pattern_allowed("db.s.t", None)


def test_unexpected_name_shapes_are_reported_once_and_kept_filterable() -> None:
    """Odd identifiers are surfaced as infos (never failures) so filtering keeps
    working, and Snowflake's own SYS$ names do not add noise."""
    report = SourceReport()
    filters = SnowflakeFilter(
        filter_config=SnowflakeFilterConfig(), structured_reporter=report
    )

    def reported_contexts() -> List[str]:
        return [ctx for entry in report.infos for ctx in entry.context]

    assert filters.is_dataset_pattern_allowed("just_a_table", "table")
    assert _cleanup_qualified_name('"two"."parts"', report) == "two.parts"
    assert log_keys(report.infos) == {"Unexpected dataset pattern"}
    assert "just_a_table" in reported_contexts()
    assert any(ctx.startswith('"two"."parts"') for ctx in reported_contexts())
    assert report.failures == []
    # Snowflake system tables are expected to have odd names and are not reported.
    assert not filters.is_dataset_pattern_allowed("SYS$_UNPIVOT_VIEW1", "table")
    assert not any("SYS$" in ctx for ctx in reported_contexts())


def test_default_schemas_are_recognised_case_insensitively() -> None:
    assert is_snowflake_default_schema("information_schema")
    assert not is_snowflake_default_schema("PUBLIC_DATA")
