"""Snowsight URL construction and dataset-type filtering in snowflake_utils."""

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeFilterConfig
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowsightUrlBuilder,
    _cleanup_qualified_name,
    is_snowflake_default_schema,
)


def test_snowsight_base_url_follows_domain_privatelink_and_override() -> None:
    public = SnowsightUrlBuilder("locator", "aws_us_west_2")
    assert public.snowsight_base_url == "https://app.snowflake.com/us-west-2/locator/"

    china = SnowsightUrlBuilder(
        "locator", "aws_cn_north_1", snowflake_domain="snowflakecomputing.cn"
    )
    assert china.snowsight_base_url.startswith("https://app.snowflake.cn/")

    override = SnowsightUrlBuilder(
        "locator", "aws_us_west_2", base_url_override="https://app.internal.example"
    )
    assert override.snowsight_base_url == "https://app.internal.example/"


def test_object_urls_use_the_object_kind_path_segment() -> None:
    builder = SnowsightUrlBuilder("locator", "aws_us_west_2")

    table_url = builder.get_external_url_for_table(
        "T", "S", "DB", SnowflakeObjectDomain.TABLE
    )
    dynamic_url = builder.get_external_url_for_table(
        "DT", "S", "DB", SnowflakeObjectDomain.DYNAMIC_TABLE
    )
    semantic_url = builder.get_external_url_for_table(
        "SV", "S", "DB", SnowflakeObjectDomain.SEMANTIC_VIEW
    )

    assert table_url is not None and table_url.endswith("/schemas/S/table/T/")
    assert dynamic_url is not None and "/dynamic-table/DT/" in dynamic_url
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
    report = SourceReport()
    filters = SnowflakeFilter(
        filter_config=SnowflakeFilterConfig(), structured_reporter=report
    )

    assert filters.is_dataset_pattern_allowed("just_a_table", "table")
    assert _cleanup_qualified_name('"two"."parts"', report) == "two.parts"
    assert len(report.infos) == 2
    # Snowflake system tables are expected to have odd names and are not reported.
    assert not filters.is_dataset_pattern_allowed("SYS$_UNPIVOT_VIEW1", "table")
    assert len(report.infos) == 2


def test_default_schemas_are_recognised_case_insensitively() -> None:
    assert is_snowflake_default_schema("information_schema")
    assert not is_snowflake_default_schema("PUBLIC_DATA")
