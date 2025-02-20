from unittest import mock

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from tests.integration.snowflake.common import default_query_results


def test_snowflake_tag_pattern():
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        tag_config = SnowflakeV2Config(
            account_id="ABC12345.ap-south-1.aws",
            username="TST_USR",
            password="TST_PWD",
            match_fully_qualified_names=True,
            schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
            tag_pattern=AllowDenyPattern(
                allow=["TEST_DB.TEST_SCHEMA.my_tag_1:my_value_1"]
            ),
            include_technical_schema=True,
            include_table_lineage=False,
            include_column_lineage=False,
            include_usage_stats=False,
            include_operational_stats=False,
            extract_tags=TagOption.without_lineage,
        )

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=tag_config),
                sink=DynamicTypedConfig(type="blackhole", config={}),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        source_report = pipeline.source.get_report()
        assert isinstance(source_report, SnowflakeV2Report)
        assert source_report.tags_scanned == 5
        assert source_report._processed_tags == {
            "TEST_DB.TEST_SCHEMA.my_tag_1:my_value_1"
        }


def test_snowflake_tag_pattern_deny():
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        tag_config = SnowflakeV2Config(
            account_id="ABC12345.ap-south-1.aws",
            username="TST_USR",
            password="TST_PWD",
            match_fully_qualified_names=True,
            schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
            tag_pattern=AllowDenyPattern(
                deny=["TEST_DB.TEST_SCHEMA.my_tag_2:my_value_2"]
            ),
            include_technical_schema=True,
            include_table_lineage=False,
            include_column_lineage=False,
            include_usage_stats=False,
            include_operational_stats=False,
            extract_tags=TagOption.without_lineage,
        )

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=tag_config),
                sink=DynamicTypedConfig(type="blackhole", config={}),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        source_report = pipeline.source.get_report()
        assert isinstance(source_report, SnowflakeV2Report)
        assert source_report.tags_scanned == 5
        assert source_report._processed_tags == {
            "OTHER_DB.OTHER_SCHEMA.my_other_tag:other",
            "TEST_DB.TEST_SCHEMA.my_tag_0:my_value_0",
            "TEST_DB.TEST_SCHEMA.my_tag_1:my_value_1",
            "TEST_DB.TEST_SCHEMA.security:pii",
        }


def test_snowflake_structured_property_pattern_deny():
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        tag_config = SnowflakeV2Config(
            account_id="ABC12345.ap-south-1.aws",
            username="TST_USR",
            password="TST_PWD",
            match_fully_qualified_names=True,
            schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
            extract_tags_as_structured_properties=True,
            structured_properties_template_cache_invalidation_interval=0,
            tag_pattern=AllowDenyPattern(
                deny=["TEST_DB.TEST_SCHEMA.my_tag_2:my_value_2"]
            ),
            structured_property_pattern=AllowDenyPattern(
                deny=["TEST_DB.TEST_SCHEMA.my_tag_[0-9]"]
            ),
            include_technical_schema=True,
            include_table_lineage=False,
            include_column_lineage=False,
            include_usage_stats=False,
            include_operational_stats=False,
            extract_tags=TagOption.without_lineage,
        )

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=tag_config),
                sink=DynamicTypedConfig(type="blackhole", config={}),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        source_report = pipeline.source.get_report()
        assert isinstance(source_report, SnowflakeV2Report)
        assert source_report.tags_scanned == 5
        assert sorted(list(source_report._processed_tags)) == [
            "snowflake.other_db.other_schema.my_other_tag",
            "snowflake.test_db.test_schema.security",
        ]
