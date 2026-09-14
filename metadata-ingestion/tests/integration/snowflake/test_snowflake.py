from datetime import datetime, timezone
from typing import cast
from unittest import mock

import pytest
import time_machine

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import (
    FROZEN_TIME,
    default_query_results,
)

pytestmark = pytest.mark.integration_batch_1


def test_snowflake_basic(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_test_events.json"
    golden_file = test_resources_dir / "snowflake_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=True,
                        include_usage_stats=True,
                        format_sql_queries=True,
                        validate_upstreams_against_patterns=False,
                        include_operational_stats=True,
                        incremental_lineage=False,
                        use_queries_v2=False,
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        profiling=GEProfilingConfig(
                            enabled=True,
                            profile_if_updated_since_days=None,
                            profile_table_row_limit=None,
                            profile_table_size_limit=None,
                            profile_table_level_only=True,
                        ),
                        extract_tags=TagOption.without_lineage,
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()
        assert not pipeline.source.get_report().warnings

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['fields'\]\[\d+\]\['glossaryTerms'\]\['auditStamp'\]\['time'\]",
                r"root\[\d+\]\['systemMetadata'\]",
            ],
        )
        report = cast(SnowflakeV2Report, pipeline.source.get_report())
        assert report.data_dictionary_cache is not None
        cache_info = report.data_dictionary_cache.as_obj()
        assert cache_info["get_tables_for_database"]["misses"] == 1
        assert cache_info["get_views_for_database"]["misses"] == 1
        # When streams query specific tables, the query will not be cached resulting in 2 cache misses
        assert cache_info["get_columns_for_schema"]["misses"] == 2
        assert cache_info["get_pk_constraints_for_schema"]["misses"] == 1
        assert cache_info["get_fk_constraints_for_schema"]["misses"] == 1


@time_machine.travel(FROZEN_TIME, tick=False)
def test_snowflake_basic_disable_queries(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """
    Test that include_queries=False properly disables query entity generation.

    Note: This test uses a simpler configuration compared to test_snowflake_basic
    (no classification, profiling, usage stats, etc.), so differences in the golden
    files will include those aspects in addition to the query-related changes.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_basic_disable_queries_test_events.json"
    golden_file = test_resources_dir / "snowflake_basic_disable_queries_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        config = SnowflakeV2Config(
            account_id="ABC12345.ap-south-1.aws",
            username="TST_USR",
            password="TST_PWD",
            match_fully_qualified_names=True,
            schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
            include_technical_schema=True,
            include_table_lineage=True,
            include_queries=False,  # This is the key difference - disable query generation
            format_sql_queries=True,
            validate_upstreams_against_patterns=False,
            incremental_lineage=False,
            use_queries_v2=False,
            start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(tzinfo=timezone.utc),
            end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(tzinfo=timezone.utc),
        )

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=config),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
                r"root\[\d+\]\['systemMetadata'\]",
            ],
        )

        report = cast(SnowflakeV2Report, pipeline.source.get_report())

        assert report.sql_aggregator is not None
        assert report.sql_aggregator.num_queries_entities_generated == 0, (
            "No query entities should be generated when include_queries=False"
        )


def test_snowflake_tags_as_structured_properties(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_structured_properties_test_events.json"
    golden_file = test_resources_dir / "snowflake_structured_properties_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        extract_tags_as_structured_properties=True,
                        structured_property_pattern=AllowDenyPattern(
                            deny=["test_db.test_schema.my_tag_2"]
                        ),
                        extract_tags=TagOption.without_lineage,
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        use_queries_v2=False,
                        include_technical_schema=True,
                        include_table_lineage=False,
                        include_column_lineage=False,
                        include_usage_stats=False,
                        include_operational_stats=False,
                        structured_properties_template_cache_invalidation_interval=0,
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()
        assert not pipeline.source.get_report().warnings

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['fields'\]\[\d+\]\['glossaryTerms'\]\['auditStamp'\]\['time'\]",
                r"root\[\d+\]\['systemMetadata'\]",
            ],
        )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_snowflake_private_link_and_incremental_mcps(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_privatelink_test_events.json"
    golden_file = test_resources_dir / "snowflake_privatelink_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.privatelink",
                        username="TST_USR",
                        password="TST_PWD",
                        schema_pattern=AllowDenyPattern(allow=["test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=True,
                        include_column_lineage=False,
                        include_views=True,
                        include_usage_stats=False,
                        format_sql_queries=True,
                        use_queries_v2=False,
                        incremental_lineage=False,
                        incremental_properties=True,
                        include_operational_stats=False,
                        platform_instance="instance1",
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[],
        )


def test_snowflake_schema_extraction_one_table_multiple_views(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = (
        tmp_path / "snowflake_test_schema_extraction_one_table_multiple_views.json"
    )
    golden_file = (
        test_resources_dir
        / "snowflake_schema_extraction_one_table_multiple_views_golden.json"
    )

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = lambda query: default_query_results(
            query, num_tables=1, num_views=3
        )

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=False,
                        include_usage_stats=False,
                        format_sql_queries=False,
                        include_column_lineage=False,
                        validate_upstreams_against_patterns=False,
                        include_operational_stats=False,
                        incremental_lineage=False,
                        use_queries_v2=True,
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()
        assert not pipeline.source.get_report().warnings

        # Verify the output.

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['fields'\]\[\d+\]\['glossaryTerms'\]\['auditStamp'\]\['time'\]",
                r"root\[\d+\]\['systemMetadata'\]",
            ],
        )
