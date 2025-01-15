import random
import string
from datetime import datetime, timezone
from typing import cast
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.datahub_classifier import (
    DataHubClassifierConfig,
    InfoTypeConfig,
    PredictionFactorsAndWeights,
    ValuesFactorConfig,
)
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from tests.integration.snowflake.common import FROZEN_TIME, default_query_results
from tests.test_helpers import mce_helpers

pytestmark = pytest.mark.integration_batch_2


def random_email():
    return (
        "".join(
            [
                random.choice(string.ascii_lowercase)
                for i in range(random.randint(10, 15))
            ]
        )
        + "@xyz.com"
    )


def random_cloud_region():
    return "".join(
        [
            random.choice(["af", "ap", "ca", "eu", "me", "sa", "us"]),
            "-",
            random.choice(["central", "north", "south", "east", "west"]),
            "-",
            str(random.randint(1, 2)),
        ]
    )


def test_snowflake_basic(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "snowflake_test_events.json"
    golden_file = test_resources_dir / "snowflake_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect, mock.patch(
        "datahub.ingestion.source.snowflake.snowflake_data_reader.SnowflakeDataReader.get_sample_data_for_table"
    ) as mock_sample_values:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        mock_sample_values.return_value = {
            "col_1": [random.randint(1, 80) for i in range(20)],
            "col_2": [random_email() for i in range(20)],
            "col_3": [random_cloud_region() for i in range(20)],
        }

        datahub_classifier_config = DataHubClassifierConfig(
            minimum_values_threshold=10,
            confidence_level_threshold=0.58,
            info_types_config={
                "Age": InfoTypeConfig(
                    prediction_factors_and_weights=PredictionFactorsAndWeights(
                        name=0, values=1, description=0, datatype=0
                    )
                ),
                "CloudRegion": InfoTypeConfig(
                    prediction_factors_and_weights=PredictionFactorsAndWeights(
                        name=0,
                        description=0,
                        datatype=0,
                        values=1,
                    ),
                    values=ValuesFactorConfig(
                        prediction_type="regex",
                        regex=[
                            r"(af|ap|ca|eu|me|sa|us)-(central|north|(north(?:east|west))|south|south(?:east|west)|east|west)-\d+"
                        ],
                    ),
                ),
            },
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
                        include_table_lineage=True,
                        include_usage_stats=True,
                        format_sql_queries=True,
                        validate_upstreams_against_patterns=False,
                        include_operational_stats=True,
                        email_as_user_identifier=True,
                        incremental_lineage=False,
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        classification=ClassificationConfig(
                            enabled=True,
                            column_pattern=AllowDenyPattern(
                                allow=[".*col_1$", ".*col_2$", ".*col_3$"]
                            ),
                            classifiers=[
                                DynamicTypedClassifierConfig(
                                    type="datahub", config=datahub_classifier_config
                                )
                            ],
                            max_workers=1,
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
        report = cast(SnowflakeV2Report, pipeline.source.get_report())
        assert report.data_dictionary_cache is not None
        cache_info = report.data_dictionary_cache.as_obj()
        assert cache_info["get_tables_for_database"]["misses"] == 1
        assert cache_info["get_views_for_database"]["misses"] == 1
        assert cache_info["get_columns_for_schema"]["misses"] == 1
        assert cache_info["get_pk_constraints_for_schema"]["misses"] == 1
        assert cache_info["get_fk_constraints_for_schema"]["misses"] == 1


def test_snowflake_tags_as_structured_properties(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    # Run the metadata ingestion pipeline.
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
                        extract_tags=TagOption.without_lineage,
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=False,
                        include_column_lineage=False,
                        include_usage_stats=False,
                        include_operational_stats=False,
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


@freeze_time(FROZEN_TIME)
def test_snowflake_private_link_and_incremental_mcps(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    # Run the metadata ingestion pipeline.
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

        # Verify the output.

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[],
        )
