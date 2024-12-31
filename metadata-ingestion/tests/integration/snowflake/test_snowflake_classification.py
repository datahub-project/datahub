import os
from functools import partial
from typing import cast
from unittest import mock

import pandas as pd
import pytest

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifierConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from tests.integration.snowflake.common import default_query_results

NUM_SAMPLE_VALUES = 100
TEST_CLASSIFY_PERFORMANCE = os.environ.get("DATAHUB_TEST_CLASSIFY_PERFORMANCE")

sample_values = ["abc@xyz.com" for _ in range(NUM_SAMPLE_VALUES)]


# Run with --durations=0 to show the timings for different combinations
@pytest.mark.skipif(
    TEST_CLASSIFY_PERFORMANCE is None,
    reason="DATAHUB_TEST_CLASSIFY_PERFORMANCE env variable is not configured",
)
@pytest.mark.parametrize(
    "num_workers,num_cols_per_table,num_tables",
    [(w, c, t) for w in [1, 2, 4, 6, 8] for c in [5, 10, 40, 80] for t in [1]],
)
def test_snowflake_classification_perf(num_workers, num_cols_per_table, num_tables):
    with mock.patch("snowflake.connector.connect") as mock_connect, mock.patch(
        "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeV2Source.get_sample_values_for_table"
    ) as mock_sample_values:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = partial(
            default_query_results, num_tables=num_tables, num_cols=num_cols_per_table
        )

        mock_sample_values.return_value = pd.DataFrame(
            data={f"col_{i}": sample_values for i in range(1, num_cols_per_table + 1)}
        )

        datahub_classifier_config = DataHubClassifierConfig(
            confidence_level_threshold=0.58,
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
                        include_column_lineage=False,
                        include_usage_stats=False,
                        include_operational_stats=False,
                        classification=ClassificationConfig(
                            enabled=True,
                            max_workers=num_workers,
                            classifiers=[
                                DynamicTypedClassifierConfig(
                                    type="datahub", config=datahub_classifier_config
                                )
                            ],
                        ),
                    ),
                ),
                sink=DynamicTypedConfig(type="blackhole", config={}),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        source_report = pipeline.source.get_report()
        assert isinstance(source_report, SnowflakeV2Report)
        assert (
            cast(SnowflakeV2Report, source_report).num_tables_classification_found
            == num_tables
        )
        assert (
            len(
                cast(SnowflakeV2Report, source_report).info_types_detected[
                    "Email_Address"
                ]
            )
            == num_tables * num_cols_per_table
        )
