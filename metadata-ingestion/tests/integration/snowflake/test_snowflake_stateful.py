from unittest import mock

from freezegun import freeze_time

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from tests.integration.snowflake.common import FROZEN_TIME, default_query_results
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def stateful_pipeline_config(include_tables: bool) -> PipelineConfig:
    return PipelineConfig(
        pipeline_name="test_snowflake",
        source=SourceConfig(
            type="snowflake",
            config=SnowflakeV2Config(
                account_id="ABC12345.ap-south-1.aws",
                username="TST_USR",
                password="TST_PWD",
                match_fully_qualified_names=True,
                schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                include_tables=include_tables,
                incremental_lineage=False,
                stateful_ingestion=StatefulStaleMetadataRemovalConfig.parse_obj(
                    {
                        "enabled": True,
                        "remove_stale_metadata": True,
                        "fail_safe_threshold": 100.0,
                        "state_provider": {
                            "type": "datahub",
                            "config": {"datahub_api": {"server": GMS_SERVER}},
                        },
                    }
                ),
            ),
        ),
        sink=DynamicTypedConfig(type="blackhole"),
    )


@freeze_time(FROZEN_TIME)
def test_stale_metadata_removal(mock_datahub_graph):
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline_run1 = Pipeline(config=stateful_pipeline_config(True))
        pipeline_run1.run()
        pipeline_run1.raise_from_status()
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        mock_checkpoint.return_value = mock_datahub_graph
        pipeline_run2 = Pipeline(config=stateful_pipeline_config(False))
        pipeline_run2.run()
        pipeline_run2.raise_from_status()
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted table should not be
    # part of the second state
    state1 = checkpoint1.state
    state2 = checkpoint2.state

    difference_dataset_urns = list(
        state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
    )
    assert sorted(difference_dataset_urns) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_10,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_3,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_4,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_5,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_6,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_7,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_8,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table_9,PROD)",
    ]
