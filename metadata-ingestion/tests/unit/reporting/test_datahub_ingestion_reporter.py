import pytest

from datahub.ingestion.reporting.datahub_ingestion_run_summary_provider import (
    DatahubIngestionRunSummaryProvider,
    DatahubIngestionRunSummaryProviderConfig,
)
from datahub.ingestion.run.pipeline_config import PipelineConfig


@pytest.mark.parametrize(
    "pipeline_config, expected_key",
    [
        (
            {
                "source": {
                    "type": "snowflake",
                    "config": {"platform_instance": "my_instance"},
                },
                "sink": {"type": "console"},
                "pipeline_name": "urn:li:ingestionSource:12345",
            },
            {
                "type": "snowflake",
                "platform_instance": "my_instance",
                "pipeline_name": "urn:li:ingestionSource:12345",
            },
        ),
        (
            {"source": {"type": "snowflake"}, "sink": {"type": "console"}},
            {"type": "snowflake"},
        ),
        (
            {
                "source": {"type": "snowflake"},
                "sink": {"type": "console"},
                "pipeline_name": "foobar",
            },
            {"type": "snowflake", "pipeline_name": "foobar"},
        ),
    ],
    ids=["all_things", "minimal", "with_pipeline_name"],
)
def test_unique_key_gen(pipeline_config, expected_key):
    config = PipelineConfig.from_dict(pipeline_config)
    key = DatahubIngestionRunSummaryProvider.generate_unique_key(config)
    assert key == expected_key


def test_default_config():
    typed_config = DatahubIngestionRunSummaryProviderConfig.parse_obj({})
    assert typed_config.sink is None
    assert typed_config.report_recipe is True
