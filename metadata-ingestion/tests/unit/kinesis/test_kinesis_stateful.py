from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)


@patch(
    "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
)
def test_stale_entity_removal_handler_is_in_workunit_processors(mock_session):
    mock_session.return_value = MagicMock()
    config = KinesisSourceConfig.model_validate(
        {
            "aws_config": {"aws_region": "us-east-1"},
            # Set platform_instance explicitly so the source __init__ doesn't trigger
            # the sts:GetCallerIdentity fallback (which would assign a MagicMock to
            # config.platform_instance and fail the downstream ContainerKey validation).
            "platform_instance": "test-instance",
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
                "fail_safe_threshold": 100.0,
            },
        }
    )
    ctx = PipelineContext(
        run_id="test-stateful",
        pipeline_name="test-pipeline",
        graph=MagicMock(),
    )
    source = KinesisSource(config, ctx)
    # get_workunit_processors() returns bound `process` methods of the enabled
    # WorkunitProcessor instances. With stateful ingestion configured, exactly one
    # of them must belong to AutoStaleEntityRemovalProcessor.
    processors = source.get_workunit_processors()
    matching = [
        p
        for p in processors
        if isinstance(getattr(p, "__self__", None), AutoStaleEntityRemovalProcessor)
    ]
    assert len(matching) == 1
