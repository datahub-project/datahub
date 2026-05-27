from functools import partial
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    auto_stale_entity_removal,
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
    processors = source.get_workunit_processors()
    # Structural check: one of the processors must be functools.partial(auto_stale_entity_removal, <handler>)
    # where <handler> is the source's stale_entity_removal_handler. We compare by the
    # handler identity (the bound arg) rather than partial-object equality (functools.partial
    # has no __eq__ so direct `in` would compare by identity and fail across .property accesses).
    handler = source.stale_entity_removal_handler
    matching = [
        p
        for p in processors
        if isinstance(p, partial)
        and p.func is auto_stale_entity_removal
        and p.args == (handler,)
    ]
    assert len(matching) == 1
