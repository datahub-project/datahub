from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stateful_ingestion_registers_stale_removal_processor(mock_qlik_api):
    config = QlikSourceConfig.model_validate(
        {
            "tenant_hostname": "tenant.example",
            "api_key": "secret",
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
            },
        }
    )
    ctx = PipelineContext(
        run_id="test-stateful",
        pipeline_name="test-pipeline",
        graph=MagicMock(),
    )
    source = QlikSenseSource(config, ctx)

    processors = source.get_workunit_processors()

    matching = [
        processor
        for processor in processors
        if isinstance(
            getattr(processor, "__self__", None), AutoStaleEntityRemovalProcessor
        )
    ]
    assert len(matching) == 1
