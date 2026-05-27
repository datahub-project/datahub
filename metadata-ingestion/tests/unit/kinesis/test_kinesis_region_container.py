"""Region container is deferred — only emitted when there's at least one KDS stream
that needs it as a parent.

DataFlows (Firehose) live directly under platform_instance, not under the Region
container. So an account+region with zero KDS streams would otherwise emit an empty
Region container as pure catalog noise.
"""

from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig


def _ctx() -> PipelineContext:
    return PipelineContext(run_id="test")


def _config(**overrides) -> KinesisSourceConfig:
    base = {
        "aws_config": {"aws_region": "us-east-1"},
        "platform_instance": "test-instance",  # skip sts call
    }
    base.update(overrides)
    return KinesisSourceConfig.model_validate(base)


class TestRegionContainerLifecycle:
    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_region_container_not_emitted_when_no_streams_and_no_firehose(
        self, mock_session
    ):
        """No KDS, no Firehose — no Region container. Empty parents are pure noise."""
        mock_session.return_value = MagicMock()
        config = _config(include_streams=True, include_firehose=False)
        source = KinesisSource(config, _ctx())
        # No streams in the mocked API → stream_extractor yields nothing
        source.stream_extractor.list_stream_names = MagicMock(return_value=iter([]))
        wus = list(source.get_workunits_internal())
        container_aspects = [
            wu for wu in wus if wu.metadata.entityUrn.startswith("urn:li:container:")
        ]
        assert container_aspects == [], (
            "Region container should not be emitted when there are no KDS streams"
        )

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_region_container_not_emitted_for_firehose_only(self, mock_session):
        """Firehose-only account+region: no KDS, but DataFlow exists. The Region
        container still shouldn't appear because DataFlow has no parent_container.
        """
        mock_session.return_value = MagicMock()
        config = _config(include_streams=True, include_firehose=True)
        source = KinesisSource(config, _ctx())
        source.stream_extractor.list_stream_names = MagicMock(return_value=iter([]))
        # Mock firehose to yield nothing too (list_delivery_streams empty)
        source.firehose_extractor.list_delivery_streams = MagicMock(
            return_value=iter([])
        )
        wus = list(source.get_workunits_internal())
        container_aspects = [
            wu for wu in wus if wu.metadata.entityUrn.startswith("urn:li:container:")
        ]
        assert container_aspects == [], (
            "Region container should not appear when only Firehose entities exist"
        )
