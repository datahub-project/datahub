from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisRegionKey, KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig


class TestKinesisRegionKey:
    def test_platform_instance_affects_guid(self):
        """Two regional containers with the same region and env but different
        platform_instance produce different GUIDs. This is the load-bearing
        property: it's what lets the connector emit collision-free Region
        containers for the same region across multiple AWS accounts."""
        a = KinesisRegionKey(region="us-east-1", platform="kinesis", env="PROD")
        b = KinesisRegionKey(
            region="us-east-1", platform="kinesis", env="PROD", instance="prod-acct"
        )
        assert a.guid() != b.guid()

    def test_different_regions_produce_different_guids(self):
        """Two containers in the same account+env but different AWS regions
        must produce distinct URNs — otherwise multi-region ingestion would
        collide on a single container."""
        east = KinesisRegionKey(
            region="us-east-1", platform="kinesis", env="PROD", instance="acct-1"
        )
        west = KinesisRegionKey(
            region="us-west-2", platform="kinesis", env="PROD", instance="acct-1"
        )
        assert east.guid() != west.guid()


class TestRegionResolutionAtInit:
    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_region_snapped_from_session_when_unset(self, mock_session):
        """When aws_region isn't in the recipe, the connector snaps the boto3
        session's resolved region onto the config so downstream extractors see a
        concrete region rather than None."""
        session = MagicMock()
        session.region_name = "us-west-2"
        mock_session.return_value = session
        # platform_instance set so __init__ doesn't attempt the sts fallback.
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {}, "platform_instance": "acct"}
        )
        source = KinesisSource(config, PipelineContext(run_id="test"))
        assert source.config.aws_config.aws_region == "us-west-2"

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_unresolvable_region_raises(self, mock_session):
        """No region in the recipe AND none resolvable from the session must fail
        fast — otherwise the source would emit malformed URNs like
        kinesis,None.events,PROD."""
        session = MagicMock()
        session.region_name = None
        mock_session.return_value = session
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {}, "platform_instance": "acct"}
        )
        with pytest.raises(ValueError):
            KinesisSource(config, PipelineContext(run_id="test"))
