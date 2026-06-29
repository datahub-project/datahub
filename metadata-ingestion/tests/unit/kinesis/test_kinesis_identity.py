"""Tests for platform_instance / account_id resolution at source __init__.

The connector resolves an effective platform_instance at init time:
  - explicit config.platform_instance wins (no resolution lookup)
  - otherwise, the AWS account_id is parsed from a resource ARN (field 4 of a
    ListStreams / DescribeStream / DescribeDeliveryStream ARN) and becomes the
    platform_instance (region is encoded in the dataset name / DataFlow id, not here)
  - a failed/empty lookup leaves platform_instance unset, with a warning
"""

from typing import Any
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig


def _ctx() -> PipelineContext:
    return PipelineContext(run_id="test")


def _config(**overrides: Any) -> KinesisSourceConfig:
    base = {"aws_config": {"aws_region": "us-east-1"}}
    base.update(overrides)
    return KinesisSourceConfig.model_validate(base)


def _session_with_kinesis(kinesis_mock: MagicMock) -> MagicMock:
    """A mock boto3 session whose client('kinesis') returns kinesis_mock; any other
    service returns a fresh MagicMock."""
    session = MagicMock()
    session.client.side_effect = lambda service, **kw: (
        kinesis_mock if service == "kinesis" else MagicMock()
    )
    return session


class TestPlatformInstanceResolution:
    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_explicit_platform_instance_wins_without_lookup(self, mock_session):
        """When platform_instance is set explicitly, the connector must NOT make an
        account-resolution lookup — the user's value is authoritative."""
        kinesis_mock = MagicMock()
        mock_session.return_value = _session_with_kinesis(kinesis_mock)
        source = KinesisSource(_config(platform_instance="prod-east"), _ctx())
        assert source.config.platform_instance == "prod-east"
        kinesis_mock.list_streams.assert_not_called()

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_account_id_derived_from_stream_arn(self, mock_session):
        """Without explicit platform_instance, the account_id is parsed from a
        resource ARN (field 4) and used as the platform_instance. Region is NOT
        folded in — collision safety lives in the dataset name / DataFlow id."""
        kinesis_mock = MagicMock()
        kinesis_mock.list_streams.return_value = {
            "StreamSummaries": [
                {
                    "StreamName": "events",
                    "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/events",
                }
            ]
        }
        mock_session.return_value = _session_with_kinesis(kinesis_mock)
        source = KinesisSource(_config(), _ctx())
        assert source.config.platform_instance == "123456789012"
        assert source.report.account_id == "123456789012"
        assert source.report.platform_instance_resolved == "123456789012"

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_account_id_derived_via_describe_when_only_names_returned(
        self, mock_session
    ):
        """Older ListStreams responses return only StreamNames; the connector then
        describes one stream to read its ARN."""
        kinesis_mock = MagicMock()
        kinesis_mock.list_streams.return_value = {"StreamNames": ["events"]}
        kinesis_mock.describe_stream.return_value = {
            "StreamDescription": {
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/events"
            }
        }
        mock_session.return_value = _session_with_kinesis(kinesis_mock)
        source = KinesisSource(_config(), _ctx())
        assert source.config.platform_instance == "123456789012"

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_lookup_failure_yields_warning_and_leaves_platform_instance_unset(
        self, mock_session
    ):
        """If the account-resolution lookup is denied/unavailable, the connector must
        NOT crash — it warns and continues with platform_instance=None."""
        kinesis_mock = MagicMock()
        kinesis_mock.list_streams.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "ListStreams",
        )
        mock_session.return_value = _session_with_kinesis(kinesis_mock)
        source = KinesisSource(_config(), _ctx())
        assert source.config.platform_instance is None
        assert source.report.account_id is None
        warnings_text = " ".join(str(w) for w in source.report.warnings)
        assert "account_id" in warnings_text
