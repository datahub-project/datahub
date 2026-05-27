"""Tests for platform_instance / account_id resolution at source __init__.

The connector resolves an effective platform_instance at init time:
  - explicit config.platform_instance wins (no AWS call)
  - otherwise, sts:GetCallerIdentity provides account_id and the resolved value is
    f"{account_id}-{region}" — disambiguating both account and region
  - sts denial leaves platform_instance unset; a warning is recorded so users know
    why their URNs lack the account_id segment
"""

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig


def _ctx() -> PipelineContext:
    return PipelineContext(run_id="test")


def _config(**overrides) -> KinesisSourceConfig:
    base = {"aws_config": {"aws_region": "us-east-1"}}
    base.update(overrides)
    return KinesisSourceConfig.model_validate(base)


class TestPlatformInstanceResolution:
    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_explicit_platform_instance_wins_and_no_sts_call_is_made(
        self, mock_session
    ):
        """When the user sets platform_instance explicitly, the connector must NOT
        call sts:GetCallerIdentity — the user's value is authoritative and the call
        would be wasted IAM surface area.
        """
        mock_session.return_value = MagicMock()
        config = _config(platform_instance="prod-east")
        source = KinesisSource(config, _ctx())
        assert source.config.platform_instance == "prod-east"
        # Verify sts was never asked for caller identity
        sts_calls = [
            c
            for c in mock_session.return_value.client.call_args_list
            if c.args and c.args[0] == "sts"
        ]
        assert sts_calls == [], (
            "sts client must not be constructed when platform_instance is explicit"
        )

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_unset_platform_instance_falls_back_to_account_id_alone(self, mock_session):
        """Without explicit platform_instance, fall back to the AWS account_id alone.
        Region is NOT folded into platform_instance — that would double-encode the
        region (once at platform_instance level, once at the Region container level)
        in the navigation tree. Region collision safety is handled by encoding region
        into the dataset URN's name (`<region>.<stream>`) and the DataFlow's flow_id.
        """
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_session.return_value.client.side_effect = lambda service, **kw: (
            mock_sts if service == "sts" else MagicMock()
        )
        config = _config()
        source = KinesisSource(config, _ctx())
        assert source.config.platform_instance == "123456789012"
        assert source.report.account_id == "123456789012"
        assert source.report.platform_instance_resolved == "123456789012"

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_sts_denial_yields_warning_and_leaves_platform_instance_unset(
        self, mock_session
    ):
        """If sts:GetCallerIdentity is denied (unusual but possible with tightly
        scoped IAM), the connector must NOT crash. It logs a warning so the user
        knows URNs won't include account_id, then continues with platform_instance=None.
        """
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "GetCallerIdentity",
        )
        mock_session.return_value.client.side_effect = lambda service, **kw: (
            mock_sts if service == "sts" else MagicMock()
        )
        config = _config()
        source = KinesisSource(config, _ctx())
        assert source.config.platform_instance is None
        assert source.report.account_id is None
        warnings_text = " ".join(str(w) for w in source.report.warnings)
        assert "account_id" in warnings_text or "sts:GetCallerIdentity" in warnings_text
