from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError, EndpointConnectionError

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.kinesis.kinesis import KinesisSource

CONFIG = {"aws_config": {"aws_region": "us-east-1"}, "include_firehose": True}


class TestTestConnection:
    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_basic_connectivity_pass(self, mock_session):
        mock_session.return_value.client.return_value.list_streams.return_value = {
            "StreamNames": []
        }
        report = KinesisSource.test_connection(CONFIG)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_basic_connectivity_fail_on_access_denied(self, mock_session):
        mock_session.return_value.client.return_value.list_streams.side_effect = (
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                "ListStreams",
            )
        )
        report = KinesisSource.test_connection(CONFIG)
        assert report.basic_connectivity.capable is False
        assert "AccessDenied" in (report.basic_connectivity.failure_reason or "")

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_lineage_capability_independent_of_basic(self, mock_session):
        mock_session.return_value.client.side_effect = lambda service, **kwargs: {
            "kinesis": MagicMock(
                list_streams=MagicMock(return_value={"StreamNames": []})
            ),
            "firehose": MagicMock(
                list_delivery_streams=MagicMock(
                    side_effect=ClientError(
                        {
                            "Error": {
                                "Code": "AccessDeniedException",
                                "Message": "no firehose",
                            }
                        },
                        "ListDeliveryStreams",
                    )
                )
            ),
        }.get(service, MagicMock())
        report = KinesisSource.test_connection(CONFIG)
        assert report.basic_connectivity.capable is True
        # The lineage capability check should have failed independently

        lineage = report.capability_report.get(SourceCapability.LINEAGE_COARSE)
        assert lineage is not None and lineage.capable is False

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_botocore_error_in_firehose_probe_does_not_misattribute_to_basic(
        self, mock_session
    ):
        """Regression for the _probe ClientError-only catch. If Firehose probe
        raises a BotoCoreError (e.g. EndpointConnectionError), it must NOT
        propagate out of _probe to the outer `except Exception` (which would
        overwrite basic_connectivity=False, telling the user their Kinesis
        connection is broken when actually only Firehose failed). Instead,
        basic_connectivity must remain True and LINEAGE_COARSE must reflect
        the Firehose-specific failure.
        """
        mock_session.return_value.client.side_effect = lambda service, **kwargs: {
            "kinesis": MagicMock(
                list_streams=MagicMock(return_value={"StreamNames": []})
            ),
            "firehose": MagicMock(
                list_delivery_streams=MagicMock(
                    side_effect=EndpointConnectionError(
                        endpoint_url="https://firehose.us-east-1.amazonaws.com"
                    )
                )
            ),
        }.get(service, MagicMock())
        report = KinesisSource.test_connection(CONFIG)
        # Critical: basic connectivity must NOT be overwritten by the
        # BotoCoreError from the Firehose probe.
        assert report.basic_connectivity.capable is True
        # The Firehose-specific failure must land in LINEAGE_COARSE with the
        # BotoCoreError class name (aws_error_code returns class names for
        # BotoCoreError subclasses).
        lineage = report.capability_report.get(SourceCapability.LINEAGE_COARSE)
        assert lineage is not None
        assert lineage.capable is False
        assert "EndpointConnectionError" in (lineage.failure_reason or "")


class TestSchemaMetadataCapability:
    """Glue Schema Registry probe: test_connection must report
    SCHEMA_METADATA capability when GSR is enabled, with capable=True/False
    derived from `glue:ListRegistries`. When GSR is disabled (the default),
    the capability is *absent* from the report — not reported as
    `capable=False`, since the user explicitly opted out and a False here
    would be misleading.
    """

    GSR_CONFIG = {
        "aws_config": {"aws_region": "us-east-1"},
        "glue_schema_registry": {"enabled": True, "registry_name": "default-registry"},
    }

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_schema_metadata_capable_when_glue_succeeds(self, mock_session):
        mock_session.return_value.client.side_effect = lambda service, **kwargs: {
            "kinesis": MagicMock(
                list_streams=MagicMock(return_value={"StreamNames": []})
            ),
            "glue": MagicMock(
                list_registries=MagicMock(return_value={"Registries": []})
            ),
        }.get(service, MagicMock())
        report = KinesisSource.test_connection(self.GSR_CONFIG)

        schema_md = report.capability_report.get(SourceCapability.SCHEMA_METADATA)
        assert schema_md is not None
        assert schema_md.capable is True

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_schema_metadata_not_capable_on_glue_access_denied(self, mock_session):
        """A typical misconfiguration: user enables GSR but their IAM role
        is missing glue:ListRegistries. The test_connection result MUST
        surface this as a clear "SCHEMA_METADATA capability=False" with the
        AWS error code in the failure_reason — otherwise users get a vague
        runtime failure mid-ingestion.
        """
        mock_session.return_value.client.side_effect = lambda service, **kwargs: {
            "kinesis": MagicMock(
                list_streams=MagicMock(return_value={"StreamNames": []})
            ),
            "glue": MagicMock(
                list_registries=MagicMock(
                    side_effect=ClientError(
                        {
                            "Error": {
                                "Code": "AccessDeniedException",
                                "Message": "no glue",
                            }
                        },
                        "ListRegistries",
                    )
                )
            ),
        }.get(service, MagicMock())
        report = KinesisSource.test_connection(self.GSR_CONFIG)

        schema_md = report.capability_report.get(SourceCapability.SCHEMA_METADATA)
        assert schema_md is not None
        assert schema_md.capable is False
        assert "AccessDenied" in (schema_md.failure_reason or "")
        assert "glue:ListRegistries" in (schema_md.failure_reason or "")

    @patch(
        "datahub.ingestion.source.kinesis.kinesis_config.AwsConnectionConfig.get_session"
    )
    def test_schema_metadata_absent_when_gsr_disabled(self, mock_session):
        """When GSR is disabled (the default), the SCHEMA_METADATA capability
        must NOT appear in the report at all — reporting capable=False would
        falsely imply the user can't get schemas if they enabled GSR.
        """
        mock_session.return_value.client.return_value.list_streams.return_value = {
            "StreamNames": []
        }
        # CONFIG (top of module) has GSR disabled by default
        report = KinesisSource.test_connection(CONFIG)

        assert SourceCapability.SCHEMA_METADATA not in report.capability_report
