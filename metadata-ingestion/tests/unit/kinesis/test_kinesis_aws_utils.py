"""Coverage for the tiny aws_error_code helper used across the Kinesis connector.

The helper unifies six near-identical isinstance ladders into one expression;
tests pin both branches (ClientError → response.Error.Code,
BotoCoreError → exception class name) so a future change can't quietly
flip them.
"""

from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
)

from datahub.ingestion.source.kinesis.kinesis_aws_utils import aws_error_code


class TestAwsErrorCode:
    def test_client_error_returns_aws_error_code_from_response_body(self):
        e = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "ListStreams",
        )
        assert aws_error_code(e) == "AccessDeniedException"

    def test_client_error_with_missing_code_returns_empty_string(self):
        # Defensive: AWS responses are well-formed in practice, but our
        # callsites pass the result into f-strings — must never raise KeyError.
        e = ClientError({"Error": {"Message": "no code"}}, "ListStreams")
        assert aws_error_code(e) == ""

    def test_no_credentials_error_returns_class_name(self):
        # NoCredentialsError is the most common BotoCoreError users hit when
        # the principal can't be resolved — must surface as a stable string.
        assert aws_error_code(NoCredentialsError()) == "NoCredentialsError"

    def test_endpoint_connection_error_returns_class_name(self):
        e = EndpointConnectionError(
            endpoint_url="https://firehose.us-east-1.amazonaws.com"
        )
        assert aws_error_code(e) == "EndpointConnectionError"

    def test_connect_timeout_error_returns_class_name(self):
        e = ConnectTimeoutError(endpoint_url="https://...")
        assert aws_error_code(e) == "ConnectTimeoutError"
