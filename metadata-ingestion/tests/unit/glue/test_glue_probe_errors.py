import pytest
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
    ProxyConnectionError,
)

from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeSoftError
from datahub.ingestion.source.aws.glue_probe import aws_call

_PRINCIPAL = "arn:aws:sts::123456789012:assumed-role/ingest-role/someone@example.com"


def _client_error(code: str, operation: str = "GetTables") -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": code,
                "Message": f"User: {_PRINCIPAL} is not authorized to perform: glue:{operation}",
            },
            "ResponseMetadata": {
                "RequestId": "req-123",
                "HostId": "",
                "HTTPStatusCode": 400,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        },
        operation,
    )


def _raised(exc: Exception, soft_on_denied: bool = False) -> Exception:
    with pytest.raises(Exception) as info:
        with aws_call(
            "glue:GetTables on database 'sales'", soft_on_denied=soft_on_denied
        ):
            raise exc
    return info.value


def test_access_denied_is_soft_only_when_asked() -> None:
    soft = _raised(_client_error("AccessDeniedException"), soft_on_denied=True)
    hard = _raised(_client_error("AccessDeniedException"))

    assert isinstance(soft, ProbeSoftError)
    assert isinstance(hard, ProbeConnectionError)


def test_the_message_names_code_action_and_request_id_but_not_the_principal() -> None:
    error = _raised(_client_error("AccessDeniedException"))

    text = str(error)
    assert "AccessDeniedException" in text
    assert "glue:GetTables on database 'sales'" in text
    assert "req-123" in text
    assert "someone@example.com" not in text
    assert "arn:aws" not in text


def test_rejected_credentials_are_a_connection_error() -> None:
    error = _raised(_client_error("UnrecognizedClientException"))

    assert isinstance(error, ProbeConnectionError)
    assert "arn:aws" not in str(error)


def test_a_missing_entity_is_a_caller_error() -> None:
    error = _raised(_client_error("EntityNotFoundException"))

    assert type(error) is ValueError


def test_no_region_is_a_recipe_error() -> None:
    error = _raised(NoRegionError())

    assert type(error) is ValueError
    assert "aws_region" in str(error)


def test_no_credentials_is_a_connection_error() -> None:
    assert isinstance(_raised(NoCredentialsError()), ProbeConnectionError)


@pytest.mark.parametrize(
    "transport_error",
    [
        EndpointConnectionError(endpoint_url="https://glue.us-east-1.amazonaws.com/"),
        ProxyConnectionError(
            proxy_url="http://user:hunter2@proxy.example:3128", error="refused"
        ),
    ],
)
def test_transport_errors_are_named_by_class_only(
    transport_error: BotoCoreError,
) -> None:
    error = _raised(transport_error)

    assert isinstance(error, ProbeConnectionError)
    assert type(transport_error).__name__ in str(error)
    assert "hunter2" not in str(error)
    assert "proxy.example" not in str(error)


def test_a_denied_role_assumption_points_at_sts_not_lake_formation() -> None:
    error = _raised(_client_error("AccessDenied", operation="AssumeRole"))

    text = str(error)
    assert isinstance(error, ProbeConnectionError)
    assert "sts:AssumeRole" in text and "aws_role" in text
    assert "Lake Formation" not in text
    assert "someone@example.com" not in text
    assert "arn:aws" not in text


def test_a_denied_role_assumption_is_never_soft() -> None:
    error = _raised(
        _client_error("AccessDenied", operation="AssumeRole"), soft_on_denied=True
    )

    assert isinstance(error, ProbeConnectionError)


def test_invalid_request_parameters_are_a_caller_error() -> None:
    error = _raised(
        ParamValidationError(
            report="Invalid length for parameter DatabaseName, value: 0-secret-ish"
        )
    )

    assert type(error) is ValueError
    assert "ParamValidationError" in str(error)
    assert "0-secret-ish" not in str(error)
