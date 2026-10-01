"""Probe provider for the AWS Glue Data Catalog: databases, tables and jobs.

Metadata only. Every record is an allowlist projection of the Glue API shape:
Glue keeps free-form parameters on databases, tables and columns, and job
arguments that routinely carry connection passwords, and register_secrets only
masks secrets that came from the recipe.
"""

from contextlib import contextmanager
from typing import Iterator, Union

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    NoRegionError,
    PartialCredentialsError,
)

from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeSoftError
from datahub.ingestion.source.aws.aws_common import aws_error_code

# A refusal of one call by IAM or Lake Formation. Glue says
# AccessDeniedException; STS (aws_role) says AccessDenied.
_ACCESS_DENIED_CODES = frozenset({"AccessDeniedException", "AccessDenied"})
# The request was not accepted as coming from valid credentials at all.
_AUTH_CODES = frozenset(
    {
        "UnrecognizedClientException",
        "InvalidClientTokenId",
        "ExpiredTokenException",
        "ExpiredToken",
        "InvalidSignatureException",
        "SignatureDoesNotMatch",
        "IncompleteSignature",
        "MissingAuthenticationTokenException",
    }
)


def _request_id_suffix(exc: ClientError) -> str:
    request_id = exc.response.get("ResponseMetadata", {}).get("RequestId")
    return f" (AWS request id {request_id})" if request_id else ""


def _translated(
    exc: Union[ClientError, BotoCoreError], action: str, soft_on_denied: bool
) -> Exception:
    """The probe's own exception for one failed AWS call.

    Worded here, never from str(exc). A ClientError renders as "An error
    occurred (Code) when calling the Op operation: <message>", and for an
    authorization failure the message names the calling principal's ARN: the
    account id, the role, and for assumed roles and SSO a session name that is
    often a person's email. botocore keeps key material out of its own text
    (it masks proxy userinfo, and the request id lives in ResponseMetadata),
    but an identity is not ours to print. The code, the action and the
    request id are enough to diagnose with.
    """
    if isinstance(exc, NoRegionError):
        return ValueError(
            "no AWS region was resolved for Glue; set aws_region in the recipe"
        )
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return ProbeConnectionError(
            f"no usable AWS credentials were found for {action} "
            f"({type(exc).__name__}); set aws_access_key_id and "
            f"aws_secret_access_key, aws_profile or aws_role, or run where the "
            f"default AWS credential chain resolves"
        )
    if isinstance(exc, ClientError):
        code = aws_error_code(exc) or "unknown error"
        suffix = _request_id_suffix(exc)
        if code in _ACCESS_DENIED_CODES:
            message = (
                f"{action} was denied ({code}){suffix}; the recipe's AWS "
                f"principal needs this IAM permission, and Lake Formation "
                f"permission where Lake Formation governs the catalog"
            )
            if soft_on_denied:
                return ProbeSoftError(message)
            return ProbeConnectionError(message)
        if code in _AUTH_CODES:
            return ProbeConnectionError(
                f"AWS did not accept the recipe's credentials on {action} "
                f"({code}){suffix}; check aws_access_key_id/"
                f"aws_secret_access_key, aws_profile or aws_role"
            )
        if code == "EntityNotFoundException":
            return ValueError(f"{action} found no such object ({code})")
        return ProbeConnectionError(f"{action} failed ({code}){suffix}")
    # Endpoint, proxy, timeout and SSL errors. Named by class only: SSLError's
    # text embeds the transport error verbatim, and a proxy error's the proxy.
    return ProbeConnectionError(f"could not complete {action} ({aws_error_code(exc)})")


@contextmanager
def aws_call(action: str, soft_on_denied: bool = False) -> Iterator[None]:
    """Run AWS SDK calls -- and iterate their paginators, which is where a
    paged call actually fails -- with errors translated. `action` names the
    IAM action and object, e.g. "glue:GetTables on database 'sales'".

    soft_on_denied turns an access denial into a ProbeSoftError; the caller
    must catch it and record the reason (see ProbeSoftError's docstring).
    """
    try:
        yield
    except (ClientError, BotoCoreError) as exc:
        raise _translated(exc, action, soft_on_denied) from exc
