import base64
import time
from typing import Any, Callable, Dict, Optional, Tuple, Union

from botocore.credentials import (
    Credentials,
    ReadOnlyCredentials,
    RefreshableCredentials,
)
from botocore.exceptions import BotoCoreError, ClientError
from opensearchpy import OpenSearch, Urllib3AWSV4SignerAuth
from pydantic import SecretStr, field_validator, model_validator
from pydantic.fields import Field

from datahub.configuration.common import (
    ConfigModel,
    ConfigurationError,
    TransparentSecretStr,
)
from datahub.configuration.validate_host_port import validate_host_port
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.aws.aws_common import (
    ASSUMED_ROLE_REFRESH_BUFFER,
    AwsConnectionConfig,
)
from datahub.utilities.config_clean import remove_protocol

# SigV4 service name for Amazon OpenSearch Service managed domains.
_AWS_SIGV4_SERVICE = "es"
_ASSUMED_ROLE_RECHECK_INTERVAL_SECONDS = 60.0
_MAX_REFRESH_BACKOFF_SECONDS = 240.0

ApiKey = Union[Tuple[str, TransparentSecretStr], TransparentSecretStr]


def _api_key_authorization(api_key: Union[Tuple[str, SecretStr], SecretStr]) -> str:
    # An (id, api_key) pair is base64-encoded as "id:api_key"; a plain string is passed
    # through as the already-encoded token.
    if isinstance(api_key, tuple):
        key_id, secret = api_key
        token = base64.b64encode(
            f"{key_id}:{secret.get_secret_value()}".encode()
        ).decode()
    else:
        token = api_key.get_secret_value()
    return f"ApiKey {token}"


class ElasticsearchConnectionConfig(ConfigModel):
    """Connection settings for an Elasticsearch/OpenSearch cluster."""

    host: str = Field(
        default="localhost:9200", description="The elastic search host URI."
    )
    username: Optional[str] = Field(
        default=None, description="The username credential."
    )
    password: Optional[TransparentSecretStr] = Field(
        default=None, description="The password credential."
    )
    api_key: Optional[ApiKey] = Field(
        default=None,
        description="API Key authentication. Accepts either a list with id and api_key (UTF-8 representation), or a base64 encoded string of id and api_key combined by ':'.",
    )

    use_ssl: bool = Field(
        default=False, description="Whether to use SSL for the connection or not."
    )

    verify_certs: bool = Field(
        default=False, description="Whether to verify SSL certificates."
    )

    ca_certs: Optional[str] = Field(
        default=None, description="Path to a certificate authority (CA) certificate."
    )

    client_cert: Optional[str] = Field(
        default=None,
        description="Path to the file containing the private key and the certificate, or cert only if using client_key.",
    )

    client_key: Optional[str] = Field(
        default=None,
        description="Path to the file containing the private key if using separate cert and key files.",
    )

    ssl_assert_hostname: bool = Field(
        default=False, description="Use hostname verification if not False."
    )

    ssl_assert_fingerprint: Optional[str] = Field(
        default=None,
        description="Verify the supplied certificate fingerprint if not None.",
    )

    url_prefix: str = Field(
        default="",
        description="There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters.",
    )

    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS connection settings for IAM authentication against an Amazon OpenSearch Service domain. When set, every request is signed with AWS Signature Version 4 using the resolved credentials (explicit keys, profile, role assumption, or the default credential chain). aws_region, use_ssl: True and verify_certs: True are required. Cannot be combined with username/password or api_key, and aws_role cannot be combined with aws_profile or explicit keys. OpenSearch Serverless collections are not supported.",
    )

    @field_validator("host", mode="after")
    @classmethod
    def host_colon_port_comma(cls, host_val: str) -> str:
        for entry in host_val.split(","):
            entry = remove_protocol(entry)
            if entry.endswith("/"):
                entry = entry[:-1]
            validate_host_port(entry)
        return host_val

    @model_validator(mode="after")
    def validate_single_auth_method(self) -> "ElasticsearchConnectionConfig":
        # opensearch-py would send both a Basic and an ApiKey Authorization
        # header, and which one the cluster honours is undefined.
        if self.api_key is not None and (
            self.username is not None or self.password is not None
        ):
            raise ValueError(
                "api_key cannot be combined with username/password; "
                "configure exactly one authentication method."
            )
        return self

    @model_validator(mode="after")
    def validate_aws_auth(self) -> "ElasticsearchConnectionConfig":
        if self.aws_config is None:
            return self
        if (
            self.username is not None
            or self.password is not None
            or self.api_key is not None
        ):
            raise ValueError(
                "aws_config cannot be combined with username/password or api_key; "
                "configure exactly one authentication method."
            )
        if not self.aws_config.aws_region:
            raise ValueError(
                "aws_config.aws_region is required for AWS IAM (SigV4) authentication."
            )
        has_explicit_keys = bool(
            self.aws_config.aws_access_key_id and self.aws_config.aws_secret_access_key
        )
        if self.aws_config.aws_role and (
            self.aws_config.aws_profile or has_explicit_keys
        ):
            # AwsConnectionConfig.get_session() prefers explicit keys, then a profile,
            # and only assumes aws_role when neither is set, so the role would be
            # silently ignored.
            raise ValueError(
                "aws_config.aws_role cannot be combined with aws_profile or explicit "
                "access keys; the role would not be assumed. To assume a role from a "
                "profile, set AWS_PROFILE in the environment and configure only aws_role."
            )
        if not (self.use_ssl and self.verify_certs):
            raise ValueError(
                "aws_config requires use_ssl: True and verify_certs: True. SigV4-signed "
                "requests must only be sent over HTTPS to a verified domain certificate; "
                "use ca_certs for a custom certificate authority."
            )
        return self

    @property
    def http_auth(self) -> Optional[Tuple[str, str]]:
        if self.username is None:
            return None
        password = self.password.get_secret_value() if self.password else ""
        return (self.username, password)

    def get_client(self, report: SourceReport, **client_kwargs: Any) -> OpenSearch:
        """Build an OpenSearch client from these settings.

        report receives credential-refresh warnings when aws_config is set.
        client_kwargs are passed through to OpenSearch, e.g. timeout or retries.
        """
        # opensearch-py has no api_key param and silently drops unknown kwargs, so it must be
        # sent as an Authorization header instead.
        extra_client_args: Dict[str, Any] = dict(client_kwargs)
        if self.api_key is not None:
            extra_client_args["headers"] = {
                "Authorization": _api_key_authorization(self.api_key)
            }
        http_auth: Optional[Union[Tuple[str, str], Urllib3AWSV4SignerAuth]] = (
            self.http_auth
        )
        if self.aws_config is not None:
            http_auth = _aws_sigv4_auth(self.aws_config, report)
        return OpenSearch(
            self.host,
            http_auth=http_auth,
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs,
            ca_certs=self.ca_certs,
            client_cert=self.client_cert,
            client_key=self.client_key,
            ssl_assert_hostname=self.ssl_assert_hostname,
            ssl_assert_fingerprint=self.ssl_assert_fingerprint,
            url_prefix=self.url_prefix,
            **extra_client_args,
        )


class _AwsConfigCredentials:
    """Credentials source for opensearch-py's SigV4 signer, which calls
    get_frozen_credentials() on every request.

    Workaround for an AwsConnectionConfig limitation: its manual aws_role branch in
    get_session() returns a static Credentials snapshot and keeps the expiry private,
    while every other credential source returns self-refreshing botocore credentials.
    get_s3_client() and the SageMaker ClientFactory work around the same gap. The
    proper fix is for that branch to return DeferredRefreshableCredentials, which
    would make this class unnecessary; do not copy it into another connector.

    AwsConnectionConfig re-assumes the role once credentials are within 5 minutes of
    expiry, so re-fetching the session periodically picks that up without rebuilding
    a boto3 session on every request.
    """

    def __init__(
        self,
        aws_config: AwsConnectionConfig,
        report: SourceReport,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._aws_config = aws_config
        self._report = report
        self._clock = clock
        self._credentials: Optional[Credentials] = None
        self._next_check_at = 0.0
        self._consecutive_failures = 0
        self._first_failure_at: Optional[float] = None
        self._expiry_failure_reported = False
        self._last_failure: Optional[Exception] = None

    def _needs_recheck(self) -> bool:
        # RefreshableCredentials (profile, instance profile, IRSA, ...) renew
        # themselves; only static credentials from a role assumption need re-fetching.
        return (
            self._aws_config.allowed_cred_refresh()
            and not isinstance(self._credentials, RefreshableCredentials)
            and self._clock() >= self._next_check_at
        )

    def _record_success(self, credentials: Credentials) -> None:
        self._credentials = credentials
        self._next_check_at = self._clock() + _ASSUMED_ROLE_RECHECK_INTERVAL_SECONDS
        self._consecutive_failures = 0
        self._first_failure_at = None
        self._expiry_failure_reported = False

    def _record_failure(self, e: Exception) -> None:
        now = self._clock()
        self._consecutive_failures += 1
        self._last_failure = e
        if self._first_failure_at is None:
            self._first_failure_at = now
        backoff = min(
            _ASSUMED_ROLE_RECHECK_INTERVAL_SECONDS
            * 2 ** (self._consecutive_failures - 1),
            _MAX_REFRESH_BACKOFF_SECONDS,
        )
        self._next_check_at = now + backoff
        if not self._expiry_failure_reported:
            self._report.warning(
                title="AWS credential refresh failed",
                message="Could not re-assume aws_role; signing with the current credentials, which are usually still valid for a few minutes, and retrying with backoff. Check STS throttling, the role's trust policy, and that the base AWS credentials are still valid.",
                context=f"{self._failure_context()} next_retry_in={backoff:.0f}s",
                exc=e,
            )

    def _failure_context(self) -> str:
        return (
            f"role={self._aws_config.aws_role} region={self._aws_config.aws_region} "
            f"consecutive_failures={self._consecutive_failures}"
        )

    def _report_if_expired(self) -> None:
        # A refresh failure means STS was called, which AwsConnectionConfig only does
        # inside its pre-expiry buffer; once failures have lasted that long the
        # signing credentials have expired and every request will be rejected.
        # Checked on every request rather than per retry so it is not delayed by
        # the backoff schedule.
        if (
            self._first_failure_at is None
            or self._expiry_failure_reported
            or self._clock() - self._first_failure_at
            < ASSUMED_ROLE_REFRESH_BUFFER.total_seconds()
        ):
            return
        self._expiry_failure_reported = True
        self._report.failure(
            title="AWS credentials expired",
            message="Assumed-role credentials could not be refreshed before they expired, so requests to the domain will be rejected. Check STS throttling, the role's trust policy, and that the base AWS credentials are still valid.",
            context=self._failure_context(),
            exc=self._last_failure,
        )

    def _resolve(self) -> Credentials:
        credentials = self._aws_config.get_session().get_credentials()
        if credentials is None:
            raise ConfigurationError(
                "No AWS credentials could be resolved from aws_config. Set "
                "aws_access_key_id/aws_secret_access_key, aws_profile or aws_role, "
                "or make credentials available through the default AWS chain."
            )
        return credentials

    def get_frozen_credentials(self) -> ReadOnlyCredentials:
        if self._credentials is None:
            self._record_success(self._resolve())
        elif self._needs_recheck():
            try:
                self._record_success(self._resolve())
            except (BotoCoreError, ClientError, ConfigurationError, ValueError) as e:
                self._record_failure(e)
        self._report_if_expired()
        assert self._credentials is not None
        return self._credentials.get_frozen_credentials()


def _aws_sigv4_auth(
    aws_config: AwsConnectionConfig, report: SourceReport
) -> Urllib3AWSV4SignerAuth:
    region = aws_config.aws_region
    assert region, "aws_region is enforced by ElasticsearchConnectionConfig validation"
    credentials = _AwsConfigCredentials(aws_config, report)
    # Resolve once up front so missing or invalid credentials fail at startup rather
    # than surfacing as a wrapped ConnectionError on the first request.
    try:
        credentials.get_frozen_credentials()
    except (BotoCoreError, ClientError, ValueError) as e:
        raise ConfigurationError(
            f"Failed to resolve AWS credentials from aws_config: {e}"
        ) from e
    return Urllib3AWSV4SignerAuth(credentials, region, _AWS_SIGV4_SERVICE)
