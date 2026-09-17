import logging
from typing import Any, Dict, Optional, Tuple

from pydantic import Field, PrivateAttr
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port

logger = logging.getLogger(__name__)


class RDSIAMConnectionMixin(SQLAlchemyConnectionConfig):
    """RDS IAM authentication for a SQLAlchemy engine, owned by the config.

    An IAM password is a short-lived token, so it cannot ride in the URL -- it
    has to be injected per connection by a `do_connect` listener, and refreshed
    as it expires. That listener therefore has to be installed on every engine
    that will authenticate this way.

    It lives on the config rather than the Source because *both* ingestion and
    `datahub recipe probe` build such engines, and the config is the only object
    both of them hold. While this setup lived on the Source, the probe could not
    reach it at all: `probe_prepare_engine` is a config method, so an AWS_IAM
    recipe either failed to connect or would have needed the probe to rebuild
    the token manager itself -- a second implementation of a credential path,
    which is precisely the drift the hook exists to prevent.

    The token manager is cached rather than rebuilt per engine: it holds the
    current token and its expiry, so a fresh instance per engine would go back
    to STS on every connect. This mirrors bigquery_connection's `_credentials`.

    A connector mixes this in and supplies the three things that genuinely
    differ: whether IAM is selected (each connector has its own auth_mode enum),
    the default port, and how its driver is told to require TLS.
    """

    _rds_iam_manager: Optional[RDSIAMTokenManager] = PrivateAttr(default=None)
    # Warned once per config, not once per connection: the listener runs on
    # every pool checkout and a per-connection warning would bury itself.
    _rds_iam_tls_warned: bool = PrivateAttr(default=False)

    def rds_iam_enabled(self) -> bool:
        """Whether this recipe selected IAM auth."""
        raise NotImplementedError

    def rds_iam_default_port(self) -> int:
        """The port to assume when host_port carries none."""
        raise NotImplementedError

    # Declared here, on the mixin that reads it, rather than separately on
    # every connector that mixes this in. It was duplicated verbatim in
    # mysql.py and postgres/source.py while this file reached it through
    # `self.aws_config  # type: ignore[attr-defined]` -- an ignore that hid a
    # required subclass contract from mypy, so a connector could mix this in
    # without the field and only find out at runtime with IAM enabled.
    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description="AWS configuration for RDS IAM authentication (only used when auth_mode is AWS_IAM). "
        "Provides full control over AWS credentials, region, profiles, role assumption, retry logic, and proxy settings. "
        "If not explicitly configured, boto3 will automatically use the default credential chain and region from "
        "environment variables (AWS_DEFAULT_REGION, AWS_REGION), AWS config files (~/.aws/config), or IAM role metadata.",
    )

    def apply_rds_iam_ssl(self, cparams: Dict[str, Any]) -> None:
        """Require TLS. IAM tokens are bearer credentials on the wire, so this
        is not optional -- but how a driver is asked differs (PyMySQL wants a
        truthy `ssl`, psycopg2 an `sslmode`), which is why it is abstract."""
        raise NotImplementedError

    def rds_iam_tls_is_verified(self, cparams: Dict[str, Any]) -> bool:
        """Whether these settings authenticate the SERVER, not merely encrypt.

        Encryption alone does not protect a bearer credential: an attacker who
        can answer for the endpoint presents any certificate, the driver
        accepts it, and the IAM token is handed over. Both defaults stop at
        encryption -- psycopg2's `sslmode=require` performs no CA or hostname
        check, and PyMySQL given a bare truthy `ssl` builds a context with
        `check_hostname=False` and `verify_mode=CERT_NONE` (verified by
        introspecting `_create_ssl_ctx`, not assumed).

        Answered per driver because the parameter that turns verification on
        differs, and getting it wrong in the safe direction is the point.
        """
        raise NotImplementedError

    def rds_iam_endpoint(self) -> Tuple[str, Optional[int]]:
        """The host and port the token must be signed for.

        Read back off the URL the engine will actually dial, not off host_port:
        `get_sql_alchemy_url()` returns `sqlalchemy_uri` in preference to
        anything built from host_port, so a recipe setting both would have had
        its token signed for one host and presented to another. Identical for
        the ordinary recipe, where the URL is built from host_port anyway.

        Falls back to host_port if the URL will not parse, which keeps a
        malformed-URL recipe reporting the error it already reported rather
        than a new one from here.

        SECURITY: only the PARSE is guarded, and only against ArgumentError.
        get_sql_alchemy_url() is called outside the try because a connector
        that cannot build its URL is not a malformed-URL recipe -- swallowing
        that and falling back to host_port signs the token for a host the
        engine may never dial, which is the exact mismatch this method was
        written to remove.
        """
        effective_url = self.get_sql_alchemy_url()
        try:
            url = make_url(effective_url)
        except ArgumentError:
            return parse_host_port(
                self.host_port, default_port=self.rds_iam_default_port()
            )
        if not url.host:
            return parse_host_port(
                self.host_port, default_port=self.rds_iam_default_port()
            )
        return url.host, url.port or self.rds_iam_default_port()

    def rds_iam_username(self) -> Optional[str]:
        """The user the engine will connect as.

        Read from the effective URL for the same reason rds_iam_endpoint reads
        host and port from it: a `sqlalchemy_uri` may point somewhere other
        than host_port, and an RDS IAM token is scoped to the user it was
        minted for. Signing for the config field while connecting as the URI's
        user is rejected by RDS with an authentication error that names
        neither.

        Falls back to the config field when the URL will not parse or carries
        no user, which is the ordinary recipe. Narrowed for the same reason as
        rds_iam_endpoint: a token minted for the wrong user is rejected by RDS
        with an error naming neither, so a connector-side failure must not be
        rewritten into a quiet fallback.
        """
        effective_url = self.get_sql_alchemy_url()
        try:
            url = make_url(effective_url)
        except ArgumentError:
            return self.username
        return url.username or self.username

    def rds_iam_token_manager(self) -> Optional[RDSIAMTokenManager]:
        """The shared token manager, or None when IAM auth is not selected.

        Raises ValueError for a recipe that asks for IAM without the pieces IAM
        needs, which is a user error and reported as one.
        """
        if not self.rds_iam_enabled():
            return None
        hostname, port = self.rds_iam_endpoint()
        username = self.rds_iam_username()
        # Reused only while it still describes this config. Both model_copy()
        # and model_copy(deep=True) carry PrivateAttrs, so a copy that changed
        # host_port or username would otherwise go on presenting a token minted
        # for the original -- to a different host, with a credential scoped to
        # the old one. Neither connector copies its config today; this is a few
        # lines so that staying true is not something anyone has to remember.
        cached = self._rds_iam_manager
        if (
            cached is not None
            and cached.endpoint == hostname
            and cached.port == port
            and cached.username == username
            # aws_config too: it decides which credentials sign the token, so a
            # copy that switched region or assumed role would otherwise keep
            # signing with the old one.
            and cached.aws_config == self.aws_config
        ):
            return cached
        if port is None:
            raise ValueError(
                "Port must be specified for RDS IAM authentication. "
                "Please provide host_port in the format 'hostname:port' "
                "(e.g., 'mydb.rds.amazonaws.com:5432')."
            )
        # `.strip()` too: "   " passes a bare falsiness check and then signs a
        # token for a user that cannot exist, turning a configuration typo
        # into an AWS round trip and an authentication error.
        if not username or not username.strip():
            raise ValueError(
                "username is required for RDS IAM authentication. "
                "Please add 'username: <your_db_username>' to your configuration."
            )
        self._rds_iam_manager = RDSIAMTokenManager(
            endpoint=hostname,
            username=username,
            port=port,
            aws_config=self.aws_config,
        )
        return self._rds_iam_manager

    def install_rds_iam_auth(self, engine: Any) -> None:
        """Attach the token-injecting listener to `engine`. A no-op unless this
        recipe selected IAM auth, so it is safe to call unconditionally."""
        manager = self.rds_iam_token_manager()
        if manager is None:
            return

        def do_connect_listener(
            _dialect: Any, _conn_rec: Any, _cargs: Any, cparams: Dict[str, Any]
        ) -> None:
            # Fetched per connection, not once: get_token() returns the cached
            # token until it nears expiry, so a long run re-authenticates
            # without the caller doing anything.
            cparams["password"] = manager.get_token()
            self.apply_rds_iam_ssl(cparams)
            self._warn_if_token_rides_unverified_tls(cparams)

        event.listen(engine, "do_connect", do_connect_listener)  # type: ignore[misc]

    def _warn_if_token_rides_unverified_tls(self, cparams: Dict[str, Any]) -> None:
        """Say so when the token is about to cross an unauthenticated channel.

        SECURITY: this warns rather than refuses, deliberately. Failing closed
        here would break every working AWS_IAM recipe that has not configured
        a CA, and the connection that then fails would be the operator's
        first notice. The warning names the exact setting instead, so the gap
        is visible and closable without a support ticket.
        """
        if self._rds_iam_tls_warned or self.rds_iam_tls_is_verified(cparams):
            return
        self._rds_iam_tls_warned = True
        logger.warning(
            "RDS IAM: the auth token is a bearer credential and this "
            "connection encrypts without verifying the server's certificate, "
            "so anything able to answer for the endpoint can present its own "
            "certificate and capture the token. Configure verified TLS: %s",
            self.rds_iam_tls_hint(),
        )

    def rds_iam_tls_hint(self) -> str:
        """The setting that turns verification on for this driver."""
        return "see your driver's TLS options"
