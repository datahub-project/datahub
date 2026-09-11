from typing import Any, Dict, Optional, Tuple

from pydantic import PrivateAttr
from sqlalchemy import event
from sqlalchemy.engine import make_url

from datahub.ingestion.source.aws.aws_common import RDSIAMTokenManager
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port


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

    def rds_iam_enabled(self) -> bool:
        """Whether this recipe selected IAM auth."""
        raise NotImplementedError

    def rds_iam_default_port(self) -> int:
        """The port to assume when host_port carries none."""
        raise NotImplementedError

    def apply_rds_iam_ssl(self, cparams: Dict[str, Any]) -> None:
        """Require TLS. IAM tokens are bearer credentials on the wire, so this
        is not optional -- but how a driver is asked differs (PyMySQL wants a
        truthy `ssl`, psycopg2 an `sslmode`), which is why it is abstract."""
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
        """
        try:
            url = make_url(self.get_sql_alchemy_url())
        except Exception:
            return parse_host_port(
                self.host_port, default_port=self.rds_iam_default_port()
            )
        if not url.host:
            return parse_host_port(
                self.host_port, default_port=self.rds_iam_default_port()
            )
        return url.host, url.port or self.rds_iam_default_port()

    def rds_iam_token_manager(self) -> Optional[RDSIAMTokenManager]:
        """The shared token manager, or None when IAM auth is not selected.

        Raises ValueError for a recipe that asks for IAM without the pieces IAM
        needs, which is a user error and reported as one.
        """
        if not self.rds_iam_enabled():
            return None
        hostname, port = self.rds_iam_endpoint()
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
            and cached.username == self.username
            # aws_config too: it decides which credentials sign the token, so a
            # copy that switched region or assumed role would otherwise keep
            # signing with the old one.
            and cached.aws_config == self.aws_config  # type: ignore[attr-defined]
        ):
            return cached
        if port is None:
            raise ValueError(
                "Port must be specified for RDS IAM authentication. "
                "Please provide host_port in the format 'hostname:port' "
                "(e.g., 'mydb.rds.amazonaws.com:5432')."
            )
        if not self.username:
            raise ValueError(
                "username is required for RDS IAM authentication. "
                "Please add 'username: <your_db_username>' to your configuration."
            )
        self._rds_iam_manager = RDSIAMTokenManager(
            endpoint=hostname,
            username=self.username,
            port=port,
            aws_config=self.aws_config,  # type: ignore[attr-defined]
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

        event.listen(engine, "do_connect", do_connect_listener)  # type: ignore[misc]
