from enum import StrEnum
from typing import Any, Dict, Optional

import pydantic
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datahub.configuration.common import PermissiveConfigModel
from datahub.ingestion.source.snowflake.constants import (
    CLIENT_PREFETCH_THREADS,
    CLIENT_SESSION_KEEP_ALIVE,
    DEFAULT_SNOWFLAKE_DOMAIN,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)


class SnowflakeAuthenticationType(StrEnum):
    """Snowflake authentication types."""

    DEFAULT_AUTHENTICATOR = "DEFAULT_AUTHENTICATOR"
    EXTERNAL_BROWSER_AUTHENTICATOR = "EXTERNAL_BROWSER_AUTHENTICATOR"
    KEY_PAIR_AUTHENTICATOR = "KEY_PAIR_AUTHENTICATOR"
    OAUTH_AUTHENTICATOR = "OAUTH_AUTHENTICATOR"


class SnowflakeConnectionConfigPermissive(
    SnowflakeConnectionConfig, PermissiveConfigModel
):
    def _load_private_key_bytes(self) -> bytes:
        """
        Load private key bytes from either the private_key string or private_key_path file.

        Returns:
            Raw private key bytes
        """
        if self.private_key is not None:
            # Fix JSON escaping issues: unescape forward slashes and convert \\n to \n
            return self.private_key.replace("\\/", "/").replace("\\n", "\n").encode()

        assert self.private_key_path, (
            "missing required private key path to read key from"
        )
        with open(self.private_key_path, "rb") as key:
            return key.read()

    def _get_password_bytes(self) -> Optional[bytes]:
        """
        Convert private_key_password to bytes, handling both str and SecretStr types.

        Returns:
            Password as bytes, or None if no password is set
        """
        if self.private_key_password is None:
            return None

        if isinstance(self.private_key_password, pydantic.SecretStr):
            return self.private_key_password.get_secret_value().encode()

        # Handle regular string (from automation config)
        return str(self.private_key_password).encode()

    def _process_private_key(self) -> bytes:
        """
        Load and process the private key for Snowflake authentication.

        Returns:
            Processed private key bytes in DER/PKCS8 format
        """
        pkey_bytes = self._load_private_key_bytes()
        password_bytes = self._get_password_bytes()

        p_key = serialization.load_pem_private_key(
            pkey_bytes,
            password=password_bytes,
            backend=default_backend(),
        )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def get_connect_args(self) -> dict:
        """
        Override get_connect_args to handle private_key_password as either str or SecretStr.
        """
        if self._computed_connect_args is not None:
            return self._computed_connect_args

        connect_args: Dict[str, Any] = {
            # Improves performance and avoids timeout errors for larger query result
            CLIENT_PREFETCH_THREADS: 10,
            CLIENT_SESSION_KEEP_ALIVE: True,
            # Let user override the default config values
            **(self.connect_args or {}),
        }

        if (
            "private_key" not in connect_args
            and self.authentication_type
            == SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
        ):
            connect_args["private_key"] = self._process_private_key()

        self._computed_connect_args = connect_args
        return connect_args

    def create_native_connection(
        self, application: str = "acryl_datahub"
    ) -> "snowflake.connector.SnowflakeConnection":
        """
        Create a native Snowflake connection.

        Args:
            application: Application name to use for the connection

        Returns:
            Native Snowflake connection
        """

        connect_args = self.get_connect_args()

        if (
            self.authentication_type
            == SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
        ):
            return snowflake.connector.connect(
                user=self.username,
                account=self.account_id,
                warehouse=self.warehouse,
                role=self.role,
                authenticator=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR,
                application=application,
                host=f"{self.account_id}.{getattr(self, 'snowflake_domain', DEFAULT_SNOWFLAKE_DOMAIN)}",
                **connect_args,
            )
        else:
            # Default authenticator
            return snowflake.connector.connect(
                user=self.username,
                password=self.password.get_secret_value() if self.password else None,
                account=self.account_id,
                warehouse=self.warehouse,
                role=self.role,
                application=application,
                host=f"{self.account_id}.{getattr(self, 'snowflake_domain', DEFAULT_SNOWFLAKE_DOMAIN)}",
                **connect_args,
            )
