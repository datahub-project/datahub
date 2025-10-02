from typing import Any, Dict

import pydantic
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datahub.configuration.common import PermissiveConfigModel
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)


class SnowflakeConnectionConfigPermissive(
    SnowflakeConnectionConfig, PermissiveConfigModel
):
    def get_connect_args(self) -> dict:
        """
        Override get_connect_args to handle private_key_password as either str or SecretStr.
        """
        if self._computed_connect_args is not None:
            return self._computed_connect_args

        from datahub.ingestion.source.snowflake.constants import (
            CLIENT_PREFETCH_THREADS,
            CLIENT_SESSION_KEEP_ALIVE,
        )

        connect_args: Dict[str, Any] = {
            # Improves performance and avoids timeout errors for larger query result
            CLIENT_PREFETCH_THREADS: 10,
            CLIENT_SESSION_KEEP_ALIVE: True,
            # Let user override the default config values
            **(self.connect_args or {}),
        }

        if (
            "private_key" not in connect_args
            and self.authentication_type == "KEY_PAIR_AUTHENTICATOR"
        ):
            if self.private_key is not None:
                # Fix JSON escaping issues: unescape forward slashes and convert \\n to \n
                pkey_bytes = (
                    self.private_key.replace("\\/", "/").replace("\\n", "\n").encode()
                )
            else:
                assert self.private_key_path, (
                    "missing required private key path to read key from"
                )
                with open(self.private_key_path, "rb") as key:
                    pkey_bytes = key.read()

            # Handle both str and SecretStr for private_key_password
            password_bytes = None
            if self.private_key_password is not None:
                if isinstance(self.private_key_password, pydantic.SecretStr):
                    password_bytes = (
                        self.private_key_password.get_secret_value().encode()
                    )
                else:
                    # Handle regular string (from automation config)
                    password_bytes = str(self.private_key_password).encode()

            p_key = serialization.load_pem_private_key(
                pkey_bytes,
                password=password_bytes,
                backend=default_backend(),
            )

            pkb: bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            connect_args["private_key"] = pkb

        self._computed_connect_args = connect_args
        return connect_args
