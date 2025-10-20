import logging
from typing import Any, Dict, Optional

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datahub.ingestion.graph.client import DataHubGraph
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub_integrations.analytics.engine import AnalyticsEngine
from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection
from datahub_integrations.propagation.snowflake.config import (
    SnowflakeAuthenticationType,
    SnowflakeConnectionConfigPermissive,
)

logger = logging.getLogger(__name__)


class SnowflakeAnalyticsEngine(AnalyticsEngine):
    """Analytics engine for Snowflake with support for both password and private key authentication."""

    def __init__(self, account: str, graph: DataHubGraph) -> None:
        self.account = account
        self.graph = graph
        self.connection = SnowflakeConnection.from_datahub(graph=graph)
        logger.info(
            f"SnowflakeAnalyticsEngine initialized with authentication_type={self.connection.authentication_type} (type={type(self.connection.authentication_type)})"
        )
        self._engine: Optional[Engine] = None

    def _get_sqlalchemy_engine(self) -> Engine:
        """Create SQLAlchemy engine with proper authentication."""
        if self._engine is not None:
            return self._engine

        # Convert our connection model to the permissive config format
        config_dict = {
            "account_id": self.connection.account,
            "warehouse": self.connection.warehouse,
            "username": self.connection.user,
            "password": self.connection.password,
            "role": self.connection.role,
            "authentication_type": self.connection.authentication_type.value,  # SnowflakeConnectionConfig expects str
            "private_key": self.connection.private_key,
            "private_key_password": self.connection.private_key_password,
        }
        logger.info(
            f"Creating SQLAlchemy engine with authentication_type={config_dict['authentication_type']} (type={type(config_dict['authentication_type'])})"
        )

        config = SnowflakeConnectionConfigPermissive.parse_obj(config_dict)
        url = config.get_sql_alchemy_url()
        self._engine = create_engine(url, **config.get_options())
        return self._engine

    def get_native_connection(self) -> Any:
        """Get native Snowflake connection for direct queries."""
        connect_args = self._get_connect_args()

        if (
            self.connection.authentication_type
            == SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
        ):
            return snowflake.connector.connect(
                user=self.connection.user,
                account=self.connection.account,
                warehouse=self.connection.warehouse,
                role=self.connection.role,
                **connect_args,
            )
        else:
            return snowflake.connector.connect(
                user=self.connection.user,
                password=self.connection.password,
                account=self.connection.account,
                warehouse=self.connection.warehouse,
                role=self.connection.role,
                **connect_args,
            )

    def _get_connect_args(self) -> Dict[str, Any]:
        """Get connection arguments including private key processing."""
        connect_args: Dict[str, Any] = {}

        if (
            self.connection.authentication_type
            == SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
            and self.connection.private_key
        ):
            # Process the private key similar to the ingestion source
            pkey_bytes = self.connection.private_key.replace("\\n", "\n").encode()

            p_key = serialization.load_pem_private_key(
                pkey_bytes,
                password=(
                    self.connection.private_key_password.encode()
                    if self.connection.private_key_password
                    else None
                ),
                backend=default_backend(),
            )

            pkb: bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            connect_args["private_key"] = pkb

        return connect_args
