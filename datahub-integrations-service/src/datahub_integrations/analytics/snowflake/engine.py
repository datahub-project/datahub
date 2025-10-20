import logging
from typing import Optional

import snowflake.connector
from datahub.ingestion.graph.client import DataHubGraph
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub_integrations.analytics.engine import AnalyticsEngine
from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection
from datahub_integrations.propagation.snowflake.config import (
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
        self._config: Optional[SnowflakeConnectionConfigPermissive] = None

    def _get_config(self) -> SnowflakeConnectionConfigPermissive:
        """
        Get or create the SnowflakeConnectionConfigPermissive instance.
        This centralizes the configuration setup and ensures consistency.
        """
        if self._config is not None:
            return self._config

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

        self._config = SnowflakeConnectionConfigPermissive.parse_obj(config_dict)
        return self._config

    def _get_sqlalchemy_engine(self) -> Engine:
        """Create SQLAlchemy engine with proper authentication."""
        if self._engine is not None:
            return self._engine

        config = self._get_config()
        logger.info(
            f"Creating SQLAlchemy engine with authentication_type={config.authentication_type}"
        )

        url = config.get_sql_alchemy_url()
        self._engine = create_engine(url, **config.get_options())
        return self._engine

    def get_native_connection(self) -> "snowflake.connector.SnowflakeConnection":
        """
        Get native Snowflake connection for direct queries.
        Delegates to SnowflakeConnectionConfigPermissive.
        """
        config = self._get_config()
        return config.create_native_connection(application="datahub_analytics")
