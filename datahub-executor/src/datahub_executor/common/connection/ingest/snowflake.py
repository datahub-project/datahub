import logging
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeConfig

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)

logger = logging.getLogger(__name__)


def extract_connection_from_snowflake_recipe(
    connection_urn: str, recipe: Dict, graph: DataHubGraph
) -> Optional[Connection]:
    # Create a dictionary representing a snowflake connection
    # Here we simply reuse the base model provided inside of our ingestion library.
    try:
        source_config = recipe.get("source", {}).get("config")
        if not source_config:
            return None
        snowflake_config = SnowflakeConfig.parse_obj_allow_extras(source_config)
        return SnowflakeConnection(connection_urn, snowflake_config, graph)
    except Exception:
        logger.exception("Failed to extract connection details from Snowflake recipe!")

    return None
