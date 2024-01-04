import logging
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.databricks.databricks_connection import (
    DatabricksConnection,
)

logger = logging.getLogger(__name__)


# Returns None if a connection cannot be extracted.
def extract_connection_from_databricks_recipe(
    connection_urn: str, recipe: Dict, graph: DataHubGraph
) -> Optional[Connection]:
    # Create a dictionary representing a redshift connection
    # Here we simply reuse the base model provided inside of our ingestion library.
    try:
        if "source" in recipe:
            source = recipe["source"]
            if "config" in source:
                source_config = source["config"]
                databricks_config = UnityCatalogSourceConfig.parse_obj_allow_extras(
                    source_config
                )
                return DatabricksConnection(connection_urn, databricks_config, graph)
    except Exception:
        logger.exception("Failed to extract connection details from Redshift recipe!")

    return None
