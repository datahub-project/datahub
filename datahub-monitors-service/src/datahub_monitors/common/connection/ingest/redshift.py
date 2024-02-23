import logging
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig

from datahub_monitors.common.connection.connection import Connection
from datahub_monitors.common.connection.redshift.redshift_connection import (
    RedshiftConnection,
)

logger = logging.getLogger(__name__)


# Returns None if a connection cannot be extracted.
def extract_connection_from_redshift_recipe(
    connection_urn: str, recipe: Dict, graph: DataHubGraph
) -> Optional[Connection]:
    # Create a dictionary representing a redshift connection
    # Here we simply reuse the base model provided inside of our ingestion library.
    try:
        if "source" in recipe:
            source = recipe["source"]
            if "config" in source:
                source_config = source["config"]
                redshift_config = RedshiftConfig.parse_obj_allow_extras(source_config)
                return RedshiftConnection(connection_urn, redshift_config, graph)
    except Exception:
        logger.exception("Failed to extract connection details from Redshift recipe!")

    return None
