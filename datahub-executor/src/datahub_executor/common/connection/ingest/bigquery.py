import logging
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

from datahub_executor.common.connection.bigquery.bigquery_connection import (
    BigQueryConnection,
)
from datahub_executor.common.connection.connection import Connection

logger = logging.getLogger(__name__)


def extract_connection_from_bigquery_recipe(
    connection_urn: str, recipe: Dict, graph: DataHubGraph
) -> Optional[Connection]:
    # Create a dictionary representing a bigquery connection
    # Here we simply reuse the base model provided inside of our ingestion library.
    try:
        source_config = recipe.get("source", {}).get("config")
        if not source_config:
            return None
        bigquery_config = BigQueryV2Config.parse_obj_allow_extras(source_config)
        return BigQueryConnection(connection_urn, bigquery_config, graph)
    except Exception:
        logger.exception("Failed to extract connection details from BigQuery recipe!")

    return None
