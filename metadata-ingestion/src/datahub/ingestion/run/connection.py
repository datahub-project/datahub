import logging

from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)


class ConnectionManager:
    """A class that helps build / manage / triage connections"""

    def test_source_connection(self, recipe_config_dict: dict) -> TestConnectionReport:
        # pulls out the source component of the dictionary
        # walks the type registry to find the source class
        # calls specific class.test_connection
        try:
            source_type = recipe_config_dict.get("source", {}).get("type")
            source_class = source_registry.get(source_type)
            return source_class.test_connection(
                recipe_config_dict.get("source", {}).get("config", {})
            )
        except Exception as e:
            logger.error(e)
            raise e
