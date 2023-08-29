import json
import logging
from typing import Any, Dict, List, Optional, Type

from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


class DataHubAssertionGraph(DataHubGraph):
    """An extension of the DataHubGraph class that provides additional functionality for Assertion evaluation"""

    def __init__(self, config: DatahubClientConfig) -> None:
        super().__init__(config)

    def get_timeseries_values(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        filter: Dict[str, Any],
    ) -> Optional[List[Aspect]]:
        """Variant of get_latest_timeseries_value() that supports querying by a custom filter"""

        query_body = {
            "urn": entity_urn,
            "entity": guess_entity_type(entity_urn),
            "aspect": aspect_type.ASPECT_NAME,
            "limit": 1,
            "filter": filter,
        }
        end_point = f"{self.config.server}/aspects?action=getTimeseriesAspectValues"

        try:
            resp: Dict = self._post_generic(end_point, query_body)
            values: list = resp.get("value", {}).get("values", [])
            aspects: List[Aspect] = []
            for value in values:
                if value.get("aspect") and value.get("aspect").get("value"):
                    aspects.append(
                        aspect_type.from_obj(
                            json.loads(value.get("aspect").get("value")),
                            tuples=False,
                        )
                    )
            return aspects
        except Exception:
            logger.exception("Error while getting timeseries values.")
        return None
