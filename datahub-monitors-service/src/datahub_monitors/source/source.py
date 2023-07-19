from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from datahub_monitors.connection.connection import Connection
from datahub_monitors.types import EntityEvent, EntityEventType


class Source(ABC):
    """Base class for a connector responsible for fetching information from external sources. Parallel concept to a normal ingestion source."""

    connection: Connection

    def __init__(self, connection: Connection):
        self.connection = connection

    @abstractmethod
    def get_entity_events(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: Dict,
    ) -> List[EntityEvent]:
        raise Exception("Not implemented")

    @abstractmethod
    def get_current_high_watermark_for_column(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        raise Exception("Not implemented")
