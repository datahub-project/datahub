"""ThoughtSpot DataHub connector."""

from datahub.ingestion.source.thoughtspot.config import (
    ThoughtSpotConfig,
    ThoughtSpotConnectionConfig,
)
from datahub.ingestion.source.thoughtspot.source import ThoughtSpotSource

__all__ = ["ThoughtSpotSource", "ThoughtSpotConfig", "ThoughtSpotConnectionConfig"]
