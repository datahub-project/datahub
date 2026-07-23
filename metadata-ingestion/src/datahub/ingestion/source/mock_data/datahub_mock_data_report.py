from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.api.source import SourceReport


@dataclass
class DataHubMockDataReport(SourceReport):
    first_urn_seen: Optional[str] = field(
        default=None,
        metadata={"description": "The first URN encountered during ingestion"},
    )
