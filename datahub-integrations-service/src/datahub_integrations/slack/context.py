from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class SearchContext:
    query: str
    page: int
    filters: Dict[str, Tuple[str, str]]  # facet field -> (value, display_name)


@dataclass
class IncidentContext:
    urn: str
    stage: Optional[str]


@dataclass
class IncidentSelectOption:
    value: str
    context: IncidentContext
