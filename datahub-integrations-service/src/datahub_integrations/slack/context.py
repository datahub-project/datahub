from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class SearchContext:
    query: str
    page: int
    filters: Dict[str, Tuple[str, str]]  # facet field -> (value, display_name)
