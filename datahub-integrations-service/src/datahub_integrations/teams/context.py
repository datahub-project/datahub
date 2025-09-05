from typing import Any, Dict

from pydantic import BaseModel


class SearchContext(BaseModel):
    """Context for search operations in Teams."""

    query: str
    page: int = 0
    filters: Dict[str, Any] = {}


class TeamsContext(BaseModel):
    """General context for Teams operations."""

    conversation_id: str
    service_url: str
    user_id: str
