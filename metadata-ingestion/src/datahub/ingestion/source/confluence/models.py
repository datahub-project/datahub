from datetime import datetime
from typing import Any, Dict, List, Optional

from dateutil import parser as date_parser
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datahub.ingestion.source.confluence.constants import (
    FOLDER_CONTENT_TYPE,
)


class ConfluenceUser(BaseModel):
    """A user from the Confluence REST ``/rest/api/user`` endpoint."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    display_name: Optional[str] = Field(default=None, alias="displayName")
    # Cloud returns publicName for users whose display name is hidden.
    public_name: Optional[str] = Field(default=None, alias="publicName")

    @property
    def name(self) -> Optional[str]:
        return self.display_name or self.public_name


class ConfluenceAncestor(BaseModel):
    """An entry in a page's ``ancestors``, ordered root first. Pages under a
    folder include the folder as an ancestor with type ``folder``."""

    model_config = ConfigDict(extra="ignore")

    id: str
    title: str = ""
    type: str = "page"

    @field_validator("id", mode="before")
    @classmethod
    def _id_as_str(cls, v: Any) -> str:
        return str(v)

    @property
    def is_folder(self) -> bool:
        return self.type == FOLDER_CONTENT_TYPE


class ConfluenceSpaceRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    key: Optional[str] = None
    name: Optional[str] = None


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date_parser.parse(value)
    except (ValueError, OverflowError):
        return None


class ConfluencePage(BaseModel):
    """A page from the v1 content API, fetched with ``PAGE_EXPAND``.

    The API nests most fields (``body.storage.value``, ``_links.webui``,
    ``version.when``); they are flattened here so the rest of the source never
    reads the raw payload.
    """

    model_config = ConfigDict(extra="ignore")

    id: str
    title: str = ""
    space: ConfluenceSpaceRef = Field(default_factory=ConfluenceSpaceRef)
    ancestors: List[ConfluenceAncestor] = Field(default_factory=list)
    storage: str = ""
    webui: Optional[str] = None
    last_modified: Optional[datetime] = None
    created: Optional[datetime] = None

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "storage" in data:
            return data
        # Any nested field can come back missing or in an unexpected shape;
        # treat those as absent rather than failing the whole page.
        body = _as_dict(data.get("body"))
        space = _as_dict(data.get("space"))
        if not space.get("key") and data.get("spaceKey"):
            space = {**space, "key": data["spaceKey"]}
        ancestors = data.get("ancestors")
        flat: Dict[str, Any] = {
            "id": str(data["id"]) if data.get("id") else None,
            "title": data.get("title") or "",
            "space": space,
            # Malformed ancestors are dropped rather than failing the page.
            "ancestors": [
                a
                for a in (ancestors if isinstance(ancestors, list) else [])
                if isinstance(a, dict) and a.get("id")
            ],
            "storage": _as_dict(body.get("storage")).get("value") or "",
            "webui": _as_dict(data.get("_links")).get("webui") or data.get("url"),
            "last_modified": _parse_timestamp(
                _as_dict(data.get("version")).get("when")
            ),
            "created": _parse_timestamp(
                _as_dict(data.get("history")).get("createdDate")
            ),
        }
        return flat

    @property
    def parent_id(self) -> Optional[str]:
        return self.ancestors[-1].id if self.ancestors else None


class PageBatch(BaseModel):
    pages: List[ConfluencePage]
    # Results the API returned, including any that could not be parsed. Paging
    # advances by this, so a batch of unreadable pages does not end the listing.
    fetched: int
    has_next: bool


class SpaceBatch(BaseModel):
    keys: List[str]
    fetched: int
    has_next: bool
