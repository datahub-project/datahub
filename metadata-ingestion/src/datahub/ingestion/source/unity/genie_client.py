import json
from datetime import datetime
from typing import Dict, Iterable, List, Literal, Optional, Set
from urllib.parse import quote

from databricks.sdk.core import ApiClient
from pydantic import BaseModel, Field

GENIE_SPACES_PATH = "/api/2.0/genie/spaces"


class GenieSpace(BaseModel):
    space_id: str = Field(min_length=1)
    title: str
    description: Optional[str] = None
    serialized_space: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None


class GenieSpacePage(BaseModel):
    spaces: List[GenieSpace] = Field(default_factory=list)
    next_page_token: Optional[str] = None


class GenieDataSource(BaseModel):
    identifier: str


class GenieDataSources(BaseModel):
    tables: List[GenieDataSource] = Field(default_factory=list)
    metric_views: List[GenieDataSource] = Field(default_factory=list)


class GenieTextInstruction(BaseModel):
    # Databricks can export empty instruction entries containing only an ID.
    content: List[str] = Field(default_factory=list)


class GenieInstructions(BaseModel):
    text_instructions: List[GenieTextInstruction] = Field(default_factory=list)


class UnsupportedGenieExportVersion(ValueError):
    pass


class InvalidGenieExport(ValueError):
    pass


class InvalidGenieListing(ValueError):
    pass


class GenieSpaceDefinition(BaseModel):
    version: Literal[1, 2]
    data_sources: GenieDataSources
    instructions: GenieInstructions = Field(default_factory=GenieInstructions)

    @classmethod
    def from_serialized(cls, serialized: str) -> "GenieSpaceDefinition":
        # Reject an unsupported version before model_validate so it is not
        # reported as a malformed export.
        payload = json.loads(serialized)
        if not isinstance(payload, dict):
            raise InvalidGenieExport("Genie agent export is not a JSON object")
        version = payload.get("version")
        # An exact type check, because bool is an int subclass and True == 1.
        if type(version) is not int:
            raise InvalidGenieExport("Genie agent export has no integer version")
        if version not in (1, 2):
            raise UnsupportedGenieExportVersion(
                f"Unsupported Genie export version: {version}"
            )
        return cls.model_validate(payload)


class GenieSpaceClient:
    def __init__(self, api_client: ApiClient) -> None:
        self.api_client = api_client

    def list_spaces(self) -> Iterable[GenieSpace]:
        # Use the authenticated SDK transport: the supported SDK range includes
        # releases that predate the generated Genie management methods.
        query: Dict[str, str] = {}
        seen_tokens: Set[str] = set()
        while True:
            page = GenieSpacePage.model_validate(
                self.api_client.do("GET", GENIE_SPACES_PATH, query=query)
            )
            yield from page.spaces
            if not page.next_page_token:
                return
            if page.next_page_token in seen_tokens:
                raise InvalidGenieListing("Genie space listing repeated a page token")
            seen_tokens.add(page.next_page_token)
            query = {"page_token": page.next_page_token}

    def get_space(self, space_id: str) -> GenieSpace:
        return GenieSpace.model_validate(
            self.api_client.do(
                "GET",
                f"{GENIE_SPACES_PATH}/{quote(space_id, safe='')}",
                query={"include_serialized_space": True},
            )
        )
