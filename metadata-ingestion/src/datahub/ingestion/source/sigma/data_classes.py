from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, root_validator

from datahub.emitter.mcp_builder import ContainerKey


class WorkspaceKey(ContainerKey):
    workspaceId: str


class WorkbookKey(ContainerKey):
    workbookId: str


class Workspace(BaseModel):
    workspaceId: str
    name: str
    createdBy: str
    createdAt: datetime
    updatedAt: datetime

    @root_validator(pre=True)
    def update_values(cls, values: Dict) -> Dict:
        # Update name if presonal workspace
        if values["name"] == "User Folder":
            values["name"] = "My documents"
        return values


class SigmaDataset(BaseModel):
    datasetId: str
    name: str
    description: str
    createdBy: str
    createdAt: datetime
    updatedAt: datetime
    url: str
    workspaceId: Optional[str] = None
    path: Optional[str] = None
    badge: Optional[str] = None

    def get_urn_part(self):
        # As element lineage api provide this id as source dataset id
        return self.url.split("/")[-1]


class Element(BaseModel):
    elementId: str
    name: str
    url: str
    type: Optional[str] = None
    vizualizationType: Optional[str] = None
    query: Optional[str] = None
    columns: List[str] = []
    upstream_sources: Dict[str, str] = {}

    def get_urn_part(self):
        return self.elementId


class Page(BaseModel):
    pageId: str
    name: str
    elements: List[Element] = []

    def get_urn_part(self):
        return self.pageId


class Workbook(BaseModel):
    workbookId: str
    name: str
    createdBy: str
    updatedBy: str
    createdAt: datetime
    updatedAt: datetime
    url: str
    path: str
    latestVersion: int
    workspaceId: Optional[str] = None
    description: Optional[str] = None
    pages: List[Page] = []
    badge: Optional[str] = None


class File(BaseModel):
    id: str
    name: str
    parentId: str
    path: str
    type: str
    badge: Optional[str] = None
    workspaceId: Optional[str] = None
