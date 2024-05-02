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


class SigmaDataset(BaseModel):
    datasetId: str
    workspaceId: str
    name: str
    description: str
    createdBy: str
    createdAt: datetime
    updatedAt: datetime
    url: str
    path: str
    badge: Optional[str] = None

    @root_validator(pre=True)
    def update_values(cls, values: Dict) -> Dict:
        # As element lineage api provide this id as source dataset id
        values["datasetId"] = values["url"].split("/")[-1]
        return values


class Element(BaseModel):
    elementId: str
    type: str
    name: str
    url: str
    vizualizationType: Optional[str] = None
    query: Optional[str] = None
    columns: List[str] = []
    upstream_sources: Dict[str, str] = {}


class Page(BaseModel):
    pageId: str
    name: str
    elements: List[Element] = []


class Workbook(BaseModel):
    workbookId: str
    workspaceId: str
    name: str
    createdBy: str
    updatedBy: str
    createdAt: datetime
    updatedAt: datetime
    url: str
    path: str
    latestVersion: int
    pages: List[Page] = []
    badge: Optional[str] = None
