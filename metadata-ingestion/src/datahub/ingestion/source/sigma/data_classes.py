from copy import deepcopy
from datetime import datetime
from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

from datahub.emitter.mcp_builder import ContainerKey


class WorkspaceKey(ContainerKey):
    workspaceId: str


class WorkbookKey(ContainerKey):
    workbookId: str


class DataModelKey(ContainerKey):
    # Use UUID (immutable across renames) rather than urlId for the container key.
    # dataModelUrlId is still captured on the Container's customProperties.
    dataModelId: str


class Workspace(BaseModel):
    workspaceId: str
    name: str
    createdBy: str
    createdAt: datetime
    updatedAt: datetime

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = deepcopy(values)
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


class DatasetUpstream(BaseModel):
    type: Literal["dataset"] = "dataset"
    # Required: used by sigma.py to correlate with SQL-parsed warehouse table names.
    name: str


class SheetUpstream(BaseModel):
    type: Literal["sheet"] = "sheet"
    name: Optional[str] = None
    element_id: str


class DataModelElementUpstream(BaseModel):
    """
    Upstream node representing a Sigma Data Model element referenced from a
    workbook. Sigma's lineage API exposes these with a ``data-model`` type and
    a nodeId of shape ``<dataModelUrlId>/<opaque_suffix>``. The opaque suffix
    is not resolvable via any public endpoint (2026-04-21 probe), so bridging
    to a specific DM element URN is done at emit time via the ``name`` field
    (the DM element's own name, confirmed stable and carried on the node).
    """

    type: Literal["data-model"] = "data-model"
    name: Optional[str] = None
    data_model_url_id: str


# "table" (warehouse) nodes are terminal in BFS — handled by the SQL-parse path.
# "join" pass-through nodes are traversed by BFS but never stored as upstreams.
ElementUpstream = Annotated[
    Union[DatasetUpstream, SheetUpstream, DataModelElementUpstream],
    Field(discriminator="type"),
]


class Element(BaseModel):
    elementId: str
    name: str
    url: str
    type: Optional[str] = None
    vizualizationType: Optional[str] = None
    query: Optional[str] = None
    columns: List[str] = []
    upstream_sources: Dict[str, "ElementUpstream"] = {}

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
    ownerId: str
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
    urlId: Optional[str] = None
    badge: Optional[str] = None
    workspaceId: Optional[str] = None


class SigmaDataModelColumn(BaseModel):
    columnId: str
    name: str
    # elementId scopes this column to a specific DM element (DM columns
    # endpoint returns all columns across all elements in one flat list).
    elementId: Optional[str] = None
    label: Optional[str] = None
    formula: Optional[str] = None


class SigmaDataModelElement(BaseModel):
    """
    A single element inside a Sigma Data Model. DM elements are all
    transformation tables (no visualizations) — Sigma's /elements endpoint
    returns them with the same shape as workbook elements.
    """

    elementId: str
    name: str
    type: Optional[str] = None
    vizualizationType: Optional[str] = None
    columns: List[SigmaDataModelColumn] = []
    # sourceIds from /lineage, filtered to this element. Entries are either
    # another elementId in the same DM (intra-DM lineage) or an
    # ``inode-<suffix>`` string resolving to a warehouse table or Sigma Dataset.
    source_ids: List[str] = []

    @model_validator(mode="before")
    @classmethod
    def _discard_api_bare_string_columns(cls, values: Dict) -> Dict:
        # The real Sigma /dataModels/{id}/elements endpoint returns ``columns``
        # as a list of bare column-name strings (the same shape as workbook
        # Element.columns: List[str]), not as rich column objects. The rich
        # SigmaDataModelColumn objects — with columnId, elementId, label,
        # formula — come from the separate /dataModels/{id}/columns endpoint
        # and are attached post-parse in SigmaAPI._assemble_data_model. Strip
        # the bare-string list here so pydantic validation does not fail on
        # real tenant responses; payloads that already carry rich column
        # dicts (e.g. crafted unit-test mocks) are left untouched.
        if isinstance(values, dict):
            raw_columns = values.get("columns")
            if isinstance(raw_columns, list) and any(
                isinstance(c, str) for c in raw_columns
            ):
                values = {**values, "columns": []}
        return values


class SigmaDataModel(BaseModel):
    dataModelId: str  # UUID — stable across renames
    name: str
    description: Optional[str] = None
    createdBy: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    url: Optional[str] = None
    # urlId is the human-readable slug used in Sigma URLs. Sigma references
    # DMs from workbook lineage by urlId (the ``<dataModelUrlId>/<suffix>``
    # prefix in workbook sourceIds), so we need it for the workbook→DM bridge.
    urlId: Optional[str] = None
    latestVersion: Optional[int] = None
    workspaceId: Optional[str] = None
    path: Optional[str] = None
    badge: Optional[str] = None
    elements: List[SigmaDataModelElement] = []

    def get_url_id(self) -> str:
        """
        Resolve the DM's URL-based identifier. Prefer the explicit ``urlId``
        field (populated from /files metadata); fall back to parsing the last
        path segment of the DM url; finally fall back to the UUID.
        """
        if self.urlId:
            return self.urlId
        if self.url:
            return self.url.split("/")[-1]
        return self.dataModelId

    def get_element_urn_part(self, element: SigmaDataModelElement) -> str:
        # ``<dataModelId>.<elementId>`` — keyed off the immutable UUID rather
        # than ``urlId``. Sigma slugs (urlId) can be reissued across renames
        # and visibility toggles, which would silently churn element Dataset
        # URNs on the next ingestion and orphan DataHub-side attachments
        # (owners, terms, lineage) to the old URNs. Matches ``DataModelKey``
        # which uses ``dataModelId`` for the Container URN. ``dataModelUrlId``
        # is still surfaced via ``DatasetProperties.customProperties`` for
        # humans. The UUID's dashes also make collision with the Sigma
        # Dataset URN shape (``sigma, <slug>, env``) effectively impossible.
        return f"{self.dataModelId}.{element.elementId}"
