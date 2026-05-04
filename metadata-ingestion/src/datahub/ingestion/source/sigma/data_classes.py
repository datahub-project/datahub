import logging
from copy import deepcopy
from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

from datahub.emitter.mcp_builder import ContainerKey

logger = logging.getLogger(__name__)


class WorkspaceKey(ContainerKey):
    workspaceId: str


class WorkbookKey(ContainerKey):
    workbookId: str


class DataModelKey(ContainerKey):
    """Container key for a Sigma Data Model.

    Keyed on ``dataModelId`` (the Sigma-assigned UUID, stable across
    renames). ``dataModelUrlId`` is surfaced on ``customProperties`` for
    deep-linking back into Sigma.

    Multi-environment note: ``ContainerKey`` already namespaces the
    generated container URN by ``platform`` and (when set) by
    ``platform_instance``, but *not* by ``env``. Operators running two
    recipes against the same tenant with different ``env`` values (e.g.
    ``DEV`` vs ``PROD``) should set a distinct ``platform_instance`` per
    environment so the DM Containers do not collide on a single URN.
    Element Datasets use ``make_dataset_urn_with_platform_instance``
    which *is* env-scoped, so this caveat only applies to the Container
    layer. This mirrors the existing ``WorkspaceKey`` / ``WorkbookKey``
    shape in this connector.
    """

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
    # Optional: Sigma's lineage payloads can carry ``name: null`` for
    # dataset upstreams. Callers guard on a missing name.
    name: Optional[str] = None


class SheetUpstream(BaseModel):
    type: Literal["sheet"] = "sheet"
    name: Optional[str] = None
    element_id: str


class DataModelElementUpstream(BaseModel):
    """DM element referenced from a workbook. Node id shape is
    ``<dataModelUrlId>/<opaque_suffix>``; bridging to a specific DM
    element URN happens at emit time via ``name``.
    """

    type: Literal["data-model"] = "data-model"
    name: Optional[str] = None
    data_model_url_id: str


# "table" nodes are terminal (handled by SQL parsing); "join" nodes are
# BFS pass-throughs and are not stored as upstreams.
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
    columns: List[str] = Field(default_factory=list)
    # name -> formula mapping populated when column entries carry formula data.
    # Populated by the model_validator below; defaults to {} for plain string columns.
    column_formulas: Dict[str, Optional[str]] = Field(default_factory=dict)
    upstream_sources: Dict[str, "ElementUpstream"] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _extract_column_formulas(cls, values: Any) -> Any:
        """Accept columns as plain strings or as dicts with name+formula.

        In production, the page-elements endpoint returns columns as plain strings
        and formulas are hydrated externally from GET /workbooks/{id}/columns via
        SigmaAPI.get_workbook_column_formulas().

        In unit tests, column entries may be passed as dicts like
        {"name": "Col", "formula": "[Source/Col]"} to bypass the API layer.
        Production formula hydration should happen after validation via the
        separate workbook columns response, not through this test helper path.
        If Sigma ever changes the page-elements endpoint to return rich column
        objects, this branch will intentionally preserve those formulas instead
        of discarding the richer payload.
        When dict entries are detected this validator:
          - Replaces `columns` with a plain list of names (backward-compatible).
          - Populates `column_formulas` with the name->formula mapping.
        """
        raw_columns = values.get("columns", [])
        if raw_columns and any(isinstance(col, dict) for col in raw_columns):
            column_names: List[str] = []
            column_formulas: Dict[str, Optional[str]] = {}
            for col in raw_columns:
                if isinstance(col, dict):
                    name = col.get("name", "")
                    if not name:
                        logger.debug(
                            "Skipping Sigma column formula without a name: %s", col
                        )
                        continue
                    column_names.append(name)
                    column_formulas[name] = col.get("formula") or None
                else:
                    column_names.append(str(col))
            values["columns"] = column_names
            values["column_formulas"] = column_formulas
        return values

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
    # Scopes this column to a specific DM element. The /columns endpoint
    # returns columns across all elements in one flat list.
    elementId: Optional[str] = None
    label: Optional[str] = None
    formula: Optional[str] = None


class SigmaDataModelElement(BaseModel):
    """A single element inside a Sigma Data Model (all transformation
    tables, no visualizations).
    """

    elementId: str
    name: str
    type: Optional[str] = None
    # NOTE: ``vizualizationType`` matches Sigma's API spelling (the typo is
    # upstream, not ours). Keep as-is so Pydantic parses the raw payload
    # without extra field aliasing.
    vizualizationType: Optional[str] = None
    columns: List[SigmaDataModelColumn] = []
    # From /lineage, filtered to this element. Each entry is either an
    # intra-DM elementId or an ``inode-<suffix>`` for external upstreams.
    source_ids: List[str] = []

    @model_validator(mode="before")
    @classmethod
    def _discard_api_bare_string_columns(cls, values: Dict) -> Dict:
        # The real /elements endpoint returns ``columns`` as bare strings.
        # Rich SigmaDataModelColumn objects come from /columns and are
        # attached in ``_assemble_data_model``. Filter to dict entries so a
        # hypothetical mixed payload retains its well-formed rows instead
        # of being dropped wholesale. If /columns later fails, the element
        # will render with an empty schema; the debug log below lets
        # operators spot that the element had column names on /elements
        # but they were intentionally discarded pending /columns.
        if isinstance(values, dict):
            raw_columns = values.get("columns")
            if isinstance(raw_columns, list) and any(
                not isinstance(c, dict) for c in raw_columns
            ):
                bare = [c for c in raw_columns if not isinstance(c, dict)]
                logger.debug(
                    "Discarded %d bare-string column(s) from /elements for "
                    "element %r (elementId=%s); rich columns will come from "
                    "/columns.",
                    len(bare),
                    values.get("name"),
                    values.get("elementId"),
                )
                values = {
                    **values,
                    "columns": [c for c in raw_columns if isinstance(c, dict)],
                }
        return values


class SigmaDataModel(BaseModel):
    dataModelId: str  # UUID; stable across renames
    name: str
    description: Optional[str] = None
    createdBy: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    url: Optional[str] = None
    # Human-readable slug used by Sigma in URLs and lineage references
    # (``<dataModelUrlId>/<suffix>`` in workbook sourceIds).
    urlId: Optional[str] = None
    latestVersion: Optional[int] = None
    workspaceId: Optional[str] = None
    path: Optional[str] = None
    badge: Optional[str] = None
    elements: List[SigmaDataModelElement] = []

    def get_url_id(self) -> str:
        """Return the DM's URL identifier: explicit ``urlId`` if set,
        else the last segment of ``url``, else the UUID. Blank / whitespace
        values are treated as missing so they do not become bridge keys that
        collide with other empty-urlId DMs.
        """
        if self.urlId and self.urlId.strip():
            return self.urlId
        if self.url:
            last_segment = self.url.split("/")[-1]
            if last_segment:
                return last_segment
        return self.dataModelId

    def get_element_urn_part(self, element: SigmaDataModelElement) -> str:
        # ``<dataModelId>.<elementId>`` keyed off the immutable UUID.
        # ``urlId`` can be reissued across renames and would churn URNs.
        # Collision with a slug is unlikely: Sigma urlIds are url-safe
        # tokens without dots.
        return f"{self.dataModelId}.{element.elementId}"
