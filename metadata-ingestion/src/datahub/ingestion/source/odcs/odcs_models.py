from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class ODCSBaseModel(BaseModel):
    """Base model for ODCS documents.

    `extra="ignore"` (D5): unknown YAML keys are dropped silently here so the
    parsed model has a clean shape. The source layer is responsible for
    detecting unknown keys against the raw dict and emitting per-field
    warnings via `report.warning(...)` so typos do not slip through silently.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class ODCSAuthoritativeDefinition(ODCSBaseModel):
    type: Optional[str] = None
    url: Optional[str] = None


class ODCSCustomProperty(ODCSBaseModel):
    property: Optional[str] = None
    value: Optional[Any] = None


class ODCSProperty(ODCSBaseModel):
    """A column / property within an ODCS schema object."""

    name: str
    logicalType: Optional[str] = None
    physicalType: Optional[str] = None
    description: Optional[Union[str, Dict[str, Any]]] = None
    primaryKey: Optional[bool] = None
    primaryKeyPosition: Optional[int] = None
    required: Optional[bool] = None
    unique: Optional[bool] = None
    partitioned: Optional[bool] = None
    partitionKeyPosition: Optional[int] = None
    classification: Optional[str] = None
    criticalDataElement: Optional[bool] = None
    encryptedName: Optional[str] = None
    tags: Optional[List[str]] = None
    authoritativeDefinitions: Optional[List[ODCSAuthoritativeDefinition]] = None
    properties: Optional[List["ODCSProperty"]] = None
    customProperties: Optional[List[ODCSCustomProperty]] = None
    quality: Optional[List["ODCSQualityRule"]] = None


class ODCSSchemaObject(ODCSBaseModel):
    """An entry in `schema[]` — typically corresponds to a table/view."""

    name: str
    physicalName: Optional[str] = None
    physicalType: Optional[str] = None
    logicalType: Optional[str] = None
    description: Optional[Union[str, Dict[str, Any]]] = None
    businessName: Optional[str] = None
    tags: Optional[List[str]] = None
    properties: Optional[List[ODCSProperty]] = None
    authoritativeDefinitions: Optional[List[ODCSAuthoritativeDefinition]] = None
    quality: Optional[List["ODCSQualityRule"]] = None


class ODCSQualityRule(ODCSBaseModel):
    """A quality rule from `schema[].quality[]` or `schema[].properties[].quality[]`."""

    name: Optional[str] = None
    description: Optional[str] = None
    # ODCS rule type: one of library | sql | custom | text.
    type: Optional[str] = None
    # Name of a library rule, e.g. "rowCount", "unique", "notNull", "duplicateCount".
    rule: Optional[str] = None
    dimension: Optional[str] = None
    severity: Optional[str] = None
    businessImpact: Optional[str] = None
    method: Optional[str] = None
    schedule: Optional[str] = None
    scheduler: Optional[str] = None
    query: Optional[str] = None
    engine: Optional[str] = None
    implementation: Optional[Union[str, Dict[str, Any]]] = None
    mustBe: Optional[Any] = None
    mustNotBe: Optional[Any] = None
    mustBeGreaterThan: Optional[float] = None
    mustBeGreaterOrEqualTo: Optional[float] = None
    mustBeLessThan: Optional[float] = None
    mustBeLessOrEqualTo: Optional[float] = None
    mustBeBetween: Optional[List[float]] = None
    mustNotBeBetween: Optional[List[float]] = None
    unit: Optional[str] = None
    tags: Optional[List[str]] = None
    # Set when the rule's YAML declares a column directly (top-level rule on a
    # column outside any schema). Most rules attach to a property via the YAML
    # tree shape rather than this field; the mapper reads it for the legacy
    # top-level `quality[]` form.
    column: Optional[str] = None


class ODCSTeamMember(ODCSBaseModel):
    username: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    role: Optional[str] = None
    dateIn: Optional[str] = None
    dateOut: Optional[str] = None
    replacedByUsername: Optional[str] = None
    comment: Optional[str] = None


class ODCSServer(ODCSBaseModel):
    server: str
    type: Optional[str] = None
    description: Optional[str] = None
    environment: Optional[str] = None
    location: Optional[str] = None
    host: Optional[str] = None
    port: Optional[Union[int, str]] = None
    database: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    project: Optional[str] = None
    dataset: Optional[str] = None
    catalog: Optional[str] = None
    account: Optional[str] = None
    region: Optional[str] = None
    warehouse: Optional[str] = None


class ODCSContract(ODCSBaseModel):
    """Top-level ODCS contract document.

    Field set is the v3.0.x / v3.1.x intersection plus a few common extensions.
    Unknown fields are dropped (see `ODCSBaseModel`); the source emits a
    warning per unknown field detected against the raw YAML dict.
    """

    apiVersion: Optional[str] = None
    kind: Optional[str] = None
    id: str
    name: Optional[str] = None
    version: Optional[str] = None
    status: Optional[str] = None
    domain: Optional[str] = None
    dataProduct: Optional[str] = None
    tenant: Optional[str] = None
    description: Optional[Union[str, Dict[str, Any]]] = None
    tags: Optional[List[str]] = None
    schema_: Optional[List[ODCSSchemaObject]] = Field(default=None, alias="schema")
    servers: Optional[List[ODCSServer]] = None
    team: Optional[List[ODCSTeamMember]] = None
    roles: Optional[List[Dict[str, Any]]] = None
    quality: Optional[List[ODCSQualityRule]] = None  # deprecated top-level form
    customProperties: Optional[List[ODCSCustomProperty]] = None
    contractCreatedTs: Optional[str] = None


ODCSProperty.model_rebuild()
ODCSSchemaObject.model_rebuild()
