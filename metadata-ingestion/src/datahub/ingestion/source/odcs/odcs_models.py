from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

# Spec-valid fields the source deliberately does not map. The unknown-field
# walker in odcs_source.py classifies YAML keys three ways: declared on the
# model (parsed), listed here (spec-valid but unmapped -- one aggregate info per
# file, no warning), or neither (genuinely unknown -- a per-field warning).
# These sets are the union of the property keys declared in the vendored v3.0.2
# and v3.1.0 JSON Schemas minus the model fields; the drift-guard unit test in
# tests/unit/odcs/test_odcs_models.py enforces that invariant so a future spec
# bump fails loudly instead of producing spurious "check spelling" warnings.

KNOWN_UNMAPPED_CONTRACT_FIELDS = frozenset(
    (
        "price",
        "slaDefaultElement",
        "slaProperties",
        "support",
    )
)

KNOWN_UNMAPPED_SCHEMA_FIELDS = frozenset(
    (
        "customProperties",
        "dataGranularityDescription",
        "id",
        "relationships",
    )
)

KNOWN_UNMAPPED_PROPERTY_FIELDS = frozenset(
    (
        "businessName",
        "examples",
        "id",
        "items",
        "logicalTypeOptions",
        "physicalName",
        "relationships",
        "transformDescription",
        "transformLogic",
        "transformSourceObjects",
    )
)

KNOWN_UNMAPPED_QUALITY_FIELDS: frozenset = frozenset()

KNOWN_UNMAPPED_SERVER_FIELDS = frozenset(
    (
        "customProperties",
        "delimiter",
        "endpointUrl",
        "format",
        "id",
        "path",
        "regionName",
        "roles",
        "serviceName",
        "stagingDir",
        "stream",
    )
)

KNOWN_UNMAPPED_TEAM_FIELDS = frozenset(
    (
        "authoritativeDefinitions",
        "customProperties",
        "id",
        "tags",
    )
)

KNOWN_UNMAPPED_TEAM_MEMBER_FIELDS = frozenset(
    (
        "authoritativeDefinitions",
        "customProperties",
        "id",
        "tags",
    )
)

# v3.1 adds `id` and `description` to authoritative-definition entries.
KNOWN_UNMAPPED_AUTHDEF_FIELDS = frozenset(("description", "id"))


class ODCSBaseModel(BaseModel):
    """Base model for ODCS documents.

    `extra="ignore"`: unknown YAML keys are dropped silently here so the
    parsed model has a clean shape. The source layer is responsible for
    classifying unknown keys against the raw dict (spec-valid-but-unmapped vs
    genuinely unknown) and reporting them via `report.info`/`report.warning`.
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
    """A quality rule from `schema[].quality[]` or `schema[].properties[].quality[]`.

    Library-rule vocabulary is version-dependent:
      - v3.0.x: the library key is `rule`; documented rules are
        `duplicateCount`, `validValues` (with the `validValues` list directly
        on the rule), and `rowCount`.
      - v3.1.0: the library key is `metric` (required for `type: library`);
        metrics are `nullValues`, `missingValues`, `invalidValues`,
        `duplicateValues`, `rowCount`, with `arguments` carrying
        `validValues` / `pattern` / `missingValues` / `properties`. `rule` is
        retained by the spec as a deprecated alias.
    """

    id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    # ODCS rule type: one of library | text | sql | custom.
    type: Optional[str] = None
    # v3.1 library metric name (canonical since 3.1).
    metric: Optional[str] = None
    # v3.0.x library rule name; deprecated alias of `metric` in v3.1.
    rule: Optional[str] = None
    # v3.1 metric arguments: validValues, pattern, missingValues, properties.
    arguments: Optional[Dict[str, Any]] = None
    # v3.0.x form: static list of valid values directly on the rule.
    validValues: Optional[List[Any]] = None
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
    customProperties: Optional[List[ODCSCustomProperty]] = None
    authoritativeDefinitions: Optional[List[ODCSAuthoritativeDefinition]] = None

    @property
    def effective_metric(self) -> Optional[str]:
        """The library metric/rule name, preferring the canonical v3.1 key."""
        value = self.metric or self.rule
        if value is None:
            return None
        return value.strip() or None

    @property
    def used_deprecated_rule_key(self) -> bool:
        """True when only the legacy `rule` key carries the library name."""
        return self.metric is None and self.rule is not None


class ODCSTeamMember(ODCSBaseModel):
    username: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    role: Optional[str] = None
    dateIn: Optional[str] = None
    dateOut: Optional[str] = None
    replacedByUsername: Optional[str] = None


class ODCSTeam(ODCSBaseModel):
    """The v3.1 canonical `team` object (`{name, description, members: [...]}`).

    v3.0.x — and, as a deprecated compatibility form, v3.1 — instead use a bare
    list of members; `ODCSContract.team` accepts both shapes.
    """

    name: Optional[str] = None
    description: Optional[str] = None
    members: Optional[List[ODCSTeamMember]] = None


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

    Field set is the v3.0.x / v3.1.x union of the sections this source maps.
    Spec-valid-but-unmapped fields are listed in
    `KNOWN_UNMAPPED_CONTRACT_FIELDS`; genuinely unknown fields get a warning
    from the source layer.
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
    team: Optional[Union[ODCSTeam, List[ODCSTeamMember]]] = None
    roles: Optional[List[Dict[str, Any]]] = None
    customProperties: Optional[List[ODCSCustomProperty]] = None
    authoritativeDefinitions: Optional[List[ODCSAuthoritativeDefinition]] = None
    contractCreatedTs: Optional[str] = None

    @property
    def team_members(self) -> List[ODCSTeamMember]:
        """Team members regardless of which spec shape `team` used."""
        if self.team is None:
            return []
        if isinstance(self.team, ODCSTeam):
            return self.team.members or []
        return self.team


ODCSProperty.model_rebuild()
ODCSSchemaObject.model_rebuild()
