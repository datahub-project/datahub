import json
from typing import Callable, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

# Spec-valid fields the source deliberately does not map. The unknown-field
# walker in odcs_source.py classifies YAML keys three ways: declared on the
# model (parsed), listed here (spec-valid but unmapped -- one aggregate info per
# file, no warning), or neither (genuinely unknown -- a per-field warning).
# These sets are the union of the property keys declared in the vendored v3.0.2,
# v3.1.0 and v3.2.0 JSON Schemas minus the model fields; the drift-guard unit test in
# tests/unit/odcs/test_odcs_models.py enforces that invariant so a future spec
# bump fails loudly instead of producing spurious "check spelling" warnings.

KNOWN_UNMAPPED_CONTRACT_FIELDS = frozenset(
    (
        "price",
        "roles",
        "slaDefaultElement",
        "slaProperties",
        "support",
    )
)

KNOWN_UNMAPPED_SCHEMA_FIELDS = frozenset(
    (
        "customProperties",
        "id",
        "relationships",
        "synonyms",
    )
)

KNOWN_UNMAPPED_PROPERTY_FIELDS = frozenset(
    (
        "businessName",
        "examples",
        "id",
        "items",
        "logicalTypeOptions",
        "map",
        "physicalName",
        "relationships",
        "transformSourceObjects",
    )
)

KNOWN_UNMAPPED_QUALITY_FIELDS: frozenset = frozenset()

KNOWN_UNMAPPED_SERVER_FIELDS = frozenset(
    (
        "customProperties",
        "delimiter",
        "encoding",
        "endpointUrl",
        "format",
        "id",
        "path",
        "regionName",
        "roles",
        "serviceName",
        "stagingDir",
        "stream",
        "workgroup",
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

# Fields the models read but no mapping uses. Reported with the spec-valid but
# unmapped keys so nothing is dropped without a trace.
READ_BUT_UNMAPPED_CONTRACT_FIELDS = frozenset(("contractCreatedTs",))
READ_BUT_UNMAPPED_SCHEMA_FIELDS = frozenset(("businessName",))
READ_BUT_UNMAPPED_PROPERTY_FIELDS = frozenset(
    (
        "classification",
        "criticalDataElement",
        "encryptedName",
        "partitionKeyPosition",
        "partitioned",
        "primaryKeyPosition",
        "unique",
    )
)
READ_BUT_UNMAPPED_QUALITY_FIELDS = frozenset(("method", "schedule", "scheduler"))
READ_BUT_UNMAPPED_SERVER_FIELDS = frozenset(("environment", "location", "region"))
READ_BUT_UNMAPPED_TEAM_MEMBER_FIELDS = frozenset(("dateIn", "replacedByUsername"))

# v3.1 adds `id` and `description` to authoritative-definition entries.
KNOWN_UNMAPPED_AUTHDEF_FIELDS = frozenset(("description", "id"))
# v3.2 context entries and enum values share these keys, which are not recorded.
KNOWN_UNMAPPED_ENTRY_FIELDS = frozenset(
    ("id", "tags", "customProperties", "authoritativeDefinitions")
)


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
    value: Optional[JsonValue] = None


class ODCSDescription(ODCSBaseModel):
    # v3.1 lets `description` be either a plain string or this object; the
    # extra keys some producers add are kept so provenance stays faithful.
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    purpose: Optional[str] = None
    usage: Optional[str] = None
    limitations: Optional[str] = None


def _str_or_none(value: object) -> Optional[str]:
    return value if isinstance(value, str) else None


def _bool_or_none(value: object) -> Optional[bool]:
    return value if isinstance(value, bool) else None


def _synonym_entries(value: object) -> Optional[List[Union[str, Dict[str, object]]]]:
    """Keep usable synonym entries (strings, or objects with a string `synonym`).

    An explicit empty list is kept (it clears earlier synonyms); a list in which
    no entry is usable is treated like a missing key, the same as a non-list.
    """
    if not isinstance(value, list):
        return None
    if not value:
        return []
    kept: List[Union[str, Dict[str, object]]] = [
        e
        for e in value
        if isinstance(e, str)
        or (isinstance(e, dict) and isinstance(e.get("synonym"), str))
    ]
    return kept or None


def _enum_text(raw: object) -> Optional[str]:
    # Shown exactly, on one line: JSON spelling (`true`, `null`) rather than
    # Python's, and a string with blanks or line breaks a single space can't
    # show is quoted. Values are rendered, not compared.
    if isinstance(raw, str):
        return raw if raw and " ".join(raw.split()) == raw else json.dumps(raw)
    if raw is None or isinstance(raw, (int, float, bool)):
        return json.dumps(raw)
    return None


def _enum_entries(value: object) -> Optional[List[Dict[str, object]]]:
    """Keep enum entries whose `value` is a scalar, `null` included.

    Bare scalars are off-spec but read as values, since their intent is clear;
    a bare `null` or an entry with no `value` key has nothing to show. An empty
    list is dropped too: the spec requires at least one value.
    """
    if not isinstance(value, list):
        return None
    kept: List[Dict[str, object]] = []
    for entry in value:
        if isinstance(entry, dict):
            if "value" not in entry:
                continue
            fields = entry
        elif entry is None:
            continue
        else:
            fields = {"value": entry}
        if _enum_text(fields["value"]) is not None:
            kept.append(fields)
    return kept or None


# The keys below were ignored before this source parsed them, so an
# off-spec value must not now reject a contract that used to ingest. Bad values
# are dropped here; lenient mode already reports them as JSON Schema warnings.


class ODCSSynonym(ODCSBaseModel):
    # v3.2 (RFC 0041). `synonym` is mapped and `status: deprecated` entries are
    # dropped.
    synonym: Optional[str] = None
    status: Optional[str] = None

    _lenient_strings = field_validator("synonym", "status", mode="before")(_str_or_none)


def _context_or_none(value: object) -> Optional[Dict[str, object]]:
    # The string shorthand is the instructions; anything but it or the object
    # form is off-spec and dropped.
    if isinstance(value, str):
        return {"instructions": value}
    return value if isinstance(value, dict) else None


def _entries_with(
    key: str,
) -> Callable[[object], Optional[List[Dict[str, object]]]]:
    """Validator keeping list entries that are objects with a string `key`."""

    def keep(value: object) -> Optional[List[Dict[str, object]]]:
        if not isinstance(value, list):
            return None
        # An empty list is valid and stays empty, so it isn't reported as dropped.
        if not value:
            return []
        return [
            e for e in value if isinstance(e, dict) and isinstance(e.get(key), str)
        ] or None

    return keep


class ODCSContextConstraint(ODCSBaseModel):
    constraint: str


class ODCSVerifiedStatement(ODCSBaseModel):
    question: str
    answer: Optional[str] = None

    _lenient_answer = field_validator("answer", mode="before")(_str_or_none)


class ODCSContext(ODCSBaseModel):
    """v3.2 (RFC 0038) guidance for AI agents and tools."""

    instructions: Optional[str] = None
    constraints: Optional[List[ODCSContextConstraint]] = None
    verifiedStatements: Optional[List[ODCSVerifiedStatement]] = None

    _lenient_instructions = field_validator("instructions", mode="before")(_str_or_none)
    _lenient_constraints = field_validator("constraints", mode="before")(
        _entries_with("constraint")
    )
    _lenient_statements = field_validator("verifiedStatements", mode="before")(
        _entries_with("question")
    )


class ODCSEnumValue(ODCSBaseModel):
    # v3.2 (RFC 0033).
    value: str
    label: Optional[str] = None
    description: Optional[str] = None

    _value_text = field_validator("value", mode="before")(_enum_text)

    _lenient_strings = field_validator("label", "description", mode="before")(
        _str_or_none
    )


class ODCSProperty(ODCSBaseModel):
    name: str
    logicalType: Optional[str] = None
    physicalType: Optional[str] = None
    description: Optional[Union[str, ODCSDescription]] = None
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
    deprecated: Optional[bool] = None
    # Spec shape is a list of objects; bare strings are accepted leniently.
    synonyms: Optional[List[Union[str, ODCSSynonym]]] = None
    # v3.2 (RFC 0034): a `measure` is an aggregate, not a physical column.
    semanticType: Optional[str] = None
    transformLogic: Optional[str] = None
    transformDescription: Optional[str] = None
    enum: Optional[List[ODCSEnumValue]] = None

    _lenient_deprecated = field_validator("deprecated", mode="before")(_bool_or_none)
    _lenient_synonyms = field_validator("synonyms", mode="before")(_synonym_entries)
    _lenient_enum = field_validator("enum", mode="before")(_enum_entries)
    _lenient_strings = field_validator(
        "semanticType", "transformLogic", "transformDescription", mode="before"
    )(_str_or_none)


class ODCSSchemaObject(ODCSBaseModel):
    name: str
    physicalName: Optional[str] = None
    physicalType: Optional[str] = None
    logicalType: Optional[str] = None
    description: Optional[Union[str, ODCSDescription]] = None
    businessName: Optional[str] = None
    tags: Optional[List[str]] = None
    properties: Optional[List[ODCSProperty]] = None
    authoritativeDefinitions: Optional[List[ODCSAuthoritativeDefinition]] = None
    quality: Optional[List["ODCSQualityRule"]] = None
    deprecated: Optional[bool] = None
    dataGranularityDescription: Optional[str] = None
    context: Optional[ODCSContext] = None

    _lenient_deprecated = field_validator("deprecated", mode="before")(_bool_or_none)
    _lenient_grain = field_validator("dataGranularityDescription", mode="before")(
        _str_or_none
    )
    _lenient_context = field_validator("context", mode="before")(_context_or_none)


class ODCSQualityArguments(ODCSBaseModel):
    # `extra="allow"` keeps engine-specific keys so they round-trip into the
    # custom-assertion provenance instead of being silently dropped.
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    validValues: Optional[List[JsonValue]] = None
    pattern: Optional[str] = None
    missingValues: Optional[List[JsonValue]] = None
    properties: Optional[List[str]] = None


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
    arguments: Optional[ODCSQualityArguments] = None
    # v3.0.x form: static list of valid values directly on the rule.
    validValues: Optional[List[JsonValue]] = None
    dimension: Optional[str] = None
    severity: Optional[str] = None
    businessImpact: Optional[str] = None
    method: Optional[str] = None
    schedule: Optional[str] = None
    scheduler: Optional[str] = None
    query: Optional[str] = None
    engine: Optional[str] = None
    implementation: Optional[Union[str, Dict[str, JsonValue]]] = None
    mustBe: Optional[Union[float, int, str, bool]] = None
    mustNotBe: Optional[Union[float, int, str, bool]] = None
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
        # Prefer the canonical v3.1 `metric` over the deprecated `rule` alias.
        value = self.metric or self.rule
        if value is None:
            return None
        return value.strip() or None

    @property
    def used_deprecated_rule_key(self) -> bool:
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
    # YAML reads unquoted values such as an AWS account ID (`warehouse`,
    # `account`) or a numeric database name as numbers.
    model_config = ConfigDict(coerce_numbers_to_str=True)

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
    # v3.2 `iceberg` server (Iceberg REST catalog).
    catalogUrl: Optional[str] = None
    namespace: Optional[str] = None


class ODCSContract(ODCSBaseModel):
    """Top-level ODCS contract document.

    Field set is the v3.0.x–v3.2.x union of the sections this source maps.
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
    description: Optional[Union[str, ODCSDescription]] = None
    tags: Optional[List[str]] = None
    schema_: Optional[List[ODCSSchemaObject]] = Field(default=None, alias="schema")
    servers: Optional[List[ODCSServer]] = None
    team: Optional[Union[ODCSTeam, List[ODCSTeamMember]]] = None
    customProperties: Optional[List[ODCSCustomProperty]] = None
    authoritativeDefinitions: Optional[List[ODCSAuthoritativeDefinition]] = None
    contractCreatedTs: Optional[str] = None
    context: Optional[ODCSContext] = None

    _lenient_context = field_validator("context", mode="before")(_context_or_none)

    @property
    def team_members(self) -> List[ODCSTeamMember]:
        if self.team is None:
            return []
        if isinstance(self.team, ODCSTeam):
            return self.team.members or []
        return self.team


ODCSProperty.model_rebuild()
ODCSSchemaObject.model_rebuild()


# Keys whose off-spec values the validators above drop instead of
# rejecting the contract. Kept next to the validators so the source's
# "dropped as off-spec" report cannot drift from them.
LENIENT_FIELDS_BY_MODEL: Dict[type, Tuple[str, ...]] = {
    ODCSContract: ("context",),
    ODCSContext: ("instructions", "constraints", "verifiedStatements"),
    ODCSVerifiedStatement: ("answer",),
    ODCSEnumValue: ("label", "description"),
    ODCSSynonym: ("status",),
    ODCSSchemaObject: ("deprecated", "dataGranularityDescription", "context"),
    ODCSProperty: (
        "deprecated",
        "synonyms",
        "semanticType",
        "transformLogic",
        "transformDescription",
        "enum",
    ),
}
