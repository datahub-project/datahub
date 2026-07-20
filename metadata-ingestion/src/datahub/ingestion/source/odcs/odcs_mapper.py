import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple, Type, TypeGuard

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.odcs.odcs_config import (
    LOWERCASE_BY_DEFAULT_PLATFORMS,
    ODCS_PLATFORM,
    SERVER_TYPE_TO_PLATFORM,
    ODCSSourceConfig,
    ServerMapping,
)
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
    ODCSServer,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    AuditStampClass,
    BooleanTypeClass,
    BytesTypeClass,
    CustomAssertionInfoClass,
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DateTypeClass,
    EdgeClass,
    EnumTypeClass,
    FieldAssertionInfoClass,
    FieldAssertionTypeClass,
    FieldMetricAssertionClass,
    FieldMetricTypeClass,
    FieldValuesAssertionClass,
    FieldValuesFailThresholdClass,
    FieldValuesFailThresholdTypeClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    LogicalParentClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    PlatformTypeClass,
    RecordTypeClass,
    RowCountTotalClass,
    SchemaAssertionCompatibilityClass,
    SchemaAssertionInfoClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaFieldSpecClass,
    SchemaMetadataClass,
    SqlAssertionInfoClass,
    SqlAssertionTypeClass,
    StringTypeClass,
    TagAssociationClass,
    TimeTypeClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)

# ODCS team `role` value -> DataHub OwnershipType. Anything not listed here
# falls back to TECHNICAL_OWNER.
_OWNER_ROLE_MAP: Dict[str, str] = {
    "owner": OwnershipTypeClass.TECHNICAL_OWNER,
    "dataOwner": OwnershipTypeClass.TECHNICAL_OWNER,
    "data_owner": OwnershipTypeClass.TECHNICAL_OWNER,
    "technical_owner": OwnershipTypeClass.TECHNICAL_OWNER,
    "technicalOwner": OwnershipTypeClass.TECHNICAL_OWNER,
    "business_owner": OwnershipTypeClass.BUSINESS_OWNER,
    "businessOwner": OwnershipTypeClass.BUSINESS_OWNER,
    "steward": OwnershipTypeClass.DATA_STEWARD,
    "data_steward": OwnershipTypeClass.DATA_STEWARD,
    "dataSteward": OwnershipTypeClass.DATA_STEWARD,
}

# ODCS library metric vocabulary, spec-exact. v3.1 names the library key
# `metric` (with `rule` as a deprecated alias); v3.0.x used `rule`. The names
# below are the only library rules either spec version defines — anything else
# falls through to a CustomAssertion so intent is preserved, never guessed.
_METRIC_NULL_VALUES = "nullValues"  # v3.1, property-level
_METRIC_MISSING_VALUES = "missingValues"  # v3.1, property-level
_METRIC_INVALID_VALUES = "invalidValues"  # v3.1, property-level
_METRIC_DUPLICATE_VALUES = "duplicateValues"  # v3.1, property- or schema-level
_METRIC_ROW_COUNT = "rowCount"  # v3.0 + v3.1, schema-level
_METRIC_DUPLICATE_COUNT = "duplicateCount"  # v3.0 name for duplicateValues
_METRIC_VALID_VALUES = "validValues"  # v3.0 name for invalidValues

_UNIT_ROWS = "rows"
_UNIT_PERCENT = "percent"

# ODCS rule types.
_RULE_TYPE_LIBRARY = "library"
_RULE_TYPE_TEXT = "text"
_RULE_TYPE_SQL = "sql"
_RULE_TYPE_CUSTOM = "custom"

# ODCS `logicalType` (and a few common `physicalType` spellings) -> the DataHub
# SchemaFieldDataType union member. ODCS logical/physical types are free-form
# strings, so this is a best-effort mapping; unmapped types fall back to
# NullType and are surfaced via SchemaBuildResult.unmapped_types rather than
# silently swallowed (a silent NullType hides real type information).
_LOGICAL_TYPE_MAP: Dict[str, Type] = {
    "string": StringTypeClass,
    "text": StringTypeClass,
    "varchar": StringTypeClass,
    "char": StringTypeClass,
    "uuid": StringTypeClass,
    "integer": NumberTypeClass,
    "int": NumberTypeClass,
    "bigint": NumberTypeClass,
    "smallint": NumberTypeClass,
    "long": NumberTypeClass,
    "number": NumberTypeClass,
    "numeric": NumberTypeClass,
    "decimal": NumberTypeClass,
    "double": NumberTypeClass,
    "float": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "bool": BooleanTypeClass,
    "date": DateTypeClass,
    "timestamp": TimeTypeClass,
    "timestamp_tz": TimeTypeClass,
    "timestamp_ntz": TimeTypeClass,
    "datetime": TimeTypeClass,
    "time": TimeTypeClass,
    "object": RecordTypeClass,
    "record": RecordTypeClass,
    "struct": RecordTypeClass,
    "json": RecordTypeClass,
    "variant": RecordTypeClass,
    "array": ArrayTypeClass,
    "list": ArrayTypeClass,
    "map": MapTypeClass,
    "bytes": BytesTypeClass,
    "binary": BytesTypeClass,
    "enum": EnumTypeClass,
}


@dataclass
class PhysicalBinding:
    """Resolution result for one `schema[]` entry's physical dataset.

    An unbound entry is never skipped — the logical ODCS dataset and its
    assertions are still emitted; only the `logicalParent` link is affected.
    `name_passthrough` marks a physical name taken verbatim from a dotted
    `physicalName` rather than composed from server fields.
    """

    index: int
    schema_entry: ODCSSchemaObject
    logical_urn: str
    physical_urn: Optional[str]
    unmapped_reason: Optional[str] = None
    name_passthrough: bool = False


@dataclass
class SchemaBuildResult:
    """Result of building canonical `schemaMetadata` for a schema entry."""

    schema_metadata: Optional[SchemaMetadataClass]
    unmapped_types: List[str] = field(default_factory=list)


def _description_to_str(description: object) -> Optional[str]:
    """ODCS allows description to be a string or an object (e.g. {purpose, usage, limitations})."""
    if description is None:
        return None
    if isinstance(description, str):
        return description.strip() or None
    if isinstance(description, dict):
        parts: List[str] = []
        for key in ("purpose", "usage", "limitations", "summary", "description"):
            v = description.get(key)
            if isinstance(v, str) and v.strip():
                parts.append(f"**{key}**: {v.strip()}")
        if parts:
            return "\n\n".join(parts)
    return None


# ----------------------------------------------------------------------------
# Platform registration
# ----------------------------------------------------------------------------


def odcs_platform_info_mcp() -> MetadataChangeProposalWrapper:
    """Emit the `odcs` DataPlatformInfo aspect at ingestion time.

    The platform is also registered at GMS boot via the bootstrap MCP added in
    PR #17332. This runtime emission is kept deliberately: ingestion is
    version-decoupled from the server, so a run may target a GMS that predates
    that entry, and emitting the aspect guarantees the platform's display name
    and logo exist regardless of server version (canonical pattern:
    confluence_source.py).

    The fields below must stay identical to the boot-time entry. DataPlatformInfo
    is a whole-aspect upsert, not a field-level merge, so a divergent run would
    clobber the registry-provided aspect rather than reinforce it -- e.g.
    omitting logoUrl here would wipe the logo on servers that already have #17332.
    """
    return MetadataChangeProposalWrapper(
        entityUrn=make_data_platform_urn(ODCS_PLATFORM),
        aspect=DataPlatformInfoClass(
            name=ODCS_PLATFORM,
            type=PlatformTypeClass.OTHERS,
            datasetNameDelimiter=".",
            displayName="Open Data Contract Standard",
            logoUrl="assets/platforms/odcslogo.png",
        ),
    )


# ----------------------------------------------------------------------------
# URN + binding resolution
# ----------------------------------------------------------------------------


def odcs_to_logical_dataset_urn(
    contract: ODCSContract,
    schema_entry: ODCSSchemaObject,
    config: ODCSSourceConfig,
) -> str:
    """Build the logical `odcs` Dataset URN for a `schema[]` entry.

    The name is derived from `logical_dataset_name_template`; the platform is
    always `odcs`. This URN is the platform-of-record identity ODCS owns.
    """
    name = config.logical_dataset_name_template.format(
        contract_id=contract.id,
        schema_name=schema_entry.name,
        contract_version=contract.version or "",
    )
    return make_dataset_urn_with_platform_instance(
        platform=ODCS_PLATFORM,
        name=name,
        platform_instance=None,
        env=config.env,
    )


@dataclass
class ResolvedServer:
    """The contract server chosen for physical binding, plus its platform."""

    server: ODCSServer
    platform: str
    mapping: Optional[ServerMapping]


# DataHub platform id -> the ODCSServer fields (in order) that qualify a table
# name, matching each platform connector's URN naming convention. `schema_` is
# the pydantic-safe name of the ODCS `schema` server field. Platforms absent
# here (e.g. oracle, whose ODCS server entry carries only host/port/serviceName)
# cannot be composed — a dotted physicalName or an explicit override is needed.
_PLATFORM_NAME_PARTS: Dict[str, Tuple[str, ...]] = {
    "postgres": ("database", "schema_"),
    "redshift": ("database", "schema_"),
    "mssql": ("database", "schema_"),
    "snowflake": ("database", "schema_"),
    "trino": ("catalog", "schema_"),
    "databricks": ("catalog", "schema_"),
    "bigquery": ("project", "dataset"),
    "mysql": ("database",),
}


def _override_for_server(
    server_name: str, overrides: List[ServerMapping]
) -> Optional[ServerMapping]:
    named = next(
        (m for m in overrides if not m.match_any and m.server == server_name), None
    )
    if named is not None:
        return named
    return next((m for m in overrides if m.match_any), None)


def _resolve_server(
    contract: ODCSContract, config: ODCSSourceConfig
) -> Tuple[Optional[ResolvedServer], Optional[str]]:
    """Pick the first contract server that maps to a DataHub platform.

    The platform comes from a matching `server_overrides` entry when it sets
    one, else from the spec-required `servers[].type` via
    SERVER_TYPE_TO_PLATFORM. Returns `(resolved, None)` on success or
    `(None, reason)` when no server is mappable.
    """
    if not contract.servers:
        return None, "contract declares no servers[]"
    unmappable: List[str] = []
    for server in contract.servers:
        mapping = _override_for_server(server.server, config.server_overrides)
        platform = (
            mapping.platform if mapping is not None and mapping.platform else None
        ) or SERVER_TYPE_TO_PLATFORM.get((server.type or "").strip().lower())
        if platform:
            return ResolvedServer(
                server=server, platform=platform, mapping=mapping
            ), None
        unmappable.append(f"{server.server} (type={server.type or 'missing'})")
    return None, (
        "no servers[] entry maps to a DataHub platform; "
        f"unmappable: {', '.join(unmappable)}"
    )


def _compose_physical_name(
    platform: str,
    server: ODCSServer,
    table: str,
    convert_to_lowercase: bool,
) -> Tuple[Optional[str], Optional[str], bool]:
    """Compose the fully-qualified physical dataset name for a platform.

    Follows each platform connector's URN naming convention (postgres:
    `database.schema.table`, bigquery: `project.dataset.table`, ...). A `table`
    that already contains a dot is passed through as-is, on the assumption the
    contract author supplied a pre-qualified physicalName. Returns
    `(name, None, passthrough)` on success or `(None, reason, False)` when the
    name cannot be composed. Lowercasing applies only to platforms whose
    connectors lowercase by default (see LOWERCASE_BY_DEFAULT_PLATFORMS).
    """
    lowercase = convert_to_lowercase and platform in LOWERCASE_BY_DEFAULT_PLATFORMS
    if "." in table:
        return table.lower() if lowercase else table, None, True
    parts_spec = _PLATFORM_NAME_PARTS.get(platform)
    if parts_spec is None:
        return (
            None,
            f"platform '{platform}' has no name-composition rule; supply a "
            "fully-qualified dotted physicalName or a physical_urn_overrides entry",
            False,
        )
    values: List[str] = []
    missing: List[str] = []
    for attr in parts_spec:
        value = getattr(server, attr)
        if value:
            values.append(str(value))
        else:
            missing.append(attr.rstrip("_"))
    if missing:
        return (
            None,
            f"server '{server.server}' is missing {'/'.join(missing)} needed to "
            f"qualify a {platform} table name",
            False,
        )
    name = ".".join([*values, table])
    return name.lower() if lowercase else name, None, False


def odcs_to_physical_bindings(
    contract: ODCSContract, config: ODCSSourceConfig
) -> List[PhysicalBinding]:
    """Resolve a PhysicalBinding per `schema[]` entry.

    Every entry yields a logical ODCS dataset URN. The physical URN — used only
    for the `logicalParent` link — is resolved via, in priority order:
      1. `physical_urn_overrides[contract.id][schema_entry.name]`
         (empty string = deliberately unbound; absent = fall through).
      2. The contract's first mappable `servers[]` entry (platform from
         `server_overrides` or the spec-required server `type`), with the
         table name qualified per the platform's naming convention.
    When neither resolves, `physical_urn` is None and `unmapped_reason`
    explains why; the logical dataset and its assertions are unaffected.
    """
    bindings: List[PhysicalBinding] = []
    if not contract.schema_:
        return bindings

    overrides = config.physical_urn_overrides.get(contract.id)
    resolved, server_reason = _resolve_server(contract, config)

    for index, schema_entry in enumerate(contract.schema_):
        logical_urn = odcs_to_logical_dataset_urn(contract, schema_entry, config)

        if overrides is not None and schema_entry.name in overrides:
            override_urn = (overrides[schema_entry.name] or "").strip()
            bindings.append(
                PhysicalBinding(
                    index=index,
                    schema_entry=schema_entry,
                    logical_urn=logical_urn,
                    physical_urn=override_urn or None,
                    unmapped_reason=(
                        None
                        if override_urn
                        else "explicitly unbound by physical_urn_overrides"
                    ),
                )
            )
            continue

        table = schema_entry.physicalName or schema_entry.name
        if not table:
            bindings.append(
                PhysicalBinding(
                    index=index,
                    schema_entry=schema_entry,
                    logical_urn=logical_urn,
                    physical_urn=None,
                    unmapped_reason="schema entry has no physicalName or name",
                )
            )
            continue
        if resolved is None:
            bindings.append(
                PhysicalBinding(
                    index=index,
                    schema_entry=schema_entry,
                    logical_urn=logical_urn,
                    physical_urn=None,
                    unmapped_reason=server_reason,
                )
            )
            continue

        physical_name, compose_reason, passthrough = _compose_physical_name(
            platform=resolved.platform,
            server=resolved.server,
            table=table,
            convert_to_lowercase=config.lowercase_physical_urns,
        )
        if physical_name is None:
            bindings.append(
                PhysicalBinding(
                    index=index,
                    schema_entry=schema_entry,
                    logical_urn=logical_urn,
                    physical_urn=None,
                    unmapped_reason=compose_reason,
                )
            )
            continue

        physical_urn = make_dataset_urn_with_platform_instance(
            platform=resolved.platform,
            name=physical_name,
            platform_instance=(
                resolved.mapping.platform_instance
                if resolved.mapping is not None
                else None
            ),
            env=resolved.mapping.env if resolved.mapping is not None else config.env,
        )
        bindings.append(
            PhysicalBinding(
                index=index,
                schema_entry=schema_entry,
                logical_urn=logical_urn,
                physical_urn=physical_urn,
                name_passthrough=passthrough,
            )
        )
    return bindings


# ----------------------------------------------------------------------------
# Shared owner / tag / property helpers
# ----------------------------------------------------------------------------


def _make_tag_associations(
    tags: Optional[List[str]], tag_prefix: Optional[str]
) -> List[TagAssociationClass]:
    if not tags:
        return []
    out: List[TagAssociationClass] = []
    seen: set = set()
    for tag in tags:
        if not tag:
            continue
        full = f"{tag_prefix}{tag}" if tag_prefix else tag
        urn = make_tag_urn(full)
        if urn in seen:
            continue
        seen.add(urn)
        out.append(TagAssociationClass(tag=urn))
    return out


def _make_owners(
    contract: ODCSContract,
    strip_owner_email_domain: bool = False,
    owner_email_domain: Optional[str] = None,
) -> List[OwnerClass]:
    """Owners from `team`, with identifier normalization to match the org's
    identity source: ODCS usernames are "username or email", so
    `strip_owner_email_domain` maps emails to their local part and
    `owner_email_domain` maps bare usernames to emails. Explicit corpuser /
    corpGroup URNs pass through untouched."""
    owners: List[OwnerClass] = []
    seen: set = set()
    for member in contract.team_members:
        if member.dateOut:
            continue
        username = member.username or member.name
        if not username:
            continue
        role_key = (member.role or "").strip()
        ownership_type = _OWNER_ROLE_MAP.get(
            role_key, OwnershipTypeClass.TECHNICAL_OWNER
        )
        if username.startswith(("urn:li:corpuser:", "urn:li:corpGroup:")):
            owner_urn = username
        else:
            identifier = username
            if strip_owner_email_domain and "@" in identifier:
                identifier = identifier.split("@", 1)[0]
            elif owner_email_domain and "@" not in identifier:
                identifier = f"{identifier}@{owner_email_domain}"
            owner_urn = make_user_urn(identifier)
        key = (owner_urn, ownership_type)
        if key in seen:
            continue
        seen.add(key)
        owners.append(OwnerClass(owner=owner_urn, type=ownership_type))
    return owners


def unmapped_owner_roles(contract: ODCSContract) -> List[str]:
    """Distinct non-empty `team[].role` values that fall back to TECHNICAL_OWNER.

    An empty/absent role legitimately defaults; a *named* role the map does not
    know (e.g. `producer`, `consumer`, or a typo) is silently coerced, so the
    source can surface it for telemetry. Mirrors the eligibility rules in
    `_make_owners` (skip departed members and members with no identifier).
    """
    roles: Set[str] = set()
    for member in contract.team_members:
        if member.dateOut:
            continue
        if not (member.username or member.name):
            continue
        role_key = (member.role or "").strip()
        if role_key and role_key not in _OWNER_ROLE_MAP:
            roles.add(role_key)
    return sorted(roles)


def _walk_properties(
    properties: Optional[List[ODCSProperty]], parent_path: str = ""
) -> Iterable[Tuple[str, ODCSProperty]]:
    """Yield (field_path, property) pairs, recursing into nested properties."""
    if not properties:
        return
    for prop in properties:
        path = f"{parent_path}.{prop.name}" if parent_path else prop.name
        yield path, prop
        if prop.properties:
            yield from _walk_properties(prop.properties, path)


# ----------------------------------------------------------------------------
# Canonical schema metadata (the new piece)
# ----------------------------------------------------------------------------


def _map_field_type(prop: ODCSProperty) -> Tuple[SchemaFieldDataTypeClass, bool]:
    """Map an ODCS property's logical/physical type to a SchemaFieldDataType.

    Returns `(data_type, is_fallback)`. `is_fallback` is True when neither the
    logicalType nor the physicalType matched a known type and the field fell
    back to NullType, so the caller can report the gap.
    """
    for candidate in (prop.logicalType, prop.physicalType):
        if not candidate:
            continue
        type_cls = _LOGICAL_TYPE_MAP.get(candidate.strip().lower())
        if type_cls is not None:
            return SchemaFieldDataTypeClass(type=type_cls()), False
    return SchemaFieldDataTypeClass(type=NullTypeClass()), True


def _native_data_type(prop: ODCSProperty) -> str:
    """Non-null native type string for a SchemaField.

    Prefers the ODCS physicalType (closest to the warehouse type), then the
    logicalType, then a stable `unknown` sentinel (the field is required by the
    schema model and must not be null).
    """
    return prop.physicalType or prop.logicalType or "unknown"


def build_schema_metadata(
    schema_entry: ODCSSchemaObject,
    tag_prefix: Optional[str] = None,
) -> SchemaBuildResult:
    """Build canonical `schemaMetadata` from an ODCS `schema[].properties[]` tree.

    Field paths are dotted (`address.street`) to mirror the nested ODCS
    `properties[]` structure. Descriptions, tags, nullability (from `required`),
    and key membership (from `primaryKey`) are carried through. Returns no
    schema metadata when the entry declares no properties.
    """
    fields: List[SchemaFieldClass] = []
    unmapped_types: List[str] = []
    for field_path, prop in _walk_properties(schema_entry.properties):
        data_type, is_fallback = _map_field_type(prop)
        if is_fallback and (prop.logicalType or prop.physicalType):
            unmapped_types.append(
                f"{field_path}:{prop.logicalType or prop.physicalType}"
            )
        tag_assoc = _make_tag_associations(prop.tags, tag_prefix)
        fields.append(
            SchemaFieldClass(
                fieldPath=field_path,
                type=data_type,
                nativeDataType=_native_data_type(prop),
                description=_description_to_str(prop.description),
                nullable=not bool(prop.required),
                isPartOfKey=bool(prop.primaryKey),
                globalTags=GlobalTagsClass(tags=tag_assoc) if tag_assoc else None,
                recursive=False,
            )
        )
    if not fields:
        return SchemaBuildResult(schema_metadata=None, unmapped_types=unmapped_types)
    return SchemaBuildResult(
        schema_metadata=SchemaMetadataClass(
            schemaName=schema_entry.name,
            platform=make_data_platform_urn(ODCS_PLATFORM),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        ),
        unmapped_types=unmapped_types,
    )


# ----------------------------------------------------------------------------
# Logical dataset aspects
# ----------------------------------------------------------------------------


def _count_quality_rules(schema_entry: ODCSSchemaObject) -> int:
    count = len(schema_entry.quality or [])
    for _, prop in _walk_properties(schema_entry.properties):
        count += len(prop.quality or [])
    return count


def _institutional_memory_mcp(
    logical_urn: str, schema_entry: ODCSSchemaObject, contract: ODCSContract
) -> Optional[MetadataChangeProposalWrapper]:
    """Link the logical dataset to author-provided authoritativeDefinitions.

    Only author-declared URLs are emitted (deterministic, not tied to the
    ingesting host's filesystem). The source-file basename is carried in
    DatasetProperties.customProperties instead.
    """
    seen: set = set()
    elements: List[InstitutionalMemoryMetadataClass] = []
    defs = list(contract.authoritativeDefinitions or [])
    defs.extend(schema_entry.authoritativeDefinitions or [])
    for _, prop in _walk_properties(schema_entry.properties):
        defs.extend(prop.authoritativeDefinitions or [])
    for d in defs:
        if not d.url or d.url in seen:
            continue
        seen.add(d.url)
        elements.append(
            InstitutionalMemoryMetadataClass(
                url=d.url,
                description=d.type or "ODCS authoritative definition",
                # Fixed epoch (not wall-clock): the link content is stable across
                # runs, so a fresh timestamp each run would churn the aspect and
                # cause needless write amplification.
                createStamp=AuditStampClass(
                    time=0,
                    actor="urn:li:corpuser:datahub",
                ),
            )
        )
    if not elements:
        return None
    return MetadataChangeProposalWrapper(
        entityUrn=logical_urn,
        aspect=InstitutionalMemoryClass(elements=elements),
    )


def odcs_to_logical_dataset_mcps(
    contract: ODCSContract,
    schema_entry: ODCSSchemaObject,
    logical_urn: str,
    source_file: Optional[str] = None,
    tag_prefix: Optional[str] = None,
    replicate_contract_metadata: bool = True,
    strip_owner_email_domain: bool = False,
    owner_email_domain: Optional[str] = None,
) -> Tuple[List[MetadataChangeProposalWrapper], List[str]]:
    """Emit the aspects ODCS owns on the logical `odcs` dataset.

    Returns `(mcps, unmapped_types)` — the MCPs to emit and any field types that
    fell back to NullType (for SourceReport telemetry).
    """
    mcps: List[MetadataChangeProposalWrapper] = []

    # Per-table description wins; the contract description is the fallback so a
    # multi-table contract doesn't smear one description across every table.
    description = _description_to_str(schema_entry.description) or _description_to_str(
        contract.description
    )
    custom_props: Dict[str, str] = {
        "odcs.id": contract.id,
        "odcs.schemaName": schema_entry.name,
        "odcs.qualityRuleCount": str(_count_quality_rules(schema_entry)),
    }
    if contract.version:
        custom_props["odcs.version"] = contract.version
    if contract.apiVersion:
        custom_props["odcs.apiVersion"] = contract.apiVersion
    if contract.status:
        custom_props["odcs.status"] = contract.status
    if schema_entry.physicalName:
        custom_props["odcs.physicalName"] = schema_entry.physicalName
    if source_file:
        custom_props["odcs.sourceFile"] = source_file
    if contract.domain:
        custom_props["odcs.domain"] = contract.domain
    if contract.dataProduct:
        custom_props["odcs.dataProduct"] = contract.dataProduct
    if contract.tenant:
        custom_props["odcs.tenant"] = contract.tenant

    if contract.name and schema_entry.name:
        display_name: Optional[str] = f"{contract.name} — {schema_entry.name}"
    else:
        display_name = schema_entry.name or contract.name

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=logical_urn,
            aspect=DatasetPropertiesClass(
                description=description,
                customProperties=custom_props,
                name=display_name,
            ),
        )
    )

    schema_result = build_schema_metadata(schema_entry, tag_prefix)
    if schema_result.schema_metadata is not None:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=logical_urn,
                aspect=schema_result.schema_metadata,
            )
        )

    contract_tags: List[str] = []
    if replicate_contract_metadata and contract.tags:
        contract_tags.extend(contract.tags)
    if schema_entry.tags:
        contract_tags.extend(schema_entry.tags)
    tag_associations = _make_tag_associations(contract_tags, tag_prefix)
    if tag_associations:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=logical_urn,
                aspect=GlobalTagsClass(tags=tag_associations),
            )
        )

    if replicate_contract_metadata:
        owners = _make_owners(
            contract,
            strip_owner_email_domain=strip_owner_email_domain,
            owner_email_domain=owner_email_domain,
        )
        if owners:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=logical_urn,
                    aspect=OwnershipClass(owners=owners),
                )
            )

    inst_mem = _institutional_memory_mcp(logical_urn, schema_entry, contract)
    if inst_mem is not None:
        mcps.append(inst_mem)

    return mcps, schema_result.unmapped_types


def odcs_to_logical_parent_mcp(
    physical_urn: str, logical_urn: str
) -> MetadataChangeProposalWrapper:
    """Emit the `logicalParent` (PhysicalInstanceOf) link on the physical dataset.

    The aspect lives on the physical dataset and points at the logical ODCS
    dataset, so ODCS never becomes platform-of-record for the physical asset.
    """
    return MetadataChangeProposalWrapper(
        entityUrn=physical_urn,
        aspect=LogicalParentClass(parent=EdgeClass(destinationUrn=logical_urn)),
    )


# ----------------------------------------------------------------------------
# Assertions — emitted against the LOGICAL dataset
#
# ODCS quality rules are the producer's published expectations for the dataset
# the contract describes, so the Assertion entities target the logical `odcs`
# dataset (the assertion's `entity` is the logical URN). Propagation of those
# expectations onto bound physical datasets is handled by a separate DataHub
# mechanism via the PhysicalInstanceOf relationship — this source never writes
# assertions against physical URNs.
# ----------------------------------------------------------------------------


@dataclass
class _RuleContext:
    """Everything a builder needs to emit one rule's assertion."""

    rule: ODCSQualityRule
    column: Optional[str]
    scope: str
    entity_urn: str  # the logical `odcs` dataset URN
    contract_id: str
    index: int

    @property
    def label(self) -> str:
        return self.rule.name or self.rule.id or f"<unnamed:{self.scope}:{self.index}>"


def _num(v: object) -> AssertionStdParameterClass:
    """Normalize a numeric threshold to a stable string form.

    Both `5` (int) and `5.0` (float) render as `"5"`; `5.5` renders as `"5.5"`.
    """
    return AssertionStdParameterClass(
        value=f"{float(v):g}",  # type: ignore[arg-type]
        type=AssertionStdParameterTypeClass.NUMBER,
    )


def _is_plain_number(value: object) -> TypeGuard[float]:
    """True for int/float but NOT bool (bool is an int subclass in Python)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _must_be_zero(rule: ODCSQualityRule) -> bool:
    """True when the rule's threshold is exactly `mustBe: 0`."""
    return _is_plain_number(rule.mustBe) and float(rule.mustBe) == 0.0


_THRESHOLD_FIELD_NAMES = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
)


def _has_any_threshold(rule: ODCSQualityRule) -> bool:
    return any(getattr(rule, name) is not None for name in _THRESHOLD_FIELD_NAMES)


def _operator_and_params_from_threshold(
    rule: ODCSQualityRule,
) -> Tuple[Optional[str], Optional[AssertionStdParametersClass]]:
    """Translate an ODCS `mustBe*` threshold into a DataHub operator + parameters.

    Returns `(None, None)` when no native operator applies — either because no
    threshold is provided or because it is an unmappable one such as
    `mustNotBeBetween`.
    """
    if rule.mustBeBetween and len(rule.mustBeBetween) == 2:
        return AssertionStdOperatorClass.BETWEEN, AssertionStdParametersClass(
            minValue=_num(rule.mustBeBetween[0]),
            maxValue=_num(rule.mustBeBetween[1]),
        )
    if rule.mustNotBeBetween and len(rule.mustNotBeBetween) == 2:
        return None, None
    if rule.mustBeGreaterThan is not None:
        return AssertionStdOperatorClass.GREATER_THAN, AssertionStdParametersClass(
            value=_num(rule.mustBeGreaterThan)
        )
    if rule.mustBeGreaterOrEqualTo is not None:
        return (
            AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            AssertionStdParametersClass(value=_num(rule.mustBeGreaterOrEqualTo)),
        )
    if rule.mustBeLessThan is not None:
        return AssertionStdOperatorClass.LESS_THAN, AssertionStdParametersClass(
            value=_num(rule.mustBeLessThan)
        )
    if rule.mustBeLessOrEqualTo is not None:
        return (
            AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            AssertionStdParametersClass(value=_num(rule.mustBeLessOrEqualTo)),
        )
    if _is_plain_number(rule.mustBe):
        return AssertionStdOperatorClass.EQUAL_TO, AssertionStdParametersClass(
            value=_num(rule.mustBe)
        )
    if _is_plain_number(rule.mustNotBe):
        return AssertionStdOperatorClass.NOT_EQUAL_TO, AssertionStdParametersClass(
            value=_num(rule.mustNotBe)
        )
    return None, None


def _fail_threshold_from_rule(
    rule: ODCSQualityRule, unit: str
) -> Optional[FieldValuesFailThresholdClass]:
    """Map an ODCS tolerance onto a FieldValues failThreshold, exactly or not at all.

    `failThreshold.value` is a long, so only exactly-representable tolerances
    map: no threshold at all (the v3.0 `validValues` form) and `mustBe: 0`
    both mean "no failing rows tolerated"; an integral `mustBeLessOrEqualTo`
    maps to COUNT (rows) or PERCENTAGE (percent). Anything else — strict
    less-than, non-integral values, other operators — returns None and the
    rule is preserved as a custom assertion instead of being approximated.
    """
    if not _has_any_threshold(rule) or _must_be_zero(rule):
        return FieldValuesFailThresholdClass(
            type=FieldValuesFailThresholdTypeClass.COUNT, value=0
        )
    tolerance = rule.mustBeLessOrEqualTo
    if (
        tolerance is not None
        and float(tolerance).is_integer()
        and not any(
            getattr(rule, name) is not None
            for name in _THRESHOLD_FIELD_NAMES
            if name != "mustBeLessOrEqualTo"
        )
    ):
        threshold_type = (
            FieldValuesFailThresholdTypeClass.PERCENTAGE
            if unit == _UNIT_PERCENT
            else FieldValuesFailThresholdTypeClass.COUNT
        )
        return FieldValuesFailThresholdClass(type=threshold_type, value=int(tolerance))
    return None


def _stable_assertion_urn(ctx: _RuleContext, rule_kind: str) -> str:
    """Stable assertion URN; the spec's `quality.id` wins when present.

    v3.1's `quality.id` is documented as "a unique identifier ... used to
    create stable, refactor-safe references", so it survives rule renames and
    reordering. The contract id is included so two contracts reusing the same
    rule id do not collide.
    """
    if ctx.rule.id:
        guid_dict = {
            "contract": ctx.contract_id,
            "odcs_rule_id": ctx.rule.id,
            "dataset": ctx.entity_urn,
        }
    else:
        guid_dict = {
            "contract": ctx.contract_id,
            "dataset": ctx.entity_urn,
            "rule_name": ctx.rule.name or f"{rule_kind}_{ctx.index}",
            "rule_kind": rule_kind,
            "field": ctx.column or "",
        }
    return make_assertion_urn(datahub_guid(guid_dict))


def _render_thresholds(rule: ODCSQualityRule) -> List[str]:
    parts: List[str] = []
    for name in _THRESHOLD_FIELD_NAMES:
        value = getattr(rule, name)
        if value is not None:
            parts.append(f"{name} {json.dumps(value)}")
    return parts


def _library_rule_logic(rule: ODCSQualityRule) -> Optional[str]:
    """Stable, human-readable rendition of a library rule for custom `logic`.

    Used when a library metric cannot be represented natively (e.g. a
    duplicate tolerance, a percent-unit rowCount, missingValues) so the
    original intent round-trips into the custom assertion body.
    """
    metric = rule.effective_metric
    if not metric:
        return None
    parts = [metric]
    if rule.arguments:
        try:
            parts.append(f"arguments={json.dumps(rule.arguments, sort_keys=True)}")
        except (TypeError, ValueError):
            pass
    if rule.validValues is not None:
        try:
            parts.append(f"validValues={json.dumps(rule.validValues)}")
        except (TypeError, ValueError):
            pass
    parts.extend(_render_thresholds(rule))
    if rule.unit:
        parts.append(f"unit {rule.unit}")
    return " ".join(parts)


def _custom_logic_for_rule(rule: ODCSQualityRule, scope: str) -> Optional[str]:
    """Resolve the `logic` body for a CustomAssertionInfo.

    Order: query > serialized implementation > library-rule rendition >
    description > "name:scope".
    """
    if rule.query:
        return rule.query
    if rule.implementation is not None:
        if isinstance(rule.implementation, str):
            stripped = rule.implementation.strip()
            if stripped:
                return stripped
        else:
            try:
                return json.dumps(rule.implementation, sort_keys=True)
            except (TypeError, ValueError):
                pass
    library_logic = _library_rule_logic(rule)
    if library_logic:
        return library_logic
    desc = _description_to_str(rule.description)
    if desc:
        return desc
    if rule.name:
        return f"{rule.name}:{scope}"
    return None


def _custom_props_for_rule(ctx: _RuleContext) -> Dict[str, str]:
    """Per-assertion customProperties, carrying ODCS provenance.

    `odcs.scope` is one of "schema" or "property" — where in the contract the
    rule came from. `odcs.id` is the contract id so an assertion can always be
    traced back to its source contract.
    """
    rule = ctx.rule
    props: Dict[str, str] = {
        "odcs.rule": "true",
        "odcs.scope": ctx.scope,
        "odcs.id": ctx.contract_id,
    }
    if rule.id:
        props["odcs.rule.id"] = rule.id
    if rule.name:
        props["odcs.rule.name"] = rule.name
    metric = rule.effective_metric
    if metric:
        props["odcs.rule.metric"] = metric
    if rule.unit:
        props["odcs.rule.unit"] = rule.unit
    if rule.arguments:
        try:
            props["odcs.rule.arguments"] = json.dumps(rule.arguments, sort_keys=True)
        except (TypeError, ValueError):
            pass
    if rule.dimension:
        props["odcs.rule.dimension"] = rule.dimension
    if rule.severity:
        props["odcs.rule.severity"] = rule.severity
    if rule.businessImpact:
        props["odcs.rule.businessImpact"] = rule.businessImpact
    if rule.type:
        props["odcs.rule.type"] = rule.type
    return props


def _rule_external_url(rule: ODCSQualityRule) -> Optional[str]:
    for definition in rule.authoritativeDefinitions or []:
        if definition.url:
            return definition.url
    return None


def _assertion_info_template(
    ctx: _RuleContext, assertion_type: str
) -> AssertionInfoClass:
    """Shared header for an AssertionInfoClass; caller sets the sub-aspect."""
    return AssertionInfoClass(
        type=assertion_type,
        source=mce_builder.make_assertion_source(),
        description=_description_to_str(ctx.rule.description),
        customProperties=_custom_props_for_rule(ctx),
        externalUrl=_rule_external_url(ctx.rule),
    )


def assertion_platform_instance_mcp(
    assertion_urn: str,
) -> MetadataChangeProposalWrapper:
    """Attach the `odcs` platform to an assertion entity.

    AssertionInfo's contract: an EXTERNAL-source assertion is expected to have
    a corresponding dataPlatformInstance aspect naming the platform it was
    ingested from (same pattern as the Snowflake DMF assertions).
    """
    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=DataPlatformInstanceClass(
            platform=make_data_platform_urn(ODCS_PLATFORM)
        ),
    )


def _build_field_values_assertion(
    ctx: _RuleContext,
    operator: str,
    parameters: Optional[AssertionStdParametersClass] = None,
    fail_threshold: Optional[FieldValuesFailThresholdClass] = None,
    exclude_nulls: bool = True,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(ctx, rule_kind="field_values")
    assert ctx.column is not None
    field_spec = SchemaFieldSpecClass(path=ctx.column, type="", nativeType="")
    field_values = FieldValuesAssertionClass(
        field=field_spec,
        operator=operator,
        parameters=parameters,
        failThreshold=fail_threshold
        or FieldValuesFailThresholdClass(
            type=FieldValuesFailThresholdTypeClass.COUNT, value=0
        ),
        excludeNulls=exclude_nulls,
    )
    info = _assertion_info_template(ctx, AssertionTypeClass.FIELD)
    info.fieldAssertion = FieldAssertionInfoClass(
        type=FieldAssertionTypeClass.FIELD_VALUES,
        entity=ctx.entity_urn,
        fieldValuesAssertion=field_values,
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_field_metric_assertion(
    ctx: _RuleContext,
    metric: str,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(ctx, rule_kind="field_metric")
    assert ctx.column is not None
    field_spec = SchemaFieldSpecClass(path=ctx.column, type="", nativeType="")
    info = _assertion_info_template(ctx, AssertionTypeClass.FIELD)
    info.fieldAssertion = FieldAssertionInfoClass(
        type=FieldAssertionTypeClass.FIELD_METRIC,
        entity=ctx.entity_urn,
        fieldMetricAssertion=FieldMetricAssertionClass(
            field=field_spec,
            metric=metric,
            operator=operator,
            parameters=params,
        ),
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_volume_assertion(
    ctx: _RuleContext,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(ctx, rule_kind="volume")
    info = _assertion_info_template(ctx, AssertionTypeClass.VOLUME)
    info.volumeAssertion = VolumeAssertionInfoClass(
        type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
        entity=ctx.entity_urn,
        rowCountTotal=RowCountTotalClass(operator=operator, parameters=params),
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_sql_assertion(
    ctx: _RuleContext,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(ctx, rule_kind="sql")
    info = _assertion_info_template(ctx, AssertionTypeClass.SQL)
    info.sqlAssertion = SqlAssertionInfoClass(
        type=SqlAssertionTypeClass.METRIC,
        entity=ctx.entity_urn,
        statement=ctx.rule.query or "",
        operator=operator,
        parameters=params,
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_custom_assertion(
    ctx: _RuleContext,
    custom_type: Optional[str] = None,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    """Build a CustomAssertionInfo for a rule, or None to signal a skip.

    A custom assertion must have a non-None `logic` body. If the rule has no
    query / implementation / library rendition / description / name, the
    caller skips with a warning rather than emit a content-less assertion.
    """
    logic = _custom_logic_for_rule(ctx.rule, ctx.scope)
    if logic is None:
        return None
    assertion_urn = _stable_assertion_urn(ctx, rule_kind="custom")
    resolved_type = custom_type or ctx.rule.effective_metric or ctx.rule.type or "odcs"
    field_urn = (
        mce_builder.make_schema_field_urn(ctx.entity_urn, ctx.column)
        if ctx.column
        else None
    )
    info = _assertion_info_template(ctx, AssertionTypeClass.CUSTOM)
    info.customAssertion = CustomAssertionInfoClass(
        type=resolved_type,
        entity=ctx.entity_urn,
        field=field_urn,
        logic=logic,
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _iter_schema_rules(
    schema_entry: ODCSSchemaObject,
) -> Iterable[Tuple[ODCSQualityRule, Optional[str], str]]:
    """Yield (rule, column, scope) for rules attached to one schema entry.

    Per spec, quality rules live at the schema-object level (scope="schema",
    no column) or the property level (scope="property", column = the dotted
    field path).
    """
    for rule in schema_entry.quality or []:
        yield rule, None, "schema"
    for field_path, prop in _walk_properties(schema_entry.properties):
        for rule in prop.quality or []:
            yield rule, field_path, "property"


@dataclass
class AssertionRoutingTrace:
    """Per-rule routing decisions for SourceReport telemetry."""

    skipped_no_body: List[str] = field(default_factory=list)
    routed_to_custom: List[str] = field(default_factory=list)
    deprecated_rule_key: List[str] = field(default_factory=list)


def _custom_or_skip(
    ctx: _RuleContext,
    trace: AssertionRoutingTrace,
    custom_type: Optional[str] = None,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    built = _build_custom_assertion(ctx, custom_type=custom_type)
    if built is None:
        trace.skipped_no_body.append(ctx.label)
        return None
    trace.routed_to_custom.append(ctx.label)
    return built


def _route_library_rule(
    ctx: _RuleContext,
    trace: AssertionRoutingTrace,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    rule = ctx.rule
    metric = rule.effective_metric
    unit = (rule.unit or _UNIT_ROWS).strip().lower()
    args = rule.arguments or {}

    if metric == _METRIC_NULL_VALUES:
        if ctx.column is None:
            return _custom_or_skip(ctx, trace)
        if _must_be_zero(rule):
            # "no null values at all" — a per-row NOT_NULL check with zero
            # tolerated failures. excludeNulls must be False or the check
            # would never see the very rows it is asserting about.
            return _build_field_values_assertion(
                ctx,
                operator=AssertionStdOperatorClass.NOT_NULL,
                fail_threshold=FieldValuesFailThresholdClass(
                    type=FieldValuesFailThresholdTypeClass.COUNT, value=0
                ),
                exclude_nulls=False,
            )
        op_opt, params_opt = _operator_and_params_from_threshold(rule)
        if op_opt is None or params_opt is None:
            return _custom_or_skip(ctx, trace)
        metric_type = (
            FieldMetricTypeClass.NULL_PERCENTAGE
            if unit == _UNIT_PERCENT
            else FieldMetricTypeClass.NULL_COUNT
        )
        return _build_field_metric_assertion(
            ctx, metric=metric_type, operator=op_opt, params=params_opt
        )

    if metric in (_METRIC_DUPLICATE_VALUES, _METRIC_DUPLICATE_COUNT):
        if args.get("properties"):
            # Multi-column uniqueness (schema-level duplicateValues) has no
            # native DataHub metric; preserve as custom.
            return _custom_or_skip(ctx, trace)
        if ctx.column is not None and _must_be_zero(rule):
            # Zero duplicates <=> the column is fully unique.
            return _build_field_metric_assertion(
                ctx,
                metric=FieldMetricTypeClass.UNIQUE_PERCENTAGE,
                operator=AssertionStdOperatorClass.EQUAL_TO,
                params=AssertionStdParametersClass(value=_num(100)),
            )
        # "at most N duplicates" has no native duplicate-count metric, and
        # inverting it into a unique-percentage bound would change semantics.
        return _custom_or_skip(ctx, trace)

    if metric in (_METRIC_INVALID_VALUES, _METRIC_VALID_VALUES):
        valid_values = args.get("validValues") or rule.validValues
        pattern = args.get("pattern")
        if ctx.column is None or (valid_values and pattern):
            return _custom_or_skip(ctx, trace)
        fail_threshold = _fail_threshold_from_rule(rule, unit)
        if fail_threshold is None:
            return _custom_or_skip(ctx, trace)
        if valid_values:
            try:
                serialized = json.dumps(valid_values)
            except (TypeError, ValueError):
                return _custom_or_skip(ctx, trace)
            return _build_field_values_assertion(
                ctx,
                operator=AssertionStdOperatorClass.IN,
                parameters=AssertionStdParametersClass(
                    value=AssertionStdParameterClass(
                        value=serialized,
                        type=AssertionStdParameterTypeClass.SET,
                    )
                ),
                fail_threshold=fail_threshold,
            )
        if pattern and isinstance(pattern, str):
            return _build_field_values_assertion(
                ctx,
                operator=AssertionStdOperatorClass.REGEX_MATCH,
                parameters=AssertionStdParametersClass(
                    value=AssertionStdParameterClass(
                        value=pattern,
                        type=AssertionStdParameterTypeClass.STRING,
                    )
                ),
                fail_threshold=fail_threshold,
            )
        return _custom_or_skip(ctx, trace)

    if metric == _METRIC_MISSING_VALUES:
        # "value is in an author-defined missing list (possibly including
        # null)" has no native metric; NOT_IN plus null handling would change
        # semantics, so the rule is preserved as custom.
        return _custom_or_skip(ctx, trace)

    if metric == _METRIC_ROW_COUNT:
        if unit == _UNIT_PERCENT:
            # A relative row count only makes sense against some baseline the
            # contract does not define; preserve as custom.
            return _custom_or_skip(ctx, trace)
        op_opt, params_opt = _operator_and_params_from_threshold(rule)
        if op_opt is None or params_opt is None:
            return _custom_or_skip(ctx, trace)
        return _build_volume_assertion(ctx, operator=op_opt, params=params_opt)

    # Unknown library metric (ODCS does not constrain engines from extending).
    return _custom_or_skip(ctx, trace)


def _route_and_build(
    ctx: _RuleContext,
    trace: AssertionRoutingTrace,
    api_version: str,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    """Route a single rule to the appropriate assertion builder.

    Returns None when the rule produces no assertion. The trace is updated
    in-place so the source can report aggregate counters.
    """
    rule = ctx.rule
    rule_type = (rule.type or _RULE_TYPE_LIBRARY).strip().lower()

    if rule_type == _RULE_TYPE_SQL:
        if rule.query:
            op_opt, params_opt = _operator_and_params_from_threshold(rule)
            if op_opt is not None and params_opt is not None:
                return _build_sql_assertion(ctx, operator=op_opt, params=params_opt)
        return _custom_or_skip(ctx, trace)

    if rule_type == _RULE_TYPE_CUSTOM:
        return _custom_or_skip(ctx, trace, custom_type=rule.engine)

    if rule_type == _RULE_TYPE_TEXT:
        return _custom_or_skip(ctx, trace, custom_type=_RULE_TYPE_TEXT)

    # library (the default)
    if rule.used_deprecated_rule_key and api_version.startswith("3.1"):
        trace.deprecated_rule_key.append(ctx.label)
    return _route_library_rule(ctx, trace)


def odcs_to_assertion_mcps(
    contract: ODCSContract,
    schema_entry: ODCSSchemaObject,
    logical_urn: str,
) -> Tuple[List[str], List[MetadataChangeProposalWrapper], AssertionRoutingTrace]:
    """Emit Assertion entities for every rule on one schema entry.

    Assertions target the logical dataset and are emitted whether or not a
    physical binding resolved. Each assertion also gets a dataPlatformInstance
    aspect attributing it to the `odcs` platform. Returns the assertion URNs,
    the MCPs, and a routing trace.
    """
    assertion_urns: List[str] = []
    mcps: List[MetadataChangeProposalWrapper] = []
    trace = AssertionRoutingTrace()
    api_version = (contract.apiVersion or "").lstrip("v")

    for index, (rule, column, scope) in enumerate(_iter_schema_rules(schema_entry)):
        ctx = _RuleContext(
            rule=rule,
            column=column,
            scope=scope,
            entity_urn=logical_urn,
            contract_id=contract.id,
            index=index,
        )
        built = _route_and_build(ctx, trace, api_version)
        if built is None:
            continue
        assertion_urn, mcp = built
        assertion_urns.append(assertion_urn)
        mcps.append(mcp)
        mcps.append(assertion_platform_instance_mcp(assertion_urn))

    return assertion_urns, mcps, trace


_SCHEMA_ASSERTION_COMPATIBILITY = {
    "EXACT_MATCH": SchemaAssertionCompatibilityClass.EXACT_MATCH,
    "SUPERSET": SchemaAssertionCompatibilityClass.SUPERSET,
    "SUBSET": SchemaAssertionCompatibilityClass.SUBSET,
}


def odcs_to_schema_assertion_mcps(
    contract: ODCSContract,
    schema_entry: ODCSSchemaObject,
    logical_urn: str,
    compatibility: str,
) -> Tuple[Optional[str], List[MetadataChangeProposalWrapper]]:
    """Emit one DATA_SCHEMA assertion pinning the contract's schema.

    The assertion carries the ODCS-derived SchemaMetadata and targets the
    logical dataset: "an instance of this model must present (at least) this
    schema". This is what makes schema drift an evaluable contract violation
    rather than an implied one. Returns (None, []) when the entry declares no
    properties — there is no schema to assert.
    """
    schema_result = build_schema_metadata(schema_entry)
    if schema_result.schema_metadata is None:
        return None, []
    assertion_urn = make_assertion_urn(
        datahub_guid(
            {
                "contract": contract.id,
                "dataset": logical_urn,
                "kind": "schema_compliance",
                "schema": schema_entry.name,
            }
        )
    )
    info = AssertionInfoClass(
        type=AssertionTypeClass.DATA_SCHEMA,
        source=mce_builder.make_assertion_source(),
        description=(
            f"Schema compliance with ODCS contract '{contract.id}' "
            f"schema '{schema_entry.name}'"
        ),
        customProperties={
            "odcs.id": contract.id,
            "odcs.scope": "schema",
            "odcs.assertion": "schema-compliance",
        },
        schemaAssertion=SchemaAssertionInfoClass(
            entity=logical_urn,
            schema=schema_result.schema_metadata,
            compatibility=_SCHEMA_ASSERTION_COMPATIBILITY[compatibility],
        ),
    )
    return assertion_urn, [
        MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info),
        assertion_platform_instance_mcp(assertion_urn),
    ]
