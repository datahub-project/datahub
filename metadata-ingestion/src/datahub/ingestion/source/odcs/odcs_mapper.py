import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple, Type

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_group_urn,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.odcs.odcs_config import (
    ODCS_PLATFORM,
    ODCSSourceConfig,
    ServerMapping,
)
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
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

# Rule-kind vocabularies recognized by `_route_and_build`. Named here so the
# membership checks read intentionally and the accepted spellings (camelCase
# and snake_case, both of which appear in real ODCS documents) live in one
# place. notNull -> a NOT_NULL FieldValuesAssertion; unique -> a UNIQUE_PERCENTAGE
# FieldMetricAssertion (uniqueness is a column metric, not a per-row value check).
_NOT_NULL_RULE_KINDS = frozenset(("notNull", "not_null"))
_UNIQUE_RULE_KINDS = frozenset(("unique",))
_VOLUME_RULE_KINDS = frozenset(("rowCount", "row_count"))

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
class UnmappedSchema:
    """A schema[] entry that could not be bound to a physical Dataset URN.

    Unlike the prior model, an unmapped schema entry is NOT skipped — the
    logical ODCS dataset is still emitted. This record only signals that no
    physical binding (and therefore no `logicalParent` link or assertions) was
    produced for the entry.
    """

    index: int
    schema_entry: ODCSSchemaObject
    reason: str


@dataclass
class PhysicalBinding:
    """Resolution result for one `schema[]` entry's physical dataset."""

    index: int
    schema_entry: ODCSSchemaObject
    logical_urn: str
    physical_urn: Optional[str]
    unmapped_reason: Optional[str] = None


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


def _select_server_mapping(
    contract: ODCSContract, mappings: List[ServerMapping]
) -> Optional[ServerMapping]:
    if not contract.servers:
        return next((m for m in mappings if m.match_any), None)
    for server in contract.servers:
        for mapping in mappings:
            if mapping.match_any:
                continue
            if mapping.server == server.server:
                return mapping
    return next((m for m in mappings if m.match_any), None)


def odcs_to_physical_bindings(
    contract: ODCSContract, config: ODCSSourceConfig
) -> List[PhysicalBinding]:
    """Resolve a PhysicalBinding per `schema[]` entry.

    Every entry yields a logical ODCS dataset URN. The physical URN is resolved
    via, in priority order:
      1. `physical_urn_overrides[contract.id][index]` (empty string = unbound).
      2. `servers_to_platform` + the entry's `physicalName` (or `name`).
    When neither resolves, `physical_urn` is None and `unmapped_reason` explains
    why; the logical dataset is still emitted.
    """
    bindings: List[PhysicalBinding] = []
    if not contract.schema_:
        return bindings

    overrides = config.physical_urn_overrides.get(contract.id)
    mapping = _select_server_mapping(contract, config.servers_to_platform)

    for index, schema_entry in enumerate(contract.schema_):
        logical_urn = odcs_to_logical_dataset_urn(contract, schema_entry, config)

        # When an override list is supplied for this contract, it is
        # authoritative: it must have one entry per schema[] entry. An empty
        # string — or an index past the end of a too-short list — means the
        # author deliberately left this entry unbound, NOT "fall back to
        # servers_to_platform" (that would silently re-bind entries the author
        # excluded).
        if overrides is not None:
            override = (overrides[index] if index < len(overrides) else "") or None
            if override:
                bindings.append(
                    PhysicalBinding(
                        index=index,
                        schema_entry=schema_entry,
                        logical_urn=logical_urn,
                        physical_urn=override,
                    )
                )
            else:
                bindings.append(
                    PhysicalBinding(
                        index=index,
                        schema_entry=schema_entry,
                        logical_urn=logical_urn,
                        physical_urn=None,
                        unmapped_reason=(
                            "physical_urn_overrides entry is empty or the override "
                            "list is shorter than schema[]; left unbound by config"
                        ),
                    )
                )
            continue

        physical_name = schema_entry.physicalName or schema_entry.name
        if not physical_name:
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
        if mapping is None:
            bindings.append(
                PhysicalBinding(
                    index=index,
                    schema_entry=schema_entry,
                    logical_urn=logical_urn,
                    physical_urn=None,
                    unmapped_reason=(
                        "no `physical_urn_overrides` entry and no matching "
                        "`servers_to_platform` mapping for the contract's servers"
                    ),
                )
            )
            continue

        physical_urn = make_dataset_urn_with_platform_instance(
            platform=mapping.platform,
            name=physical_name,
            platform_instance=mapping.platform_instance,
            env=mapping.env,
        )
        bindings.append(
            PhysicalBinding(
                index=index,
                schema_entry=schema_entry,
                logical_urn=logical_urn,
                physical_urn=physical_urn,
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


def _make_owners(contract: ODCSContract) -> List[OwnerClass]:
    if not contract.team:
        return []
    owners: List[OwnerClass] = []
    seen: set = set()
    for member in contract.team:
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
        elif username.startswith("group:"):
            # `group:` is a DataHub-side extension — ODCS has no group principal
            # (team[].username is "username or email"). Lets a contract author
            # target a corpGroup rather than a corpUser.
            owner_urn = make_group_urn(username[len("group:") :])
        else:
            owner_urn = make_user_urn(username)
        key = (owner_urn, ownership_type)
        if key in seen:
            continue
        seen.add(key)
        owners.append(OwnerClass(owner=owner_urn, type=ownership_type))
    return owners


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


def _count_quality_rules(schema_entry: ODCSSchemaObject, contract: ODCSContract) -> int:
    count = len(schema_entry.quality or [])
    for _, prop in _walk_properties(schema_entry.properties):
        count += len(prop.quality or [])
    count += len(contract.quality or [])
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
    defs = list(schema_entry.authoritativeDefinitions or [])
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
) -> Tuple[List[MetadataChangeProposalWrapper], List[str]]:
    """Emit the aspects ODCS owns on the logical `odcs` dataset.

    Returns `(mcps, unmapped_types)` — the MCPs to emit and any field types that
    fell back to NullType (for SourceReport telemetry).
    """
    mcps: List[MetadataChangeProposalWrapper] = []

    description = _description_to_str(contract.description)
    custom_props: Dict[str, str] = {
        "odcs.id": contract.id,
        "odcs.schemaName": schema_entry.name,
        "odcs.qualityRuleCount": str(_count_quality_rules(schema_entry, contract)),
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
        owners = _make_owners(contract)
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
# Assertions (target-agnostic builders, carried over from the prior model)
# ----------------------------------------------------------------------------


def _num(v: object) -> AssertionStdParameterClass:
    """Normalize a numeric threshold to a stable string form.

    Both `5` (int) and `5.0` (float) render as `"5"`; `5.5` renders as `"5.5"`.
    """
    return AssertionStdParameterClass(
        value=f"{float(v):g}",  # type: ignore[arg-type]
        type=AssertionStdParameterTypeClass.NUMBER,
    )


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
    if rule.mustBe is not None and isinstance(rule.mustBe, (int, float)):
        return AssertionStdOperatorClass.EQUAL_TO, AssertionStdParametersClass(
            value=_num(rule.mustBe)
        )
    if rule.mustNotBe is not None and isinstance(rule.mustNotBe, (int, float)):
        return AssertionStdOperatorClass.NOT_EQUAL_TO, AssertionStdParametersClass(
            value=_num(rule.mustNotBe)
        )
    return None, None


def _stable_assertion_urn(
    logical_urn: str,
    dataset_urn: str,
    rule_name: Optional[str],
    rule_kind: str,
    column: Optional[str],
    fallback_index: int,
) -> str:
    guid_dict = {
        "logical": logical_urn,
        "dataset": dataset_urn,
        "rule_name": rule_name or f"{rule_kind}_{fallback_index}",
        "rule_kind": rule_kind,
        "field": column or "",
    }
    return make_assertion_urn(datahub_guid(guid_dict))


def _custom_logic_for_rule(rule: ODCSQualityRule, scope: str) -> Optional[str]:
    """Resolve the `logic` body for a CustomAssertionInfo.

    Order: query > serialized implementation > description > "name:scope".
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
    desc = _description_to_str(rule.description)
    if desc:
        return desc
    if rule.name:
        return f"{rule.name}:{scope}"
    return None


def _custom_props_for_rule(rule: ODCSQualityRule, scope: str) -> Dict[str, str]:
    """Per-assertion customProperties, including the contract scope tag.

    `scope` is one of "contract", "schema", or "property" — emitted as
    `odcs.scope` so downstream consumers can distinguish where in the contract
    a rule came from.
    """
    props: Dict[str, str] = {"odcs.rule": "true", "odcs.scope": scope}
    if rule.name:
        props["odcs.rule.name"] = rule.name
    if rule.dimension:
        props["odcs.rule.dimension"] = rule.dimension
    if rule.severity:
        props["odcs.rule.severity"] = rule.severity
    if rule.businessImpact:
        props["odcs.rule.businessImpact"] = rule.businessImpact
    if rule.type:
        props["odcs.rule.type"] = rule.type
    if rule.rule:
        props["odcs.rule.library"] = rule.rule
    return props


def _assertion_info_template(
    rule: ODCSQualityRule,
    scope: str,
    assertion_type: str,
) -> AssertionInfoClass:
    """Shared header for an AssertionInfoClass; caller sets the sub-aspect."""
    return AssertionInfoClass(
        type=assertion_type,
        source=mce_builder.make_assertion_source(),
        description=_description_to_str(rule.description),
        customProperties=_custom_props_for_rule(rule, scope),
    )


def _build_field_values_assertion(
    rule: ODCSQualityRule,
    operator: str,
    dataset_urn: str,
    column: str,
    logical_urn: str,
    scope: str,
    fallback_index: int,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(
        logical_urn=logical_urn,
        dataset_urn=dataset_urn,
        rule_name=rule.name,
        rule_kind="field_values",
        column=column,
        fallback_index=fallback_index,
    )
    field_spec = SchemaFieldSpecClass(path=column, type="", nativeType="")
    field_values = FieldValuesAssertionClass(
        field=field_spec,
        operator=operator,
        failThreshold=FieldValuesFailThresholdClass(
            type=FieldValuesFailThresholdTypeClass.COUNT,
            value=0,
        ),
        excludeNulls=operator != AssertionStdOperatorClass.NOT_NULL,
    )
    info = _assertion_info_template(rule, scope, AssertionTypeClass.FIELD)
    info.fieldAssertion = FieldAssertionInfoClass(
        type=FieldAssertionTypeClass.FIELD_VALUES,
        entity=dataset_urn,
        fieldValuesAssertion=field_values,
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_field_metric_assertion(
    rule: ODCSQualityRule,
    metric: str,
    operator: str,
    value: float,
    dataset_urn: str,
    column: str,
    logical_urn: str,
    scope: str,
    fallback_index: int,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    """Build a FieldMetricAssertion (e.g. uniqueness) against a column.

    Used for ODCS `unique` rules: uniqueness is a column-level metric
    (UNIQUE_PERCENTAGE == 100), not a per-row value comparison, so it cannot be
    modeled as a FieldValuesAssertion.
    """
    assertion_urn = _stable_assertion_urn(
        logical_urn=logical_urn,
        dataset_urn=dataset_urn,
        rule_name=rule.name,
        rule_kind="field_metric",
        column=column,
        fallback_index=fallback_index,
    )
    field_spec = SchemaFieldSpecClass(path=column, type="", nativeType="")
    info = _assertion_info_template(rule, scope, AssertionTypeClass.FIELD)
    info.fieldAssertion = FieldAssertionInfoClass(
        type=FieldAssertionTypeClass.FIELD_METRIC,
        entity=dataset_urn,
        fieldMetricAssertion=FieldMetricAssertionClass(
            field=field_spec,
            metric=metric,
            operator=operator,
            parameters=AssertionStdParametersClass(value=_num(value)),
        ),
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_volume_assertion(
    rule: ODCSQualityRule,
    dataset_urn: str,
    logical_urn: str,
    scope: str,
    fallback_index: int,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(
        logical_urn=logical_urn,
        dataset_urn=dataset_urn,
        rule_name=rule.name,
        rule_kind="volume",
        column=None,
        fallback_index=fallback_index,
    )
    info = _assertion_info_template(rule, scope, AssertionTypeClass.VOLUME)
    info.volumeAssertion = VolumeAssertionInfoClass(
        type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
        entity=dataset_urn,
        rowCountTotal=RowCountTotalClass(operator=operator, parameters=params),
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_sql_assertion(
    rule: ODCSQualityRule,
    dataset_urn: str,
    logical_urn: str,
    scope: str,
    fallback_index: int,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(
        logical_urn=logical_urn,
        dataset_urn=dataset_urn,
        rule_name=rule.name,
        rule_kind="sql",
        column=None,
        fallback_index=fallback_index,
    )
    info = _assertion_info_template(rule, scope, AssertionTypeClass.SQL)
    info.sqlAssertion = SqlAssertionInfoClass(
        type=SqlAssertionTypeClass.METRIC,
        entity=dataset_urn,
        statement=rule.query or "",
        operator=operator,
        parameters=params,
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _build_custom_assertion(
    rule: ODCSQualityRule,
    dataset_urn: str,
    logical_urn: str,
    scope: str,
    fallback_index: int,
    column: Optional[str] = None,
    logic_override: Optional[str] = None,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    """Build a CustomAssertionInfo for a rule, or None to signal a skip.

    A custom assertion must have a non-None `logic` body. If the rule has no
    query / implementation / description / name, the caller skips with a warning
    rather than emit a content-less custom assertion.
    """
    logic = (
        logic_override
        if logic_override is not None
        else _custom_logic_for_rule(rule, scope)
    )
    if logic is None:
        return None
    assertion_urn = _stable_assertion_urn(
        logical_urn=logical_urn,
        dataset_urn=dataset_urn,
        rule_name=rule.name,
        rule_kind="custom",
        column=column,
        fallback_index=fallback_index,
    )
    custom_type = rule.rule or rule.type or "odcs"
    field_urn = (
        mce_builder.make_schema_field_urn(dataset_urn, column) if column else None
    )
    info = _assertion_info_template(rule, scope, AssertionTypeClass.CUSTOM)
    info.customAssertion = CustomAssertionInfoClass(
        type=custom_type,
        entity=dataset_urn,
        field=field_urn,
        logic=logic,
    )
    return assertion_urn, MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=info
    )


def _iter_schema_rules(
    schema_entry: ODCSSchemaObject,
) -> Iterable[Tuple[ODCSQualityRule, Optional[str], str]]:
    """Yield (rule, column, scope) for rules attached to one schema entry."""
    for rule in schema_entry.quality or []:
        yield rule, rule.column, "schema"
    for field_path, prop in _walk_properties(schema_entry.properties):
        for rule in prop.quality or []:
            yield rule, field_path, "property"


def _iter_contract_rules(
    contract: ODCSContract,
) -> Iterable[Tuple[ODCSQualityRule, Optional[str], str]]:
    """Yield (rule, column, scope) for the deprecated top-level `quality[]`."""
    for rule in contract.quality or []:
        yield rule, rule.column, "contract"


@dataclass
class AssertionRoutingTrace:
    """Per-rule routing decisions for SourceReport telemetry."""

    skipped_no_body: List[str] = field(default_factory=list)
    routed_to_custom: List[str] = field(default_factory=list)


def _route_and_build(
    rule: ODCSQualityRule,
    column: Optional[str],
    scope: str,
    dataset_urn: str,
    logical_urn: str,
    index: int,
    trace: AssertionRoutingTrace,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    """Route a single rule to the appropriate assertion builder.

    Returns None when the rule produces no assertion. The trace is updated
    in-place so the source can report aggregate counters.
    """
    rule_kind = (rule.rule or "").strip()
    rule_type = (rule.type or "").strip().lower()
    rule_label = rule.name or f"<unnamed:{scope}:{index}>"

    # Library `notNull` on a column -> FieldValuesAssertion(NOT_NULL).
    if column and rule_kind in _NOT_NULL_RULE_KINDS:
        return _build_field_values_assertion(
            rule=rule,
            operator=AssertionStdOperatorClass.NOT_NULL,
            dataset_urn=dataset_urn,
            column=column,
            logical_urn=logical_urn,
            scope=scope,
            fallback_index=index,
        )

    # Library `unique` on a column -> FieldMetricAssertion(UNIQUE_PERCENTAGE == 100).
    if column and rule_kind in _UNIQUE_RULE_KINDS:
        return _build_field_metric_assertion(
            rule=rule,
            metric=FieldMetricTypeClass.UNIQUE_PERCENTAGE,
            operator=AssertionStdOperatorClass.EQUAL_TO,
            value=100,
            dataset_urn=dataset_urn,
            column=column,
            logical_urn=logical_urn,
            scope=scope,
            fallback_index=index,
        )

    # `mustNotBeBetween` has no native operator; render a stable, human-readable
    # logic string so the bounds round-trip into the custom assertion body.
    logic_override: Optional[str] = None
    if rule.mustNotBeBetween and len(rule.mustNotBeBetween) == 2:
        low = f"{float(rule.mustNotBeBetween[0]):g}"
        high = f"{float(rule.mustNotBeBetween[1]):g}"
        logic_override = f"value not between {low} and {high}"

    if rule_kind in _VOLUME_RULE_KINDS:
        op_opt, params_opt = _operator_and_params_from_threshold(rule)
        if op_opt is None or params_opt is None:
            built = _build_custom_assertion(
                rule=rule,
                dataset_urn=dataset_urn,
                logical_urn=logical_urn,
                scope=scope,
                fallback_index=index,
                column=column,
                logic_override=logic_override,
            )
            if built is None:
                trace.skipped_no_body.append(rule_label)
                return None
            trace.routed_to_custom.append(rule_label)
            return built
        return _build_volume_assertion(
            rule=rule,
            dataset_urn=dataset_urn,
            logical_urn=logical_urn,
            scope=scope,
            fallback_index=index,
            operator=op_opt,
            params=params_opt,
        )

    if rule_type == "sql":
        if not rule.query:
            built = _build_custom_assertion(
                rule=rule,
                dataset_urn=dataset_urn,
                logical_urn=logical_urn,
                scope=scope,
                fallback_index=index,
                column=column,
                logic_override=logic_override,
            )
            if built is None:
                trace.skipped_no_body.append(rule_label)
                return None
            trace.routed_to_custom.append(rule_label)
            return built
        op_opt, params_opt = _operator_and_params_from_threshold(rule)
        if op_opt is None or params_opt is None:
            built = _build_custom_assertion(
                rule=rule,
                dataset_urn=dataset_urn,
                logical_urn=logical_urn,
                scope=scope,
                fallback_index=index,
                column=column,
                logic_override=logic_override,
            )
            if built is None:
                trace.skipped_no_body.append(rule_label)
                return None
            trace.routed_to_custom.append(rule_label)
            return built
        return _build_sql_assertion(
            rule=rule,
            dataset_urn=dataset_urn,
            logical_urn=logical_urn,
            scope=scope,
            fallback_index=index,
            operator=op_opt,
            params=params_opt,
        )

    built = _build_custom_assertion(
        rule=rule,
        dataset_urn=dataset_urn,
        logical_urn=logical_urn,
        scope=scope,
        fallback_index=index,
        column=column,
        logic_override=logic_override,
    )
    if built is None:
        trace.skipped_no_body.append(rule_label)
        return None
    trace.routed_to_custom.append(rule_label)
    return built


def odcs_to_assertion_mcps(
    contract: ODCSContract,
    schema_entry: ODCSSchemaObject,
    physical_urn: str,
    logical_urn: str,
) -> Tuple[List[str], List[MetadataChangeProposalWrapper], AssertionRoutingTrace]:
    """Emit Assertion entities for every rule that applies, targeting the physical dataset.

    Only called when a physical binding resolved (strict gating). Includes
    schema-entry rules (scope=schema/property) and contract-level rules
    (scope=contract). Returns the assertion URNs, the MCPs, and a routing trace.
    """
    assertion_urns: List[str] = []
    mcps: List[MetadataChangeProposalWrapper] = []
    trace = AssertionRoutingTrace()
    index = 0

    for rule, column, scope in _iter_schema_rules(schema_entry):
        built = _route_and_build(
            rule=rule,
            column=column,
            scope=scope,
            dataset_urn=physical_urn,
            logical_urn=logical_urn,
            index=index,
            trace=trace,
        )
        index += 1
        if built is None:
            continue
        assertion_urn, mcp = built
        assertion_urns.append(assertion_urn)
        mcps.append(mcp)

    for rule, column, scope in _iter_contract_rules(contract):
        built = _route_and_build(
            rule=rule,
            column=column,
            scope=scope,
            dataset_urn=physical_urn,
            logical_urn=logical_urn,
            index=index,
            trace=trace,
        )
        index += 1
        if built is None:
            continue
        assertion_urn, mcp = built
        assertion_urns.append(assertion_urn)
        mcps.append(mcp)

    return assertion_urns, mcps, trace
