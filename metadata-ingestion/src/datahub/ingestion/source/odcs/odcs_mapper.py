import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_urn,
    make_dataset_urn_with_platform_instance,
    make_group_urn,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig, ServerMapping
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DataContractPropertiesClass,
    DataQualityContractClass,
    DatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    FieldAssertionInfoClass,
    FieldAssertionTypeClass,
    FieldValuesAssertionClass,
    FieldValuesFailThresholdClass,
    FieldValuesFailThresholdTypeClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    RowCountTotalClass,
    SchemaFieldSpecClass,
    SqlAssertionInfoClass,
    SqlAssertionTypeClass,
    TagAssociationClass,
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
# place.
_FIELD_RULE_KINDS = frozenset(("unique", "notNull", "not_null"))
_NOT_NULL_RULE_KINDS = frozenset(("notNull", "not_null"))
_VOLUME_RULE_KINDS = frozenset(("rowCount", "row_count"))


@dataclass
class AssertionUrnsByKind:
    """Assertion URNs grouped by the DataContract sub-aspect they belong to."""

    data_quality: List[str] = field(default_factory=list)
    schema: List[str] = field(default_factory=list)
    freshness: List[str] = field(default_factory=list)


@dataclass
class UnmappedSchema:
    """A schema[] entry that could not be bound to a Dataset URN."""

    index: int
    schema_entry: ODCSSchemaObject
    reason: str


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


def _select_server_mapping(
    contract: ODCSContract, mappings: List[ServerMapping]
) -> Optional[ServerMapping]:
    if not contract.servers:
        catch_all = next((m for m in mappings if m.match_any), None)
        return catch_all
    for server in contract.servers:
        for mapping in mappings:
            if mapping.match_any:
                continue
            if mapping.server == server.server:
                return mapping
    return next((m for m in mappings if m.match_any), None)


def odcs_to_dataset_urns(
    contract: ODCSContract, config: ODCSSourceConfig
) -> Tuple[List[Tuple[ODCSSchemaObject, str]], List[UnmappedSchema]]:
    """Resolve one Dataset URN per `schema[]` entry (D1 fan-out).

    Resolution per entry:
      1. If `dataset_urn_overrides[contract.id]` is set, use that for the FIRST
         schema entry only. (The current `dataset_urn_overrides` shape is
         `Dict[str, str]` — single URN per contract id; widening is deferred.)
      2. Otherwise, resolve via `servers_to_platform` + `physicalName` (or `name`).

    Returns (bindings, unmapped):
      - `bindings`: list of (schema_entry, dataset_urn). One per entry that
        bound successfully. Order matches `contract.schema_`.
      - `unmapped`: list of UnmappedSchema for entries that did not bind.
        Caller is expected to emit a warning per entry.

    A contract with no `schema[]` returns ([], []) — caller must handle it as
    a no-op (no datasets to emit; no warning by itself, since the source-level
    "no servers mapping" warning may already cover it).
    """
    bindings: List[Tuple[ODCSSchemaObject, str]] = []
    unmapped: List[UnmappedSchema] = []

    if not contract.schema_:
        return bindings, unmapped

    override = config.dataset_urn_overrides.get(contract.id)
    mapping = _select_server_mapping(contract, config.servers_to_platform)

    for index, schema_entry in enumerate(contract.schema_):
        physical_name = schema_entry.physicalName or schema_entry.name
        if not physical_name:
            unmapped.append(
                UnmappedSchema(
                    index=index,
                    schema_entry=schema_entry,
                    reason="schema entry has no physicalName or name",
                )
            )
            continue

        # Override applies only to the first schema entry (D9 — widening deferred).
        if override and index == 0:
            bindings.append((schema_entry, override))
            continue

        if mapping is None:
            unmapped.append(
                UnmappedSchema(
                    index=index,
                    schema_entry=schema_entry,
                    reason=(
                        "no `dataset_urn_overrides` entry and no matching "
                        "`servers_to_platform` mapping for the contract's servers"
                    ),
                )
            )
            continue

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=mapping.platform,
            name=physical_name,
            platform_instance=mapping.platform_instance,
            env=mapping.env,
        )
        bindings.append((schema_entry, dataset_urn))

    return bindings, unmapped


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
            # `group:` prefix lets a contract author target a group rather than a user.
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


def odcs_to_dataset_mcps(
    contract: ODCSContract,
    schema_entry: ODCSSchemaObject,
    dataset_urn: str,
    tag_prefix: Optional[str] = None,
    replicate_contract_metadata: bool = True,
) -> Iterable[MetadataChangeProposalWrapper]:
    """Emit dataset-level MCPs for one fanned-out (schema_entry, dataset_urn).

    Always emits per-schema-entry data: DatasetProperties (description from the
    contract; customProperties carry contract+entry identity) and
    EditableSchemaMetadata (D7 — always Editable).

    `replicate_contract_metadata=False` skips contract-level Ownership and
    contract-level GlobalTags so manual UI edits to those aspects survive
    subsequent ingest runs. Per-table data (DatasetProperties,
    EditableSchemaMetadata) is always written; schema-entry-level tags are
    always merged into GlobalTags regardless of the flag.
    """
    description = _description_to_str(contract.description)
    custom_props: Dict[str, str] = {
        "odcs.id": contract.id,
    }
    if contract.version:
        custom_props["odcs.version"] = contract.version
    if contract.apiVersion:
        custom_props["odcs.apiVersion"] = contract.apiVersion
    if contract.status:
        custom_props["odcs.status"] = contract.status
    if schema_entry.physicalName:
        custom_props["odcs.tableName"] = schema_entry.physicalName
    if contract.domain:
        custom_props["odcs.domain"] = contract.domain
    if contract.dataProduct:
        custom_props["odcs.dataProduct"] = contract.dataProduct
    if contract.tenant:
        custom_props["odcs.tenant"] = contract.tenant

    # Display name: prefer "<contract.name> — <schema_entry.name>" when both
    # exist; degenerate to whichever is set. None is acceptable.
    if contract.name and schema_entry.name:
        display_name: Optional[str] = f"{contract.name} — {schema_entry.name}"
    else:
        display_name = schema_entry.name or contract.name

    yield MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            description=description,
            customProperties=custom_props,
            name=display_name,
        ),
    )

    # Tags: contract-level tags only when replicating; schema-entry-level
    # tags always emit (per-table data).
    contract_tags: List[str] = []
    if replicate_contract_metadata and contract.tags:
        contract_tags.extend(contract.tags)
    if schema_entry.tags:
        contract_tags.extend(schema_entry.tags)
    tag_associations = _make_tag_associations(contract_tags, tag_prefix)
    if tag_associations:
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=GlobalTagsClass(tags=tag_associations),
        )

    if replicate_contract_metadata:
        owners = _make_owners(contract)
        if owners:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=OwnershipClass(owners=owners),
            )

    # EditableSchemaMetadata (D7 — always Editable). Walks this schema entry's
    # properties only; other schema entries get their own EditableSchemaMetadata
    # emission against their own dataset URN.
    field_infos: List[EditableSchemaFieldInfoClass] = []
    for field_path, prop in _walk_properties(schema_entry.properties):
        prop_desc = _description_to_str(prop.description)
        prop_tag_assoc = _make_tag_associations(prop.tags, tag_prefix)
        if not prop_desc and not prop_tag_assoc:
            continue
        field_infos.append(
            EditableSchemaFieldInfoClass(
                fieldPath=field_path,
                description=prop_desc,
                globalTags=(
                    GlobalTagsClass(tags=prop_tag_assoc) if prop_tag_assoc else None
                ),
            )
        )
    if field_infos:
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(editableSchemaFieldInfo=field_infos),
        )


def _num(v: object) -> AssertionStdParameterClass:
    """Normalize a numeric threshold to a stable string form.

    Both `5` (int) and `5.0` (float) render as `"5"`; `5.5` renders as `"5.5"`.
    Single coercion path so `mustBe`, `mustBeGreaterThan`, etc. all hit the
    same string.
    """
    return AssertionStdParameterClass(
        value=f"{float(v):g}",  # type: ignore[arg-type]
        type=AssertionStdParameterTypeClass.NUMBER,
    )


def _operator_and_params_from_threshold(
    rule: ODCSQualityRule,
) -> Tuple[Optional[str], Optional[AssertionStdParametersClass]]:
    """Translate an ODCS `mustBe*` threshold into a DataHub operator + parameters.

    Returns `(operator, parameters)` for a natively-modeled threshold, or
    `(None, None)` when no native operator applies — either because no threshold
    is provided or because it is an unmappable one such as `mustNotBeBetween`.
    """
    if rule.mustBeBetween and len(rule.mustBeBetween) == 2:
        return AssertionStdOperatorClass.BETWEEN, AssertionStdParametersClass(
            minValue=_num(rule.mustBeBetween[0]),
            maxValue=_num(rule.mustBeBetween[1]),
        )
    if rule.mustNotBeBetween and len(rule.mustNotBeBetween) == 2:
        # No NOT_BETWEEN operator; the caller routes this to a
        # CustomAssertionInfo with an explicit `logic` string (D6).
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
    contract_urn: str,
    dataset_urn: str,
    rule_name: Optional[str],
    rule_kind: str,
    column: Optional[str],
    fallback_index: int,
) -> str:
    guid_dict = {
        "contract": contract_urn,
        "dataset": dataset_urn,
        "rule_name": rule_name or f"{rule_kind}_{fallback_index}",
        "rule_kind": rule_kind,
        "field": column or "",
    }
    return make_assertion_urn(datahub_guid(guid_dict))


def _custom_logic_for_rule(rule: ODCSQualityRule, scope: str) -> Optional[str]:
    """Resolve the `logic` body for a CustomAssertionInfo per D6.

    Order of preference:
      1. `rule.query`
      2. serialized `rule.implementation`
      3. `rule.description`
      4. `rule.name + ":" + scope` (last resort)
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

    `scope` is one of "contract", "schema", or "property" (D1) — emitted as
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
    """Construct the shared header for an AssertionInfoClass.

    Caller is responsible for setting the type-specific sub-aspect
    (volumeAssertion / sqlAssertion / etc.) via `setattr` on the returned
    object — or by replacing this with a builder pattern. Pulled out to
    deduplicate the body shared between volume / sql / field / custom builders
    (prior PR review — Code Quality M2).
    """
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
    contract_urn: str,
    scope: str,
    fallback_index: int,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(
        contract_urn=contract_urn,
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


def _build_volume_assertion(
    rule: ODCSQualityRule,
    dataset_urn: str,
    contract_urn: str,
    scope: str,
    fallback_index: int,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(
        contract_urn=contract_urn,
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
    contract_urn: str,
    scope: str,
    fallback_index: int,
    operator: str,
    params: AssertionStdParametersClass,
) -> Tuple[str, MetadataChangeProposalWrapper]:
    assertion_urn = _stable_assertion_urn(
        contract_urn=contract_urn,
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
    contract_urn: str,
    scope: str,
    fallback_index: int,
    column: Optional[str] = None,
    logic_override: Optional[str] = None,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    """Build a CustomAssertionInfo for a rule, or None to signal a skip.

    Per D6: a custom assertion must have a non-None `logic` body. If the rule
    has no query / implementation / description / name, the caller should skip
    with a warning rather than emit a content-less custom assertion.

    `logic_override` short-circuits the normal `_custom_logic_for_rule` priority
    chain. Used by routes (e.g. `mustNotBeBetween`) that need a specific,
    documented `logic` string regardless of what the rule's other fields say.
    """
    logic = (
        logic_override
        if logic_override is not None
        else _custom_logic_for_rule(rule, scope)
    )
    if logic is None:
        return None
    assertion_urn = _stable_assertion_urn(
        contract_urn=contract_urn,
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
    """Yield (rule, column, scope) for rules attached to one schema entry.

    `scope` is "schema" for `schema_entry.quality[]` and "property" for rules
    attached to a property under `schema_entry.properties[]` (recursively).
    """
    for rule in schema_entry.quality or []:
        # Schema-level quality may carry an explicit `column` field for a
        # rule that targets a column without nesting it under properties.
        yield rule, rule.column, "schema"
    for field_path, prop in _walk_properties(schema_entry.properties):
        for rule in prop.quality or []:
            yield rule, field_path, "property"


def _iter_contract_rules(
    contract: ODCSContract,
) -> Iterable[Tuple[ODCSQualityRule, Optional[str], str]]:
    """Yield (rule, column, scope) for the deprecated top-level `quality[]`.

    These contract-scoped rules are replicated to each fanned-out dataset's
    Assertion set with `odcs.scope=contract`.
    """
    for rule in contract.quality or []:
        yield rule, rule.column, "contract"


@dataclass
class AssertionRoutingTrace:
    """Per-rule routing decisions for SourceReport telemetry (D6).

    The mapper records what happened to each rule so the source can populate
    its `rules_skipped_no_threshold` / `rules_routed_to_custom` lists without
    re-deriving routing in two places.
    """

    skipped_no_body: List[str] = field(default_factory=list)
    routed_to_custom: List[str] = field(default_factory=list)


def _route_and_build(
    rule: ODCSQualityRule,
    column: Optional[str],
    scope: str,
    dataset_urn: str,
    contract_urn: str,
    index: int,
    trace: AssertionRoutingTrace,
) -> Optional[Tuple[str, MetadataChangeProposalWrapper]]:
    """Route a single rule to the appropriate builder per D6.

    Returns None when the rule produces no assertion (e.g. skipped because
    there is no body to model). The trace is updated in-place so the source
    can report aggregate counters.
    """
    rule_kind = (rule.rule or "").strip()
    rule_type = (rule.type or "").strip().lower()
    rule_label = rule.name or f"<unnamed:{scope}:{index}>"

    # Library rules tied to a specific field — FieldValuesAssertion.
    if column and rule_kind in _FIELD_RULE_KINDS:
        operator = (
            AssertionStdOperatorClass.NOT_NULL
            if rule_kind in _NOT_NULL_RULE_KINDS
            else AssertionStdOperatorClass.EQUAL_TO
        )
        return _build_field_values_assertion(
            rule=rule,
            operator=operator,
            dataset_urn=dataset_urn,
            column=column,
            contract_urn=contract_urn,
            scope=scope,
            fallback_index=index,
        )

    # `mustNotBeBetween` has a documented explicit logic format (D6) so the
    # bounds round-trip into the assertion body in a stable, human-readable
    # form rather than collapsing to "<rule_name>:<scope>" via the default
    # `_custom_logic_for_rule` priority chain. Computed once and reused at
    # every custom-routing site below.
    logic_override: Optional[str] = None
    if rule.mustNotBeBetween and len(rule.mustNotBeBetween) == 2:
        # `:g` strips the trailing ".0" on whole numbers (1.0 -> "1") so the
        # rendered logic string matches what a contract author wrote.
        low = f"{float(rule.mustNotBeBetween[0]):g}"
        high = f"{float(rule.mustNotBeBetween[1]):g}"
        logic_override = f"value not between {low} and {high}"

    # Library volume rule.
    if rule_kind in _VOLUME_RULE_KINDS:
        op_opt, params_opt = _operator_and_params_from_threshold(rule)
        if op_opt is None or params_opt is None:
            # No real threshold (or `mustNotBeBetween` etc.) — route to custom.
            built = _build_custom_assertion(
                rule=rule,
                dataset_urn=dataset_urn,
                contract_urn=contract_urn,
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
            contract_urn=contract_urn,
            scope=scope,
            fallback_index=index,
            operator=op_opt,
            params=params_opt,
        )

    # SQL rule.
    if rule_type == "sql":
        if not rule.query:
            built = _build_custom_assertion(
                rule=rule,
                dataset_urn=dataset_urn,
                contract_urn=contract_urn,
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
                contract_urn=contract_urn,
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
            contract_urn=contract_urn,
            scope=scope,
            fallback_index=index,
            operator=op_opt,
            params=params_opt,
        )

    # Anything else — custom assertion preserving the body, or skip.
    built = _build_custom_assertion(
        rule=rule,
        dataset_urn=dataset_urn,
        contract_urn=contract_urn,
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
    dataset_urn: str,
    contract_urn: str,
) -> Tuple[
    AssertionUrnsByKind,
    List[MetadataChangeProposalWrapper],
    AssertionRoutingTrace,
]:
    """Emit Assertion entities for every rule that applies to this dataset.

    Includes schema-entry rules (scope=schema/property) and contract-level
    rules (scope=contract) replicated per dataset. Returns:
      - URNs grouped for the DataContractProperties aspect.
      - The list of MCPs to emit.
      - A trace of routing decisions for SourceReport telemetry.
    """
    grouped = AssertionUrnsByKind()
    mcps: List[MetadataChangeProposalWrapper] = []
    trace = AssertionRoutingTrace()
    index = 0

    # Walk schema-entry rules first (deterministic ordering for stable URNs
    # under the fallback-index path).
    for rule, column, scope in _iter_schema_rules(schema_entry):
        built = _route_and_build(
            rule=rule,
            column=column,
            scope=scope,
            dataset_urn=dataset_urn,
            contract_urn=contract_urn,
            index=index,
            trace=trace,
        )
        index += 1
        if built is None:
            continue
        assertion_urn, mcp = built
        grouped.data_quality.append(assertion_urn)
        mcps.append(mcp)

    # Then replicate contract-level rules to this dataset.
    for rule, column, scope in _iter_contract_rules(contract):
        built = _route_and_build(
            rule=rule,
            column=column,
            scope=scope,
            dataset_urn=dataset_urn,
            contract_urn=contract_urn,
            index=index,
            trace=trace,
        )
        index += 1
        if built is None:
            continue
        assertion_urn, mcp = built
        grouped.data_quality.append(assertion_urn)
        mcps.append(mcp)

    return grouped, mcps, trace


def odcs_to_contract_urn(contract_id: str, dataset_urn: str) -> str:
    """Build a stable DataContract URN seeded by the dataset URN and ODCS contract id.

    The `entity` key matches the canonical seeding pattern in
    `datahub/api/entities/datacontract/datacontract.py`. We add `odcs_id` as a
    secondary dimension so two distinct ODCS files about the same dataset don't
    collide on the same DataContract URN.
    """
    return f"urn:li:dataContract:{datahub_guid({'entity': dataset_urn, 'odcs_id': contract_id})}"


def odcs_to_contract_mcp(
    contract: ODCSContract,
    dataset_urn: str,
    contract_urn: str,
    assertion_urns: AssertionUrnsByKind,
    raw_yaml: Optional[str],
    raw_yaml_size_limit: int,
) -> Tuple[MetadataChangeProposalWrapper, bool]:
    """Build the DataContractProperties MCP. Returns (mcp, raw_truncated)."""
    raw_truncated = False
    raw_to_emit: Optional[str] = raw_yaml
    if raw_yaml is not None and len(raw_yaml.encode("utf-8")) > raw_yaml_size_limit:
        raw_truncated = True
        raw_to_emit = None

    properties = DataContractPropertiesClass(
        entity=dataset_urn,
        dataQuality=[
            DataQualityContractClass(assertion=urn)
            for urn in assertion_urns.data_quality
        ]
        or None,
        rawContract=raw_to_emit,
    )
    return (
        MetadataChangeProposalWrapper(entityUrn=contract_urn, aspect=properties),
        raw_truncated,
    )
