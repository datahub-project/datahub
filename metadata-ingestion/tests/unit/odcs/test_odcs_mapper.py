"""Unit tests for the ODCS mapper — logical-model architecture."""

from typing import Any, Dict, List, Optional

from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_mapper import (
    AssertionRoutingTrace,
    _make_owners,
    _route_and_build,
    build_schema_metadata,
    odcs_platform_info_mcp,
    odcs_to_assertion_mcps,
    odcs_to_logical_dataset_mcps,
    odcs_to_logical_dataset_urn,
    odcs_to_logical_parent_mcp,
    odcs_to_physical_bindings,
)
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
    ODCSQualityRule,
    ODCSSchemaObject,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    BooleanTypeClass,
    DataPlatformInfoClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    LogicalParentClass,
    NullTypeClass,
    NumberTypeClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


def _make_contract(
    *,
    contract_id: str = "c1",
    name: Optional[str] = None,
    servers: Optional[List[Dict[str, Any]]] = None,
    schema: Optional[List[Dict[str, Any]]] = None,
    team: Optional[List[Dict[str, Any]]] = None,
    quality: Optional[List[Dict[str, Any]]] = None,
    description: Any = None,
    tags: Optional[List[str]] = None,
) -> ODCSContract:
    payload: Dict[str, Any] = {
        "id": contract_id,
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "1.0.0",
        "status": "active",
        "name": name,
        "servers": servers,
        "schema": schema,
        "team": team,
        "quality": quality,
        "description": description,
        "tags": tags,
    }
    return ODCSContract.model_validate(payload)


def _config(
    *,
    physical_overrides: Optional[Dict[str, List[str]]] = None,
    mappings: Optional[List[Dict[str, Any]]] = None,
) -> ODCSSourceConfig:
    return ODCSSourceConfig.model_validate(
        {
            "path": "/tmp/ignored",
            "physical_urn_overrides": physical_overrides or {},
            "servers_to_platform": mappings or [],
        }
    )


def _first_schema(contract: ODCSContract) -> ODCSSchemaObject:
    assert contract.schema_
    return contract.schema_[0]


# ---------------------------------------------------------------------------
# Logical dataset URN + physical binding resolution
# ---------------------------------------------------------------------------


def test_logical_dataset_urn_is_on_odcs_platform() -> None:
    contract = _make_contract(schema=[{"name": "orders"}])
    config = _config()
    urn = odcs_to_logical_dataset_urn(contract, _first_schema(contract), config)
    assert "urn:li:dataPlatform:odcs" in urn
    assert "c1.orders" in urn


def test_binding_resolves_physical_via_server_mapping() -> None:
    contract = _make_contract(
        servers=[{"server": "prod-postgres", "type": "postgres"}],
        schema=[{"name": "t1", "physicalName": "tbl1"}],
    )
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])

    bindings = odcs_to_physical_bindings(contract, config)

    assert len(bindings) == 1
    assert "urn:li:dataPlatform:odcs" in bindings[0].logical_urn
    assert bindings[0].physical_urn is not None
    assert "postgres" in bindings[0].physical_urn
    assert "tbl1" in bindings[0].physical_urn


def test_binding_unmapped_still_yields_logical_urn() -> None:
    # No server mapping: the logical dataset is still produced; only the
    # physical binding is absent (the strict-gating contract).
    contract = _make_contract(schema=[{"name": "t1", "physicalName": "tbl1"}])
    config = _config()

    bindings = odcs_to_physical_bindings(contract, config)

    assert len(bindings) == 1
    assert bindings[0].logical_urn
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None


def test_binding_physical_override_per_index() -> None:
    contract = _make_contract(
        schema=[{"name": "a"}, {"name": "b"}],
    )
    config = _config(
        physical_overrides={
            "c1": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.a,PROD)", ""]
        }
    )

    bindings = odcs_to_physical_bindings(contract, config)

    assert bindings[0].physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.a,PROD)"
    )
    # Empty string leaves the second entry unbound.
    assert bindings[1].physical_urn is None


# ---------------------------------------------------------------------------
# Canonical schema metadata
# ---------------------------------------------------------------------------


def test_schema_metadata_maps_types_nullable_and_keys() -> None:
    schema_entry = ODCSSchemaObject.model_validate(
        {
            "name": "orders",
            "properties": [
                {"name": "id", "logicalType": "integer", "primaryKey": True},
                {"name": "email", "logicalType": "string", "required": True},
                {"name": "active", "physicalType": "boolean"},
            ],
        }
    )
    result = build_schema_metadata(schema_entry)
    sm = result.schema_metadata
    assert isinstance(sm, SchemaMetadataClass)
    assert "urn:li:dataPlatform:odcs" in sm.platform
    by_path = {f.fieldPath: f for f in sm.fields}

    assert isinstance(by_path["id"].type.type, NumberTypeClass)
    assert by_path["id"].isPartOfKey is True
    assert isinstance(by_path["email"].type.type, StringTypeClass)
    assert by_path["email"].nullable is False
    assert isinstance(by_path["active"].type.type, BooleanTypeClass)
    # No declared type for active beyond physical "boolean" -> not a fallback.
    assert result.unmapped_types == []


def test_schema_metadata_nested_paths_and_native_type() -> None:
    schema_entry = ODCSSchemaObject.model_validate(
        {
            "name": "users",
            "properties": [
                {
                    "name": "address",
                    "logicalType": "object",
                    "properties": [
                        {
                            "name": "city",
                            "logicalType": "string",
                            "physicalType": "VARCHAR(64)",
                        },
                    ],
                }
            ],
        }
    )
    sm = build_schema_metadata(schema_entry).schema_metadata
    assert sm is not None
    paths = {f.fieldPath: f for f in sm.fields}
    assert "address" in paths
    assert "address.city" in paths
    # physicalType wins for nativeDataType.
    assert paths["address.city"].nativeDataType == "VARCHAR(64)"


def test_schema_metadata_unknown_type_falls_back_to_null_and_is_reported() -> None:
    schema_entry = ODCSSchemaObject.model_validate(
        {
            "name": "t",
            "properties": [{"name": "weird", "logicalType": "quaternion"}],
        }
    )
    result = build_schema_metadata(schema_entry)
    assert result.schema_metadata is not None
    field = result.schema_metadata.fields[0]
    assert isinstance(field.type.type, NullTypeClass)
    assert field.nativeDataType == "quaternion"
    assert any("quaternion" in u for u in result.unmapped_types)


def test_schema_metadata_none_when_no_properties() -> None:
    schema_entry = ODCSSchemaObject.model_validate({"name": "empty"})
    assert build_schema_metadata(schema_entry).schema_metadata is None


# ---------------------------------------------------------------------------
# Logical dataset aspects
# ---------------------------------------------------------------------------


def test_logical_dataset_mcps_carry_provenance_and_schema() -> None:
    contract = _make_contract(
        name="Orders Contract",
        description="A contract",
        tags=["pii"],
        schema=[
            {
                "name": "orders",
                "physicalName": "public.orders",
                "properties": [{"name": "id", "logicalType": "integer"}],
                "quality": [{"rule": "rowCount", "mustBeGreaterThan": 0}],
            }
        ],
    )
    mcps, _unmapped = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn="urn:li:dataset:(urn:li:dataPlatform:odcs,c1.orders,PROD)",
        source_file="orders.odcs.yaml",
    )
    aspects = [m.aspect for m in mcps]

    props = next(a for a in aspects if isinstance(a, DatasetPropertiesClass))
    assert props.customProperties["odcs.id"] == "c1"
    assert props.customProperties["odcs.schemaName"] == "orders"
    assert props.customProperties["odcs.physicalName"] == "public.orders"
    assert props.customProperties["odcs.sourceFile"] == "orders.odcs.yaml"
    assert props.customProperties["odcs.qualityRuleCount"] == "1"

    assert any(isinstance(a, SchemaMetadataClass) for a in aspects)
    assert any(isinstance(a, GlobalTagsClass) for a in aspects)


def test_logical_dataset_no_contract_aspect_emitted() -> None:
    # The pivot drops dataContract entirely — no dataContract MCP should appear.
    contract = _make_contract(schema=[{"name": "t", "physicalName": "t"}])
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn="urn:li:dataset:(urn:li:dataPlatform:odcs,c1.t,PROD)",
    )
    for m in mcps:
        assert m.entityUrn is not None
        assert "dataContract" not in m.entityUrn
        assert "DataContract" not in type(m.aspect).__name__


def test_institutional_memory_from_authoritative_definitions() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "authoritativeDefinitions": [
                    {"type": "specification", "url": "https://example.com/spec"}
                ],
            }
        ]
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn="urn:li:dataset:(urn:li:dataPlatform:odcs,c1.t,PROD)",
    )
    inst = next(
        m.aspect for m in mcps if isinstance(m.aspect, InstitutionalMemoryClass)
    )
    assert inst.elements[0].url == "https://example.com/spec"


def test_replicate_false_skips_ownership() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[{"username": "alice", "role": "owner"}],
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn="urn:li:dataset:(urn:li:dataPlatform:odcs,c1.t,PROD)",
        replicate_contract_metadata=False,
    )
    assert not any(isinstance(m.aspect, OwnershipClass) for m in mcps)


def test_owners_group_prefix_maps_to_corp_group() -> None:
    contract = _make_contract(
        team=[{"username": "group:data-eng", "role": "owner"}],
    )
    owners = _make_owners(contract)
    assert owners[0].owner == "urn:li:corpGroup:data-eng"
    assert owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


# ---------------------------------------------------------------------------
# logicalParent + platform info
# ---------------------------------------------------------------------------


def test_logical_parent_links_physical_to_logical() -> None:
    physical = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)"
    logical = "urn:li:dataset:(urn:li:dataPlatform:odcs,c1.orders,PROD)"
    mcp = odcs_to_logical_parent_mcp(physical, logical)
    assert mcp.entityUrn == physical
    assert isinstance(mcp.aspect, LogicalParentClass)
    assert mcp.aspect.parent is not None
    assert mcp.aspect.parent.destinationUrn == logical


def test_platform_info_registers_odcs() -> None:
    mcp = odcs_platform_info_mcp()
    assert mcp.entityUrn == "urn:li:dataPlatform:odcs"
    assert isinstance(mcp.aspect, DataPlatformInfoClass)
    assert mcp.aspect.name == "odcs"


# ---------------------------------------------------------------------------
# Assertions — target the physical dataset, seeded by the logical URN
# ---------------------------------------------------------------------------

_PHYSICAL = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)"
_LOGICAL = "urn:li:dataset:(urn:li:dataPlatform:odcs,c1.orders,PROD)"


def test_assertions_target_physical_dataset() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "orders",
                "quality": [{"rule": "rowCount", "mustBeGreaterThan": 0}],
            }
        ]
    )
    urns, mcps, _trace = odcs_to_assertion_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        physical_urn=_PHYSICAL,
        logical_urn=_LOGICAL,
    )
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.VOLUME
    assert info.volumeAssertion is not None
    assert info.volumeAssertion.entity == _PHYSICAL


def test_assertion_urn_is_seeded_by_logical_urn() -> None:
    # Two different logical URNs over the same physical dataset must not collide.
    rule = ODCSQualityRule(rule="rowCount", mustBeGreaterThan=0, name="r")
    trace = AssertionRoutingTrace()
    a = _route_and_build(rule, None, "schema", _PHYSICAL, _LOGICAL, 0, trace)
    b = _route_and_build(
        rule,
        None,
        "schema",
        _PHYSICAL,
        "urn:li:dataset:(urn:li:dataPlatform:odcs,other.orders,PROD)",
        0,
        trace,
    )
    assert a is not None and b is not None
    assert a[0] != b[0]


def test_mustnotbetween_routes_to_custom_with_logic() -> None:
    rule = ODCSQualityRule(rule="rowCount", mustNotBeBetween=[1, 10], name="r")
    trace = AssertionRoutingTrace()
    built = _route_and_build(rule, None, "schema", _PHYSICAL, _LOGICAL, 0, trace)
    assert built is not None
    info = built[1].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.customAssertion is not None
    assert info.customAssertion.logic == "value not between 1 and 10"


def test_rule_with_no_body_is_skipped() -> None:
    rule = ODCSQualityRule(type="text")  # no query/impl/description/name
    trace = AssertionRoutingTrace()
    built = _route_and_build(rule, None, "schema", _PHYSICAL, _LOGICAL, 0, trace)
    assert built is None
    assert trace.skipped_no_body
