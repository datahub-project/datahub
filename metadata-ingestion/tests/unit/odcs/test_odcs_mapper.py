"""Unit tests for the ODCS mapper — spec-exact routing, logical-dataset targeting."""

import json
from typing import Any, Dict, List, Optional

import pytest

from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_mapper import (
    _make_owners,
    _operator_and_params_from_threshold,
    build_schema_metadata,
    odcs_platform_info_mcp,
    odcs_to_assertion_mcps,
    odcs_to_logical_dataset_mcps,
    odcs_to_logical_dataset_urn,
    odcs_to_logical_parent_mcp,
    odcs_to_physical_bindings,
    odcs_to_schema_assertion_mcps,
    unmapped_owner_roles,
)
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
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
    BooleanTypeClass,
    CustomAssertionInfoClass,
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    FieldAssertionInfoClass,
    FieldAssertionTypeClass,
    FieldMetricAssertionClass,
    FieldMetricTypeClass,
    FieldValuesAssertionClass,
    FieldValuesFailThresholdTypeClass,
    InstitutionalMemoryClass,
    LogicalParentClass,
    NullTypeClass,
    NumberTypeClass,
    OwnershipTypeClass,
    SchemaAssertionCompatibilityClass,
    StringTypeClass,
)

LOGICAL_URN = "urn:li:dataset:(urn:li:dataPlatform:odcs,c1.t,PROD)"


def _make_contract(
    *,
    contract_id: str = "c1",
    api_version: str = "v3.1.0",
    name: Optional[str] = None,
    servers: Optional[List[Dict[str, Any]]] = None,
    schema: Optional[List[Dict[str, Any]]] = None,
    team: Optional[List[Dict[str, Any]]] = None,
    description: Any = None,
    tags: Optional[List[str]] = None,
    authoritative_definitions: Optional[List[Dict[str, Any]]] = None,
) -> ODCSContract:
    payload: Dict[str, Any] = {
        "id": contract_id,
        "apiVersion": api_version,
        "kind": "DataContract",
        "version": "1.0.0",
        "status": "active",
        "name": name,
        "servers": servers,
        "schema": schema,
        "team": team,
        "description": description,
        "tags": tags,
        "authoritativeDefinitions": authoritative_definitions,
    }
    return ODCSContract.model_validate(payload)


def _config(**overrides: Any) -> ODCSSourceConfig:
    payload: Dict[str, Any] = {"path": "/tmp/ignored"}
    payload.update(overrides)
    return ODCSSourceConfig.model_validate(payload)


def _first_schema(contract: ODCSContract) -> ODCSSchemaObject:
    assert contract.schema_
    return contract.schema_[0]


def _route_single(
    rule: Dict[str, Any],
    *,
    on_column: Optional[str] = "col",
    api_version: str = "v3.1.0",
) -> Any:
    """Build a one-rule contract and return (urns, mcps, trace)."""
    if on_column:
        schema = [
            {
                "name": "t",
                "properties": [
                    {"name": on_column, "logicalType": "string", "quality": [rule]}
                ],
            }
        ]
    else:
        schema = [{"name": "t", "quality": [rule]}]
    contract = _make_contract(api_version=api_version, schema=schema)
    return odcs_to_assertion_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )


def _field_assertion(info: AssertionInfoClass) -> FieldAssertionInfoClass:
    assert info.fieldAssertion is not None
    return info.fieldAssertion


def _field_values(info: AssertionInfoClass) -> FieldValuesAssertionClass:
    fva = _field_assertion(info).fieldValuesAssertion
    assert fva is not None
    return fva


def _field_metric(info: AssertionInfoClass) -> FieldMetricAssertionClass:
    fma = _field_assertion(info).fieldMetricAssertion
    assert fma is not None
    return fma


def _custom(info: AssertionInfoClass) -> CustomAssertionInfoClass:
    assert info.customAssertion is not None
    return info.customAssertion


def _custom_logic(info: AssertionInfoClass) -> str:
    logic = _custom(info).logic
    assert logic is not None
    return logic


def _param(params: Optional[AssertionStdParametersClass]) -> AssertionStdParameterClass:
    assert params is not None
    assert params.value is not None
    return params.value


def _single_info(mcps: List) -> AssertionInfoClass:
    infos = [m.aspect for m in mcps if isinstance(m.aspect, AssertionInfoClass)]
    assert len(infos) == 1
    return infos[0]


# ---------------------------------------------------------------------------
# Logical dataset URN + physical binding resolution
# ---------------------------------------------------------------------------


def test_logical_dataset_urn_is_on_odcs_platform() -> None:
    contract = _make_contract(schema=[{"name": "orders"}])
    urn = odcs_to_logical_dataset_urn(contract, _first_schema(contract), _config())
    assert "urn:li:dataPlatform:odcs" in urn
    assert "c1.orders" in urn


def test_binding_platform_derived_from_server_type() -> None:
    contract = _make_contract(
        servers=[
            {
                "server": "prod-postgres",
                "type": "postgres",
                "database": "appdb",
                "schema": "public",
            }
        ],
        schema=[{"name": "t1", "physicalName": "tbl1"}],
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert len(bindings) == 1
    assert bindings[0].physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.tbl1,PROD)"
    )


@pytest.mark.parametrize(
    "server,expected_name,expected_platform",
    [
        (
            {"type": "postgres", "database": "db", "schema": "sch"},
            "db.sch.tbl",
            "postgres",
        ),
        (
            {"type": "redshift", "database": "db", "schema": "sch"},
            "db.sch.tbl",
            "redshift",
        ),
        (
            {"type": "sqlserver", "database": "db", "schema": "dbo"},
            "db.dbo.tbl",
            "mssql",
        ),
        (
            {"type": "bigquery", "project": "proj", "dataset": "ds"},
            "proj.ds.tbl",
            "bigquery",
        ),
        (
            {"type": "databricks", "catalog": "cat", "schema": "sch"},
            "cat.sch.tbl",
            "databricks",
        ),
        (
            {"type": "trino", "catalog": "cat", "schema": "sch"},
            "cat.sch.tbl",
            "trino",
        ),
        ({"type": "mysql", "database": "db"}, "db.tbl", "mysql"),
        (
            # Snowflake is lowercased by default.
            {"type": "snowflake", "database": "ANALYTICS", "schema": "PUBLIC"},
            "analytics.public.tbl",
            "snowflake",
        ),
    ],
)
def test_binding_name_composition_per_platform(
    server: Dict[str, Any], expected_name: str, expected_platform: str
) -> None:
    server = {"server": "srv", **server}
    table = "TBL" if server["type"] == "snowflake" else "tbl"
    contract = _make_contract(
        servers=[server], schema=[{"name": "t1", "physicalName": table}]
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert bindings[0].physical_urn == (
        f"urn:li:dataset:(urn:li:dataPlatform:{expected_platform},{expected_name},PROD)"
    )


def test_binding_snowflake_lowercase_can_be_disabled() -> None:
    contract = _make_contract(
        servers=[
            {
                "server": "srv",
                "type": "snowflake",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
            }
        ],
        schema=[{"name": "t1", "physicalName": "TBL"}],
    )
    bindings = odcs_to_physical_bindings(
        contract, _config(lowercase_physical_urns=False)
    )
    assert bindings[0].physical_urn is not None
    assert "ANALYTICS.PUBLIC.TBL" in bindings[0].physical_urn


def test_binding_dotted_physical_name_passes_through() -> None:
    contract = _make_contract(
        servers=[
            {"server": "srv", "type": "postgres", "database": "db", "schema": "sch"}
        ],
        schema=[{"name": "t1", "physicalName": "otherdb.othersch.tbl"}],
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert bindings[0].physical_urn is not None
    assert "otherdb.othersch.tbl" in bindings[0].physical_urn
    assert bindings[0].name_passthrough


def test_binding_missing_server_field_leaves_unbound() -> None:
    contract = _make_contract(
        servers=[{"server": "srv", "type": "postgres", "database": "db"}],  # no schema
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None
    assert "schema" in bindings[0].unmapped_reason


def test_binding_oracle_needs_dotted_name_or_override() -> None:
    contract = _make_contract(
        servers=[{"server": "srv", "type": "oracle"}],
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None


def test_binding_unmappable_server_type_leaves_unbound() -> None:
    contract = _make_contract(
        servers=[{"server": "srv", "type": "kafka"}],
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None
    assert "kafka" in bindings[0].unmapped_reason


def test_binding_no_servers_leaves_unbound() -> None:
    contract = _make_contract(schema=[{"name": "t1", "physicalName": "tbl1"}])
    bindings = odcs_to_physical_bindings(contract, _config())
    assert len(bindings) == 1
    assert bindings[0].logical_urn
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None


def test_server_override_refines_env_instance_and_platform() -> None:
    contract = _make_contract(
        servers=[
            {"server": "srv", "type": "postgres", "database": "db", "schema": "sch"}
        ],
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(
        contract,
        _config(
            server_overrides=[
                {
                    "server": "srv",
                    "env": "DEV",
                    "platform_instance": "inst1",
                    "platform": "redshift",
                }
            ]
        ),
    )
    urn = bindings[0].physical_urn
    assert urn is not None
    assert "urn:li:dataPlatform:redshift" in urn
    assert "inst1." in urn
    assert urn.endswith(",DEV)")


def test_server_override_to_unmapped_platform_leaves_unbound_with_reason() -> None:
    """Forcing a platform with no known naming convention cannot compose a
    qualified name — the entry stays unbound with an actionable reason."""
    contract = _make_contract(
        servers=[
            {"server": "srv", "type": "postgres", "database": "db", "schema": "sch"}
        ],
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(
        contract,
        _config(server_overrides=[{"server": "srv", "platform": "greenplum"}]),
    )
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None
    assert "greenplum" in bindings[0].unmapped_reason


def test_server_override_match_any_supplies_platform_for_unknown_type() -> None:
    contract = _make_contract(
        servers=[
            {
                "server": "custom-server",
                "type": "customdb",
                "database": "db",
                "schema": "sch",
            }
        ],
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(
        contract,
        _config(
            server_overrides=[
                {"server": "*", "match_any": True, "platform": "postgres"}
            ]
        ),
    )
    assert bindings[0].physical_urn is not None
    assert "urn:li:dataPlatform:postgres,db.sch.tbl," in bindings[0].physical_urn


def test_physical_urn_overrides_by_schema_name() -> None:
    contract = _make_contract(
        servers=[
            {"server": "srv", "type": "postgres", "database": "db", "schema": "sch"}
        ],
        schema=[{"name": "a"}, {"name": "b"}, {"name": "c"}],
    )
    explicit = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.a,PROD)"
    bindings = odcs_to_physical_bindings(
        contract,
        _config(physical_urn_overrides={"c1": {"a": explicit, "b": ""}}),
    )
    # a: explicit URN wins.
    assert bindings[0].physical_urn == explicit
    # b: empty string deliberately unbinds — no server fallback.
    assert bindings[1].physical_urn is None
    # c: absent from the override map — falls back to server derivation.
    assert bindings[2].physical_urn is not None
    assert "urn:li:dataPlatform:postgres,db.sch.c," in bindings[2].physical_urn


# ---------------------------------------------------------------------------
# Canonical schema metadata
# ---------------------------------------------------------------------------


def test_schema_metadata_maps_types_nullable_and_keys() -> None:
    entry = ODCSSchemaObject.model_validate(
        {
            "name": "t",
            "properties": [
                {
                    "name": "id",
                    "logicalType": "number",
                    "primaryKey": True,
                    "required": True,
                },
                {"name": "email", "logicalType": "string"},
                {"name": "active", "logicalType": "boolean"},
            ],
        }
    )
    result = build_schema_metadata(entry)
    assert result.schema_metadata is not None
    fields = {f.fieldPath: f for f in result.schema_metadata.fields}
    assert isinstance(fields["id"].type.type, NumberTypeClass)
    assert fields["id"].isPartOfKey
    assert not fields["id"].nullable
    assert isinstance(fields["email"].type.type, StringTypeClass)
    assert fields["email"].nullable
    assert isinstance(fields["active"].type.type, BooleanTypeClass)


def test_schema_metadata_nested_paths_and_native_type() -> None:
    entry = ODCSSchemaObject.model_validate(
        {
            "name": "t",
            "properties": [
                {
                    "name": "address",
                    "logicalType": "object",
                    "properties": [
                        {
                            "name": "city",
                            "logicalType": "string",
                            "physicalType": "varchar(80)",
                        }
                    ],
                }
            ],
        }
    )
    result = build_schema_metadata(entry)
    assert result.schema_metadata is not None
    paths = [f.fieldPath for f in result.schema_metadata.fields]
    assert "address" in paths
    assert "address.city" in paths
    city = next(
        f for f in result.schema_metadata.fields if f.fieldPath == "address.city"
    )
    assert city.nativeDataType == "varchar(80)"


def test_schema_metadata_unknown_type_falls_back_to_null_and_is_reported() -> None:
    entry = ODCSSchemaObject.model_validate(
        {
            "name": "t",
            "properties": [{"name": "weird", "logicalType": "hyperloglog"}],
        }
    )
    result = build_schema_metadata(entry)
    assert result.schema_metadata is not None
    assert isinstance(result.schema_metadata.fields[0].type.type, NullTypeClass)
    assert result.unmapped_types == ["weird:hyperloglog"]


def test_schema_metadata_none_when_no_properties() -> None:
    entry = ODCSSchemaObject.model_validate({"name": "t"})
    result = build_schema_metadata(entry)
    assert result.schema_metadata is None


# ---------------------------------------------------------------------------
# Logical dataset aspects
# ---------------------------------------------------------------------------


def test_logical_dataset_mcps_carry_provenance_and_schema() -> None:
    contract = _make_contract(
        name="Contract Name",
        schema=[
            {
                "name": "t",
                "physicalName": "t_phys",
                "properties": [{"name": "id", "logicalType": "number"}],
            }
        ],
        description={"purpose": "P", "usage": "U"},
    )
    mcps, unmapped = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
        source_file="c.odcs.yaml",
    )
    assert unmapped == []
    props = next(m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass))
    assert props.customProperties["odcs.id"] == "c1"
    assert props.customProperties["odcs.schemaName"] == "t"
    assert props.customProperties["odcs.physicalName"] == "t_phys"
    assert props.customProperties["odcs.sourceFile"] == "c.odcs.yaml"
    assert props.name == "Contract Name — t"
    assert props.description is not None and "**purpose**: P" in props.description


def test_schema_description_wins_over_contract_description() -> None:
    contract = _make_contract(
        schema=[{"name": "t", "description": "Table-level description."}],
        description="Contract-level description.",
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )
    props = next(m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass))
    assert props.description == "Table-level description."


def test_contract_description_is_fallback() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}], description="Contract-level description."
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )
    props = next(m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass))
    assert props.description == "Contract-level description."


def test_institutional_memory_includes_root_authoritative_definitions() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "authoritativeDefinitions": [
                    {"url": "https://schema.example/def", "type": "businessDefinition"}
                ],
            }
        ],
        authoritative_definitions=[
            {"url": "https://root.example/contract", "type": "implementation"}
        ],
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )
    memory = next(
        m.aspect for m in mcps if isinstance(m.aspect, InstitutionalMemoryClass)
    )
    urls = [e.url for e in memory.elements]
    assert "https://root.example/contract" in urls
    assert "https://schema.example/def" in urls


def test_replicate_false_skips_ownership() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[{"username": "alice", "role": "owner"}],
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
        replicate_contract_metadata=False,
    )
    from datahub.metadata.schema_classes import OwnershipClass

    assert not [m for m in mcps if isinstance(m.aspect, OwnershipClass)]


def test_owners_roles_dedup_and_dateout() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[
            {"username": "alice", "role": "owner"},
            {"username": "bob", "role": "dataSteward"},
            {"username": "bob", "role": "dataSteward"},
            {"username": "dave", "role": "owner", "dateOut": "2024-01-01"},
            {"username": "erin", "role": "someUnmappedRole"},
        ],
    )
    owners = _make_owners(contract)
    by_urn = {o.owner: o.type for o in owners}
    assert by_urn["urn:li:corpuser:alice"] == OwnershipTypeClass.TECHNICAL_OWNER
    assert by_urn["urn:li:corpuser:bob"] == OwnershipTypeClass.DATA_STEWARD
    assert by_urn["urn:li:corpuser:erin"] == OwnershipTypeClass.TECHNICAL_OWNER
    assert "urn:li:corpuser:dave" not in by_urn
    assert len(owners) == 3


def test_unmapped_owner_roles_surfaces_only_named_unknown_roles() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[
            {"username": "alice", "role": "owner"},  # mapped -> excluded
            {"username": "bob"},  # no role -> legitimate default, excluded
            {"username": "carol", "role": "producer"},  # named unknown -> reported
            {"username": "dan", "role": "producer"},  # dedup
            {"username": "eve", "role": "consumer"},  # named unknown -> reported
            {
                "username": "frank",
                "role": "approver",
                "dateOut": "2024-01-01",  # departed -> excluded
            },
        ],
    )
    assert unmapped_owner_roles(contract) == ["consumer", "producer"]


def test_owner_strip_email_domain() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[
            {"username": "alice@acme.example", "role": "owner"},
            {"username": "bob", "role": "owner"},
            {"username": "urn:li:corpGroup:data-eng", "role": "owner"},
        ],
    )
    owners = _make_owners(contract, strip_owner_email_domain=True)
    urns = {o.owner for o in owners}
    # Email loses its domain, bare username and explicit URN pass through.
    assert urns == {
        "urn:li:corpuser:alice",
        "urn:li:corpuser:bob",
        "urn:li:corpGroup:data-eng",
    }


def test_owner_append_email_domain() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[
            {"username": "alice", "role": "owner"},
            {"username": "bob@other.example", "role": "owner"},
            {"username": "urn:li:corpuser:carol", "role": "owner"},
        ],
    )
    owners = _make_owners(contract, owner_email_domain="acme.example")
    urns = {o.owner for o in owners}
    # Bare username gains the domain; existing email and explicit URN untouched.
    assert urns == {
        "urn:li:corpuser:alice@acme.example",
        "urn:li:corpuser:bob@other.example",
        "urn:li:corpuser:carol",
    }


def test_owner_no_normalization_passthrough() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        team=[{"username": "alice@acme.example", "role": "owner"}],
    )
    owners = _make_owners(contract)
    assert owners[0].owner == "urn:li:corpuser:alice@acme.example"


def test_logical_parent_links_physical_to_logical() -> None:
    physical = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.t,PROD)"
    mcp = odcs_to_logical_parent_mcp(physical, LOGICAL_URN)
    assert mcp.entityUrn == physical
    assert isinstance(mcp.aspect, LogicalParentClass)
    assert mcp.aspect.parent is not None
    assert mcp.aspect.parent.destinationUrn == LOGICAL_URN


def test_platform_info_registers_odcs() -> None:
    mcp = odcs_platform_info_mcp()
    assert isinstance(mcp.aspect, DataPlatformInfoClass)
    assert mcp.aspect.name == "odcs"
    assert mcp.aspect.displayName == "Open Data Contract Standard"


# ---------------------------------------------------------------------------
# Quality routing — library metrics (spec-exact)
# ---------------------------------------------------------------------------


def test_null_values_must_be_zero_builds_not_null() -> None:
    urns, mcps, trace = _route_single({"metric": "nullValues", "mustBe": 0})
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.FIELD
    fa = info.fieldAssertion
    assert fa is not None
    assert fa.type == FieldAssertionTypeClass.FIELD_VALUES
    assert fa.entity == LOGICAL_URN
    fva = fa.fieldValuesAssertion
    assert fva is not None
    assert fva.operator == AssertionStdOperatorClass.NOT_NULL
    assert not fva.excludeNulls
    assert fva.failThreshold.type == FieldValuesFailThresholdTypeClass.COUNT
    assert fva.failThreshold.value == 0
    assert not trace.routed_to_custom


def test_null_values_count_threshold_builds_null_count_metric() -> None:
    _, mcps, _ = _route_single({"metric": "nullValues", "mustBeLessOrEqualTo": 5})
    info = _single_info(mcps)
    fa = info.fieldAssertion
    assert fa is not None and fa.type == FieldAssertionTypeClass.FIELD_METRIC
    fma = fa.fieldMetricAssertion
    assert fma is not None
    assert fma.metric == FieldMetricTypeClass.NULL_COUNT
    assert fma.operator == AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO


def test_null_values_percent_threshold_builds_null_percentage_metric() -> None:
    _, mcps, _ = _route_single(
        {"metric": "nullValues", "mustBeLessThan": 1, "unit": "percent"}
    )
    fma = _field_metric(_single_info(mcps))
    assert fma.metric == FieldMetricTypeClass.NULL_PERCENTAGE
    assert fma.operator == AssertionStdOperatorClass.LESS_THAN


def test_null_values_without_column_routes_to_custom() -> None:
    _, mcps, trace = _route_single(
        {"metric": "nullValues", "mustBe": 0}, on_column=None
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert trace.routed_to_custom


@pytest.mark.parametrize("metric_name", ["duplicateValues", "duplicateCount"])
def test_duplicate_values_zero_builds_unique_percentage(metric_name: str) -> None:
    _, mcps, _ = _route_single({"metric": metric_name, "mustBe": 0})
    fa = _single_info(mcps).fieldAssertion
    assert fa is not None
    fma = fa.fieldMetricAssertion
    assert fma is not None
    assert fma.metric == FieldMetricTypeClass.UNIQUE_PERCENTAGE
    assert fma.operator == AssertionStdOperatorClass.EQUAL_TO
    assert _param(fma.parameters).value == "100"


def test_duplicate_values_tolerance_routes_to_custom() -> None:
    _, mcps, trace = _route_single({"metric": "duplicateValues", "mustBeLessThan": 10})
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert "duplicateValues" in _custom_logic(info)
    assert "mustBeLessThan" in _custom_logic(info)


def test_duplicate_values_multi_column_routes_to_custom() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "duplicateValues",
            "mustBe": 0,
            "arguments": {"properties": ["a", "b"]},
        },
        on_column=None,
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM


def test_invalid_values_no_threshold_builds_in_with_zero_tolerance() -> None:
    _, mcps, _ = _route_single(
        {"metric": "invalidValues", "arguments": {"validValues": ["a", "b"]}}
    )
    fva = _field_values(_single_info(mcps))
    assert fva.operator == AssertionStdOperatorClass.IN
    assert fva.excludeNulls
    assert fva.failThreshold.value == 0
    param = _param(fva.parameters)
    assert param.type == AssertionStdParameterTypeClass.SET
    assert json.loads(param.value) == ["a", "b"]


def test_invalid_values_count_tolerance_maps_exactly() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "invalidValues",
            "arguments": {"validValues": ["a"]},
            "mustBeLessOrEqualTo": 5,
            "unit": "rows",
        }
    )
    fva = _field_values(_single_info(mcps))
    assert fva.failThreshold.type == FieldValuesFailThresholdTypeClass.COUNT
    assert fva.failThreshold.value == 5


def test_invalid_values_percent_tolerance_maps_exactly() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "invalidValues",
            "arguments": {"validValues": ["a"]},
            "mustBeLessOrEqualTo": 2,
            "unit": "percent",
        }
    )
    fva = _field_values(_single_info(mcps))
    assert fva.failThreshold.type == FieldValuesFailThresholdTypeClass.PERCENTAGE
    assert fva.failThreshold.value == 2


def test_invalid_values_unrepresentable_tolerance_routes_to_custom() -> None:
    # Strict less-than has no exact failThreshold representation.
    _, mcps, trace = _route_single(
        {
            "metric": "invalidValues",
            "arguments": {"validValues": ["a"]},
            "mustBeLessThan": 5,
        }
    )
    assert _single_info(mcps).type == AssertionTypeClass.CUSTOM
    assert trace.routed_to_custom


def test_invalid_values_pattern_builds_regex_match() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "invalidValues",
            "mustBe": 0,
            "arguments": {"pattern": "^[A-Z]+$"},
        }
    )
    fva = _field_values(_single_info(mcps))
    assert fva.operator == AssertionStdOperatorClass.REGEX_MATCH
    assert _param(fva.parameters).value == "^[A-Z]+$"
    assert _param(fva.parameters).type == AssertionStdParameterTypeClass.STRING


def test_invalid_values_pattern_and_valid_values_routes_to_custom() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "invalidValues",
            "arguments": {"validValues": ["a"], "pattern": "x"},
        }
    )
    assert _single_info(mcps).type == AssertionTypeClass.CUSTOM


def test_v30_valid_values_rule_with_direct_list_builds_in() -> None:
    _, mcps, _ = _route_single(
        {"rule": "validValues", "validValues": ["pounds", "kg"]},
        api_version="v3.0.2",
    )
    fva = _field_values(_single_info(mcps))
    assert fva.operator == AssertionStdOperatorClass.IN
    assert json.loads(_param(fva.parameters).value) == ["pounds", "kg"]


def test_missing_values_routes_to_custom_preserving_arguments() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "missingValues",
            "arguments": {"missingValues": [None, "", "N/A"]},
            "mustBeLessOrEqualTo": 100,
        }
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert "missingValues" in _custom_logic(info)
    assert info.customProperties["odcs.rule.arguments"] == json.dumps(
        {"missingValues": [None, "", "N/A"]}, sort_keys=True
    )


def test_row_count_builds_volume_assertion() -> None:
    _, mcps, _ = _route_single(
        {"metric": "rowCount", "mustBeGreaterOrEqualTo": 100}, on_column=None
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.VOLUME
    va = info.volumeAssertion
    assert va is not None
    assert va.entity == LOGICAL_URN
    assert va.rowCountTotal is not None
    assert va.rowCountTotal.operator == (
        AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    )


def test_row_count_percent_routes_to_custom() -> None:
    _, mcps, _ = _route_single(
        {"metric": "rowCount", "mustBeLessThan": 5, "unit": "percent"},
        on_column=None,
    )
    assert _single_info(mcps).type == AssertionTypeClass.CUSTOM


def test_must_not_be_between_routes_to_custom_with_rendered_logic() -> None:
    _, mcps, _ = _route_single(
        {"metric": "rowCount", "mustNotBeBetween": [1, 10]}, on_column=None
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert "mustNotBeBetween" in _custom_logic(info)


def test_unknown_metric_routes_to_custom() -> None:
    _, mcps, trace = _route_single({"metric": "entropy", "mustBeLessThan": 3})
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert _custom(info).type == "entropy"
    assert trace.routed_to_custom


# ---------------------------------------------------------------------------
# Quality routing — sql / custom / text types
# ---------------------------------------------------------------------------


def test_sql_rule_with_query_and_threshold_builds_sql_assertion() -> None:
    _, mcps, _ = _route_single(
        {"type": "sql", "query": "SELECT COUNT(*) FROM t", "mustBe": 0},
        on_column=None,
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.SQL
    sql = info.sqlAssertion
    assert sql is not None
    assert sql.entity == LOGICAL_URN
    assert sql.statement == "SELECT COUNT(*) FROM t"
    assert sql.operator == AssertionStdOperatorClass.EQUAL_TO


def test_sql_rule_without_threshold_routes_to_custom_with_query_logic() -> None:
    _, mcps, _ = _route_single({"type": "sql", "query": "SELECT 1"}, on_column=None)
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert _custom_logic(info) == "SELECT 1"


def test_custom_rule_uses_engine_and_implementation() -> None:
    _, mcps, _ = _route_single(
        {
            "type": "custom",
            "engine": "soda",
            "implementation": "checks for t:\n  - row_count > 0",
        },
        on_column=None,
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert _custom(info).type == "soda"
    assert "row_count > 0" in _custom_logic(info)


def test_text_rule_uses_description_as_logic() -> None:
    _, mcps, _ = _route_single(
        {"type": "text", "description": "Data must be fresh."}, on_column=None
    )
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert _custom_logic(info) == "Data must be fresh."


def test_rule_with_no_body_is_skipped() -> None:
    urns, mcps, trace = _route_single({}, on_column=None)
    assert urns == []
    assert mcps == []
    assert trace.skipped_no_body


def test_deprecated_rule_key_recorded_for_v31_docs() -> None:
    _, _, trace = _route_single(
        {"rule": "rowCount", "mustBeGreaterThan": 0}, on_column=None
    )
    assert trace.deprecated_rule_key
    _, _, trace_30 = _route_single(
        {"rule": "rowCount", "mustBeGreaterThan": 0},
        on_column=None,
        api_version="v3.0.2",
    )
    assert not trace_30.deprecated_rule_key


# ---------------------------------------------------------------------------
# Assertion identity, provenance, platform attribution
# ---------------------------------------------------------------------------


def test_assertion_urn_seeded_by_rule_id_is_stable_across_renames() -> None:
    urns_a, _, _ = _route_single(
        {"id": "q1", "name": "first name", "metric": "nullValues", "mustBe": 0}
    )
    urns_b, _, _ = _route_single(
        {"id": "q1", "name": "renamed", "metric": "nullValues", "mustBe": 0}
    )
    assert urns_a == urns_b


def test_assertion_urn_differs_across_contracts_with_same_rule_id() -> None:
    rule = {"id": "q1", "metric": "nullValues", "mustBe": 0}
    schema = [{"name": "t", "properties": [{"name": "col", "quality": [rule]}]}]
    urns = []
    for cid in ("c1", "c2"):
        contract = _make_contract(contract_id=cid, schema=schema)
        contract_urns, _, _ = odcs_to_assertion_mcps(
            contract=contract,
            schema_entry=_first_schema(contract),
            logical_urn=f"urn:li:dataset:(urn:li:dataPlatform:odcs,{cid}.t,PROD)",
        )
        urns.extend(contract_urns)
    assert len(set(urns)) == 2


def test_assertion_provenance_properties() -> None:
    _, mcps, _ = _route_single(
        {
            "id": "q9",
            "name": "n",
            "metric": "nullValues",
            "mustBe": 0,
            "unit": "rows",
            "severity": "high",
        }
    )
    info = _single_info(mcps)
    props = info.customProperties
    assert props["odcs.id"] == "c1"
    assert props["odcs.rule.id"] == "q9"
    assert props["odcs.rule.metric"] == "nullValues"
    assert props["odcs.rule.unit"] == "rows"
    assert props["odcs.rule.severity"] == "high"
    assert props["odcs.scope"] == "property"


def test_every_assertion_gets_platform_instance_aspect() -> None:
    urns, mcps, _ = _route_single({"metric": "nullValues", "mustBe": 0})
    platform_aspects = [
        m for m in mcps if isinstance(m.aspect, DataPlatformInstanceClass)
    ]
    assert len(urns) == 1
    assert len(platform_aspects) == 1
    assert platform_aspects[0].entityUrn == urns[0]
    assert "odcs" in platform_aspects[0].aspect.platform


def test_assertion_external_url_from_authoritative_definitions() -> None:
    _, mcps, _ = _route_single(
        {
            "metric": "nullValues",
            "mustBe": 0,
            "authoritativeDefinitions": [
                {"url": "https://rules.example/q1", "type": "definition"}
            ],
        }
    )
    assert _single_info(mcps).externalUrl == "https://rules.example/q1"


# ---------------------------------------------------------------------------
# Schema-compliance (DATA_SCHEMA) assertion
# ---------------------------------------------------------------------------


def test_schema_assertion_pins_contract_schema_on_logical_dataset() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "properties": [{"name": "id", "logicalType": "number"}],
            }
        ]
    )
    urn, mcps = odcs_to_schema_assertion_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
        compatibility="SUPERSET",
    )
    assert urn is not None
    info = _single_info(mcps)
    assert info.type == AssertionTypeClass.DATA_SCHEMA
    sa = info.schemaAssertion
    assert sa is not None
    assert sa.entity == LOGICAL_URN
    assert sa.compatibility == SchemaAssertionCompatibilityClass.SUPERSET
    assert [f.fieldPath for f in sa.schema.fields] == ["id"]
    # dataPlatformInstance rides along.
    assert any(isinstance(m.aspect, DataPlatformInstanceClass) for m in mcps)


def test_schema_assertion_skipped_without_properties() -> None:
    contract = _make_contract(schema=[{"name": "t"}])
    urn, mcps = odcs_to_schema_assertion_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
        compatibility="EXACT_MATCH",
    )
    assert urn is None
    assert mcps == []


# ---------------------------------------------------------------------------
# Threshold translation
# ---------------------------------------------------------------------------


def test_threshold_operator_mapping_covers_all_mustbe_forms() -> None:
    cases = [
        ({"mustBeBetween": [1, 2]}, AssertionStdOperatorClass.BETWEEN),
        ({"mustBeGreaterThan": 1}, AssertionStdOperatorClass.GREATER_THAN),
        (
            {"mustBeGreaterOrEqualTo": 1},
            AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        ),
        ({"mustBeLessThan": 1}, AssertionStdOperatorClass.LESS_THAN),
        (
            {"mustBeLessOrEqualTo": 1},
            AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
        ),
        ({"mustBe": 5}, AssertionStdOperatorClass.EQUAL_TO),
        ({"mustNotBe": 5}, AssertionStdOperatorClass.NOT_EQUAL_TO),
    ]
    for fields, expected_op in cases:
        rule = ODCSQualityRule.model_validate(fields)
        op, params = _operator_and_params_from_threshold(rule)
        assert op == expected_op, fields
        assert params is not None

    # No native operator cases.
    for fields in ({"mustNotBeBetween": [1, 2]}, {}, {"mustBe": "a-string"}):
        rule = ODCSQualityRule.model_validate(fields)
        op, params = _operator_and_params_from_threshold(rule)
        assert op is None and params is None, fields


def test_boolean_must_be_is_not_treated_as_number() -> None:
    rule = ODCSQualityRule.model_validate({"mustBe": True})
    op, params = _operator_and_params_from_threshold(rule)
    assert op is None and params is None
