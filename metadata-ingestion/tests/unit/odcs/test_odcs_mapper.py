"""Unit tests for the ODCS mapper — spec-exact routing, logical-dataset targeting."""

import json
from typing import Any, Dict, List, Optional, Type, cast

import pytest
from pydantic import BaseModel

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.odcs.odcs_config import (
    ODCSSourceConfig,
    SchemaAssertionCompatibility,
)
from datahub.ingestion.source.odcs.odcs_mapper import (
    RefusalKind,
    _is_storable_link_url,
    _make_owners,
    _operator_and_params_from_threshold,
    build_schema_metadata,
    odcs_to_assertion_mcps,
    odcs_to_data_product_urn,
    odcs_to_logical_dataset_mcps,
    odcs_to_logical_dataset_urn,
    odcs_to_logical_parent_mcp,
    odcs_to_physical_bindings,
    odcs_to_schema_assertion_mcps,
    substitute_object_placeholder,
    unmapped_owner_roles,
    unstorable_authoritative_definition_urls,
)
from datahub.ingestion.source.odcs.odcs_models import (
    LENIENT_FIELDS_BY_MODEL,
    ODCSContext,
    ODCSContract,
    ODCSEnumValue,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
    ODCSSynonym,
)
from datahub.metadata.schema_classes import (
    AiContextClass,
    ArrayTypeClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    BooleanTypeClass,
    CustomAssertionInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DeprecationClass,
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
from datahub.utilities.urns.error import InvalidUrnError

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


def _logical_description(contract: ODCSContract) -> Optional[str]:
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )
    props = next(m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass))
    return props.description


def test_dataset_description_combines_table_grain_and_contract() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "description": "One row per order.",
                "dataGranularityDescription": "One row per order_id.",
            }
        ],
        description={"purpose": "Revenue analysis.", "usage": "Join on order_id."},
    )
    assert _logical_description(contract) == (
        "One row per order.\n\n**Grain:** One row per order_id.\n\n"
        "**purpose**: Revenue analysis.\n\n**usage**: Join on order_id."
    )


def test_dataset_description_blank_grain_ignored() -> None:
    contract = _make_contract(
        schema=[{"name": "t", "description": "T.", "dataGranularityDescription": "   "}]
    )
    assert _logical_description(contract) == "T."


def test_grain_off_spec_value_dropped() -> None:
    entry = ODCSSchemaObject.model_validate(
        {"name": "t", "dataGranularityDescription": {"x": 1}}
    )
    assert entry.dataGranularityDescription is None


def test_contract_description_alone_when_table_has_none() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}], description="Contract-level description."
    )
    assert _logical_description(contract) == "Contract-level description."


def test_description_object_renders_non_spec_extra_prose_keys() -> None:
    contract = _make_contract(
        schema=[{"name": "t"}],
        description={"purpose": "P", "summary": "S", "authoritativeDefinitions": []},
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )
    props = next(m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass))
    assert props.description is not None
    assert "**purpose**: P" in props.description
    assert "**summary**: S" in props.description
    assert "authoritativeDefinitions" not in props.description


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


def test_institutional_memory_skips_urls_gms_rejects() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "authoritativeDefinitions": [
                    {"url": "jdbc:postgresql://db.example/t", "type": "implementation"}
                ],
                "properties": [
                    {
                        "name": "col",
                        "logicalType": "string",
                        "authoritativeDefinitions": [
                            {"url": "docs/col.md", "type": "businessDefinition"},
                            {"url": "https://example.com/def", "type": "glossary"},
                        ],
                    }
                ],
            }
        ],
        authoritative_definitions=[
            {"url": "../docs/orders.md", "type": "implementation"}
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
    assert [e.url for e in memory.elements] == ["https://example.com/def"]
    assert unstorable_authoritative_definition_urls(contract) == [
        "../docs/orders.md",
        "docs/col.md",
        "jdbc:postgresql://db.example/t",
    ]


@pytest.mark.parametrize(
    "url, storable",
    [
        ("HTTPS://Example.com/x", True),
        ("mailto:data-team@example.com", True),
        ("ftp://files.example.com/spec.pdf", True),
        ("jdbc:postgresql://db.example/t", False),
        ("../docs/x.md", False),
        ("javascript:alert(1)", False),
        ("https://[2001:db8::1/x", False),
        ("https://wiki.example/My Page", False),
        ("https://grafana.example/d?var={a}", False),
        ("https://wiki.example/My\u00a0Page", False),
    ],
)
def test_is_storable_link_url(url: str, storable: bool) -> None:
    assert _is_storable_link_url(url) == storable


def test_institutional_memory_omitted_when_every_url_skipped() -> None:
    # An empty institutionalMemory UPSERT would wipe links added in the UI.
    # `https://[2001:db8::1/x` makes urlparse raise; it must be skipped, not
    # abort the contract.
    contract = _make_contract(
        schema=[{"name": "t"}],
        authoritative_definitions=[
            {"url": "../docs/orders.md", "type": "implementation"},
            {"url": "https://[2001:db8::1/x", "type": "implementation"},
        ],
    )
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
    )
    assert not any(isinstance(m.aspect, InstitutionalMemoryClass) for m in mcps)
    assert unstorable_authoritative_definition_urls(contract) == [
        "../docs/orders.md",
        "https://[2001:db8::1/x",
    ]


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


def test_unknown_argument_keys_survive_into_provenance() -> None:
    # `arguments` is typed but allows extras so engine-specific keys are not
    # silently dropped -- they still round-trip into the assertion provenance.
    _, mcps, _ = _route_single(
        {
            "metric": "missingValues",
            "arguments": {"missingValues": ["N/A"], "engineOption": "strict"},
        }
    )
    info = _single_info(mcps)
    assert info.customProperties["odcs.rule.arguments"] == json.dumps(
        {"engineOption": "strict", "missingValues": ["N/A"]}, sort_keys=True
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
        compatibility=SchemaAssertionCompatibility.SUPERSET,
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
        compatibility=SchemaAssertionCompatibility.EXACT_MATCH,
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


def test_data_product_urn_takes_id_or_full_urn() -> None:
    assert odcs_to_data_product_urn("orders_product") == (
        "urn:li:dataProduct:orders_product"
    )
    assert odcs_to_data_product_urn(" urn:li:dataProduct:orders_product ") == (
        "urn:li:dataProduct:orders_product"
    )


def test_data_product_display_name_cannot_be_an_id() -> None:
    with pytest.raises(InvalidUrnError):
        odcs_to_data_product_urn("Orders, Retail (EU)")


def _logical_mcps(schema: List[Dict[str, Any]]) -> List[Any]:
    contract = _make_contract(api_version="v3.2.0", schema=schema)
    mcps, _ = odcs_to_logical_dataset_mcps(
        contract, _first_schema(contract), LOGICAL_URN
    )
    return mcps


def _aspects_for(mcps: List[Any], urn: str, cls: type) -> List[Any]:
    return [m.aspect for m in mcps if m.entityUrn == urn and isinstance(m.aspect, cls)]


def test_vector_maps_to_array_type() -> None:
    contract = _make_contract(
        api_version="v3.2.0",
        schema=[
            {
                "name": "t",
                "properties": [
                    {
                        "name": "col_a",
                        "logicalType": "vector",
                        "physicalType": "vector(3)",
                    }
                ],
            }
        ],
    )
    result = build_schema_metadata(_first_schema(contract))
    assert result.schema_metadata is not None
    field = result.schema_metadata.fields[0]
    assert isinstance(field.type.type, ArrayTypeClass)
    assert field.nativeDataType == "vector(3)"
    assert result.unmapped_types == []


def test_deprecated_schema_and_properties_emit_deprecation() -> None:
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "deprecated": True,
                "properties": [
                    {"name": "col_a", "deprecated": True},
                    {
                        "name": "obj",
                        "properties": [{"name": "col_b", "deprecated": True}],
                    },
                    {"name": "col_c"},
                ],
            }
        ]
    )
    (dataset_dep,) = _aspects_for(mcps, LOGICAL_URN, DeprecationClass)
    assert dataset_dep.deprecated is True
    assert dataset_dep.actor == "urn:li:corpuser:datahub"
    assert "c1" in dataset_dep.note
    for path in ("col_a", "obj.col_b"):
        field_urn = make_schema_field_urn(LOGICAL_URN, path)
        assert len(_aspects_for(mcps, field_urn, DeprecationClass)) == 1
    assert not _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_c"), DeprecationClass
    )


def test_explicit_deprecated_false_clears_deprecation() -> None:
    """An explicit `deprecated: false` is a deliberate statement and clears an
    earlier deprecation; an absent key emits nothing."""
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "deprecated": False,
                "properties": [
                    {"name": "col_a", "deprecated": False},
                    {"name": "col_b"},
                ],
            }
        ]
    )
    (dataset_dep,) = _aspects_for(mcps, LOGICAL_URN, DeprecationClass)
    assert dataset_dep.deprecated is False
    (field_dep,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), DeprecationClass
    )
    assert field_dep.deprecated is False
    assert not _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_b"), DeprecationClass
    )


def test_absent_deprecated_emits_nothing() -> None:
    mcps = _logical_mcps([{"name": "t", "properties": [{"name": "col_a"}]}])
    assert not [m for m in mcps if isinstance(m.aspect, DeprecationClass)]


def test_property_synonyms_emit_field_ai_context() -> None:
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "properties": [
                    {
                        "name": "col_a",
                        "synonyms": [
                            {"synonym": "alias_a", "locale": "en"},
                            "alias_b",
                            {"description": "entry missing synonym"},
                        ],
                    },
                    {"name": "col_b", "synonyms": []},
                ],
            }
        ]
    )
    (ai,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), AiContextClass
    )
    assert ai.synonyms == ["alias_a", "alias_b"]
    # A present-but-empty key clears earlier synonyms.
    (cleared,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_b"), AiContextClass
    )
    assert cleared.synonyms == []


@pytest.mark.parametrize(
    "server_db,physical_name",
    [
        ("${DB}", "tbl"),  # variable in a composed server field
        ("${DB:-analytics}", "tbl"),  # POSIX default form is still unresolved
        ("analytics", "${TABLE}"),  # variable in the table name
        ("analytics", "${DB}.public.tbl"),  # dotted pass-through name
    ],
)
def test_binding_unresolved_variable_leaves_unbound(
    server_db: str, physical_name: str
) -> None:
    contract = _make_contract(
        api_version="v3.2.0",
        servers=[
            {
                "server": "srv",
                "type": "postgres",
                "database": server_db,
                "schema": "public",
            }
        ],
        schema=[{"name": "t1", "physicalName": physical_name}],
    )
    bindings = odcs_to_physical_bindings(contract, _config())
    assert bindings[0].physical_urn is None
    assert bindings[0].unmapped_reason is not None
    assert "unresolved ODCS variable" in bindings[0].unmapped_reason
    assert bindings[0].unresolved_variable


def test_schema_assertion_excludes_measures() -> None:
    # A v3.2 measure is an aggregate (e.g. SUM(amount)), not a physical column,
    # so asserting it exists on the physical table would always fail.
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "properties": [
                    {"name": "region", "semanticType": "dimension"},
                    {
                        "name": "total_amount",
                        "semanticType": "measure",
                        "transformLogic": "SUM(amount)",
                    },
                    {"name": "amount", "logicalType": "number"},
                ],
            }
        ]
    )
    _, mcps = odcs_to_schema_assertion_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
        compatibility=SchemaAssertionCompatibility.SUPERSET,
    )
    sa = _single_info(mcps).schemaAssertion
    assert sa is not None
    assert [f.fieldPath for f in sa.schema.fields] == ["region", "amount"]


def test_deprecated_synonyms_are_not_emitted() -> None:
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "properties": [
                    {
                        "name": "col_a",
                        "synonyms": [
                            {"synonym": "current"},
                            {"synonym": "retired", "status": "Deprecated"},
                        ],
                    },
                    {
                        "name": "col_b",
                        "synonyms": [{"synonym": "old", "status": "deprecated"}],
                    },
                ],
            }
        ]
    )
    (ai,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), AiContextClass
    )
    assert ai.synonyms == ["current"]
    (only_deprecated,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_b"), AiContextClass
    )
    assert only_deprecated.synonyms == []


@pytest.mark.parametrize(
    "server,expected_urn",
    [
        (
            {"type": "glue", "account": "123456789012", "database": "sales"},
            "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)",
        ),
        (
            # Iceberg REST catalog served by AWS Glue: namespace = Glue database.
            {
                "type": "iceberg",
                "catalog": "main",
                "catalogUrl": "https://glue.eu-west-1.amazonaws.com/iceberg",
                "namespace": "sales",
            },
            "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)",
        ),
        (
            {
                "type": "iceberg",
                "catalog": "main",
                "catalogUrl": "https://polaris.example.com/api/catalog",
                "namespace": "sales",
            },
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales.orders,PROD)",
        ),
    ],
)
def test_binding_glue_and_iceberg_servers(
    server: Dict[str, Any], expected_urn: str
) -> None:
    contract = _make_contract(
        servers=[{"server": "lake", **server}],
        schema=[{"name": "t", "physicalName": "orders"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn == expected_urn


def test_binding_iceberg_without_namespace_leaves_unbound() -> None:
    contract = _make_contract(
        servers=[
            {
                "server": "lake",
                "type": "iceberg",
                "catalog": "main",
                "catalogUrl": "https://polaris.example.com/api/catalog",
            }
        ],
        schema=[{"name": "t", "physicalName": "orders"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn is None
    assert binding.unmapped_reason and "namespace" in binding.unmapped_reason


def test_non_string_synonym_status_does_not_fail_the_contract() -> None:
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "properties": [
                    {
                        "name": "col_a",
                        "synonyms": [
                            {"synonym": "a", "status": 1},
                            {"synonym": "b", "status": True},
                        ],
                    }
                ],
            }
        ]
    )
    (ai,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), AiContextClass
    )
    assert ai.synonyms == ["a", "b"]


@pytest.mark.parametrize(
    "catalog_url",
    [
        "https://glue-fips.us-east-1.amazonaws.com/iceberg",
        "https://glue.cn-north-1.amazonaws.com.cn/iceberg",
    ],
)
def test_binding_iceberg_on_glue_endpoint_variants(catalog_url: str) -> None:
    contract = _make_contract(
        servers=[
            {
                "server": "lake",
                "type": "iceberg",
                "catalog": "main",
                "catalogUrl": catalog_url,
                "namespace": "sales",
            }
        ],
        schema=[{"name": "t", "physicalName": "orders"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)"
    )


def test_binding_iceberg_catalog_url_variable_leaves_unbound() -> None:
    contract = _make_contract(
        servers=[
            {
                "server": "lake",
                "type": "iceberg",
                "catalog": "main",
                "catalogUrl": "${CATALOG_URL}",
                "namespace": "sales",
            }
        ],
        schema=[{"name": "t", "physicalName": "orders"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn is None
    assert binding.unmapped_reason and "catalogUrl" in binding.unmapped_reason


def test_binding_glue_names_are_lowercased() -> None:
    # Glue stores database and table names in lowercase.
    contract = _make_contract(
        servers=[{"server": "g", "type": "glue", "database": "Sales"}],
        schema=[{"name": "t", "physicalName": "Orders"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)"
    )


def test_property_without_synonyms_key_emits_no_ai_context() -> None:
    mcps = _logical_mcps([{"name": "t", "properties": [{"name": "col_a"}]}])
    assert not _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), AiContextClass
    )


def test_malformed_synonyms_and_deprecated_never_fail_the_contract() -> None:
    # Lenient mode already reports these as JSON Schema warnings; the model
    # drops them instead of rejecting the whole contract.
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "deprecated": "maybe",
                "properties": [
                    {
                        "name": "col_a",
                        "deprecated": "maybe",
                        "semanticType": 5,
                        "synonyms": [42, None, {"synonym": 5}, "ok", ["x"]],
                    },
                    {"name": "col_b", "synonyms": "abc"},
                ],
            }
        ]
    )
    (ai,) = _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), AiContextClass
    )
    assert ai.synonyms == ["ok"]
    assert not [m for m in mcps if isinstance(m.aspect, DeprecationClass)]
    assert not _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_b"), AiContextClass
    )


def test_measures_stay_in_logical_schema_and_nested_ones_leave_the_assertion() -> None:
    contract = _make_contract(
        api_version="v3.2.0",
        schema=[
            {
                "name": "t",
                "properties": [
                    {"name": "total", "semanticType": "measure"},
                    {
                        "name": "s",
                        "logicalType": "object",
                        "properties": [
                            {"name": "a", "logicalType": "number"},
                            {"name": "avg_a", "semanticType": "Measure"},
                        ],
                    },
                ],
            }
        ],
    )
    schema_entry = _first_schema(contract)
    logical = build_schema_metadata(schema_entry).schema_metadata
    assert logical is not None
    assert {"total", "s.avg_a"} <= {f.fieldPath for f in logical.fields}
    _, mcps = odcs_to_schema_assertion_mcps(
        contract=contract,
        schema_entry=schema_entry,
        logical_urn=LOGICAL_URN,
        compatibility=SchemaAssertionCompatibility.SUPERSET,
    )
    sa = _single_info(mcps).schemaAssertion
    assert sa is not None
    assert [f.fieldPath for f in sa.schema.fields] == ["s", "s.a"]


def _iceberg_binding(
    *, server: Dict[str, Any], physical_name: str = "orders", **config: Any
) -> Any:
    contract = _make_contract(
        servers=[{"server": "lake", **server}],
        schema=[{"name": "t", "physicalName": physical_name}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config(**config))
    return binding


def test_binding_variable_server_type_is_refused_not_skipped() -> None:
    contract = _make_contract(
        servers=[
            {"server": "prod", "type": "${PLATFORM}", "database": "prod_db"},
            {"server": "dev", "type": "postgres", "database": "dev_db", "schema": "s"},
        ],
        schema=[{"name": "t"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn is None
    assert binding.unresolved_variable
    assert binding.unmapped_reason and "${PLATFORM}" in binding.unmapped_reason


@pytest.mark.parametrize(
    "warehouse,expected_urn",
    [
        (None, "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)"),
        (
            "123456789012",
            "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)",
        ),
        ("123456789012:s3tablescatalog/bucket", None),
    ],
)
def test_binding_iceberg_on_glue_only_for_the_default_catalog(
    warehouse: Optional[str], expected_urn: Optional[str]
) -> None:
    server: Dict[str, Any] = {
        "type": "iceberg",
        "catalogUrl": "https://glue.us-east-1.amazonaws.com/iceberg",
        "namespace": "sales",
    }
    if warehouse is not None:
        server["warehouse"] = warehouse
    binding = _iceberg_binding(server=server)
    assert binding.physical_urn == expected_urn
    if expected_urn is None:
        assert binding.unmapped_reason and "warehouse" in binding.unmapped_reason


@pytest.mark.parametrize(
    "catalog_url",
    [
        "https://glue.us-east-1.amazonaws.com.attacker.example/iceberg",
        "https://s3tables.us-east-1.amazonaws.com/iceberg",
    ],
)
def test_binding_iceberg_non_glue_hosts_bind_to_iceberg(catalog_url: str) -> None:
    binding = _iceberg_binding(
        server={"type": "iceberg", "catalogUrl": catalog_url, "namespace": "sales"}
    )
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales.orders,PROD)"
    )


@pytest.mark.parametrize(
    "catalog_url,expected_name",
    [
        ("https://glue.us-east-1.amazonaws.com/iceberg", "glue,sales.orders"),
        ("https://polaris.example.com/api/catalog", "iceberg,Sales.Orders"),
    ],
)
def test_binding_iceberg_lowercases_only_for_glue(
    catalog_url: str, expected_name: str
) -> None:
    binding = _iceberg_binding(
        server={"type": "iceberg", "catalogUrl": catalog_url, "namespace": "Sales"},
        physical_name="Orders",
    )
    assert binding.physical_urn == (
        f"urn:li:dataset:(urn:li:dataPlatform:{expected_name},PROD)"
    )


def test_binding_override_platform_ignores_catalog_url_variable() -> None:
    # catalogUrl only decides the platform; an override already decides it.
    binding = _iceberg_binding(
        server={"type": "iceberg", "catalogUrl": "${URL}", "namespace": "sales"},
        server_overrides=[{"server": "lake", "platform": "iceberg"}],
    )
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales.orders,PROD)"
    )


def test_synonyms_with_no_valid_entry_leave_existing_ones_alone() -> None:
    # Like a non-list value, a list with no usable entry is treated as absent;
    # only an explicit empty list clears.
    mcps = _logical_mcps(
        [
            {
                "name": "t",
                "properties": [
                    {"name": "col_a", "synonyms": [42, None, {"synonym": 5}]}
                ],
            }
        ]
    )
    assert not _aspects_for(
        mcps, make_schema_field_urn(LOGICAL_URN, "col_a"), AiContextClass
    )


def test_binding_variable_in_server_name_is_unmappable_not_a_variable() -> None:
    contract = _make_contract(
        servers=[{"server": "${ENV}_wh", "type": "kafka"}],
        schema=[{"name": "t"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn is None
    assert not binding.unresolved_variable


def test_binding_iceberg_warehouse_variable_is_a_variable_refusal() -> None:
    binding = _iceberg_binding(
        server={
            "type": "iceberg",
            "catalogUrl": "https://glue.us-east-1.amazonaws.com/iceberg",
            "namespace": "sales",
            "warehouse": "${WH}",
        }
    )
    assert binding.physical_urn is None
    assert binding.unresolved_variable
    assert binding.unmapped_reason and "warehouse" in binding.unmapped_reason


def test_binding_numeric_server_fields_are_accepted() -> None:
    # YAML reads an unquoted account ID as an int; it must not reject the contract.
    binding = _iceberg_binding(
        server={
            "type": "iceberg",
            "catalogUrl": "https://glue.us-east-1.amazonaws.com/iceberg",
            "namespace": "sales",
            "warehouse": 123456789012,
        }
    )
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)"
    )
    contract = _make_contract(
        servers=[
            {"server": "g", "type": "glue", "account": 123456789012, "database": 2024}
        ],
        schema=[{"name": "t", "physicalName": "orders"}],
    )
    (glue,) = odcs_to_physical_bindings(contract, _config())
    assert glue.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,2024.orders,PROD)"
    )


def test_binding_iceberg_catalog_url_without_scheme() -> None:
    binding = _iceberg_binding(
        server={
            "type": "iceberg",
            "catalogUrl": "glue.us-east-1.amazonaws.com/iceberg",
            "namespace": "Sales",
        },
        physical_name="Orders",
    )
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,sales.orders,PROD)"
    )


@pytest.mark.parametrize(
    "server,expected_platform",
    [
        # warehouse is a compute warehouse here, not part of the table name.
        (
            {
                "type": "snowflake",
                "database": "db",
                "schema": "sch",
                "warehouse": "${WH}",
            },
            "snowflake",
        ),
        # warehouse only decides the binding for a Glue Iceberg catalog.
        (
            {
                "type": "iceberg",
                "catalogUrl": "https://polaris.example.com/api/catalog",
                "namespace": "sales",
                "warehouse": "${WH}",
            },
            "iceberg",
        ),
    ],
)
def test_binding_warehouse_variable_ignored_where_it_does_not_bind(
    server: Dict[str, Any], expected_platform: str
) -> None:
    binding = _iceberg_binding(server=server)
    assert binding.physical_urn is not None
    assert f"urn:li:dataPlatform:{expected_platform}," in binding.physical_urn


def test_binding_missing_server_field_is_not_a_variable_refusal() -> None:
    contract = _make_contract(
        servers=[{"server": "pg", "type": "postgres", "database": "db"}],
        schema=[{"name": "t"}],
    )
    (binding,) = odcs_to_physical_bindings(contract, _config())
    assert binding.physical_urn is None
    assert binding.refusal is not None
    assert binding.refusal.kind is RefusalKind.UNMAPPABLE
    assert not binding.unresolved_variable


def test_binding_explicitly_unbound_is_its_own_kind() -> None:
    contract = _make_contract(
        servers=[{"server": "pg", "type": "postgres", "database": "db", "schema": "s"}],
        schema=[{"name": "t"}],
    )
    (binding,) = odcs_to_physical_bindings(
        contract, _config(physical_urn_overrides={"c1": {"t": ""}})
    )
    assert binding.physical_urn is None
    assert binding.refusal is not None
    assert binding.refusal.kind is RefusalKind.EXPLICITLY_UNBOUND


def test_binding_malformed_catalog_url_does_not_raise() -> None:
    binding = _iceberg_binding(
        server={
            "type": "iceberg",
            "catalogUrl": "glue.us-east-1.amazonaws.com]",
            "namespace": "sales",
        }
    )
    assert binding.physical_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales.orders,PROD)"
    )


def _field_descriptions(properties: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    entry = ODCSSchemaObject.model_validate(
        {"name": "orders", "properties": properties}
    )
    schema = build_schema_metadata(entry).schema_metadata
    assert schema is not None
    return {f.fieldPath: f.description for f in schema.fields}


@pytest.mark.parametrize(
    "extra, expected",
    [
        (
            {
                "transformDescription": "Sum of line items.",
                "transformLogic": "SUM(amount)",
            },
            "Order total.\n\n**Derivation:** Sum of line items."
            "\n\n```sql\nSUM(amount)\n```",
        ),
        (
            {"transformLogic": "SUM(amount)"},
            "Order total.\n\n**Derivation:**\n\n```sql\nSUM(amount)\n```",
        ),
        (
            {"transformDescription": "Sum of line items."},
            "Order total.\n\n**Derivation:** Sum of line items.",
        ),
        (
            # A fence in the logic must not close the block early.
            {"transformLogic": "SELECT '```' AS x"},
            "Order total.\n\n**Derivation:**\n\n````sql\nSELECT '```' AS x\n````",
        ),
        ({"transformDescription": "  ", "transformLogic": ""}, "Order total."),
        ({}, "Order total."),
    ],
)
def test_field_description_appends_derivation(
    extra: Dict[str, Any], expected: str
) -> None:
    descriptions = _field_descriptions(
        [{"name": "total", "description": "Order total.", **extra}]
    )
    assert descriptions["total"] == expected


def test_field_description_derivation_without_base_description() -> None:
    descriptions = _field_descriptions(
        [{"name": "total", "transformLogic": "SUM(amount)"}]
    )
    assert descriptions["total"] == "**Derivation:**\n\n```sql\nSUM(amount)\n```"


def test_field_description_derivation_on_nested_field() -> None:
    descriptions = _field_descriptions(
        [
            {
                "name": "address",
                "logicalType": "object",
                "properties": [{"name": "city", "transformLogic": "upper(city)"}],
            }
        ]
    )
    assert "upper(city)" in (descriptions["address.city"] or "")


def test_transform_fields_off_spec_values_dropped() -> None:
    prop = ODCSProperty.model_validate(
        {"name": "total", "transformLogic": 5, "transformDescription": ["x"]}
    )
    assert prop.transformLogic is None
    assert prop.transformDescription is None


def test_substitute_object_placeholder_all_forms() -> None:
    query = (
        "SELECT count(*) FROM {object} a JOIN ${object} b ON a.id = b.id "
        "WHERE x IN (SELECT id FROM {object})"
    )
    assert substitute_object_placeholder(query, "db.sch.orders") == (
        "SELECT count(*) FROM db.sch.orders a JOIN db.sch.orders b ON a.id = b.id "
        "WHERE x IN (SELECT id FROM db.sch.orders)"
    )


def _object_rule_infos(bound_table: Optional[str]) -> List[AssertionInfoClass]:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "quality": [
                    {
                        "type": "sql",
                        "query": "SELECT count(*) FROM {object}",
                        "mustBe": 0,
                    },
                    # No threshold: routed to a custom assertion carrying the query.
                    {"type": "sql", "query": "SELECT count(*) FROM {object}"},
                ],
            }
        ]
    )
    _, mcps, _ = odcs_to_assertion_mcps(
        contract=contract,
        schema_entry=_first_schema(contract),
        logical_urn=LOGICAL_URN,
        bound_table=bound_table,
    )
    return [m.aspect for m in mcps if isinstance(m.aspect, AssertionInfoClass)]


def test_sql_rule_object_replaced_in_sql_and_custom_fallback() -> None:
    sql_info, custom_info = _object_rule_infos("db.sch.orders")
    assert sql_info.sqlAssertion is not None
    assert sql_info.sqlAssertion.statement == "SELECT count(*) FROM db.sch.orders"
    assert _custom_logic(custom_info) == "SELECT count(*) FROM db.sch.orders"


def test_sql_rule_object_kept_when_unbound() -> None:
    sql_info, _ = _object_rule_infos(None)
    assert sql_info.sqlAssertion is not None
    assert sql_info.sqlAssertion.statement == "SELECT count(*) FROM {object}"


def test_field_description_allowed_values_after_derivation() -> None:
    descriptions = _field_descriptions(
        [
            {
                "name": "status",
                "description": "Order status.",
                "transformLogic": "upper(s)",
                "enum": [
                    {
                        "value": "open",
                        "label": "Open",
                        "description": "Not shipped yet",
                    },
                    {"value": "shipped", "label": "shipped"},
                    {"value": "void", "description": "Cancelled"},
                    "legacy",
                    {"label": "no value"},
                ],
            }
        ]
    )
    assert (descriptions["status"] or "").endswith(
        "```\n\n**Allowed values:**\n"
        "- `open` (Open): Not shipped yet\n"
        "- `shipped`\n"
        "- `void`: Cancelled\n"
        "- `legacy`"
    )


def test_field_description_allowed_values_on_nested_field() -> None:
    descriptions = _field_descriptions(
        [
            {
                "name": "address",
                "logicalType": "object",
                "properties": [{"name": "country", "enum": [{"value": "DE"}]}],
            }
        ]
    )
    assert descriptions["address.country"] == "**Allowed values:**\n- `DE`"


def test_enum_values_render_in_json_spelling() -> None:
    prop = ODCSProperty.model_validate(
        {
            "name": "s",
            "enum": [
                True,
                0,
                1.5,
                {"value": False},
                {"value": None, "label": "Unknown"},
            ],
        }
    )
    assert [e.value for e in prop.enum or []] == [
        "true",
        "0",
        "1.5",
        "false",
        "null",
    ]
    assert (prop.enum or [])[-1].label == "Unknown"


def test_enum_values_whitespace_shown_exactly() -> None:
    prop = ODCSProperty.model_validate(
        {
            "name": "s",
            "enum": [{"value": ""}, "  ", "open", "a  b", "x\ny", "a b"],
        }
    )
    assert [e.value for e in prop.enum or []] == [
        '""',
        '"  "',
        "open",
        '"a  b"',
        '"x\\ny"',
        "a b",
    ]


def test_enum_value_model_normalizes_its_own_value() -> None:
    assert ODCSEnumValue.model_validate({"value": "a\n- b"}).value == '"a\\n- b"'


def test_allowed_values_cannot_break_the_markdown() -> None:
    descriptions = _field_descriptions(
        [
            {
                "name": "s",
                "enum": [
                    {"value": "a`b", "label": "A\nB"},
                    {"value": "`x", "description": "d1\n\n- injected"},
                    {"value": "x`"},
                    {"value": "a\n\nb"},
                ],
            }
        ]
    )
    assert descriptions["s"] == (
        "**Allowed values:**\n- ``a`b`` (A B)\n- `` `x ``: d1 - injected\n"
        '- `` x` ``\n- `"a\\n\\nb"`'
    )


def test_enum_off_spec_dropped() -> None:
    assert ODCSProperty.model_validate({"name": "s", "enum": "open"}).enum is None
    prop = ODCSProperty.model_validate(
        {"name": "s", "enum": [{"label": "x"}, None, {"value": [1]}, {"value": {}}]}
    )
    assert prop.enum is None


def test_binding_table_name_excludes_platform_instance() -> None:
    contract = _make_contract(
        servers=[
            {"server": "srv", "type": "postgres", "database": "db", "schema": "sch"}
        ],
        schema=[{"name": "t1", "physicalName": "tbl"}],
    )
    bindings = odcs_to_physical_bindings(
        contract,
        _config(server_overrides=[{"server": "srv", "platform_instance": "inst1"}]),
    )
    assert "inst1.db.sch.tbl" in (bindings[0].physical_urn or "")
    assert bindings[0].table_name == "db.sch.tbl"


def test_override_that_is_not_a_dataset_urn_is_refused() -> None:
    contract = _make_contract(schema=[{"name": "a"}, {"name": "b"}, {"name": "c"}])
    bindings = odcs_to_physical_bindings(
        contract,
        _config(
            physical_urn_overrides={
                "c1": {
                    "a": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.a,PROD)",
                    "b": "urn:li:datset:typo",
                    "c": "appdb.public.c",
                }
            }
        ),
    )
    assert bindings[0].table_name == "db.sch.a"
    for refused, value in (
        (bindings[1], "urn:li:datset:typo"),
        (bindings[2], "appdb.public.c"),
    ):
        assert refused.physical_urn is None
        assert refused.table_name is None
        assert refused.refusal is not None
        assert refused.refusal.kind is RefusalKind.INVALID_OVERRIDE
        assert value in refused.refusal.message


def test_override_table_name_strips_contract_server_instance() -> None:
    contract = _make_contract(
        servers=[{"server": "srv", "type": "postgres", "database": "db"}],
        schema=[{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}],
    )
    bindings = odcs_to_physical_bindings(
        contract,
        _config(
            server_overrides=[
                {"server": "srv", "platform_instance": "inst1"},
                {"server": "other-srv", "platform_instance": "db"},
            ],
            physical_urn_overrides={
                "c1": {
                    "a": "urn:li:dataset:(urn:li:dataPlatform:postgres,inst1.db.sch.a,PROD)",
                    "b": "urn:li:dataset:(urn:li:dataPlatform:postgres,other.db.sch.b,PROD)",
                    "c": "urn:li:dataset:(urn:li:dataPlatform:snowflake,inst1.db.sch.c,PROD)",
                    "d": "urn:li:dataset:(urn:li:dataPlatform:postgres,INST1.db.sch.d,PROD)",
                }
            },
        ),
    )
    assert bindings[0].table_name == "db.sch.a"
    # A prefix no server_overrides entry names is part of the table path.
    assert bindings[1].table_name == "other.db.sch.b"
    # A configured instance the contract's server doesn't confirm is not guessed.
    assert bindings[2].physical_urn is not None
    assert bindings[2].table_name is None
    # The contract's own instance is stripped in any case.
    assert bindings[3].table_name == "db.sch.d"


def test_override_table_name_unknown_without_a_resolved_server() -> None:
    contract = _make_contract(schema=[{"name": "a"}, {"name": "b"}])
    bindings = odcs_to_physical_bindings(
        contract,
        _config(
            server_overrides=[{"server": "srv", "platform_instance": "inst1"}],
            physical_urn_overrides={
                "c1": {
                    "a": "urn:li:dataset:(urn:li:dataPlatform:postgres,inst1.db.sch.a,PROD)",
                    "b": "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.b,PROD)",
                }
            },
        ),
    )
    assert bindings[0].table_name is None
    assert bindings[1].table_name == "db.sch.b"


def test_override_table_name_ignores_other_platform_and_case() -> None:
    contract = _make_contract(schema=[{"name": "a"}, {"name": "b"}])
    bindings = odcs_to_physical_bindings(
        contract,
        _config(
            server_overrides=[
                {
                    "server": "sf",
                    "platform": "snowflake",
                    "platform_instance": "analytics",
                },
                {"server": "pg", "platform_instance": "pg1"},
            ],
            physical_urn_overrides={
                "c1": {
                    "a": "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.t,PROD)",
                    "b": "urn:li:dataset:(urn:li:dataPlatform:postgres,PG1.db.public.t,PROD)",
                }
            },
        ),
    )
    # A snowflake instance can't prefix a postgres URN.
    assert bindings[0].table_name == "analytics.public.t"
    assert bindings[1].table_name is None


@pytest.mark.parametrize(
    "override, kind",
    [
        (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,${SCHEMA}.t,PROD)",
            RefusalKind.UNRESOLVED_VARIABLE,
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.t,NOTANENV)",
            RefusalKind.INVALID_OVERRIDE,
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.t,prod)",
            RefusalKind.INVALID_OVERRIDE,
        ),
        ("urn:li:chart:(looker,x)", RefusalKind.INVALID_OVERRIDE),
    ],
)
def test_override_with_variable_or_bad_env_is_refused(
    override: str, kind: RefusalKind
) -> None:
    contract = _make_contract(schema=[{"name": "t"}])
    (binding,) = odcs_to_physical_bindings(
        contract, _config(physical_urn_overrides={"c1": {"t": override}})
    )
    assert binding.physical_urn is None
    assert binding.table_name is None
    assert binding.refusal_kind is kind


def test_doubled_braces_are_not_the_object_placeholder() -> None:
    assert (
        substitute_object_placeholder(
            "{{object}} {{object} {object}} {${object} {object} ${object}", "db.t"
        )
        == "{{object}} {{object} {object}} {${object} db.t db.t"
    )


def test_override_table_name_kept_when_server_has_no_mapping() -> None:
    contract = _make_contract(
        servers=[{"server": "srv", "type": "postgres", "database": "db"}],
        schema=[{"name": "a"}],
    )
    (binding,) = odcs_to_physical_bindings(
        contract,
        _config(
            physical_urn_overrides={
                "c1": {
                    "a": "urn:li:dataset:(urn:li:dataPlatform:postgres,inst1.db.sch.a,PROD)"
                }
            },
        ),
    )
    assert binding.table_name == "inst1.db.sch.a"


@pytest.mark.parametrize(
    "lowercase, expected", [(True, "db.sch.mytable"), (False, "DB.Sch.MyTable")]
)
def test_binding_table_name_follows_lowercase_setting(
    lowercase: bool, expected: str
) -> None:
    contract = _make_contract(
        servers=[
            {"server": "srv", "type": "snowflake", "database": "DB", "schema": "Sch"}
        ],
        schema=[{"name": "t1", "physicalName": "MyTable"}],
    )
    bindings = odcs_to_physical_bindings(
        contract, _config(lowercase_physical_urns=lowercase)
    )
    assert bindings[0].table_name == expected


def test_substitute_object_placeholder_leaves_longer_tokens() -> None:
    query = "SELECT ${object_id}, ${object:-x} FROM {objects}"
    assert substitute_object_placeholder(query, "db.sch.orders") == query


def test_dataset_description_context_section() -> None:
    contract = _make_contract(
        schema=[{"name": "t", "description": "T.", "context": "Prefer the daily view."}]
    )
    contract = contract.model_copy(
        update={
            "context": ODCSContract.model_validate(
                {
                    "id": "c1",
                    "context": {
                        "instructions": "Use for revenue analysis.",
                        "constraints": [
                            {"constraint": "Do not join without tenant_id."}
                        ],
                        "verifiedStatements": [
                            {
                                "question": "How is revenue defined?",
                                "answer": "SUM(amount) of shipped orders.",
                            },
                            {"question": "Which orders are late?"},
                        ],
                    },
                }
            ).context
        }
    )
    assert _logical_description(contract) == (
        "T.\n\n**Guidance for AI and analysts:**\n\n"
        "Use for revenue analysis.\n\nPrefer the daily view.\n\n"
        "**Constraints:**\n- Do not join without tenant_id.\n\n"
        "**Verified Q&A:**\n"
        "- Q: How is revenue defined? A: SUM(amount) of shipped orders.\n"
        "- Q: Which orders are late?"
    )


def test_context_string_form_is_instructions() -> None:
    contract = ODCSContract.model_validate({"id": "c1", "context": "Use daily."})
    assert contract.context == ODCSContext(instructions="Use daily.")


def test_context_list_items_are_single_lines_and_blanks_skipped() -> None:
    contract = ODCSContract.model_validate(
        {
            "id": "c1",
            "context": {
                "constraints": [{"constraint": "  "}, {"constraint": "No\njoins."}],
                "verifiedStatements": [
                    {"question": "  ", "answer": "x"},
                    {"question": "Q\n1?", "answer": "A\n\n- 1"},
                ],
            },
            "schema": [{"name": "t"}],
        }
    )
    assert _logical_description(contract) == (
        "**Guidance for AI and analysts:**\n\n"
        "**Constraints:**\n- No joins.\n\n"
        "**Verified Q&A:**\n- Q: Q 1? A: A - 1"
    )


def test_context_string_with_variable_kept_verbatim() -> None:
    contract = ODCSContract.model_validate(
        {"id": "c1", "context": "Filter on ${REGION}.", "schema": [{"name": "t"}]}
    )
    assert "Filter on ${REGION}." in (_logical_description(contract) or "")


def test_context_off_spec_dropped_and_blank_context_adds_nothing() -> None:
    assert ODCSContract.model_validate({"id": "c1", "context": 5}).context is None
    contract = ODCSContract.model_validate(
        {
            "id": "c1",
            "context": {"instructions": "  ", "constraints": [{"id": "x"}, "bad"]},
            "schema": [{"name": "t", "description": "T."}],
        }
    )
    assert _logical_description(contract) == "T."
    # No usable entry left is a dropped field, not an empty list.
    assert contract.context is not None
    assert contract.context.constraints is None


def test_lenient_fields_by_model_matches_the_lenient_validators() -> None:
    # The source's "dropped as off-spec" report reads this map; a lenient
    # validator missing from it would drop values without a report.
    # `ODCSSynonym.synonym` is pre-filtered by the list validator instead.
    reported = {
        (model, field_name)
        for model, fields in LENIENT_FIELDS_BY_MODEL.items()
        for field_name in fields
    }
    lenient = {
        (model, field_name)
        for model in LENIENT_FIELDS_BY_MODEL
        for name, decorator in cast(
            Type[BaseModel], model
        ).__pydantic_decorators__.field_validators.items()
        if name.startswith("_lenient")
        for field_name in decorator.info.fields
    } - {(ODCSSynonym, "synonym")}
    assert lenient <= reported
