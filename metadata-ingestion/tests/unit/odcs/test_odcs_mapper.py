"""Unit tests for the ODCS mapper module — refactored API (D1 fan-out)."""

from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_mapper import (
    _description_to_str,
    _make_owners,
    _num,
    _operator_and_params_from_threshold,
    _walk_properties,
    odcs_to_assertion_mcps,
    odcs_to_contract_urn,
    odcs_to_dataset_mcps,
    odcs_to_dataset_urns,
)
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DatasetPropertiesClass,
    EditableSchemaMetadataClass,
    FieldAssertionInfoClass,
    GlobalTagsClass,
    OwnershipClass,
    OwnershipTypeClass,
    SqlAssertionInfoClass,
    VolumeAssertionInfoClass,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    overrides: Optional[Dict[str, str]] = None,
    mappings: Optional[List[Dict[str, Any]]] = None,
) -> ODCSSourceConfig:
    return ODCSSourceConfig.model_validate(
        {
            "path": "/tmp/ignored",
            "dataset_urn_overrides": overrides or {},
            "servers_to_platform": mappings or [],
        }
    )


# ---------------------------------------------------------------------------
# odcs_to_dataset_urns — fan-out resolver
# ---------------------------------------------------------------------------


def test_dataset_urns_single_schema_one_binding() -> None:
    contract = _make_contract(
        servers=[{"server": "prod-postgres", "type": "postgres"}],
        schema=[{"name": "t1", "physicalName": "tbl1"}],
    )
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])

    bindings, unmapped = odcs_to_dataset_urns(contract, config)

    assert len(bindings) == 1
    assert len(unmapped) == 0
    schema_entry, urn = bindings[0]
    assert schema_entry.physicalName == "tbl1"
    assert "postgres" in urn
    assert "tbl1" in urn


def test_dataset_urns_multi_schema_three_bindings() -> None:
    contract = _make_contract(
        servers=[{"server": "prod-postgres", "type": "postgres"}],
        schema=[
            {"name": "a", "physicalName": "a"},
            {"name": "b", "physicalName": "b"},
            {"name": "c", "physicalName": "c"},
        ],
    )
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])

    bindings, unmapped = odcs_to_dataset_urns(contract, config)

    assert len(bindings) == 3
    assert len(unmapped) == 0
    physical_names = [s.physicalName for s, _ in bindings]
    assert physical_names == ["a", "b", "c"]
    # All distinct URNs.
    urns = [u for _, u in bindings]
    assert len(set(urns)) == 3


def test_dataset_urns_one_schema_missing_physical_name() -> None:
    contract = _make_contract(
        servers=[{"server": "prod-postgres", "type": "postgres"}],
        schema=[
            {"name": "a", "physicalName": "a"},
            # name="" + no physicalName -> unmapped, even with required pydantic name field.
            {"name": "", "physicalName": None},
            {"name": "c", "physicalName": "c"},
        ],
    )
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])

    bindings, unmapped = odcs_to_dataset_urns(contract, config)

    assert len(bindings) == 2
    assert len(unmapped) == 1
    assert unmapped[0].index == 1
    assert "physicalName" in unmapped[0].reason or "name" in unmapped[0].reason


def test_dataset_urns_no_servers_uses_catch_all_mapping() -> None:
    contract = _make_contract(
        servers=None,
        schema=[{"name": "t", "physicalName": "tbl"}],
    )
    config = _config(
        mappings=[{"server": "*", "platform": "snowflake", "match_any": True}]
    )

    bindings, unmapped = odcs_to_dataset_urns(contract, config)
    assert len(bindings) == 1
    assert len(unmapped) == 0
    assert "snowflake" in bindings[0][1]


def test_dataset_urns_unmappable_server_all_unmapped() -> None:
    contract = _make_contract(
        servers=[{"server": "weird", "type": "unknown"}],
        schema=[
            {"name": "a", "physicalName": "a"},
            {"name": "b", "physicalName": "b"},
        ],
    )
    config = _config(mappings=[{"server": "other", "platform": "postgres"}])

    bindings, unmapped = odcs_to_dataset_urns(contract, config)

    assert len(bindings) == 0
    assert len(unmapped) == 2


def test_dataset_urn_override_applies_to_first_schema_only() -> None:
    """`dataset_urn_overrides` is single-URN-per-contract; the override takes the
    first schema and the rest fall through to server-mapping resolution (D9)."""
    contract = _make_contract(
        servers=[{"server": "prod-postgres", "type": "postgres"}],
        schema=[
            {"name": "first", "physicalName": "first"},
            {"name": "second", "physicalName": "second"},
        ],
    )
    config = _config(
        overrides={"c1": "urn:li:dataset:(urn:li:dataPlatform:override,foo,PROD)"},
        mappings=[{"server": "prod-postgres", "platform": "postgres"}],
    )

    bindings, unmapped = odcs_to_dataset_urns(contract, config)
    assert len(unmapped) == 0
    assert len(bindings) == 2
    # First entry took the override.
    assert bindings[0][1] == "urn:li:dataset:(urn:li:dataPlatform:override,foo,PROD)"
    # Second entry fell through to server-mapping resolution.
    assert "postgres" in bindings[1][1]
    assert "second" in bindings[1][1]


# ---------------------------------------------------------------------------
# odcs_to_dataset_mcps — per-schema-entry emission
# ---------------------------------------------------------------------------


def _aspect_types(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    return [type(m.aspect).__name__ for m in mcps]


def _editable_schema_paths(
    mcps: List[MetadataChangeProposalWrapper],
) -> List[str]:
    """Return the field paths in EditableSchemaMetadata for these MCPs."""
    paths: List[str] = []
    for m in mcps:
        if isinstance(m.aspect, EditableSchemaMetadataClass):
            for field in m.aspect.editableSchemaFieldInfo:
                paths.append(field.fieldPath)
    return paths


def _build_four_table_contract() -> ODCSContract:
    """customers (with email pii) + inventory (with sku tagged 'inventory')."""
    return _make_contract(
        contract_id="ecommerce-1",
        name="EcommerceContract",
        servers=[{"server": "prod-postgres", "type": "postgres"}],
        team=[{"username": "priya", "role": "owner"}],
        tags=["retail"],
        schema=[
            {
                "name": "customers",
                "physicalName": "customers",
                "properties": [
                    {"name": "customer_id", "logicalType": "number"},
                    {
                        "name": "email",
                        "logicalType": "string",
                        "tags": ["pii"],
                        "description": "Shopper email",
                    },
                ],
            },
            {
                "name": "orders",
                "physicalName": "orders",
                "properties": [{"name": "order_id", "logicalType": "number"}],
            },
            {
                "name": "products",
                "physicalName": "products",
                "properties": [{"name": "product_id", "logicalType": "number"}],
            },
            {
                "name": "inventory",
                "physicalName": "inventory",
                "properties": [
                    {
                        "name": "sku",
                        "logicalType": "string",
                        "tags": ["inventory"],
                        "description": "Vendor SKU",
                    }
                ],
            },
        ],
    )


def test_dataset_mcps_per_table_tags_attach_to_correct_urn() -> None:
    """C1 regression: customers.email tag must NOT bleed onto inventory URN
    and inventory.sku tag must NOT bleed onto customers URN."""
    contract = _build_four_table_contract()
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])
    bindings, _ = odcs_to_dataset_urns(contract, config)
    assert len(bindings) == 4

    by_table: Dict[str, List[MetadataChangeProposalWrapper]] = {}
    for schema_entry, dataset_urn in bindings:
        mcps = list(
            odcs_to_dataset_mcps(
                contract=contract,
                schema_entry=schema_entry,
                dataset_urn=dataset_urn,
            )
        )
        assert schema_entry.physicalName is not None
        by_table[schema_entry.physicalName] = mcps
        # Each MCP must target this dataset URN — never another table's URN.
        for m in mcps:
            assert m.entityUrn == dataset_urn

    # email tag on customers' EditableSchemaMetadata, NOT inventory's.
    customer_paths = _editable_schema_paths(by_table["customers"])
    inventory_paths = _editable_schema_paths(by_table["inventory"])
    assert "email" in customer_paths
    assert "email" not in inventory_paths
    assert "sku" in inventory_paths
    assert "sku" not in customer_paths


def test_dataset_mcps_replicate_metadata_true_emits_ownership_per_table() -> None:
    contract = _build_four_table_contract()
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])
    bindings, _ = odcs_to_dataset_urns(contract, config)

    for schema_entry, dataset_urn in bindings:
        mcps = list(
            odcs_to_dataset_mcps(
                contract=contract,
                schema_entry=schema_entry,
                dataset_urn=dataset_urn,
                replicate_contract_metadata=True,
            )
        )
        ownership = [m.aspect for m in mcps if isinstance(m.aspect, OwnershipClass)]
        assert len(ownership) == 1
        assert any("priya" in o.owner for o in ownership[0].owners)


def test_dataset_mcps_replicate_metadata_false_suppresses_contract_owners_and_tags() -> (
    None
):
    contract = _build_four_table_contract()
    config = _config(mappings=[{"server": "prod-postgres", "platform": "postgres"}])
    bindings, _ = odcs_to_dataset_urns(contract, config)

    # customers entry has its OWN per-table behavior — let's check inventory which has
    # only per-property tags. With replicate=False, contract-level "retail" tag must
    # not appear, but per-property "inventory" tag still does (via EditableSchemaMetadata).
    inventory_entry, inventory_urn = next(
        (s, u) for s, u in bindings if s.physicalName == "inventory"
    )
    mcps = list(
        odcs_to_dataset_mcps(
            contract=contract,
            schema_entry=inventory_entry,
            dataset_urn=inventory_urn,
            replicate_contract_metadata=False,
        )
    )

    # No Ownership emitted at all when replicate=False.
    assert not any(isinstance(m.aspect, OwnershipClass) for m in mcps)
    # No GlobalTags carrying "retail" (contract-level) — there are no schema-level
    # tags on inventory either, so GlobalTags should be absent entirely.
    for m in mcps:
        if isinstance(m.aspect, GlobalTagsClass):
            for t in m.aspect.tags:
                assert "retail" not in t.tag

    # Per-property tags survive (sku is tagged 'inventory' inside EditableSchemaMetadata).
    paths = _editable_schema_paths(mcps)
    assert "sku" in paths


def test_dataset_mcps_description_string_renders() -> None:
    contract = _make_contract(
        contract_id="d1",
        servers=[{"server": "s", "type": "postgres"}],
        schema=[{"name": "t", "physicalName": "t"}],
        description="A simple string description.",
    )
    config = _config(mappings=[{"server": "s", "platform": "postgres"}])
    bindings, _ = odcs_to_dataset_urns(contract, config)
    schema_entry, urn = bindings[0]
    mcps = list(odcs_to_dataset_mcps(contract, schema_entry, urn))
    dataset_props = next(
        m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass)
    )
    assert dataset_props.description == "A simple string description."


def test_dataset_mcps_description_dict_renders_as_markdown() -> None:
    contract = _make_contract(
        contract_id="d2",
        servers=[{"server": "s", "type": "postgres"}],
        schema=[{"name": "t", "physicalName": "t"}],
        description={"purpose": "p", "usage": "u", "limitations": "l"},
    )
    config = _config(mappings=[{"server": "s", "platform": "postgres"}])
    bindings, _ = odcs_to_dataset_urns(contract, config)
    schema_entry, urn = bindings[0]
    mcps = list(odcs_to_dataset_mcps(contract, schema_entry, urn))
    dataset_props = next(
        m.aspect for m in mcps if isinstance(m.aspect, DatasetPropertiesClass)
    )
    assert dataset_props.description is not None
    assert "**purpose**" in dataset_props.description
    assert "**usage**" in dataset_props.description


# ---------------------------------------------------------------------------
# _make_owners / _walk_properties / _description_to_str
# ---------------------------------------------------------------------------


def test_make_owners_dedups_on_urn_and_type() -> None:
    contract = _make_contract(
        team=[
            {"username": "alice", "role": "owner"},
            {"username": "alice", "role": "owner"},
            {"username": "alice", "role": "dataSteward"},
        ],
    )
    owners = _make_owners(contract)
    assert len(owners) == 2
    types = {o.type for o in owners}
    assert OwnershipTypeClass.TECHNICAL_OWNER in types
    assert OwnershipTypeClass.DATA_STEWARD in types


def test_make_owners_group_prefix_yields_corp_group() -> None:
    contract = _make_contract(
        team=[{"username": "group:platform-team", "role": "owner"}],
    )
    owners = _make_owners(contract)
    assert len(owners) == 1
    assert owners[0].owner.startswith("urn:li:corpGroup:")
    assert "platform-team" in owners[0].owner


def test_make_owners_dateOut_excludes_member() -> None:
    contract = _make_contract(
        team=[
            {"username": "alice", "role": "owner"},
            {"username": "bob", "role": "owner", "dateOut": "2024-01-01"},
        ],
    )
    owners = _make_owners(contract)
    assert len(owners) == 1
    assert "alice" in owners[0].owner


def test_description_to_str_plain_string_trimmed() -> None:
    assert _description_to_str("  hello  ") == "hello"


def test_description_to_str_dict_joins_with_markdown_bold() -> None:
    result = _description_to_str({"purpose": "p", "usage": "u", "limitations": "l"})
    assert result is not None
    assert "**purpose**: p" in result
    assert "**usage**: u" in result
    assert "**limitations**: l" in result


def test_description_to_str_none_or_empty_returns_none() -> None:
    assert _description_to_str(None) is None
    assert _description_to_str("") is None
    assert _description_to_str("   ") is None
    assert _description_to_str({}) is None


def test_walk_properties_nested_path() -> None:
    grand = ODCSProperty(name="grand")
    child = ODCSProperty(name="child", properties=[grand])
    parent = ODCSProperty(name="parent", properties=[child])

    paths = [path for path, _ in _walk_properties([parent])]
    assert "parent" in paths
    assert "parent.child" in paths
    assert "parent.child.grand" in paths


# ---------------------------------------------------------------------------
# odcs_to_assertion_mcps — rule routing + scope
# ---------------------------------------------------------------------------


def _run_assertions_for_first_schema(
    contract: ODCSContract,
) -> Any:
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.t,PROD)"
    contract_urn = odcs_to_contract_urn(contract.id, dataset_urn)
    schema_entry = contract.schema_[0] if contract.schema_ else None
    assert schema_entry is not None
    return odcs_to_assertion_mcps(contract, schema_entry, dataset_urn, contract_urn)


def test_assertion_schema_level_rule_has_scope_schema() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [
                    {
                        "name": "rc",
                        "rule": "rowCount",
                        "mustBeGreaterOrEqualTo": 100,
                    }
                ],
            }
        ]
    )
    _, mcps, _ = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.customProperties is not None
    assert info.customProperties.get("odcs.scope") == "schema"


def test_assertion_property_rule_has_scope_property_and_dotted_path() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "properties": [
                    {
                        "name": "parent",
                        "properties": [
                            {
                                "name": "child",
                                "properties": [
                                    {
                                        "name": "grand",
                                        "quality": [
                                            {"rule": "notNull", "name": "g_nn"}
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    )
    _, mcps, _ = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.customProperties.get("odcs.scope") == "property"
    fa = info.fieldAssertion
    assert fa is not None and fa.fieldValuesAssertion is not None
    assert fa.fieldValuesAssertion.field.path == "parent.child.grand"


def test_assertion_contract_level_rule_replicates_with_scope_contract() -> None:
    """Contract-level (top-level `quality[]`) rules must replicate to each
    fanned-out dataset, tagged `odcs.scope=contract`."""
    contract = _make_contract(
        servers=[{"server": "s", "type": "postgres"}],
        schema=[
            {"name": "t1", "physicalName": "t1"},
            {"name": "t2", "physicalName": "t2"},
        ],
        quality=[
            {
                "name": "all_have_rows",
                "type": "sql",
                "query": "SELECT COUNT(*) FROM t",
                "mustBeGreaterThan": 0,
            }
        ],
    )
    config = _config(mappings=[{"server": "s", "platform": "postgres"}])
    bindings, _ = odcs_to_dataset_urns(contract, config)

    contract_scoped_seen = 0
    seen_urns: List[str] = []
    for schema_entry, dataset_urn in bindings:
        contract_urn = odcs_to_contract_urn(contract.id, dataset_urn)
        _, mcps, _ = odcs_to_assertion_mcps(
            contract, schema_entry, dataset_urn, contract_urn
        )
        for m in mcps:
            info = m.aspect
            if isinstance(info, AssertionInfoClass):
                if info.customProperties.get("odcs.scope") == "contract":
                    contract_scoped_seen += 1
                    assert m.entityUrn is not None
                    seen_urns.append(m.entityUrn)
    # Replicated to each of the 2 datasets, distinct URNs.
    assert contract_scoped_seen == 2
    assert len(set(seen_urns)) == 2


def test_assertion_notnull_field_assertion() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "properties": [{"name": "id", "quality": [{"rule": "notNull"}]}],
            }
        ]
    )
    _, mcps, _ = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.FIELD
    assert isinstance(info.fieldAssertion, FieldAssertionInfoClass)
    fva = info.fieldAssertion.fieldValuesAssertion
    assert fva is not None
    assert fva.operator == AssertionStdOperatorClass.NOT_NULL


def test_assertion_unique_field_assertion() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "properties": [{"name": "email", "quality": [{"rule": "unique"}]}],
            }
        ]
    )
    _, mcps, _ = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.fieldAssertion is not None
    fva = info.fieldAssertion.fieldValuesAssertion
    assert fva is not None
    assert fva.operator == AssertionStdOperatorClass.EQUAL_TO


def test_assertion_rowcount_with_threshold_yields_volume_assertion() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [{"rule": "rowCount", "mustBeGreaterOrEqualTo": 100}],
            }
        ]
    )
    _, mcps, trace = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.VOLUME
    assert isinstance(info.volumeAssertion, VolumeAssertionInfoClass)
    rct = info.volumeAssertion.rowCountTotal
    assert rct is not None
    assert rct.operator == AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    assert rct.parameters.value is not None
    assert rct.parameters.value.value == "100"
    # Not routed to custom; not skipped.
    assert trace.routed_to_custom == []
    assert trace.skipped_no_body == []


def test_assertion_rowcount_no_threshold_routes_to_custom() -> None:
    """D6: rowCount with no threshold is NOT fabricated as 0/GREATER_THAN_OR_EQUAL.
    With description as logic-source, it's routed to a CustomAssertion."""
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [
                    {
                        "name": "rc_no_thresh",
                        "rule": "rowCount",
                        "description": "row count rule without a threshold",
                    }
                ],
            }
        ]
    )
    _, mcps, trace = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert "rc_no_thresh" in trace.routed_to_custom


def test_assertion_must_not_be_between_routes_to_custom_with_explicit_logic() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [
                    {
                        "name": "rc_excl",
                        "rule": "rowCount",
                        "mustNotBeBetween": [1, 10],
                        "description": "row count must not fall between 1 and 10",
                    }
                ],
            }
        ]
    )
    _, mcps, trace = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert "rc_excl" in trace.routed_to_custom
    # D6: the logic field must carry the explicit, human-readable bounds
    # rather than falling through the default priority chain (query →
    # implementation → description → name:scope). The literal format
    # ("value not between {low} and {high}") is documented in the connector's
    # `odcs_post.md` and the ODCS final design.
    assert isinstance(info.customAssertion, CustomAssertionInfoClass)
    assert info.customAssertion.logic == "value not between 1 and 10"


def test_assertion_sql_rule_with_query_yields_sql_assertion() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [
                    {
                        "name": "no_nulls",
                        "type": "sql",
                        "query": "SELECT COUNT(*) FROM t WHERE x IS NULL",
                        "mustBe": 0,
                    }
                ],
            }
        ]
    )
    _, mcps, _ = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.SQL
    assert isinstance(info.sqlAssertion, SqlAssertionInfoClass)
    assert info.sqlAssertion.statement == "SELECT COUNT(*) FROM t WHERE x IS NULL"
    assert info.sqlAssertion.operator == AssertionStdOperatorClass.EQUAL_TO


def test_assertion_sql_rule_without_query_routes_to_custom() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [
                    {
                        "name": "sql_no_query",
                        "type": "sql",
                        "description": "SQL rule but no query body",
                    }
                ],
            }
        ]
    )
    _, mcps, trace = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert "sql_no_query" in trace.routed_to_custom


def test_assertion_rule_with_no_operator_no_body_is_skipped() -> None:
    """D6: a rule with no operator AND no body cannot be modeled — skip with trace."""
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "quality": [
                    # No name (so the fallback can't synthesize logic from rule.name),
                    # no description, no query, no implementation, no rule.
                    {}
                ],
            }
        ]
    )
    _, mcps, trace = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 0
    assert len(trace.skipped_no_body) == 1


def test_assertion_unknown_library_with_description_routes_to_custom() -> None:
    contract = _make_contract(
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "properties": [
                    {
                        "name": "score",
                        "quality": [
                            {
                                "rule": "range",
                                "mustBeBetween": [0, 100],
                                "description": "score must fall in [0, 100]",
                            }
                        ],
                    }
                ],
            }
        ]
    )
    _, mcps, trace = _run_assertions_for_first_schema(contract)
    assert len(mcps) == 1
    info = mcps[0].aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert isinstance(info.customAssertion, CustomAssertionInfoClass)
    assert info.customAssertion.type == "range"
    assert info.customAssertion.logic is not None
    assert "score must fall" in info.customAssertion.logic
    assert len(trace.routed_to_custom) == 1


# ---------------------------------------------------------------------------
# URN stability under fan-out (TENSION 1 invariants)
# ---------------------------------------------------------------------------


def _stable_contract() -> ODCSContract:
    return _make_contract(
        contract_id="stable-id",
        schema=[
            {
                "name": "t",
                "physicalName": "t",
                "properties": [
                    {
                        "name": "id",
                        "quality": [{"rule": "notNull", "name": "id_not_null"}],
                    }
                ],
            }
        ],
    )


def test_urn_stability_same_inputs_yield_same_urns() -> None:
    contract = _stable_contract()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.a,PROD)"

    contract_urn1 = odcs_to_contract_urn(contract.id, dataset_urn)
    contract_urn2 = odcs_to_contract_urn(contract.id, dataset_urn)
    assert contract_urn1 == contract_urn2

    assert contract.schema_ is not None
    schema_entry = contract.schema_[0]
    _, mcps1, _ = odcs_to_assertion_mcps(
        contract, schema_entry, dataset_urn, contract_urn1
    )
    _, mcps2, _ = odcs_to_assertion_mcps(
        contract, schema_entry, dataset_urn, contract_urn2
    )
    assert [m.entityUrn for m in mcps1] == [m.entityUrn for m in mcps2]


def test_urn_stability_distinct_dataset_urns_yield_distinct_urns_no_collision() -> None:
    contract = _stable_contract()
    dataset_a = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.a,PROD)"
    dataset_b = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.b,PROD)"

    contract_urn_a = odcs_to_contract_urn(contract.id, dataset_a)
    contract_urn_b = odcs_to_contract_urn(contract.id, dataset_b)
    assert contract_urn_a != contract_urn_b

    assert contract.schema_ is not None
    schema_entry = contract.schema_[0]
    _, mcps_a, _ = odcs_to_assertion_mcps(
        contract, schema_entry, dataset_a, contract_urn_a
    )
    _, mcps_b, _ = odcs_to_assertion_mcps(
        contract, schema_entry, dataset_b, contract_urn_b
    )
    urns_a = [m.entityUrn for m in mcps_a]
    urns_b = [m.entityUrn for m in mcps_b]
    assert urns_a and urns_b
    assert set(urns_a).isdisjoint(set(urns_b))


# ---------------------------------------------------------------------------
# _num normalization (M3)
# ---------------------------------------------------------------------------


def test_num_int_and_float_render_identically() -> None:
    assert _num(5).value == _num(5.0).value == "5"


def test_num_decimal_renders_with_decimal() -> None:
    assert _num(5.5).value == "5.5"


def test_num_zero_and_negative() -> None:
    assert _num(0).value == "0"
    assert _num(-3).value == "-3"


# ---------------------------------------------------------------------------
# _operator_and_params_from_threshold invariant
# ---------------------------------------------------------------------------


def test_operator_and_params_never_returns_operator_without_params() -> None:
    """`_route_and_build` routes to a native assertion only when BOTH the
    operator and params are non-None (guard: `op is None or params is None`).
    That guard is only safe if the threshold mapper never pairs a real operator
    with `None` params — the contract this refactor relies on after removing the
    `_ROUTE_TO_CUSTOM` sentinel. Lock it across the threshold rule shapes:
    natively-modeled ones return both halves; unmappable / absent ones return
    `(None, None)`.
    """
    natively_modeled = [
        {"rule": "rowCount", "mustBeBetween": [1, 10]},
        {"rule": "rowCount", "mustBeGreaterThan": 0},
        {"rule": "rowCount", "mustBe": 5},
        {"rule": "rowCount", "mustNotBe": 5},
    ]
    no_native_operator = [
        # No NOT_BETWEEN operator — routed to custom with explicit logic.
        {"rule": "rowCount", "mustNotBeBetween": [1, 10]},
        # No threshold at all.
        {"rule": "rowCount"},
    ]

    for payload in natively_modeled + no_native_operator:
        rule = ODCSQualityRule.model_validate(payload)
        operator, params = _operator_and_params_from_threshold(rule)
        # The invariant the guard depends on: an operator never travels alone.
        assert not (operator is not None and params is None)

    for payload in natively_modeled:
        rule = ODCSQualityRule.model_validate(payload)
        operator, params = _operator_and_params_from_threshold(rule)
        assert operator is not None
        assert params is not None

    for payload in no_native_operator:
        rule = ODCSQualityRule.model_validate(payload)
        assert _operator_and_params_from_threshold(rule) == (None, None)
