import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from unittest import mock

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.openapi import RelatedEntity
from datahub.ingestion.source.dbt.dbt_cloud import (
    DBTCloudConfig,
    DBTCloudSource,
    _DiscoveryContractData,
)
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTConstraint,
    DBTContract,
    DBTModelPerformance,
    DBTNode,
    DBTSourceReport,
    _ExistingContract,
    _RelationResolver,
    parse_dbt_constraint,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    DBTRunMetadata,
    DBTRunResult,
    _parse_model_run,
    extract_contract_columns,
    extract_dbt_entities,
    load_run_results,
)
from datahub.ingestion.source.dbt.dbt_tests import DBTTest
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionRunEventClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DataContractPropertiesClass,
    DataQualityContractClass,
    StatusClass,
)

_ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,test_db.test_schema.orders,PROD)"
_PREFLIGHT_MESSAGE = (
    "This model has an enforced contract that failed.\n"
    "> in macro assert_columns_equivalent"
)


def _make_source(**config_overrides: Any) -> DBTCoreSource:
    # A graph with no existing contracts, so contracts are written.
    ctx = PipelineContext(
        run_id="test-contract",
        pipeline_name="dbt-source",
        graph=_graph_with_contracts({}),
    )
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="snowflake",
        ingest_contracts=True,
        enable_meta_mapping=False,
        **config_overrides,
    )
    return DBTCoreSource(config, ctx)


def _column(name: str, *constraints: DBTConstraint) -> DBTColumn:
    return DBTColumn(
        name=name,
        comment="",
        description="",
        index=0,
        data_type="bigint",
        constraints=list(constraints),
    )


def _make_model(
    *,
    name: str = "orders",
    alias: Optional[str] = None,
    adapter: str = "snowflake",
    materialization: str = "table",
    columns: Optional[List[DBTColumn]] = None,
    model_constraints: Optional[List[DBTConstraint]] = None,
) -> DBTNode:
    return DBTNode(
        dbt_name=f"model.test_pkg.{name}",
        dbt_adapter=adapter,
        dbt_package_name="test_pkg",
        database="test_db",
        schema="test_schema",
        name=alias or name,
        alias=alias,
        dbt_file_path=f"models/{name}.sql",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=[],
        materialization=materialization,
        catalog_type="BASE TABLE",
        missing_from_catalog=False,
        meta={},
        query_tag={},
        tags=[],
        owner="",
        language="sql",
        raw_code=None,
        compiled_code=None,
        columns=columns or [],
        model_constraints=model_constraints or [],
        contract=DBTContract(enforced=True, checksum=f"checksum_{name}"),
    )


def _make_test(
    name: str,
    upstreams: List[str],
    *,
    tagged: bool = False,
    qualified_test_name: str = "custom_check",
    column_name: Optional[str] = None,
    kw_args: Optional[Dict[str, Any]] = None,
    attached_node: Optional[str] = None,
) -> DBTNode:
    return DBTNode(
        dbt_name=f"test.test_pkg.{name}",
        dbt_adapter="snowflake",
        dbt_package_name="test_pkg",
        database=None,
        schema="test_schema",
        name=name,
        alias=None,
        dbt_file_path="models/schema.yml",
        node_type="test",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=upstreams,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=True,
        meta={},
        query_tag={},
        tags=["dbt:contract"] if tagged else [],
        owner="",
        language="sql",
        raw_code=None,
        compiled_code=None,
        columns=[],
        test_info=DBTTest(
            qualified_test_name=qualified_test_name,
            column_name=column_name,
            kw_args=kw_args or {},
            attached_node=attached_node,
        ),
    )


def _perf(
    status: str = "success",
    message: Optional[str] = None,
    built_relation: bool = True,
) -> DBTModelPerformance:
    return DBTModelPerformance(
        run_id="inv-1",
        status=status,
        start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
        message=message,
        built_relation=built_relation,
    )


def _contract_mcps(
    source: DBTCoreSource,
    models: Sequence[DBTNode],
    tests: Sequence[DBTNode] = (),
    other_nodes: Sequence[DBTNode] = (),
) -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []
    for item in source.create_contract_mcps(
        non_test_nodes=list(models),
        test_nodes=list(tests),
        all_nodes_map={n.dbt_name: n for n in [*models, *other_nodes]},
    ):
        metadata = item.metadata if isinstance(item, MetadataWorkUnit) else item
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        mcps.append(metadata)
    return mcps


def _dq_urns_by_entity(
    mcps: List[MetadataChangeProposalWrapper],
) -> Dict[str, List[str]]:
    return {
        mcp.aspect.entity: [c.assertion for c in mcp.aspect.dataQuality or []]
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    }


def _dq_urns(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    (urns,) = _dq_urns_by_entity(mcps).values()
    return urns


def _assertion_infos(
    mcps: List[MetadataChangeProposalWrapper],
) -> Dict[str, AssertionInfoClass]:
    return {
        str(mcp.entityUrn): mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, AssertionInfoClass)
    }


def _run_events(
    mcps: List[MetadataChangeProposalWrapper],
) -> List[AssertionRunEventClass]:
    return [
        mcp.aspect for mcp in mcps if isinstance(mcp.aspect, AssertionRunEventClass)
    ]


def _constraint_info(
    source: DBTCoreSource, node: DBTNode, constraint_type: str, operator: str
) -> AssertionInfoClass:
    for assertion in source._create_constraint_assertions(node, _ENTITY_URN):
        info = next(
            m.aspect for m in assertion.mcps if isinstance(m.aspect, AssertionInfoClass)
        )
        assert info.customProperties is not None and info.customAssertion is not None
        if (
            info.customProperties["constraint_type"] == constraint_type
            and info.customAssertion.operator == operator
        ):
            return info
    raise AssertionError(f"no {constraint_type}/{operator} assertion")


def test_contract_extraction_from_manifest() -> None:
    manifest_entities = {
        "model.test_pkg.orders": {
            "name": "orders",
            "database": "test_db",
            "schema": "test_schema",
            "resource_type": "model",
            "original_file_path": "models/orders.sql",
            "config": {
                "materialized": "table",
                "contract": {"enforced": True, "alias_types": True},
            },
            "contract": {"checksum": "abc123"},
            "columns": {
                "id": {
                    "name": "id",
                    "data_type": "bigint",
                    "constraints": [
                        {
                            "type": "foreign_key",
                            "to": "ref('customers')",
                            "to_columns": ["id"],
                        }
                    ],
                }
            },
            "constraints": [{"type": "primary_key", "columns": ["id"]}],
            "description": "",
            "meta": {},
            "tags": [],
        }
    }
    catalog_entities = {
        "model.test_pkg.orders": {
            "metadata": {"type": "table"},
            "columns": {"id": {"name": "id", "type": "BIGINT", "index": 0}},
        }
    }
    (node,) = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=catalog_entities,
        sources_results=[],
        manifest_adapter="snowflake",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=DBTSourceReport(),
    )
    assert node.contract is not None and node.contract.checksum == "abc123"
    assert node.model_constraints[0].type == "primary_key"
    assert node.contract_columns is not None
    assert node.contract_columns[0].constraints[0].to == "ref('customers')"
    # Declared type, not the catalog's BIGINT.
    assert node.contract_columns[0].data_type == "bigint"


def test_extract_contract_columns_preserves_declared_types() -> None:
    cols = extract_contract_columns(
        {
            "columns": {
                "id": {
                    "name": "id",
                    "data_type": "bigint",
                    "constraints": [{"type": "primary_key"}],
                },
                "email": {"name": "email", "data_type": "string"},
                "broken": {"name": "broken"},
            }
        },
        tag_prefix="dbt:",
    )
    assert [c.data_type for c in cols] == ["bigint", "string", ""]
    assert [c.type for c in cols[0].constraints] == ["primary_key"]


def test_primary_key_expands_to_unique_and_per_column_not_null() -> None:
    source = _make_source()
    node = _make_model(
        model_constraints=[
            DBTConstraint(type="primary_key", columns=["order_id", "line_id"])
        ]
    )
    assertions = source._create_constraint_assertions(node, _ENTITY_URN)
    # One multi-column uniqueness assertion plus one not-null per column.
    assert len(assertions) == 3

    unique = _constraint_info(
        source, node, "primary_key", AssertionStdOperatorClass.EQUAL_TO
    )
    assert unique.customAssertion is not None
    assert (
        unique.customAssertion.aggregation
        == AssertionStdAggregationClass.UNIQUE_PROPOTION
    )
    assert unique.customAssertion.fields is not None
    assert len(unique.customAssertion.fields) == 2
    assert unique.type == AssertionTypeClass.CUSTOM
    # Unnamed constraints leave the title to the UI, as for dbt tests.
    assert unique.description is None


def test_named_primary_key_titles_only_its_unique_half() -> None:
    source = _make_source()
    node = _make_model(
        model_constraints=[
            DBTConstraint(type="primary_key", name="pk", columns=["a", "b"])
        ]
    )
    assertions = source._create_constraint_assertions(node, _ENTITY_URN)
    infos = _assertion_infos([m for a in assertions for m in a.mcps]).values()
    assert sorted(str(info.description) for info in infos) == ["None", "None", "pk"]


def test_duplicate_check_keeps_the_named_primary_key_declaration() -> None:
    source = _make_source()
    node = _make_model(
        columns=[_column("id", DBTConstraint(type="unique"))],
        model_constraints=[
            DBTConstraint(type="primary_key", name="pk_orders", columns=["id"])
        ],
    )
    unique = _constraint_info(
        source, node, "primary_key", AssertionStdOperatorClass.EQUAL_TO
    )
    assert unique.description == "pk_orders"


def test_duplicate_check_keeps_the_named_not_null_title() -> None:
    # On Postgres both are DDL-enforced, so they tie; the primary key's
    # not-null half is untitled, so the named not_null wins.
    source = _make_source()
    node = _make_model(
        adapter="postgres",
        columns=[_column("id", DBTConstraint(type="not_null", name="nn_id"))],
        model_constraints=[
            DBTConstraint(type="primary_key", name="pk_orders", columns=["id"])
        ],
    )
    not_null = _constraint_info(
        source, node, "not_null", AssertionStdOperatorClass.NOT_NULL
    )
    assert not_null.description == "nn_id"


def test_named_constraint_becomes_assertion_name() -> None:
    source = _make_source()
    node = _make_model(
        columns=[
            _column(
                "age",
                DBTConstraint(type="check", name="ck_age", expression="age >= 0"),
            )
        ]
    )
    (assertion,) = source._create_constraint_assertions(node, _ENTITY_URN)
    info = next(
        m.aspect for m in assertion.mcps if isinstance(m.aspect, AssertionInfoClass)
    )
    assert info.description == "ck_age"
    assert info.customProperties is not None
    assert info.customProperties["expression"] == "age >= 0"


@pytest.mark.parametrize(
    "adapter, constraint_type, expected",
    [
        ("postgres", "unique", "database"),
        ("snowflake", "not_null", "database"),
        ("snowflake", "unique", "dbt_contract"),
        ("bigquery", "unique", "unsupported"),
        ("spark", "not_null", "dbt_contract"),
        ("databricks", "check", "database"),
        ("athena", "not_null", "unsupported"),
        ("athena", "custom", "dbt_contract"),
        ("some_new_adapter", "not_null", "dbt_contract"),
    ],
)
def test_enforced_by_follows_dbt_adapter_matrix(
    adapter: str, constraint_type: str, expected: str
) -> None:
    source = _make_source()
    node = _make_model(adapter=adapter)
    assert source._enforced_by_for_constraint(node, constraint_type).value == expected


@pytest.mark.parametrize(
    "adapter, expected",
    [
        # BigQuery enforces NOT NULL but only defines primary keys, and
        # declaring one does not make its columns NOT NULL there.
        ("bigquery", "dbt_contract"),
        # Snowflake and Databricks enforce the NOT NULL of a primary key.
        ("snowflake", "database"),
        ("databricks", "database"),
    ],
)
def test_primary_key_not_null_half_enforcement(adapter: str, expected: str) -> None:
    source = _make_source()
    node = _make_model(
        adapter=adapter,
        columns=[_column("id", DBTConstraint(type="primary_key"))],
    )
    info = _constraint_info(
        source, node, "primary_key", AssertionStdOperatorClass.NOT_NULL
    )
    assert info.customProperties is not None
    assert info.customProperties["enforced_by"] == expected


def test_constraint_urns_are_distinct_for_ambiguous_column_names() -> None:
    source = _make_source()
    node = _make_model(
        model_constraints=[
            DBTConstraint(type="unique", columns=["a_b", "c"]),
            DBTConstraint(type="unique", columns=["a", "b_c"]),
        ],
        columns=[
            _column(
                "status",
                DBTConstraint(type="check", expression="status <> ''"),
                DBTConstraint(type="check", expression="length(status) > 0"),
            )
        ],
    )
    urns = [a.urn for a in source._create_constraint_assertions(node, _ENTITY_URN)]
    assert len(urns) == 4
    assert len(set(urns)) == 4


def test_same_check_at_two_levels_is_emitted_once() -> None:
    source = _make_source()
    node = _make_model(
        columns=[
            _column(
                "id",
                DBTConstraint(type="not_null"),
                DBTConstraint(type="primary_key"),
            )
        ]
    )
    assertions = source._create_constraint_assertions(node, _ENTITY_URN)
    not_null = [
        info
        for info in _assertion_infos([m for a in assertions for m in a.mcps]).values()
        if info.customAssertion is not None
        and info.customAssertion.operator == AssertionStdOperatorClass.NOT_NULL
    ]
    assert len(not_null) == 1
    # Snowflake enforces NOT NULL but not primary keys; the stronger wins.
    assert not_null[0].customProperties is not None
    assert not_null[0].customProperties["enforced_by"] == "database"
    dq_urns = _dq_urns(_contract_mcps(source, [node]))
    assert len(dq_urns) == len(set(dq_urns))


def test_constraints_on_views_are_not_database_enforced() -> None:
    source = _make_source()
    node = _make_model(adapter="postgres", materialization="view")
    assert source._enforced_by_for_constraint(node, "not_null").value == (
        "dbt_contract"
    )


def test_named_constraint_urn_ignores_expression() -> None:
    source = _make_source()

    def urn(expression: str) -> str:
        node = _make_model(
            columns=[
                _column(
                    "age",
                    DBTConstraint(type="check", name="ck_age", expression=expression),
                )
            ]
        )
        (assertion,) = source._create_constraint_assertions(node, _ENTITY_URN)
        return assertion.urn

    assert urn("age >= 0") == urn("age > 0")


def _fixture_contract(kind: str) -> Tuple[List[str], List[str], int]:
    """Contract assertion URNs, contract entries and NOT_NULL count from a
    real dbt manifest (``dbt parse`` or ``dbt compile`` of the same project)."""
    manifest = json.loads(
        (
            Path(__file__).parent / "artifacts" / "contracts" / f"manifest_{kind}.json"
        ).read_text()
    )
    nodes = extract_dbt_entities(
        all_manifest_entities={**manifest["nodes"], **manifest["sources"]},
        all_catalog_entities={},
        sources_results=[],
        manifest_adapter="duckdb",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=DBTSourceReport(),
    )
    tests = [n for n in nodes if n.node_type == "test"]
    others = [n for n in nodes if n.node_type != "test"]
    (orders,) = [n for n in others if n.dbt_name.endswith(".orders")]
    mcps = _contract_mcps(_make_source(), [orders], tests, others)
    infos = _assertion_infos(mcps)
    not_null = [
        info
        for info in infos.values()
        if info.customAssertion is not None
        and info.customAssertion.operator == AssertionStdOperatorClass.NOT_NULL
    ]
    return sorted(infos), _dq_urns(mcps), len(not_null)


def test_parsed_and_compiled_manifests_give_the_same_contract() -> None:
    # The compiled manifest renders foreign key targets (a ref and a source
    # with an identifier) as relations; the parsed one keeps ref()/source().
    parsed = _fixture_contract("parse")
    assert parsed == _fixture_contract("compile")
    assertion_urns, contract_entries, num_not_null = parsed
    # Schema, PK unique, not-null on ID (column ``ID`` and PK ``[id]``), two
    # foreign keys and a check.
    assert len(assertion_urns) == 6
    assert num_not_null == 1
    # Plus the matching not_null and relationships (to a source) tests.
    assert len(contract_entries) == 8


def test_composite_foreign_key_keeps_column_pairs() -> None:
    source = _make_source()

    def fk(columns: List[str], to_columns: List[str]) -> Tuple[str, AssertionInfoClass]:
        node = _make_model(
            model_constraints=[
                DBTConstraint(
                    type="foreign_key",
                    columns=columns,
                    to="ref('dim')",
                    to_columns=to_columns,
                )
            ]
        )
        (assertion,) = source._create_constraint_assertions(node, _ENTITY_URN)
        (info,) = _assertion_infos(assertion.mcps).values()
        return assertion.urn, info

    urn, info = fk(["b_id", "a_id"], ["y", "x"])
    assert urn == fk(["a_id", "b_id"], ["x", "y"])[0]
    assert urn != fk(["a_id", "b_id"], ["y", "x"])[0]
    assert info.customProperties is not None
    assert info.customProperties["columns"] == "b_id,a_id"
    assert info.customProperties["to_columns"] == "y,x"


def test_unresolvable_foreign_key_target_has_one_identity() -> None:
    source = _make_source()

    def urn(to: str) -> str:
        node = _make_model(
            columns=[_column("x_id", DBTConstraint(type="foreign_key", to=to))]
        )
        (assertion,) = source._create_constraint_assertions(node, _ENTITY_URN)
        return assertion.urn

    # A ref to another project, raw and as compiled.
    assert urn("ref('other_project', 'dim_x')") == urn('"db"."other"."dim_x"')


def test_column_case_does_not_split_assertions() -> None:
    source = _make_source()
    node = _make_model(
        columns=[_column("ID", DBTConstraint(type="not_null"))],
        model_constraints=[DBTConstraint(type="primary_key", columns=["id"])],
    )
    # One unique plus one not-null, not a not-null per spelling.
    assert len(source._create_constraint_assertions(node, _ENTITY_URN)) == 2


def test_cloud_enum_style_constraint_types_are_supported() -> None:
    source = _make_source()
    node = _make_model(
        model_constraints=[
            parse_dbt_constraint({"type": "PrimaryKey", "columns": ["id"]}),
            parse_dbt_constraint({"type": "NOT_NULL", "columns": ["email"]}),
        ]
    )
    assert len(source._create_constraint_assertions(node, _ENTITY_URN)) == 3
    assert not source.report.contract_constraints_skipped_unsupported


def test_unknown_constraint_type_is_reported_not_raised() -> None:
    source = _make_source()
    node = _make_model(columns=[_column("id", DBTConstraint(type="exclusion"))])
    assert source._create_constraint_assertions(node, _ENTITY_URN) == []
    assert len(source.report.contract_constraints_skipped_unsupported) == 1


@pytest.mark.parametrize("name", [None, "fk_customer"])
def test_foreign_key_urn_ignores_how_to_is_rendered(name: Optional[str]) -> None:
    source = _make_source()
    customers = _make_model(name="customers")

    def urn(to: str) -> str:
        node = _make_model(
            columns=[
                _column(
                    "customer_id",
                    DBTConstraint(
                        type="foreign_key", name=name, to=to, to_columns=["id"]
                    ),
                )
            ]
        )
        (assertion,) = source._create_constraint_assertions(
            node, _ENTITY_URN, _RelationResolver([node, customers])
        )
        return assertion.urn

    assert urn("ref('customers')") == urn('"test_db"."test_schema"."customers"')


def test_contract_urn_matches_canonical_entity_guid() -> None:
    source = _make_source()
    node = _make_model()
    entity_urn = node.get_urn("dbt", source.config.env, None)
    mcps = _contract_mcps(source, [node])
    contract_urns = {
        mcp.entityUrn
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    }
    assert contract_urns == {
        f"urn:li:dataContract:{mce_builder.datahub_guid({'entity': entity_urn})}"
    }


def test_contract_test_urn_uses_filtered_upstreams() -> None:
    source = _make_source()
    model = _make_model()
    # The orphan upstream is not in all_nodes_map, so the test has a single
    # valid upstream and keeps the backwards-compatible URN.
    test_node = _make_test(
        "orders_check", [model.dbt_name, "model.test_pkg.orphan"], tagged=True
    )
    urns = _dq_urns(_contract_mcps(source, [model], [test_node]))
    assert source._make_test_assertion_urn(test_node, model.dbt_name, 1) in urns


@pytest.mark.parametrize("declared_on_orders", [True, False])
def test_tagged_multi_model_test_attaches_to_declaring_model_only(
    declared_on_orders: bool,
) -> None:
    # dbt Cloud does not say which model a test is declared on; such a test
    # is not attached to any of the models it reads.
    source = _make_source()
    orders = _make_model(name="orders")
    customers = _make_model(name="customers")
    test_node = _make_test(
        "relationships_orders_customer_id",
        [orders.dbt_name, customers.dbt_name, "macro.dbt.test_relationships"],
        tagged=True,
        qualified_test_name="relationships",
        attached_node=orders.dbt_name if declared_on_orders else None,
    )
    by_entity = _dq_urns_by_entity(
        _contract_mcps(source, [orders, customers], [test_node])
    )
    orders_urn = orders.get_urn("dbt", source.config.env, None)
    customers_urn = customers.get_urn("dbt", source.config.env, None)
    assert (
        source._make_test_assertion_urn(test_node, orders.dbt_name, 2)
        in (by_entity[orders_urn])
    ) is declared_on_orders
    assert (
        source._make_test_assertion_urn(test_node, customers.dbt_name, 2)
        not in (by_entity[customers_urn])
    )


def test_constraint_matching_tests_are_auto_linked() -> None:
    source = _make_source()
    model = _make_model(
        columns=[_column("email", DBTConstraint(type="not_null"))],
        model_constraints=[DBTConstraint(type="unique", columns=["id", "email"])],
    )
    not_null = _make_test(
        "not_null_orders_email",
        [model.dbt_name],
        qualified_test_name="not_null",
        column_name="email",
    )
    # A single-column unique test does not prove a composite unique constraint.
    unique_id = _make_test(
        "unique_orders_id",
        [model.dbt_name],
        qualified_test_name="unique",
        column_name="id",
    )
    unrelated = _make_test(
        "accepted_values_orders_status",
        [model.dbt_name],
        qualified_test_name="accepted_values",
        column_name="status",
    )
    # A filtered test checks only some rows.
    filtered = _make_test(
        "not_null_orders_email_filtered",
        [model.dbt_name],
        qualified_test_name="not_null",
        column_name="email",
    )
    assert filtered.test_info is not None
    filtered.test_info.where = "status = 'shipped'"
    row_filtered = _make_test(
        "expect_orders_email_not_null",
        [model.dbt_name],
        qualified_test_name="dbt_expectations.expect_column_values_to_not_be_null",
        column_name="email",
        kw_args={"row_condition": "status = 'shipped'"},
    )
    urns = _dq_urns(
        _contract_mcps(
            source, [model], [not_null, unique_id, unrelated, filtered, row_filtered]
        )
    )
    assert source._make_test_assertion_urn(not_null, model.dbt_name, 1) in urns
    assert source._make_test_assertion_urn(filtered, model.dbt_name, 1) not in urns
    assert source._make_test_assertion_urn(row_filtered, model.dbt_name, 1) not in urns
    assert source._make_test_assertion_urn(unique_id, model.dbt_name, 1) not in urns
    assert source._make_test_assertion_urn(unrelated, model.dbt_name, 1) not in urns


@pytest.mark.parametrize(
    "constraint_to",
    [
        "ref('customers')",
        # dbt compiles FK targets to the relation after run/build.
        '"test_db"."test_schema"."dim_customers"',
        "`test_db`.`test_schema`.`dim_customers`",
    ],
)
def test_relationships_test_matches_foreign_key_target(constraint_to: str) -> None:
    source = _make_source()
    # The referenced model is aliased, so its relation name differs from its
    # model name.
    customers = _make_model(name="customers", alias="dim_customers")
    orders = _make_model(
        model_constraints=[
            DBTConstraint(
                type="foreign_key",
                columns=["customer_id"],
                to=constraint_to,
                to_columns=["id"],
            )
        ]
    )
    matched = _make_test(
        "relationships_orders_customer",
        [orders.dbt_name, customers.dbt_name],
        qualified_test_name="relationships",
        column_name="customer_id",
        kw_args={"to": "ref('customers')", "field": "id"},
    )
    mismatched = _make_test(
        "relationships_orders_store",
        [orders.dbt_name],
        qualified_test_name="relationships",
        column_name="customer_id",
        kw_args={"to": "ref('stores')", "field": "id"},
    )
    urns = _dq_urns(
        _contract_mcps(source, [orders], [matched, mismatched], other_nodes=[customers])
    )
    assert source._make_test_assertion_urn(matched, orders.dbt_name, 2) in urns
    assert source._make_test_assertion_urn(mismatched, orders.dbt_name, 1) not in urns


def test_relationships_test_attaches_only_to_its_declaring_model() -> None:
    # orders.manager_id -> employees.id has the same shape as employees'
    # self-referencing foreign key, but the column is orders'.
    source = _make_source()
    employees = _make_model(
        name="employees",
        columns=[
            _column(
                "manager_id",
                DBTConstraint(
                    type="foreign_key", to="ref('employees')", to_columns=["id"]
                ),
            )
        ],
    )
    orders = _make_model(name="orders")
    test_node = _make_test(
        "relationships_orders_manager_id",
        [orders.dbt_name, employees.dbt_name],
        qualified_test_name="relationships",
        column_name="manager_id",
        kw_args={"to": "ref('employees')", "field": "id"},
        attached_node=orders.dbt_name,
    )
    by_entity = _dq_urns_by_entity(
        _contract_mcps(source, [orders, employees], [test_node])
    )
    employees_urn = employees.get_urn("dbt", source.config.env, None)
    assert (
        source._make_test_assertion_urn(test_node, employees.dbt_name, 2)
        not in by_entity[employees_urn]
    )


def test_quoted_test_column_matches_constraint() -> None:
    # With ``quote: true`` dbt passes the quoted column name to tests.
    source = _make_source()
    model = _make_model(columns=[_column("ID", DBTConstraint(type="not_null"))])
    test_node = _make_test(
        "not_null_orders_id",
        [model.dbt_name],
        qualified_test_name="not_null",
        column_name='"ID"',
    )
    urns = _dq_urns(_contract_mcps(source, [model], [test_node]))
    assert source._make_test_assertion_urn(test_node, model.dbt_name, 1) in urns


@pytest.mark.parametrize(
    "expression, matches",
    [
        ("dim_customers (id)", True),
        ('references "test_schema"."dim_customers" ("id")', True),
        ("dim_stores (id)", False),
        ("not parseable", False),
    ],
)
def test_foreign_key_expression_matches_only_its_target(
    expression: str, matches: bool
) -> None:
    source = _make_source()
    customers = _make_model(name="customers", alias="dim_customers")
    orders = _make_model(
        model_constraints=[
            DBTConstraint(
                type="foreign_key", columns=["customer_id"], expression=expression
            )
        ]
    )
    test_node = _make_test(
        "relationships_orders_customer",
        [orders.dbt_name, customers.dbt_name],
        qualified_test_name="relationships",
        column_name="customer_id",
        kw_args={"to": "ref('customers')", "field": "id"},
    )
    urns = _dq_urns(
        _contract_mcps(source, [orders], [test_node], other_nodes=[customers])
    )
    assert (source._make_test_assertion_urn(test_node, orders.dbt_name, 2) in urns) is (
        matches
    )


def test_unresolvable_foreign_key_target_is_not_matched_by_name() -> None:
    # A ref to a model outside this manifest (or another package's model of
    # the same name) could be any schema's relation.
    resolver = _RelationResolver([_make_model(name="customers")])
    assert resolver.resolve("ref('customers')")[0] == (
        "test_db",
        "test_schema",
        "customers",
    )
    assert resolver.resolve("ref('other_project', 'customers')")[0] is None
    assert resolver.resolve("ref('missing')")[0] is None


def test_unqualified_ref_prefers_root_project_over_package() -> None:
    root = _make_model(name="customers")
    shadow = _make_model(name="customers", alias="pkg_customers")
    shadow.dbt_name = "model.other_pkg.customers"
    assert _RelationResolver([root, shadow]).resolve("ref('customers')")[0] is None
    resolver = _RelationResolver([root, shadow], root_package="test_pkg")
    assert resolver.resolve("ref('customers')")[0] == (
        "test_db",
        "test_schema",
        "customers",
    )
    assert resolver.resolve("ref('other_pkg', 'customers')")[0] == (
        "test_db",
        "test_schema",
        "pkg_customers",
    )


def test_versioned_ref_resolves_to_its_version() -> None:
    versions = []
    for version in ("1", "2"):
        node = _make_model(name="orders", alias=f"orders_v{version}")
        node.dbt_name = f"model.test_pkg.orders.v{version}"
        versions.append(node)
    resolver = _RelationResolver(versions)
    assert resolver.resolve("ref('orders', v=2)")[0] == (
        "test_db",
        "test_schema",
        "orders_v2",
    )
    assert resolver.resolve("ref('test_pkg', 'orders', version='1')")[0] == (
        "test_db",
        "test_schema",
        "orders_v1",
    )
    # Without a version, dbt picks the latest, which the nodes do not record.
    assert resolver.resolve("ref('orders')")[0] is None


def test_unversioned_ref_uses_latest_version_for_fk_identity() -> None:
    source = _make_source()
    versions = []
    for version in ("1", "2"):
        node = _make_model(name="orders", alias=f"orders_v{version}")
        node.dbt_name = f"model.test_pkg.orders.v{version}"
        node.latest_version = "2"
        versions.append(node)

    def urn(to: str) -> str:
        node = _make_model(
            name="returns",
            columns=[_column("order_id", DBTConstraint(type="foreign_key", to=to))],
        )
        (assertion,) = source._create_constraint_assertions(
            node, _ENTITY_URN, _RelationResolver([node, *versions])
        )
        return assertion.urn

    # `dbt parse` keeps the raw ref; `dbt compile` renders the latest version.
    assert urn("ref('orders')") == urn('"test_db"."test_schema"."orders_v2"')


def test_test_urns_omitted_when_test_definitions_disabled() -> None:
    source = _make_source(entities_enabled={"test_definitions": "NO"})
    model = _make_model()
    test_node = _make_test("orders_check", [model.dbt_name], tagged=True)
    urns = _dq_urns(_contract_mcps(source, [model], [test_node]))
    entity_urn = model.get_urn("dbt", source.config.env, None)
    assert urns == [source._create_schema_assertion_for_contract(model, entity_urn).urn]


def test_contract_skipped_when_node_type_emission_disabled() -> None:
    source = _make_source(entities_enabled={"models": "NO", "test_results": "NO"})
    node = _make_model()
    node.model_performances = [_perf()]
    assert _contract_mcps(source, [node]) == []


@pytest.mark.parametrize("test_results", ["ONLY", "NO"])
def test_split_test_results_recipes(test_results: str) -> None:
    # The documented two-recipe setup: one recipe with test_results: Only,
    # the other with test_results: No. Together they emit everything once.
    source = _make_source(entities_enabled={"test_results": test_results})
    node = _make_model()
    node.model_performances = [_perf()]
    mcps = _contract_mcps(source, [node])
    schema_urn = source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    if test_results == "ONLY":
        assert schema_urn in {e.assertionUrn for e in _run_events(mcps)}
        assert _assertion_infos(mcps) == {}
        assert _written_contracts(mcps) == []
    else:
        assert _run_events(mcps) == []
        assert schema_urn in _assertion_infos(mcps)
        assert _written_contracts(mcps) == [_contract_urn(node)]


def test_model_success_reports_schema_and_ddl_enforced_constraints() -> None:
    source = _make_source()
    node = _make_model(
        adapter="postgres",
        columns=[_column("id", DBTConstraint(type="not_null"))],
        # Same constraint at model level; emitted and reported once.
        model_constraints=[DBTConstraint(type="not_null", columns=["id"])],
    )
    node.model_performances = [_perf()]
    mcps = _contract_mcps(source, [node])
    events = _run_events(mcps)
    assert len(events) == 2
    assert all(e.result is not None and e.result.type == "SUCCESS" for e in events)
    assert len(_dq_urns(mcps)) == 2


@pytest.mark.parametrize(
    "adapter, materialization",
    [
        # dbt never applies constraints to views, and applies them to an
        # incremental model only when its table is (re)created.
        ("postgres", "view"),
        ("postgres", "incremental"),
        # Databricks adds constraints after writing, so a later failed build
        # would leave a stale pass.
        ("databricks", "table"),
    ],
)
def test_success_without_constraint_pass_reports_schema_only(
    adapter: str, materialization: str
) -> None:
    source = _make_source()
    node = _make_model(
        adapter=adapter,
        materialization=materialization,
        columns=[_column("id", DBTConstraint(type="not_null"))],
    )
    node.model_performances = [_perf()]
    (event,) = _run_events(_contract_mcps(source, [node]))
    assert event.assertionUrn == (
        source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    )


@pytest.mark.parametrize(
    "perf",
    [
        _perf(built_relation=False),  # e.g. `dbt compile`
        _perf(status="error", message="Database Error: relation does not exist"),
    ],
)
def test_runs_that_say_nothing_about_the_contract_emit_no_events(
    perf: DBTModelPerformance,
) -> None:
    source = _make_source()
    node = _make_model(
        adapter="postgres", columns=[_column("id", DBTConstraint(type="not_null"))]
    )
    node.model_performances = [perf]
    assert _run_events(_contract_mcps(source, [node])) == []


def test_preflight_failure_fails_schema_assertion_only() -> None:
    source = _make_source()
    node = _make_model(
        adapter="postgres", columns=[_column("id", DBTConstraint(type="not_null"))]
    )
    node.model_performances = [_perf(status="error", message=_PREFLIGHT_MESSAGE)]
    (event,) = _run_events(_contract_mcps(source, [node]))
    assert event.result is not None and event.result.type == "FAILURE"
    assert event.result.nativeResults is not None
    assert "enforced contract that failed" in event.result.nativeResults["message"]


def test_run_events_honor_test_results_toggle() -> None:
    source = _make_source(entities_enabled={"test_results": "NO"})
    node = _make_model()
    node.model_performances = [_perf()]
    assert _run_events(_contract_mcps(source, [node])) == []


def test_parse_model_run_infers_missing_timing_only_for_contracts() -> None:
    metadata = DBTRunMetadata(
        dbt_schema_version="https://schemas.getdbt.com/dbt/run-results/v6.json",
        dbt_version="1.9.3",
        generated_at="2025-05-08T15:51:03.466215Z",
        invocation_id="no-timing",
    )
    result = DBTRunResult.model_validate(
        {
            "status": "error",
            "unique_id": "model.test_pkg.orders",
            "message": _PREFLIGHT_MESSAGE,
            "timing": [
                {
                    "name": "execute",
                    "started_at": "2025-05-08T15:50:00.000000Z",
                    "completed_at": None,
                }
            ],
        }
    )
    assert _parse_model_run(metadata, result) is None
    perf = _parse_model_run(metadata, result, allow_missing_timing=True)
    assert perf is not None
    assert perf.timing_inferred
    assert perf.start_time.isoformat().startswith("2025-05-08T15:50:00")
    assert perf.end_time.isoformat().startswith("2025-05-08T15:51:03")


@pytest.mark.parametrize("which, built", [("build", True), ("compile", False)])
def test_load_run_results_marks_relation_building_commands(
    which: str, built: bool
) -> None:
    fixture = json.loads(
        (
            Path(__file__).parent / "artifacts" / "run_results_contract_preflight.json"
        ).read_text()
    )
    fixture["args"]["which"] = which
    source = _make_source()
    node = _make_model()
    load_run_results(source.config, fixture, {"model.test_pkg.orders": node})
    (perf,) = node.model_performances
    assert perf.message is not None and "enforced contract" in perf.message
    assert perf.built_relation is built


def test_run_results_without_command_warns_when_contracts_enabled() -> None:
    fixture = json.loads(
        (
            Path(__file__).parent / "artifacts" / "run_results_contract_preflight.json"
        ).read_text()
    )
    del fixture["args"]
    source = _make_source()
    with mock.patch.object(source._artifacts, "load_json", return_value=fixture):
        source._load_run_results_files(
            ["run_results.json"], None, [_make_model()], multi_project=False
        )
    assert any(
        w.title == "run_results without a dbt command" for w in source.report.warnings
    )


def test_inferred_timing_is_not_emitted_as_process_run() -> None:
    source = _make_source()
    node = _make_model()
    inferred = _perf()
    inferred.timing_inferred = True
    node.model_performances = [inferred]
    assert list(source._create_dataprocess_instance_mcps(node, None)) == []
    node.model_performances = [_perf()]
    assert list(source._create_dataprocess_instance_mcps(node, None))


def _related_contracts(
    contracts: Dict[str, DataContractPropertiesClass], entity_urn: str
) -> List[RelatedEntity]:
    return [
        RelatedEntity(urn=urn, relationship_type="ContractFor")
        for urn, properties in contracts.items()
        if properties.entity == entity_urn
    ]


def _graph_with_contracts(
    contracts: Dict[str, DataContractPropertiesClass],
    removed: bool = False,
    ghost_urns: Sequence[str] = (),
    removed_urns: Sequence[str] = (),
) -> mock.Mock:
    def get_entities(
        entity_name: str, urns: List[str], aspects: List[str], **kwargs: Any
    ) -> Dict[str, Dict[str, Any]]:
        return {
            urn: {
                DataContractPropertiesClass.ASPECT_NAME: (contracts[urn], None),
                StatusClass.ASPECT_NAME: (
                    StatusClass(removed=removed or urn in removed_urns),
                    None,
                ),
            }
            for urn in urns
            if urn in contracts
        }

    graph = mock.Mock()
    graph.get_entities.side_effect = get_entities
    # Ghosts are still linked to the dataset but have no aspects left.
    graph.get_related_entities.side_effect = lambda entity_urn, **_: [
        *_related_contracts(contracts, entity_urn),
        *(
            RelatedEntity(urn=urn, relationship_type="ContractFor")
            for urn in ghost_urns
        ),
    ]
    return graph


def _contract_urn(node: DBTNode, env: str = "PROD") -> str:
    entity_urn = node.get_urn("dbt", env, None)
    return f"urn:li:dataContract:{mce_builder.datahub_guid({'entity': entity_urn})}"


# A contract created in the UI gets a random URN.
_UI_CONTRACT_URN = "urn:li:dataContract:5f0c1a52-ui-created"


def _existing_contract(*dq_urns: str) -> DataContractPropertiesClass:
    return DataContractPropertiesClass(
        entity=_ENTITY_URN,
        dataQuality=[DataQualityContractClass(assertion=urn) for urn in dq_urns],
    )


def _written_contracts(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    return [
        str(mcp.entityUrn)
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]


def test_existing_non_dbt_contract_is_left_untouched() -> None:
    source = _make_source()
    node = _make_model()
    source.ctx.graph = _graph_with_contracts(
        {_UI_CONTRACT_URN: _existing_contract("urn:li:assertion:authored")}
    )
    mcps = _contract_mcps(source, [node])
    assert _written_contracts(mcps) == []
    assert source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn in (
        _assertion_infos(mcps)
    )
    assert source.report.num_contracts_skipped_existing == 1


def test_dbt_contract_is_rewritten_in_place_with_fresh_entries() -> None:
    source = _make_source()
    node = _make_model()
    schema_urn = source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    source.ctx.graph = _graph_with_contracts(
        {_contract_urn(node): _existing_contract(schema_urn, "urn:li:assertion:old")}
    )
    mcps = _contract_mcps(source, [node])
    assert _written_contracts(mcps) == [_contract_urn(node)]
    assert _dq_urns(mcps) == [schema_urn]


def test_soft_deleted_contract_at_dbt_urn_is_revived() -> None:
    source = _make_source()
    node = _make_model()
    source.ctx.graph = _graph_with_contracts(
        {_contract_urn(node): _existing_contract("urn:li:assertion:authored")},
        removed=True,
    )
    assert _written_contracts(_contract_mcps(source, [node])) == [_contract_urn(node)]


@pytest.mark.parametrize("replace", [False, True])
def test_ui_contract_listing_dbt_schema_assertion_is_not_dbts(replace: bool) -> None:
    source = _make_source(replace_existing_contracts_with_dbt=replace)
    node = _make_model()
    schema_urn = source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    source.ctx.graph = _graph_with_contracts(
        {_UI_CONTRACT_URN: _existing_contract(schema_urn)}
    )
    written = _written_contracts(_contract_mcps(source, [node]))
    assert written == ([_UI_CONTRACT_URN] if replace else [])


def test_yaml_contract_at_dbt_urn_without_dbt_schema_assertion_is_skipped() -> None:
    source = _make_source()
    node = _make_model()
    source.ctx.graph = _graph_with_contracts(
        {_contract_urn(node): _existing_contract("urn:li:assertion:authored")}
    )
    assert _written_contracts(_contract_mcps(source, [node])) == []
    assert source.report.num_contracts_skipped_existing == 1


def test_skipped_contract_without_status_gets_no_new_status() -> None:
    source = _make_source()
    pending = source._create_contract_assertions_for_node(
        _make_model(), [], {}, set(), _RelationResolver([])
    )[1]
    skipped = _ExistingContract(
        urn=_UI_CONTRACT_URN, properties=_existing_contract(), status=None
    )
    assert list(source._skip_existing_contracts(pending, [skipped])) == []


def test_new_contract_uses_the_fixed_urn() -> None:
    source = _make_source()
    node = _make_model()
    source.ctx.graph = _graph_with_contracts({})
    assert _written_contracts(_contract_mcps(source, [node])) == [_contract_urn(node)]


def test_replace_existing_contracts_writes_to_the_existing_urn() -> None:
    source = _make_source(replace_existing_contracts_with_dbt=True)
    node = _make_model()
    source.ctx.graph = _graph_with_contracts(
        {_UI_CONTRACT_URN: _existing_contract("urn:li:assertion:authored")}
    )
    mcps = _contract_mcps(source, [node])
    assert _written_contracts(mcps) == [_UI_CONTRACT_URN]
    assert _dq_urns(mcps) == [
        source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    ]


def test_multiple_existing_contracts_are_left_untouched() -> None:
    source = _make_source(replace_existing_contracts_with_dbt=True)
    node = _make_model()
    source.ctx.graph = _graph_with_contracts(
        {
            _UI_CONTRACT_URN: _existing_contract(),
            _contract_urn(node): _existing_contract(),
        }
    )
    assert _written_contracts(_contract_mcps(source, [node])) == []
    assert source.report.warnings


def test_renamed_model_with_same_relation_keeps_its_contract() -> None:
    source = _make_source()
    orders = _make_model(name="orders")
    first = _contract_mcps(source, [orders])
    (properties,) = [
        mcp.aspect
        for mcp in first
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]

    source = _make_source()
    renamed = _make_model(name="orders_v2", alias="orders")
    source.ctx.graph = _graph_with_contracts({_contract_urn(orders): properties})
    assert _written_contracts(_contract_mcps(source, [renamed])) == [
        _contract_urn(orders)
    ]
    assert source.report.num_contracts_skipped_existing == 0


def test_soft_deleted_contract_does_not_count_as_multiple() -> None:
    source = _make_source()
    node = _make_model()
    schema_urn = source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    source.ctx.graph = _graph_with_contracts(
        {
            _UI_CONTRACT_URN: _existing_contract(),
            _contract_urn(node): _existing_contract(schema_urn),
        },
        removed_urns=[_UI_CONTRACT_URN],
    )
    assert _written_contracts(_contract_mcps(source, [node])) == [_contract_urn(node)]
    assert not source.report.warnings


def test_all_soft_deleted_contracts_revive_the_dataset_contract() -> None:
    source = _make_source()
    node = _make_model()
    source.ctx.graph = _graph_with_contracts(
        {
            _UI_CONTRACT_URN: _existing_contract(),
            _contract_urn(node): _existing_contract(),
        },
        removed=True,
    )
    assert _written_contracts(_contract_mcps(source, [node])) == [_contract_urn(node)]
    assert not source.report.warnings


@pytest.mark.parametrize("other", [None, "authored", "dbt"])
def test_contract_without_properties_is_ignored(other: Optional[str]) -> None:
    source = _make_source()
    node = _make_model()
    schema_urn = source._create_schema_assertion_for_contract(node, _ENTITY_URN).urn
    contracts = {}
    if other == "authored":
        contracts[_UI_CONTRACT_URN] = _existing_contract("urn:li:assertion:authored")
    elif other == "dbt":
        contracts[_contract_urn(node)] = _existing_contract(schema_urn)
    source.ctx.graph = _graph_with_contracts(
        contracts, ghost_urns=["urn:li:dataContract:ghost"]
    )
    written = _written_contracts(_contract_mcps(source, [node]))
    expected = {
        None: [_contract_urn(node)],
        "authored": [],
        "dbt": [_contract_urn(node)],
    }
    assert written == expected[other]
    assert not source.report.warnings


@pytest.mark.parametrize("test_results, fails", [("YES", True), ("ONLY", False)])
def test_no_graph_with_stale_removal_fails_at_startup(
    test_results: str, fails: bool
) -> None:
    # Unwritten contracts would drop out of the checkpoint and be soft-deleted.
    # A test_results: Only recipe writes no contracts, so it is not affected.
    source = _make_source(
        stateful_ingestion={"enabled": True},
        write_semantics="OVERRIDE",
        entities_enabled={"test_results": test_results},
    )
    source.ctx.graph = None
    with mock.patch.object(
        source.state_provider, "is_stateful_ingestion_configured", return_value=True
    ):
        try:
            next(iter(source.get_workunits_internal()))
            needs_graph = False
        except ConfigurationError as e:
            needs_graph = "ingest_contracts" in str(e)
        except Exception:
            # The run goes on past the check (and fails on the missing files).
            needs_graph = False
    assert needs_graph is fails


@pytest.mark.parametrize("replace", [False, True])
def test_no_graph_writes_contract_only_when_replacing(replace: bool) -> None:
    source = _make_source(replace_existing_contracts_with_dbt=replace)
    source.ctx.graph = None
    node = _make_model()
    mcps = _contract_mcps(source, [node])
    assert _assertion_infos(mcps)
    if replace:
        assert _written_contracts(mcps) == [_contract_urn(node)]
    else:
        assert _written_contracts(mcps) == []
        assert source.report.warnings


def test_contract_read_failure_is_reported_as_failure() -> None:
    # A failure makes stale entity removal keep the previous state.
    source = _make_source()
    graph = mock.Mock()
    graph.get_related_entities.side_effect = RuntimeError("GMS unavailable")
    source.ctx.graph = graph
    mcps = _contract_mcps(source, [_make_model()])
    assert _assertion_infos(mcps)
    assert _written_contracts(mcps) == []
    assert source.report.failures


def test_one_failing_model_does_not_stop_the_others() -> None:
    source = _make_source()
    broken = _make_model(name="broken")
    healthy = _make_model(name="healthy")
    original = source._create_schema_assertion_for_contract

    def fail_for_broken(node: DBTNode, entity_urn: str, **kwargs: Any) -> Any:
        if node is broken:
            raise RuntimeError("boom")
        return original(node, entity_urn, **kwargs)

    with mock.patch.object(
        source, "_create_schema_assertion_for_contract", side_effect=fail_for_broken
    ):
        mcps = _contract_mcps(source, [broken, healthy])
    assert list(_dq_urns_by_entity(mcps)) == [
        healthy.get_urn("dbt", source.config.env, None)
    ]
    assert source.report.node_emission_failures == 1


class _WriteThroughGraph:
    """Fake graph whose reads see every aspect written so far, like a sink
    that writes each workunit as it is produced."""

    def __init__(self) -> None:
        self.aspects: Dict[str, Dict[str, Any]] = {}
        self.mock = mock.Mock()
        self.mock.get_entities.side_effect = self.get_entities
        self.mock.get_related_entities.side_effect = lambda entity_urn, **_: (
            _related_contracts(
                {
                    urn: aspects[DataContractPropertiesClass.ASPECT_NAME]
                    for urn, aspects in self.aspects.items()
                    if DataContractPropertiesClass.ASPECT_NAME in aspects
                },
                entity_urn,
            )
        )
        self.mock.get_config.return_value = {"statefulIngestionCapable": True}

    def write(self, mcp: Any) -> None:
        if isinstance(mcp, MetadataChangeProposalWrapper) and mcp.aspect is not None:
            self.aspects.setdefault(str(mcp.entityUrn), {})[mcp.aspect.ASPECT_NAME] = (
                mcp.aspect
            )

    def get_entities(
        self, entity_name: str, urns: List[str], aspects: List[str], **kwargs: Any
    ) -> Dict[str, Dict[str, Any]]:
        return {
            urn: {
                name: (aspect, None)
                for name, aspect in self.aspects[urn].items()
                if name in aspects
            }
            for urn in urns
            if urn in self.aspects
        }


def _run_stateful(
    tmp_path: Path,
    *,
    enforced: bool,
    previous_urns: List[str],
    graph: Any,
) -> Tuple[List[Any], List[str]]:
    """Run the source through its workunit processors, incl. stale removal.

    Returns the workunits and the URNs in this run's checkpoint.
    """
    manifest = _build_contract_manifest()
    manifest["nodes"]["model.contract_test.orders"]["config"]["contract"][
        "enforced"
    ] = enforced
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    write_through = graph if isinstance(graph, _WriteThroughGraph) else None
    graph_mock = write_through.mock if write_through else graph
    graph_mock.get_config.return_value = {"statefulIngestionCapable": True}
    ctx = PipelineContext(
        run_id="next-run", pipeline_name="dbt-contracts", graph=graph_mock
    )
    source = DBTCoreSource(
        DBTCoreConfig(
            manifest_path=str(manifest_path),
            target_platform="postgres",
            ingest_contracts=True,
            enable_meta_mapping=False,
            write_semantics="OVERRIDE",
            # The fail-safe would otherwise hide removals in this tiny project.
            stateful_ingestion={
                "enabled": True,
                "remove_stale_metadata": True,
                "fail_safe_threshold": 100.0,
            },
        ),
        ctx,
    )
    previous = Checkpoint(
        job_name="dbt_stateful_ingestion",
        pipeline_name="dbt-contracts",
        run_id="previous-run",
        state=GenericCheckpointState(urns=previous_urns),
    )
    workunits: List[Any] = []
    with mock.patch.object(
        source.state_provider, "get_last_checkpoint", return_value=previous
    ):
        for wu in source.get_workunits():
            if write_through:
                write_through.write(wu.metadata)
            workunits.append(wu)
    checkpoint = source.state_provider.get_current_checkpoint(
        StaleEntityRemovalHandler.compute_job_id("dbt")
    )
    assert checkpoint is not None
    return workunits, list(checkpoint.state.urns)


def _soft_deleted(workunits: List[Any]) -> List[str]:
    return [
        str(wu.metadata.entityUrn)
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, StatusClass)
        and wu.metadata.aspect.removed
    ]


_E2E_CONTRACT_URN = "urn:li:dataContract:" + mce_builder.datahub_guid(
    {
        "entity": "urn:li:dataset:(urn:li:dataPlatform:dbt,"
        "test_db.test_schema.orders,PROD)"
    }
)


def test_stateful_read_failure_does_not_soft_delete_contract(tmp_path: Path) -> None:
    graph = mock.Mock()
    graph.get_related_entities.side_effect = RuntimeError("GMS unavailable")
    workunits, _ = _run_stateful(
        tmp_path, enforced=True, previous_urns=[_E2E_CONTRACT_URN], graph=graph
    )
    assert _E2E_CONTRACT_URN not in _soft_deleted(workunits)


def test_stateful_dropped_contract_is_soft_deleted(tmp_path: Path) -> None:
    graph = _WriteThroughGraph()
    _, state = _run_stateful(tmp_path, enforced=True, previous_urns=[], graph=graph)
    assert _E2E_CONTRACT_URN in state
    workunits, _ = _run_stateful(
        tmp_path, enforced=False, previous_urns=state, graph=graph
    )
    assert _E2E_CONTRACT_URN in _soft_deleted(workunits)


def test_stateful_skipped_contract_is_not_soft_deleted(tmp_path: Path) -> None:
    # Tracked by an earlier run, but no longer dbt's (e.g. its schema assertion
    # was removed from the contract in DataHub).
    graph = _WriteThroughGraph()
    authored = _existing_contract("urn:li:assertion:authored")
    # The earlier run wrote it, so it has a status aspect.
    graph.aspects[_UI_CONTRACT_URN] = {
        DataContractPropertiesClass.ASPECT_NAME: authored,
        StatusClass.ASPECT_NAME: StatusClass(removed=False),
    }
    workunits, state = _run_stateful(
        tmp_path, enforced=True, previous_urns=[_UI_CONTRACT_URN], graph=graph
    )
    assert _UI_CONTRACT_URN not in _soft_deleted(workunits)
    assert _UI_CONTRACT_URN not in state
    assert (
        graph.aspects[_UI_CONTRACT_URN][DataContractPropertiesClass.ASPECT_NAME]
        is authored
    )


def _build_contract_manifest() -> Dict[str, Any]:
    """One contracted model and one contract-tagged not_null test."""
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v10.json",
            "dbt_version": "1.8.0",
            "generated_at": "2024-01-01T00:00:00.000000Z",
            "adapter_type": "postgres",
            "project_name": "contract_test",
            "invocation_id": "test-invocation",
            "env": {},
        },
        "nodes": {
            "model.contract_test.orders": {
                "database": "test_db",
                "schema": "test_schema",
                "name": "orders",
                "resource_type": "model",
                "package_name": "contract_test",
                "original_file_path": "models/orders.sql",
                "unique_id": "model.contract_test.orders",
                "alias": "orders",
                "config": {
                    "materialized": "table",
                    "contract": {"enforced": True, "alias_types": False},
                },
                "tags": [],
                "description": "",
                "columns": {
                    "id": {
                        "name": "id",
                        "data_type": "bigint",
                        "constraints": [{"type": "primary_key"}],
                    },
                    "email": {
                        "name": "email",
                        "data_type": "varchar(255)",
                        "constraints": [{"type": "not_null"}],
                    },
                },
                "constraints": [
                    {
                        "type": "check",
                        "name": "ck_email",
                        "expression": "email like '%@%'",
                        "columns": ["email"],
                    }
                ],
                "contract": {"checksum": "contract-checksum-abc"},
                "meta": {},
                "depends_on": {"macros": [], "nodes": []},
                "compiled_code": "SELECT * FROM raw.orders",
                "raw_code": "SELECT * FROM {{ source('raw', 'orders') }}",
                "language": "sql",
            },
            "test.contract_test.orders_email_not_null": {
                "database": "test_db",
                "schema": "test_schema",
                "name": "orders_email_not_null",
                "resource_type": "test",
                "package_name": "contract_test",
                "original_file_path": "models/orders.yml",
                "unique_id": "test.contract_test.orders_email_not_null",
                "alias": "orders_email_not_null",
                "config": {"materialized": "test", "tags": ["contract"]},
                "tags": ["contract"],
                "description": "",
                "columns": {},
                "meta": {},
                "depends_on": {
                    "macros": ["macro.dbt.test_not_null"],
                    "nodes": ["model.contract_test.orders"],
                },
                "compiled_code": "select * from orders where email is null",
                "raw_code": "{{ test_not_null(**kwargs) }}",
                "language": "sql",
                "column_name": "email",
                "attached_node": "model.contract_test.orders",
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "email", "model": "{{ ref('orders') }}"},
                    "namespace": None,
                },
            },
        },
        "sources": {},
        "macros": {},
        "parent_map": {},
        "child_map": {},
        "disabled": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "docs": {},
        "semantic_models": {},
    }


def test_contract_ingestion_end_to_end(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_build_contract_manifest()))
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
                    "dbt_version": "1.8.0",
                    "generated_at": "2024-01-01T00:00:00.000000Z",
                    "invocation_id": "test-invocation",
                    "env": {},
                },
                "nodes": {
                    "model.contract_test.orders": {
                        "metadata": {
                            "type": "BASE TABLE",
                            "schema": "test_schema",
                            "name": "orders",
                            "database": "test_db",
                            "owner": None,
                        },
                        "columns": {
                            "id": {"type": "BIGINT", "index": 0, "name": "id"},
                            "email": {"type": "TEXT", "index": 1, "name": "email"},
                        },
                        "stats": {},
                    }
                },
                "sources": {},
                "errors": None,
            }
        )
    )
    source = DBTCoreSource(
        DBTCoreConfig(
            manifest_path=str(manifest_path),
            catalog_path=str(catalog_path),
            target_platform="postgres",
            ingest_contracts=True,
            enable_meta_mapping=False,
            write_semantics="OVERRIDE",
        ),
        PipelineContext(
            run_id="contract-e2e",
            pipeline_name="dbt-contract-e2e",
            graph=_graph_with_contracts({}),
        ),
    )
    mcps = [
        wu if isinstance(wu, MetadataChangeProposalWrapper) else wu.metadata
        for wu in source.get_workunits_internal()
    ]
    contract_mcps = [m for m in mcps if isinstance(m, MetadataChangeProposalWrapper)]
    dq_urns = _dq_urns(contract_mcps)
    infos = _assertion_infos(contract_mcps)

    native_types = sorted(
        str(infos[urn].customAssertion.nativeType)  # type: ignore[union-attr]
        for urn in dq_urns
        if "constraint_type" in (infos[urn].customProperties or {})
    )
    # PK on id -> unique + not_null; not_null on email; named check.
    assert native_types == [
        "dbt_constraint_check",
        "dbt_constraint_not_null",
        "dbt_constraint_not_null",
        "dbt_constraint_unique",
    ]
    schema_info = next(
        i
        for i in infos.values()
        if i.customAssertion and i.customAssertion.nativeType == "dbt_contract_schema"
    )
    assert schema_info.customAssertion is not None
    assert schema_info.customAssertion.logic == "id: bigint\nemail: varchar(255)"

    # The tagged test is referenced by the contract and emitted by the test path.
    test_urns = [
        urn
        for urn in dq_urns
        if "dbt_test_upstream_unique_id" in (infos[urn].customProperties or {})
    ]
    assert len(test_urns) == 1


def _cloud_source(**overrides: Any) -> DBTCloudSource:
    config: Dict[str, Any] = {
        "access_url": "https://test.getdbt.com",
        "token": "dummy_token",
        "account_id": 123456,
        "project_id": 1234567,
        "job_id": 12345678,
        "run_id": 123456789,
        "target_platform": "snowflake",
        "ingest_contracts": True,
        **overrides,
    }
    return DBTCloudSource(
        DBTCloudConfig(**config),
        PipelineContext(run_id="test-contract", pipeline_name="dbt-cloud"),
    )


def _cloud_model_node(environment_id: int = 42) -> Dict[str, Any]:
    return {
        "uniqueId": "model.test_pkg.orders",
        "name": "orders",
        "resourceType": "model",
        "materializedType": "table",
        "database": "DB",
        "schema": "SCHEMA",
        "type": "BASE TABLE",
        "owner": None,
        "comment": "",
        "description": "",
        "meta": {},
        "tags": [],
        "columns": [],
        "dependsOn": [],
        "packageName": "test_pkg",
        "alias": "orders",
        "status": "error",
        "error": _PREFLIGHT_MESSAGE,
        "skip": False,
        "rawCode": "select 1",
        "rawSql": None,
        "compiledCode": "select 1",
        "compiledSql": None,
        "environmentId": environment_id,
        "jobId": 11,
        "runId": 22,
    }


_CLOUD_CONTRACT = _DiscoveryContractData(
    contract_enforced=True,
    model_constraints=[DBTConstraint(type="not_null", columns=["id"])],
    contract_columns=[_column("id")],
)


def _discovery_page(
    unique_ids: List[str], end_cursor: Optional[str] = None
) -> Dict[str, Any]:
    return {
        "environment": {
            "applied": {
                "models": {
                    "edges": [
                        {
                            "node": {
                                "uniqueId": unique_id,
                                "contractEnforced": True,
                                "constraints": [
                                    {
                                        "name": None,
                                        "type": "not_null",
                                        "expression": None,
                                        "columns": ["id"],
                                    }
                                ],
                                "catalog": {
                                    "columns": [
                                        {"name": "id", "description": "", "type": "INT"}
                                    ]
                                },
                            }
                        }
                        for unique_id in unique_ids
                    ],
                    "pageInfo": {
                        "hasNextPage": end_cursor is not None,
                        "endCursor": end_cursor,
                    },
                }
            }
        }
    }


def test_fetch_discovery_contract_data_paginates() -> None:
    with mock.patch.object(
        DBTCloudSource,
        "_send_graphql_query",
        side_effect=[_discovery_page(["model.a"], "c1"), _discovery_page(["model.b"])],
    ) as mock_send:
        result = DBTCloudSource._fetch_discovery_contract_data(
            metadata_endpoint="https://metadata.test.getdbt.com/graphql",
            token="test-token",
            environment_id=42,
        )
    assert set(result) == {"model.a", "model.b"}
    assert result["model.a"].model_constraints[0].columns == ["id"]
    assert result["model.a"].contract_columns[0].data_type == "INT"
    assert mock_send.call_args_list[1].kwargs["variables"]["after"] == "c1"


def test_recorded_discovery_response_constraints_are_supported() -> None:
    # A real Discovery API response for the contract query, with names
    # replaced: the constraint type strings are kept exactly as returned.
    recorded = json.loads(
        (
            Path(__file__).parent / "artifacts" / "dbt_cloud_discovery_contracts.json"
        ).read_text()
    )
    with mock.patch.object(
        DBTCloudSource, "_send_graphql_query", return_value=recorded
    ):
        result = DBTCloudSource._fetch_discovery_contract_data(
            metadata_endpoint="https://metadata.test.getdbt.com/graphql",
            token="test-token",
            environment_id=42,
        )
    orders = result["model.my_project.orders"]
    assert [c.type for c in orders.model_constraints] == ["unique", "foreign_key"]
    source = _cloud_source()
    node = _make_model(model_constraints=orders.model_constraints)
    assert len(source._create_constraint_assertions(node, _ENTITY_URN)) == 2
    assert not source.report.contract_constraints_skipped_unsupported


def test_fetch_discovery_contract_data_missing_end_cursor_raises() -> None:
    page = _discovery_page(["model.a"])
    page["environment"]["applied"]["models"]["pageInfo"]["hasNextPage"] = True
    with mock.patch.object(DBTCloudSource, "_send_graphql_query", return_value=page):
        with pytest.raises(ValueError, match="endCursor"):
            DBTCloudSource._fetch_discovery_contract_data(
                metadata_endpoint="https://metadata.test.getdbt.com/graphql",
                token="test-token",
                environment_id=42,
            )


def test_cloud_contract_uses_run_environment_by_default() -> None:
    source = _cloud_source()
    with mock.patch.object(
        DBTCloudSource,
        "_fetch_discovery_contract_data",
        return_value={"model.test_pkg.orders": _CLOUD_CONTRACT},
    ) as mock_fetch:
        parsed = source._parse_into_dbt_node(_cloud_model_node(environment_id=42))
    assert mock_fetch.call_args.kwargs["environment_id"] == 42
    assert parsed.contract is not None
    assert [c.columns for c in parsed.model_constraints] == [["id"]]
    (perf,) = parsed.model_performances
    assert perf.status == "error"
    assert perf.timing_inferred


@pytest.mark.parametrize("ingest_contracts", [True, False])
def test_cloud_models_query_requests_timings_only_for_contracts(
    ingest_contracts: bool,
) -> None:
    source = _cloud_source(ingest_contracts=ingest_contracts)
    with mock.patch.object(
        DBTCloudSource, "_send_graphql_query", side_effect=RuntimeError("stop")
    ) as mock_send:
        source._fetch_nodes_and_exposures()
    queries = [call.kwargs["query"] for call in mock_send.call_args_list]
    assert any("models" in q for q in queries)
    assert any("executeStartedAt" in q for q in queries) is ingest_contracts


def test_cloud_model_performance_uses_execute_timings() -> None:
    source = _cloud_source()
    node = {
        **_cloud_model_node(),
        "executeStartedAt": "2025-01-01T00:00:00Z",
        "executeCompletedAt": "2025-01-01T00:01:00Z",
    }
    (perf,) = source._extract_model_performance(node)
    assert perf.start_time == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert perf.end_time == datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("status, built", [("success", False), ("error", True)])
def test_cloud_model_without_timings(status: str, built: bool) -> None:
    # A success without timings did not run the model, so it reports nothing;
    # a preflight error still does.
    source = _cloud_source()
    (perf,) = source._extract_model_performance(
        {**_cloud_model_node(), "status": status}
    )
    assert perf.built_relation is built


def test_cloud_error_without_timings_uses_run_generated_at() -> None:
    # Re-ingesting the same failed run must not add a new result.
    source = _cloud_source()
    (perf,) = source._extract_model_performance(
        {**_cloud_model_node(), "runGeneratedAt": "2025-01-01T00:02:00Z"}
    )
    assert perf.end_time == datetime(2025, 1, 1, 0, 2, tzinfo=timezone.utc)


def test_cloud_contract_skipped_for_run_from_other_environment() -> None:
    source = _cloud_source(environment_id=42)
    with mock.patch.object(
        DBTCloudSource,
        "_fetch_discovery_contract_data",
        return_value={"model.test_pkg.orders": _CLOUD_CONTRACT},
    ) as mock_fetch:
        parsed = source._parse_into_dbt_node(_cloud_model_node(environment_id=7))
    mock_fetch.assert_not_called()
    assert parsed.contract is None
    assert parsed.model_performances == []
    assert source.report.warnings


def test_cloud_discovery_failure_is_a_failure_and_cached() -> None:
    source = _cloud_source()
    with mock.patch.object(
        DBTCloudSource,
        "_fetch_discovery_contract_data",
        side_effect=RuntimeError("simulated API failure"),
    ) as mock_fetch:
        assert source._get_discovery_contract_data(42) == {}
        assert source._get_discovery_contract_data(42) == {}
    assert mock_fetch.call_count == 1
    # A failure makes stale entity removal keep the previous contracts.
    assert source.report.failures


def test_cloud_skips_model_performance_when_contracts_disabled() -> None:
    source = _cloud_source(ingest_contracts=False)
    parsed = source._parse_into_dbt_node(_cloud_model_node())
    assert parsed.contract is None
    assert parsed.model_performances == []


def test_cloud_test_connection_probes_contracts_in_run_environment() -> None:
    # Without environment_id, the contract query uses the run's environment.
    with mock.patch.object(DBTCloudSource, "_send_graphql_query") as mock_send:
        mock_send.side_effect = [
            {"job": {"id": 1}},
            {"job": {"models": [{"environmentId": 42}]}},
            {"environment": None},
        ]
        report = DBTCloudSource.test_connection(
            {
                "access_url": "https://test.getdbt.com",
                "token": "dummy_token",
                "account_id": 123456,
                "project_id": 1234567,
                "job_id": 12345678,
                "target_platform": "snowflake",
                "ingest_contracts": True,
            }
        )
    assert "runGeneratedAt" in mock_send.call_args_list[1].args[2]
    assert mock_send.call_args_list[2].args[3]["environmentId"] == 42
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False


def test_cloud_test_connection_rejects_unknown_environment() -> None:
    # A pinned environment_id still probes the run's timing fields.
    with mock.patch.object(DBTCloudSource, "_send_graphql_query") as mock_send:
        mock_send.side_effect = [
            {"job": {"id": 1}},
            {"job": {"models": [{"environmentId": 7}]}},
            {"environment": None},
        ]
        report = DBTCloudSource.test_connection(
            {
                "access_url": "https://test.getdbt.com",
                "token": "dummy_token",
                "account_id": 123456,
                "project_id": 1234567,
                "job_id": 12345678,
                "target_platform": "snowflake",
                "ingest_contracts": True,
                "environment_id": 42,
            }
        )
    assert "runGeneratedAt" in mock_send.call_args_list[1].args[2]
    assert mock_send.call_args_list[2].args[3]["environmentId"] == 42
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
