from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudConfig, DBTCloudSource
from datahub.ingestion.source.dbt.dbt_common import DBTNode, DBTSourceReport
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    extract_dbt_entities,
)
from datahub.metadata.schema_classes import AssertionInfoClass


def test_contract_dataclass():
    """Test DBTContract dataclass."""
    from datahub.ingestion.source.dbt.dbt_common import DBTContract

    contract = DBTContract(
        enforced=True,
        alias_types=True,
        checksum="abc123",
    )
    assert contract.enforced is True
    assert contract.alias_types is True
    assert contract.checksum == "abc123"

    # Test default values
    contract_defaults = DBTContract(enforced=False)
    assert contract_defaults.enforced is False
    assert contract_defaults.alias_types is True
    assert contract_defaults.checksum is None


def test_constraint_dataclass():
    """Test DBTConstraint dataclass."""
    from datahub.ingestion.source.dbt.dbt_common import DBTConstraint

    constraint = DBTConstraint(
        type="not_null",
        name="nn_col",
    )
    assert constraint.type == "not_null"
    assert constraint.name == "nn_col"

    # Test with all fields
    constraint_full = DBTConstraint(
        type="primary_key",
        name="pk_id",
        expression="id > 0",
        columns=["id", "name"],
    )
    assert constraint_full.type == "primary_key"
    assert constraint_full.columns == ["id", "name"]


def test_contract_config_options():
    """Test contract configuration options in DBTCommonConfig."""
    config = DBTCoreConfig(
        manifest_path="dummy_path",
        target_platform="postgres",
        ingest_contracts=True,
        contract_test_tag="custom_contract",
        ingest_column_constraints_as_assertions=False,
    )
    assert config.ingest_contracts is True
    assert config.contract_test_tag == "custom_contract"
    assert config.ingest_column_constraints_as_assertions is False


def test_contract_config_defaults():
    """Test default values for contract configuration."""
    config = DBTCoreConfig(
        manifest_path="dummy_path",
        target_platform="postgres",
    )
    assert config.ingest_contracts is False
    assert config.contract_test_tag == "contract"
    assert config.ingest_column_constraints_as_assertions is True


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
    nodes = extract_dbt_entities(
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
    assert len(nodes) == 1
    node = nodes[0]
    assert node.contract is not None
    assert node.contract.enforced is True
    assert node.contract.checksum == "abc123"
    assert node.model_constraints[0].type == "primary_key"
    assert node.columns[0].constraints[0].to == "ref('customers')"
    assert node.columns[0].constraints[0].to_columns == ["id"]
    assert node.contract_columns is not None
    assert node.contract_columns[0].data_type == "bigint"


def test_column_constraints_extracted() -> None:
    """Test that column constraints are extracted from DBTColumn."""
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint

    # Create a column with constraints
    col = DBTColumn(
        name="id",
        comment="",
        description="ID column",
        index=0,
        data_type="integer",
        constraints=[
            DBTConstraint(type="not_null"),
            DBTConstraint(type="primary_key"),
        ],
    )
    assert len(col.constraints) == 2
    assert col.constraints[0].type == "not_null"
    assert col.constraints[1].type == "primary_key"


def _make_contracted_source(
    *, ingest_column_constraints: bool = True, **config_overrides: Any
) -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-contract", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="snowflake",
        ingest_contracts=True,
        ingest_column_constraints_as_assertions=ingest_column_constraints,
        enable_meta_mapping=False,
        **config_overrides,
    )
    return DBTCoreSource(config, ctx)


def _make_contracted_node(
    *,
    dbt_name: str = "model.test_pkg.orders",
    name: str = "orders",
    adapter: str = "snowflake",
    columns: Optional[List[Any]] = None,
    model_constraints: Optional[List[Any]] = None,
) -> DBTNode:
    from datahub.ingestion.source.dbt.dbt_common import DBTContract

    return DBTNode(
        dbt_name=dbt_name,
        dbt_adapter=adapter,
        dbt_package_name="test_pkg",
        database="TEST_DB",
        schema="TEST_SCHEMA",
        name=name,
        alias=name,
        dbt_file_path=f"models/{name}.sql",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description=f"test contracted model {name}",
        upstream_nodes=[],
        materialization="table",
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
        contract=DBTContract(
            enforced=True,
            alias_types=True,
            checksum=f"checksum_{name}",
        ),
    )


def _assertion_info_from_mcps(mcps: List[Any]) -> AssertionInfoClass:
    for mcp in mcps:
        if isinstance(mcp.aspect, AssertionInfoClass):
            return mcp.aspect
    raise AssertionError("expected an AssertionInfoClass aspect in MCPs")


def test_constraint_assertion_unique_uses_unique_proportion() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint
    from datahub.metadata.schema_classes import (
        AssertionStdAggregationClass,
        AssertionStdOperatorClass,
    )

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="order_id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[DBTConstraint(type="unique")],
            )
        ],
    )

    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )

    assert len(results) == 1
    info = _assertion_info_from_mcps(results[0][1])
    assert info.type == "CUSTOM"
    custom = info.customAssertion
    assert custom is not None
    assert custom.operator == AssertionStdOperatorClass.EQUAL_TO
    assert custom.aggregation == AssertionStdAggregationClass.UNIQUE_PROPOTION
    assert custom.nativeType == "dbt_constraint_unique"
    assert custom.parameters is not None
    assert custom.parameters.value is not None
    assert custom.parameters.value.value == "1.0"


def test_constraint_assertion_primary_key_emits_not_null_and_unique() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint
    from datahub.metadata.schema_classes import (
        AssertionStdAggregationClass,
        AssertionStdOperatorClass,
    )

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[DBTConstraint(type="primary_key")],
            )
        ],
    )

    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )

    assert len(results) == 2
    customs = [
        _assertion_info_from_mcps(mcps).customAssertion for _, mcps, _ in results
    ]
    assert all(c is not None for c in customs)
    operators = {c.operator for c in customs if c is not None}
    aggregations = {c.aggregation for c in customs if c is not None}
    assert AssertionStdOperatorClass.EQUAL_TO in operators
    assert AssertionStdOperatorClass.NOT_NULL in operators
    assert AssertionStdAggregationClass.UNIQUE_PROPOTION in aggregations
    assert AssertionStdAggregationClass.IDENTITY in aggregations


def test_composite_primary_key_emits_single_multi_column_assertion() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTConstraint
    from datahub.metadata.schema_classes import AssertionStdAggregationClass

    source = _make_contracted_source()
    node = _make_contracted_node(
        model_constraints=[
            DBTConstraint(
                type="primary_key",
                columns=["customer_id", "order_date"],
            )
        ],
    )

    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )

    # One multi-column unique + one not_null per column.
    assert len(results) == 3
    infos = [_assertion_info_from_mcps(mcps) for _, mcps, _ in results]

    unique_infos = [
        i
        for i in infos
        if i.customAssertion is not None
        and i.customAssertion.aggregation
        == AssertionStdAggregationClass.UNIQUE_PROPOTION
    ]
    assert len(unique_infos) == 1
    unique_fields = unique_infos[0].customAssertion.fields  # type: ignore[union-attr]
    assert unique_fields is not None and len(unique_fields) == 2

    not_null_infos = [
        i
        for i in infos
        if i.customAssertion is not None
        and i.customAssertion.aggregation == AssertionStdAggregationClass.IDENTITY
    ]
    assert len(not_null_infos) == 2


def test_constraint_assertion_foreign_key_preserves_to_and_to_columns() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint
    from datahub.metadata.schema_classes import (
        AssertionStdAggregationClass,
        AssertionStdOperatorClass,
    )

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="customer_id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[
                    DBTConstraint(
                        type="foreign_key",
                        name="fk_customer",
                        to="ref('customers')",
                        to_columns=["id"],
                    )
                ],
            )
        ],
    )

    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )

    assert len(results) == 1
    info = _assertion_info_from_mcps(results[0][1])
    assert info.type == "CUSTOM"
    assert info.customAssertion is not None
    assert info.customAssertion.operator == AssertionStdOperatorClass._NATIVE_
    assert info.customAssertion.aggregation == AssertionStdAggregationClass._NATIVE_
    assert info.customProperties is not None
    assert info.customProperties.get("to") == "ref('customers')"
    assert info.customProperties.get("to_columns") == "id"
    assert info.customProperties.get("expression") is None
    assert info.customProperties.get("columns") == "customer_id"


def test_constraint_assertion_check_emits_expression_in_custom_props() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="age",
                comment="",
                description="",
                index=0,
                data_type="int",
                constraints=[DBTConstraint(type="check", expression="age >= 0")],
            )
        ],
    )

    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )

    assert len(results) == 1
    info = _assertion_info_from_mcps(results[0][1])
    assert info.customProperties is not None
    assert info.customProperties.get("expression") == "age >= 0"
    assert info.customProperties.get("constraint_type") == "check"


def test_constraint_assertion_unknown_type_is_reported_not_raised() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="something",
                comment="",
                description="",
                index=0,
                data_type="int",
                constraints=[DBTConstraint(type="totally_made_up")],
            )
        ],
    )

    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )

    assert results == []
    skipped = list(source.report.contract_constraints_skipped_unsupported)
    assert len(skipped) == 1
    assert "totally_made_up" in skipped[0]
    assert node.dbt_name in skipped[0]


def test_constraint_assertion_enforced_by_is_adapter_aware() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint

    source = _make_contracted_source()

    snowflake_node = _make_contracted_node(
        adapter="snowflake",
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[DBTConstraint(type="unique")],
            )
        ],
    )
    snowflake_results = source._create_constraint_assertions(
        node=snowflake_node,
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)",
    )
    sf_info = _assertion_info_from_mcps(snowflake_results[0][1])
    assert sf_info.customProperties is not None
    # Snowflake's ``unique`` is metadata-only per dbt's constraint matrix.
    assert sf_info.customProperties.get("enforced_by") == "dbt_contract"

    postgres_node = _make_contracted_node(
        adapter="postgres",
        dbt_name="model.test_pkg.pg_orders",
        name="pg_orders",
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[DBTConstraint(type="unique")],
            )
        ],
    )
    postgres_results = source._create_constraint_assertions(
        node=postgres_node,
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)",
    )
    pg_info = _assertion_info_from_mcps(postgres_results[0][1])
    assert pg_info.customProperties is not None
    assert pg_info.customProperties.get("enforced_by") == "database"


def _make_test_node(
    test_dbt_name: str,
    upstream_dbt_names: List[str],
    *,
    contract_tag: Optional[str] = "dbt:contract",
    qualified_test_name: Optional[str] = None,
    column_name: Optional[str] = None,
) -> DBTNode:
    from datahub.ingestion.source.dbt.dbt_tests import DBTTest

    tags = [contract_tag] if contract_tag else []
    test_info = None
    if qualified_test_name is not None:
        test_info = DBTTest(
            qualified_test_name=qualified_test_name,
            column_name=column_name,
            kw_args={},
        )
    return DBTNode(
        dbt_name=test_dbt_name,
        dbt_adapter="snowflake",
        dbt_package_name="test_pkg",
        database=None,
        schema="TEST_SCHEMA",
        name=test_dbt_name.split(".")[-1],
        alias=None,
        dbt_file_path=f"tests/{test_dbt_name}.sql",
        node_type="test",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=upstream_dbt_names,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=True,
        meta={},
        query_tag={},
        tags=tags,
        owner="",
        language="sql",
        raw_code=None,
        compiled_code=None,
        columns=[],
        test_info=test_info,
    )


def test_contract_test_urn_matches_when_upstream_filtered() -> None:
    source = _make_contracted_source()

    contracted_model = _make_contracted_node(
        dbt_name="model.test_pkg.orders", name="orders"
    )
    # One valid upstream + one orphan. all_nodes_map only contains the
    # valid one, so the filtered upstream count should be 1.
    test_node = _make_test_node(
        test_dbt_name="test.test_pkg.orders_has_id",
        upstream_dbt_names=["model.test_pkg.orders", "model.test_pkg.orphan"],
    )
    all_nodes_map = {contracted_model.dbt_name: contracted_model}

    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[contracted_model],
            test_nodes=[test_node],
            all_nodes_map=all_nodes_map,
        )
    )

    from datahub.metadata.schema_classes import DataContractPropertiesClass

    contract_props = [
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]
    assert len(contract_props) == 1
    dq_urns = [c.assertion for c in (contract_props[0].dataQuality or [])]
    expected_urn = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=None,
    )
    assert expected_urn in dq_urns


def test_contract_test_urn_matches_for_multi_upstream() -> None:
    source = _make_contracted_source()

    model_a = _make_contracted_node(dbt_name="model.test_pkg.a", name="a")
    model_b = _make_contracted_node(dbt_name="model.test_pkg.b", name="b")
    test_node = _make_test_node(
        test_dbt_name="test.test_pkg.shared_test",
        upstream_dbt_names=["model.test_pkg.a", "model.test_pkg.b"],
    )
    all_nodes_map = {model_a.dbt_name: model_a, model_b.dbt_name: model_b}

    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[model_a, model_b],
            test_nodes=[test_node],
            all_nodes_map=all_nodes_map,
        )
    )

    from datahub.metadata.schema_classes import DataContractPropertiesClass

    contract_props = [
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]
    assert len(contract_props) == 2

    expected_a = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=model_a.dbt_name,
    )
    expected_b = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=model_b.dbt_name,
    )
    assert expected_a != expected_b

    urn_a = model_a.get_urn(
        target_platform="dbt",
        env=source.config.env,
        data_platform_instance=source.config.platform_instance,
    )
    urn_b = model_b.get_urn(
        target_platform="dbt",
        env=source.config.env,
        data_platform_instance=source.config.platform_instance,
    )
    expected_by_entity = {urn_a: expected_a, urn_b: expected_b}

    for props in contract_props:
        dq_urns = [c.assertion for c in (props.dataQuality or [])]
        assert props.entity in expected_by_entity
        assert expected_by_entity[props.entity] in dq_urns


def test_extract_contract_columns_preserves_declared_types() -> None:
    from datahub.ingestion.source.dbt.dbt_core import extract_contract_columns

    manifest_node = {
        "columns": {
            "id": {
                "name": "id",
                "data_type": "bigint",
                "description": "primary key",
                "constraints": [{"type": "primary_key"}],
                "meta": {},
                "tags": [],
            },
            "email": {
                "name": "email",
                # Deliberately generic — this is what a contract with
                # alias_types=true looks like at the manifest level.
                "data_type": "string",
                "description": "",
                "constraints": [{"type": "not_null"}],
                "meta": {},
                "tags": [],
            },
            "created_at": {
                "name": "created_at",
                "data_type": "timestamp",
                "description": "",
                "meta": {},
                "tags": [],
            },
        }
    }
    cols = extract_contract_columns(manifest_node, tag_prefix="dbt:")
    assert [c.name for c in cols] == ["id", "email", "created_at"]
    assert [c.data_type for c in cols] == ["bigint", "string", "timestamp"]
    # Constraints must survive extraction so the downstream constraint
    # assertion emitter can see them without re-reading the manifest.
    id_col = cols[0]
    email_col = cols[1]
    assert [c.type for c in id_col.constraints] == ["primary_key"]
    assert [c.type for c in email_col.constraints] == ["not_null"]


def test_extract_contract_columns_handles_missing_data_type() -> None:
    from datahub.ingestion.source.dbt.dbt_core import extract_contract_columns

    manifest_node = {
        "columns": {"broken": {"name": "broken", "description": "no type declared"}}
    }
    cols = extract_contract_columns(manifest_node, tag_prefix="dbt:")
    assert len(cols) == 1
    assert cols[0].name == "broken"
    assert cols[0].data_type == ""


def test_extract_contract_columns_applies_tag_prefix() -> None:
    from datahub.ingestion.source.dbt.dbt_core import extract_contract_columns

    manifest_node = {
        "columns": {
            "id": {
                "name": "id",
                "data_type": "int",
                "tags": ["pii", "sensitive"],
            }
        }
    }
    cols = extract_contract_columns(manifest_node, tag_prefix="dbt:")
    assert cols[0].tags == ["dbt:pii", "dbt:sensitive"]


def test_schema_assertion_uses_contract_columns_when_available() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn

    source = _make_contracted_source()

    declared_id = DBTColumn(
        name="id",
        comment="",
        description="declared primary key",
        index=0,
        data_type="bigint",
    )
    # Catalog column's data_type intentionally drifts from the declared
    # one so the assertion below proves which source was used.
    catalog_id = DBTColumn(
        name="id",
        comment="",
        description="from catalog",
        index=0,
        data_type="VARCHAR(16)",
    )
    node = _make_contracted_node(columns=[catalog_id])
    node.contract_columns = [declared_id]

    schema_metadata = source._build_schema_metadata_for_node(node)
    assert schema_metadata is not None
    assert len(schema_metadata.fields) == 1
    assert schema_metadata.fields[0].fieldPath == "id"
    assert schema_metadata.fields[0].nativeDataType == "bigint"


def test_schema_assertion_falls_back_to_node_columns_when_no_contract_columns() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="BIGINT",
            )
        ]
    )
    assert node.contract_columns is None

    schema_metadata = source._build_schema_metadata_for_node(node)
    assert len(schema_metadata.fields) == 1
    assert schema_metadata.fields[0].fieldPath == "id"
    assert schema_metadata.fields[0].nativeDataType == "BIGINT"


def _build_contract_manifest() -> Dict[str, Any]:
    """Minimal manifest.json with one contracted model + one contract-tagged test."""
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v10.json",
            "dbt_version": "1.8.0",
            "generated_at": "2024-01-01T00:00:00.000000Z",
            "adapter_type": "postgres",
            "project_name": "contract_test",
            "project_id": "contract_test",
            "user_id": None,
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
                "path": "orders.sql",
                "original_file_path": "models/orders.sql",
                "unique_id": "model.contract_test.orders",
                "fqn": ["contract_test", "orders"],
                "alias": "orders",
                "checksum": {"name": "sha256", "checksum": "abc123"},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                    "contract": {"enforced": True, "alias_types": False},
                },
                "tags": [],
                "description": "Orders contracted model",
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "primary key",
                        "meta": {},
                        "data_type": "bigint",
                        "constraints": [{"type": "primary_key"}],
                        "tags": [],
                    },
                    "email": {
                        "name": "email",
                        "description": "",
                        "meta": {},
                        "data_type": "varchar(255)",
                        "constraints": [{"type": "not_null"}],
                        "tags": [],
                    },
                    "age": {
                        "name": "age",
                        "description": "",
                        "meta": {},
                        "data_type": "int",
                        "constraints": [
                            {
                                "type": "check",
                                "name": "ck_age_non_negative",
                                "expression": "age >= 0",
                            }
                        ],
                        "tags": [],
                    },
                    "status": {
                        "name": "status",
                        "description": "",
                        "meta": {},
                        "data_type": "varchar(32)",
                        "constraints": [{"type": "unique"}],
                        "tags": [],
                    },
                },
                "constraints": [
                    {
                        "type": "foreign_key",
                        "name": "fk_customer",
                        "expression": "customers(id)",
                        "columns": ["id"],
                    }
                ],
                "contract": {
                    "enforced": True,
                    "alias_types": False,
                    "checksum": "contract-checksum-abc",
                },
                "meta": {},
                "sources": [],
                "depends_on": {"macros": [], "nodes": []},
                "refs": [],
                "docs": {"show": True},
                "compiled": True,
                "compiled_code": "SELECT * FROM raw.orders",
                "raw_code": "SELECT * FROM {{ source('raw', 'orders') }}",
                "language": "sql",
                "build_path": None,
                "deferred": False,
                "unrendered_config": {},
                "created_at": 1704067200.0,
            },
            "test.contract_test.orders_email_not_null": {
                "database": "test_db",
                "schema": "test_schema",
                "name": "orders_email_not_null",
                "resource_type": "test",
                "package_name": "contract_test",
                "path": "not_null_orders_email.sql",
                "original_file_path": "models/orders.yml",
                "unique_id": "test.contract_test.orders_email_not_null",
                "fqn": ["contract_test", "orders_email_not_null"],
                "alias": "orders_email_not_null",
                "checksum": {"name": "none", "checksum": ""},
                "config": {
                    "enabled": True,
                    "materialized": "test",
                    "tags": ["contract"],
                    "meta": {},
                    "severity": "ERROR",
                    "where": None,
                },
                # Top-level tags are what DBTNode.tags reads; the contract
                # detection logic matches on these.
                "tags": ["contract"],
                "description": "",
                "columns": {},
                "meta": {},
                "sources": [],
                "depends_on": {
                    "macros": ["macro.dbt.test_not_null"],
                    "nodes": ["model.contract_test.orders"],
                },
                "refs": [{"name": "orders", "package": None, "version": None}],
                "docs": {"show": True},
                "compiled": True,
                "compiled_code": "select * from orders where email is null",
                "raw_code": "{{ test_not_null(**kwargs) }}",
                "language": "sql",
                "build_path": None,
                "deferred": False,
                "unrendered_config": {},
                "created_at": 1704067200.0,
                "column_name": "email",
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "email", "model": "{{ ref('orders') }}"},
                    "namespace": None,
                },
            },
        },
        "sources": {},
        "macros": {},
        "parent_map": {
            "model.contract_test.orders": [],
            "test.contract_test.orders_email_not_null": ["model.contract_test.orders"],
        },
        "child_map": {
            "model.contract_test.orders": ["test.contract_test.orders_email_not_null"],
            "test.contract_test.orders_email_not_null": [],
        },
        "disabled": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "docs": {},
        "semantic_models": {},
    }


def _build_contract_catalog() -> Dict[str, Any]:
    """Catalog matching ``_build_contract_manifest``.

    Types deliberately differ in case (``BIGINT`` vs ``bigint``) so the
    end-to-end test can verify the schema assertion sources from
    ``contract_columns`` and not from the catalog.
    """
    return {
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
                    "comment": None,
                    "owner": "test_owner",
                },
                "columns": {
                    "id": {
                        "type": "BIGINT",
                        "index": 0,
                        "name": "id",
                        "comment": None,
                    },
                    "email": {
                        "type": "VARCHAR(255)",
                        "index": 1,
                        "name": "email",
                        "comment": None,
                    },
                    "age": {
                        "type": "INTEGER",
                        "index": 2,
                        "name": "age",
                        "comment": None,
                    },
                    "status": {
                        "type": "VARCHAR(32)",
                        "index": 3,
                        "name": "status",
                        "comment": None,
                    },
                },
                "stats": {},
            }
        },
        "sources": {},
        "errors": None,
    }


def test_contract_ingestion_end_to_end(tmp_path: Any) -> None:
    """End-to-end: write manifest/catalog to disk, run a real DBTCoreSource."""
    import json as _json

    from datahub.metadata.schema_classes import (
        AssertionInfoClass,
        AssertionStdAggregationClass,
        AssertionStdOperatorClass,
        AssertionTypeClass,
        DataContractPropertiesClass,
    )

    manifest_path = tmp_path / "manifest.json"
    catalog_path = tmp_path / "catalog.json"
    manifest_path.write_text(_json.dumps(_build_contract_manifest()))
    catalog_path.write_text(_json.dumps(_build_contract_catalog()))

    ctx = PipelineContext(run_id="contract-e2e-test", pipeline_name="dbt-contract-e2e")
    config = DBTCoreConfig(
        manifest_path=str(manifest_path),
        catalog_path=str(catalog_path),
        target_platform="postgres",
        ingest_contracts=True,
        ingest_column_constraints_as_assertions=True,
        enable_meta_mapping=False,
        # OVERRIDE so the source doesn't require a real GMS graph.
        write_semantics="OVERRIDE",
    )
    source = DBTCoreSource(config, ctx)

    # get_workunits_internal yields a mix of MetadataWorkUnit and
    # MetadataChangeProposalWrapper.
    workunits = list(source.get_workunits_internal())

    def _extract_aspect_and_urn(wu: Any) -> Tuple[Any, Optional[str]]:
        if isinstance(wu, MetadataChangeProposalWrapper):
            return wu.aspect, wu.entityUrn
        meta = getattr(wu, "metadata", None)
        if meta is None:
            return None, None
        return getattr(meta, "aspect", None), getattr(meta, "entityUrn", None)

    emitted = [_extract_aspect_and_urn(wu) for wu in workunits]
    aspects = [aspect for aspect, _ in emitted if aspect is not None]

    # Data Contract entity is emitted.
    contract_props = [a for a in aspects if isinstance(a, DataContractPropertiesClass)]
    assert len(contract_props) == 1
    props = contract_props[0]
    assert not props.schema
    assert props.dataQuality is not None and len(props.dataQuality) >= 1

    contract_assertions = [
        a
        for a in aspects
        if isinstance(a, AssertionInfoClass) and a.type == AssertionTypeClass.CUSTOM
    ]
    schema_assertions = [
        a
        for a in contract_assertions
        if a.customAssertion is not None
        and a.customAssertion.nativeType == "dbt_contract_schema"
    ]
    assert len(schema_assertions) == 1
    schema_logic = schema_assertions[0].customAssertion.logic  # type: ignore[union-attr]
    assert schema_logic is not None
    assert "id: bigint" in schema_logic
    assert "email: varchar(255)" in schema_logic
    assert "age: int" in schema_logic
    assert "status: varchar(32)" in schema_logic

    unique_assertions = [
        a
        for a in contract_assertions
        if a.customAssertion is not None
        and a.customAssertion.aggregation
        == AssertionStdAggregationClass.UNIQUE_PROPOTION
    ]
    not_null_assertions = [
        a
        for a in contract_assertions
        if a.customAssertion is not None
        and a.customAssertion.operator == AssertionStdOperatorClass.NOT_NULL
    ]
    native_assertions = [
        a
        for a in contract_assertions
        if a.customAssertion is not None
        and a.customAssertion.nativeType
        in {"dbt_constraint_check", "dbt_constraint_foreign_key"}
    ]

    # PK on id → 1 unique + 1 not_null; unique on status → 1 more unique;
    # not_null on email → 1 more not_null. At least 2 of each.
    assert len(unique_assertions) >= 2
    assert len(not_null_assertions) >= 2

    # ``age`` has a check constraint, and the model carries a foreign_key —
    # both should reach custom properties as native-assertion expressions.
    expressions = {
        a.customProperties.get("expression")
        for a in native_assertions
        if a.customProperties is not None
    }
    assert "age >= 0" in expressions
    assert "customers(id)" in expressions

    expected_test_urn = source._make_test_assertion_urn(
        test_dbt_name="test.contract_test.orders_email_not_null",
        upstream_dbt_name=None,
    )
    dq_urns = {c.assertion for c in (props.dataQuality or [])}
    assert expected_test_urn in dq_urns

    # The test assertion URN the contract references must also have been
    # actually emitted somewhere in the workstream — guards against orphan
    # references between the contract path and create_test_entity_mcps.
    emitted_test_urns = {
        urn
        for _, urn in emitted
        if urn is not None and urn.startswith("urn:li:assertion:")
    }
    assert expected_test_urn in emitted_test_urns


def _base_dbt_cloud_config(**overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "access_url": "https://test.getdbt.com",
        "token": "dummy_token",
        "account_id": 123456,
        "project_id": 1234567,
        "job_id": 12345678,
        "run_id": 123456789,
        "target_platform": "snowflake",
    }
    base.update(overrides)
    return base


def test_dbt_cloud_config_requires_environment_id_when_ingesting_contracts() -> None:
    with pytest.raises(
        ValidationError,
        match="environment_id is required when ingest_contracts=true",
    ):
        DBTCloudConfig(**_base_dbt_cloud_config(ingest_contracts=True))


def test_dbt_cloud_config_accepts_environment_id_when_ingesting_contracts() -> None:
    config = DBTCloudConfig(
        **_base_dbt_cloud_config(
            ingest_contracts=True,
            environment_id=999,
        )
    )
    assert config.ingest_contracts is True
    assert config.environment_id == 999


def test_dbt_cloud_config_no_environment_id_when_not_ingesting_contracts() -> None:
    config = DBTCloudConfig(**_base_dbt_cloud_config())
    assert config.ingest_contracts is False
    assert config.environment_id is None


def test_fetch_discovery_contract_data_parses_single_page() -> None:

    fake_response = {
        "environment": {
            "applied": {
                "models": {
                    "edges": [
                        {
                            "node": {
                                "uniqueId": "model.test_pkg.orders",
                                "contractEnforced": True,
                                "constraints": [
                                    {
                                        "name": "pk_orders",
                                        "type": "primary_key",
                                        "expression": None,
                                        "columns": ["id"],
                                    },
                                    {
                                        "name": "nn_email",
                                        "type": "not_null",
                                        "expression": None,
                                        "columns": ["email"],
                                    },
                                ],
                                "catalog": {
                                    "columns": [
                                        {
                                            "name": "id",
                                            "description": "primary key",
                                            "type": "BIGINT",
                                        },
                                        {
                                            "name": "email",
                                            "description": "",
                                            "type": "VARCHAR",
                                        },
                                    ]
                                },
                            }
                        },
                        {
                            "node": {
                                "uniqueId": "model.test_pkg.no_contract",
                                "contractEnforced": False,
                                "constraints": [],
                                "catalog": {"columns": []},
                            }
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
    }

    with mock.patch.object(
        DBTCloudSource,
        "_send_graphql_query",
        return_value=fake_response,
    ) as mock_send:
        result = DBTCloudSource._fetch_discovery_contract_data(
            metadata_endpoint="https://metadata.test.getdbt.com/graphql",
            token="test-token",
            environment_id=42,
        )

    assert mock_send.call_count == 1

    # Models with contractEnforced=false are still indexed so the caller
    # can distinguish "not fetched" from "fetched, no contract".
    assert set(result.keys()) == {
        "model.test_pkg.orders",
        "model.test_pkg.no_contract",
    }

    contracted = result["model.test_pkg.orders"]
    assert contracted.contract_enforced is True
    assert len(contracted.model_constraints) == 2
    constraint_types = {c.type for c in contracted.model_constraints}
    assert constraint_types == {"primary_key", "not_null"}
    assert [c.name for c in contracted.contract_columns] == ["id", "email"]
    assert [c.data_type for c in contracted.contract_columns] == ["BIGINT", "VARCHAR"]

    unenforced = result["model.test_pkg.no_contract"]
    assert unenforced.contract_enforced is False
    assert unenforced.model_constraints == []
    assert unenforced.contract_columns == []


def test_fetch_discovery_contract_data_paginates() -> None:

    page_1 = {
        "environment": {
            "applied": {
                "models": {
                    "edges": [
                        {
                            "node": {
                                "uniqueId": "model.a",
                                "contractEnforced": True,
                                "constraints": [],
                                "catalog": {"columns": []},
                            }
                        }
                    ],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                }
            }
        }
    }
    page_2 = {
        "environment": {
            "applied": {
                "models": {
                    "edges": [
                        {
                            "node": {
                                "uniqueId": "model.b",
                                "contractEnforced": True,
                                "constraints": [],
                                "catalog": {"columns": []},
                            }
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
    }

    with mock.patch.object(
        DBTCloudSource,
        "_send_graphql_query",
        side_effect=[page_1, page_2],
    ) as mock_send:
        result = DBTCloudSource._fetch_discovery_contract_data(
            metadata_endpoint="https://metadata.test.getdbt.com/graphql",
            token="test-token",
            environment_id=42,
        )

    assert mock_send.call_count == 2
    assert set(result.keys()) == {"model.a", "model.b"}
    second_call_variables = mock_send.call_args_list[1].kwargs["variables"]
    assert second_call_variables["after"] == "cursor-1"


def test_parse_into_dbt_node_reads_contract_from_discovery_api() -> None:
    from datahub.ingestion.source.dbt.dbt_cloud import (
        _DiscoveryContractData,
    )
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint

    config = DBTCloudConfig(
        **_base_dbt_cloud_config(
            ingest_contracts=True,
            environment_id=42,
        )
    )
    ctx = PipelineContext(run_id="test-contract", pipeline_name="dbt-cloud")
    source = DBTCloudSource(config, ctx)

    source._discovery_contract_data = {
        "model.test_pkg.orders": _DiscoveryContractData(
            contract_enforced=True,
            model_constraints=[
                DBTConstraint(type="primary_key", columns=["id"]),
                DBTConstraint(type="not_null", columns=["email"]),
            ],
            contract_columns=[
                DBTColumn(
                    name="id",
                    comment="",
                    description="",
                    index=0,
                    data_type="BIGINT",
                ),
                DBTColumn(
                    name="email",
                    comment="",
                    description="",
                    index=1,
                    data_type="VARCHAR",
                ),
            ],
        )
    }

    raw_node: Dict[str, Any] = {
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
        "status": "success",
        "rawCode": "select 1",
        "rawSql": None,
        "compiledCode": "select 1",
        "compiledSql": None,
    }

    parsed = source._parse_into_dbt_node(raw_node)

    assert parsed.contract is not None
    assert parsed.contract.enforced is True
    assert len(parsed.model_constraints) == 2
    assert parsed.contract_columns is not None
    assert [c.name for c in parsed.contract_columns] == ["id", "email"]


def test_parse_into_dbt_node_no_contract_when_discovery_data_missing() -> None:

    config = DBTCloudConfig(
        **_base_dbt_cloud_config(
            ingest_contracts=True,
            environment_id=42,
        )
    )
    ctx = PipelineContext(run_id="test-contract", pipeline_name="dbt-cloud")
    source = DBTCloudSource(config, ctx)
    source._discovery_contract_data = {}

    raw_node: Dict[str, Any] = {
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
        # meta.contract is intentionally set here to assert the old
        # fallback path (which read contract data from meta) is gone.
        "meta": {
            "contract": {
                "enforced": True,
                "alias_types": True,
                "checksum": "should-be-ignored",
            }
        },
        "tags": [],
        "columns": [],
        "dependsOn": [],
        "packageName": "test_pkg",
        "alias": "orders",
        "status": "success",
        "rawCode": "select 1",
        "rawSql": None,
        "compiledCode": "select 1",
        "compiledSql": None,
    }

    parsed = source._parse_into_dbt_node(raw_node)
    assert parsed.contract is None
    assert parsed.contract_columns is None
    assert parsed.model_constraints == []


def test_discovery_api_failure_is_warned_not_raised() -> None:

    config = DBTCloudConfig(
        **_base_dbt_cloud_config(
            ingest_contracts=True,
            environment_id=42,
        )
    )
    ctx = PipelineContext(run_id="test-contract", pipeline_name="dbt-cloud")
    source = DBTCloudSource(config, ctx)

    with mock.patch.object(
        DBTCloudSource,
        "_fetch_discovery_contract_data",
        side_effect=RuntimeError("simulated API failure"),
    ):
        data = source._get_discovery_contract_data()

    assert data == {}
    # Cached result — a second call must not re-hit the mock.
    data_again = source._get_discovery_contract_data()
    assert data_again == {}

    assert any("contract" in str(w).lower() for w in source.report.warnings)


def test_parse_dbt_constraint_reads_fk_to_fields() -> None:
    from datahub.ingestion.source.dbt.dbt_common import parse_dbt_constraint

    constraint = parse_dbt_constraint(
        {
            "type": "foreign_key",
            "name": "fk_customer",
            "columns": ["customer_id"],
            "to": "ref('customers')",
            "to_columns": ["id"],
        }
    )
    assert constraint.to == "ref('customers')"
    assert constraint.to_columns == ["id"]
    assert constraint.expression is None

    camel = parse_dbt_constraint(
        {
            "type": "foreign_key",
            "to": "customers",
            "toColumns": ["id"],
            "warnUnenforced": True,
            "warnUnsupported": False,
        }
    )
    assert camel.to_columns == ["id"]
    assert camel.warn_unenforced is True
    assert camel.warn_unsupported is False


def test_schema_assertion_urn_includes_env() -> None:
    prod = _make_contracted_source(env="PROD")
    dev = _make_contracted_source(env="DEV")
    node = _make_contracted_node()
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.orders,PROD)"

    prod_urn, _ = prod._create_schema_assertion_for_contract(node, entity_urn)
    dev_urn, _ = dev._create_schema_assertion_for_contract(node, entity_urn)
    assert prod_urn != dev_urn


def test_contract_omits_test_urns_when_test_definitions_disabled() -> None:
    from datahub.metadata.schema_classes import DataContractPropertiesClass

    source = _make_contracted_source(entities_enabled={"test_definitions": "NO"})
    contracted_model = _make_contracted_node()
    test_node = _make_test_node(
        test_dbt_name="test.test_pkg.orders_has_id",
        upstream_dbt_names=["model.test_pkg.orders"],
    )
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[contracted_model],
            test_nodes=[test_node],
            all_nodes_map={contracted_model.dbt_name: contracted_model},
        )
    )
    contract_props = [
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]
    assert len(contract_props) == 1
    dq_urns = [c.assertion for c in (contract_props[0].dataQuality or [])]
    test_urn = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=None,
    )
    assert test_urn not in dq_urns
    assert dq_urns  # schema assertion still lands in dataQuality


def test_contract_skipped_when_node_type_emission_disabled() -> None:
    from datahub.metadata.schema_classes import DataContractPropertiesClass

    source = _make_contracted_source(entities_enabled={"models": "NO"})
    contracted_model = _make_contracted_node()
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[contracted_model],
            test_nodes=[],
            all_nodes_map={contracted_model.dbt_name: contracted_model},
        )
    )
    assert not [
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]


def test_column_constraints_read_from_contract_columns() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint
    from datahub.metadata.schema_classes import AssertionStdOperatorClass

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="BIGINT",
                constraints=[],
            )
        ],
    )
    node.contract_columns = [
        DBTColumn(
            name="id",
            comment="",
            description="",
            index=0,
            data_type="bigint",
            constraints=[DBTConstraint(type="not_null")],
        )
    ]
    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )
    assert len(results) == 1
    info = _assertion_info_from_mcps(results[0][1])
    assert info.customAssertion is not None
    assert info.customAssertion.operator == AssertionStdOperatorClass.NOT_NULL


def test_get_columns_tolerates_null_constraints() -> None:
    from datahub.ingestion.source.dbt.dbt_core import get_columns

    columns = get_columns(
        "model.test.orders",
        {"columns": {"id": {"name": "id", "type": "bigint", "index": 0}}},
        {"columns": {"id": {"name": "id", "data_type": "bigint", "constraints": None}}},
        "dbt:",
    )
    assert len(columns) == 1
    assert columns[0].constraints == []


def test_unnamed_native_constraints_on_same_columns_get_distinct_urns() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint

    source = _make_contracted_source()
    node = _make_contracted_node(
        columns=[
            DBTColumn(
                name="status",
                comment="",
                description="",
                index=0,
                data_type="varchar",
                constraints=[
                    DBTConstraint(type="check", expression="status <> ''"),
                    DBTConstraint(type="check", expression="length(status) > 0"),
                ],
            )
        ],
    )
    results = source._create_constraint_assertions(
        node=node, entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,test,PROD)"
    )
    urns = [urn for urn, _, _ in results]
    assert len(urns) == 2
    assert len(set(urns)) == 2


def test_data_contract_urn_matches_canonical_entity_guid() -> None:
    from datahub.metadata.schema_classes import DataContractPropertiesClass

    source = _make_contracted_source()
    node = _make_contracted_node()
    entity_urn = node.get_urn(
        target_platform="dbt",
        env=source.config.env,
        data_platform_instance=source.config.platform_instance,
    )
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[node],
            test_nodes=[],
            all_nodes_map={node.dbt_name: node},
        )
    )
    contract_urns = {
        mcp.entityUrn
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    }
    expected = f"urn:li:dataContract:{mce_builder.datahub_guid({'entity': entity_urn})}"
    assert contract_urns == {expected}


def test_fetch_discovery_contract_data_missing_end_cursor_raises() -> None:

    truncated = {
        "environment": {
            "applied": {
                "models": {
                    "edges": [
                        {
                            "node": {
                                "uniqueId": "model.a",
                                "contractEnforced": True,
                                "constraints": [],
                                "catalog": {"columns": []},
                            }
                        }
                    ],
                    "pageInfo": {"hasNextPage": True, "endCursor": None},
                }
            }
        }
    }
    with mock.patch.object(
        DBTCloudSource, "_send_graphql_query", return_value=truncated
    ):
        with pytest.raises(ValueError, match="endCursor"):
            DBTCloudSource._fetch_discovery_contract_data(
                metadata_endpoint="https://metadata.test.getdbt.com/graphql",
                token="test-token",
                environment_id=42,
            )


def test_dbt_cloud_test_connection_probes_discovery_when_ingesting_contracts() -> None:

    with mock.patch.object(DBTCloudSource, "_send_graphql_query") as mock_send:
        mock_send.return_value = {"environment": {"applied": {"models": {}}}}
        report = DBTCloudSource.test_connection(
            {
                **_base_dbt_cloud_config(
                    ingest_contracts=True,
                    environment_id=42,
                )
            }
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert mock_send.call_count == 2
    discovery_calls = [
        call
        for call in mock_send.call_args_list
        if "environmentId" in (call.kwargs.get("variables") or call.args[3])
    ]
    assert len(discovery_calls) == 1


def test_schema_assertion_is_custom_and_in_data_quality() -> None:
    from datahub.metadata.schema_classes import (
        AssertionTypeClass,
        DataContractPropertiesClass,
    )

    source = _make_contracted_source()
    node = _make_contracted_node()
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[node],
            test_nodes=[],
            all_nodes_map={node.dbt_name: node},
        )
    )
    schema_urn, schema_mcps = source._create_schema_assertion_for_contract(
        node,
        "urn:li:dataset:(urn:li:dataPlatform:dbt,test.orders,PROD)",
    )
    info = _assertion_info_from_mcps(schema_mcps)
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.source is not None
    assert info.customAssertion is not None
    assert info.customAssertion.nativeType == "dbt_contract_schema"

    contract_props = [
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    ]
    assert len(contract_props) == 1
    assert not contract_props[0].schema
    dq_urns = [c.assertion for c in (contract_props[0].dataQuality or [])]
    assert schema_urn in dq_urns


def test_model_success_emits_schema_and_ddl_constraint_run_events() -> None:
    from datetime import datetime, timezone

    from datahub.ingestion.source.dbt.dbt_common import (
        DBTColumn,
        DBTConstraint,
        DBTModelPerformance,
    )
    from datahub.metadata.schema_classes import AssertionRunEventClass

    source = _make_contracted_source()
    node = _make_contracted_node(
        adapter="postgres",
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[
                    DBTConstraint(type="not_null"),
                    DBTConstraint(type="unique"),
                ],
            )
        ],
    )
    node.model_performances = [
        DBTModelPerformance(
            run_id="inv-success",
            status="success",
            start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
        )
    ]
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[node],
            test_nodes=[],
            all_nodes_map={node.dbt_name: node},
        )
    )
    run_events = [
        mcp.aspect for mcp in mcps if isinstance(mcp.aspect, AssertionRunEventClass)
    ]
    assert run_events
    assert all(e.result is not None and e.result.type == "SUCCESS" for e in run_events)
    assert all(e.runId == "inv-success" for e in run_events)
    # Schema + postgres not_null + postgres unique are all DDL-enforced.
    assert len(run_events) == 3


def test_preflight_failure_emits_schema_failure_only() -> None:
    from datetime import datetime, timezone

    from datahub.ingestion.source.dbt.dbt_common import (
        DBTColumn,
        DBTConstraint,
        DBTModelPerformance,
    )
    from datahub.metadata.schema_classes import AssertionRunEventClass

    source = _make_contracted_source()
    node = _make_contracted_node(
        adapter="postgres",
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="bigint",
                constraints=[DBTConstraint(type="not_null")],
            )
        ],
    )
    node.model_performances = [
        DBTModelPerformance(
            run_id="inv-preflight",
            status="error",
            start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
            message=(
                "This model has an enforced contract that failed.\n"
                "> in macro assert_columns_equivalent"
            ),
        )
    ]
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[node],
            test_nodes=[],
            all_nodes_map={node.dbt_name: node},
        )
    )
    run_events = [
        mcp.aspect for mcp in mcps if isinstance(mcp.aspect, AssertionRunEventClass)
    ]
    assert len(run_events) == 1
    assert run_events[0].result is not None
    assert run_events[0].result.type == "FAILURE"
    assert run_events[0].result.nativeResults is not None
    assert (
        "enforced contract that failed" in run_events[0].result.nativeResults["message"]
    )


def test_unrelated_model_error_emits_no_contract_run_events() -> None:
    from datetime import datetime, timezone

    from datahub.ingestion.source.dbt.dbt_common import DBTModelPerformance
    from datahub.metadata.schema_classes import AssertionRunEventClass

    source = _make_contracted_source()
    node = _make_contracted_node()
    node.model_performances = [
        DBTModelPerformance(
            run_id="inv-sql",
            status="error",
            start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
            message="Database Error: relation does not exist",
        )
    ]
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[node],
            test_nodes=[],
            all_nodes_map={node.dbt_name: node},
        )
    )
    assert not [
        mcp.aspect for mcp in mcps if isinstance(mcp.aspect, AssertionRunEventClass)
    ]


def test_auto_link_matching_not_null_test_without_tag() -> None:
    from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTConstraint
    from datahub.metadata.schema_classes import DataContractPropertiesClass

    source = _make_contracted_source()
    model = _make_contracted_node(
        columns=[
            DBTColumn(
                name="email",
                comment="",
                description="",
                index=0,
                data_type="varchar",
                constraints=[DBTConstraint(type="not_null")],
            )
        ],
    )
    test_node = _make_test_node(
        test_dbt_name="test.test_pkg.not_null_orders_email",
        upstream_dbt_names=[model.dbt_name],
        contract_tag=None,
        qualified_test_name="not_null",
        column_name="email",
    )
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[model],
            test_nodes=[test_node],
            all_nodes_map={model.dbt_name: model},
        )
    )
    props = next(
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    )
    expected = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=None,
    )
    dq_urns = [c.assertion for c in (props.dataQuality or [])]
    assert expected in dq_urns


def test_unrelated_test_is_not_auto_linked() -> None:
    from datahub.metadata.schema_classes import DataContractPropertiesClass

    source = _make_contracted_source()
    model = _make_contracted_node()
    test_node = _make_test_node(
        test_dbt_name="test.test_pkg.accepted_values_orders_status",
        upstream_dbt_names=[model.dbt_name],
        contract_tag=None,
        qualified_test_name="accepted_values",
        column_name="status",
    )
    mcps = list(
        source.create_contract_mcps(
            non_test_nodes=[model],
            test_nodes=[test_node],
            all_nodes_map={model.dbt_name: model},
        )
    )
    props = next(
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, DataContractPropertiesClass)
    )
    unexpected = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=None,
    )
    dq_urns = [c.assertion for c in (props.dataQuality or [])]
    assert unexpected not in dq_urns


def test_parse_model_run_keeps_message_and_falls_back_without_timing() -> None:
    from datahub.ingestion.source.dbt.dbt_core import (
        DBTRunMetadata,
        DBTRunResult,
        _parse_model_run,
    )

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
            "message": "This model has an enforced contract that failed.",
            "timing": [],
        }
    )
    performance = _parse_model_run(metadata, result)
    assert performance is not None
    assert performance.message is not None
    assert "enforced contract that failed" in performance.message
    assert performance.run_id == "no-timing"


def test_load_run_results_contract_preflight_fixture() -> None:
    import json
    from pathlib import Path

    from datahub.ingestion.source.dbt.dbt_core import load_run_results

    fixture = (
        Path(__file__).parent / "artifacts" / "run_results_contract_preflight.json"
    )
    source = _make_contracted_source()
    node = _make_contracted_node()
    loaded = load_run_results(
        source.config,
        json.loads(fixture.read_text()),
        [node],
    )
    assert loaded[0].model_performances
    perf = loaded[0].model_performances[0]
    assert perf.status == "error"
    assert perf.message is not None
    assert "enforced contract that failed" in perf.message
    assert perf.run_id == "contract-preflight-invocation"


def test_cloud_extracts_model_performance_from_job_status() -> None:
    from datetime import datetime

    ctx = PipelineContext(run_id="cloud-perf", pipeline_name="dbt-cloud")
    config = DBTCloudConfig(
        **_base_dbt_cloud_config(
            ingest_contracts=True,
            environment_id=42,
        )
    )
    source = DBTCloudSource(config, ctx)
    perfs = source._extract_model_performance(
        {
            "skip": False,
            "status": "error",
            "error": "This model has an enforced contract that failed.",
            "jobId": 11,
            "runId": 22,
        }
    )
    assert len(perfs) == 1
    assert perfs[0].status == "error"
    assert perfs[0].run_id == "job11-run22"
    assert perfs[0].message is not None
    assert "enforced contract that failed" in perfs[0].message
    assert isinstance(perfs[0].start_time, datetime)

    assert source._extract_model_performance({"skip": True, "status": "success"}) == []
    assert source._extract_model_performance({"skip": False, "status": "skipped"}) == []
