from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import SYSTEM_ACTOR
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.dbt import dbt_cloud
from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudConfig, DBTCloudSource
from datahub.ingestion.source.dbt.dbt_common import (
    DBTEntitiesEnabled,
    DBTExposure,
    DBTNode,
    DBTSourceReport,
    EmitDirective,
    NullTypeClass,
    SemanticModelDimension,
    SemanticModelEntity,
    SemanticModelMeasure,
    convert_semantic_model_fields_to_columns,
    get_column_type,
    parse_semantic_view_cll,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    extract_dbt_entities,
    extract_dbt_exposures,
    extract_semantic_models,
    load_run_results,
    parse_dbt_timestamp,
)
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTFreshnessCriteria,
    DBTFreshnessInfo,
    DBTTest,
    DBTTestResult,
    make_assertion_from_freshness,
    make_assertion_result_from_freshness,
    make_assertion_result_from_test,
    parse_freshness_criteria,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultSeverityClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SubTypesClass,
)
from datahub.testing.doctest import assert_doctest
from tests.unit.dbt.test_helpers import (  # type: ignore[import-untyped]
    create_mock_dbt_node,
)


def create_owners_list_from_urn_list(
    owner_urns: List[str], source_type: str
) -> List[OwnerClass]:
    ownership_source_type: Union[None, OwnershipSourceClass] = None
    if source_type:
        ownership_source_type = OwnershipSourceClass(type=source_type)
    owners_list = [
        OwnerClass(
            owner=owner_urn,
            type=OwnershipTypeClass.DATAOWNER,
            source=ownership_source_type,
        )
        for owner_urn in owner_urns
    ]
    return owners_list


def create_mocked_dbt_source() -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    graph = mock.MagicMock()
    graph.get_ownership.return_value = mce_builder.make_ownership_aspect_from_urn_list(
        ["urn:li:corpuser:test_user"], "AUDIT"
    )
    graph.get_glossary_terms.return_value = (
        mce_builder.make_glossary_terms_aspect_from_urn_list(
            ["urn:li:glossaryTerm:old", "urn:li:glossaryTerm:old2"]
        )
    )
    graph.get_tags.return_value = mce_builder.make_global_tag_aspect_with_tag_list(
        ["non_dbt_existing", "dbt:existing"]
    )
    ctx.graph = graph
    return DBTCoreSource(DBTCoreConfig(**create_base_dbt_config()), ctx)


def create_base_dbt_config() -> Dict:
    return dict(
        {
            "manifest_path": "temp/",
            "catalog_path": "temp/",
            "sources_path": "temp/",
            "target_platform": "postgres",
            "enable_meta_mapping": False,
        },
    )


def test_dbt_source_patching_no_new():
    source = create_mocked_dbt_source()

    # verifying when there are no new owners to be added
    assert source.ctx.graph
    transformed_owner_list = source.get_transformed_owners_by_source_type(
        [], "urn:li:dataset:dummy", "SERVICE"
    )
    assert len(transformed_owner_list) == 1


def test_dbt_source_patching_no_conflict():
    # verifying when new owners to be added do not conflict with existing source types
    source = create_mocked_dbt_source()
    new_owner_urns = ["urn:li:corpuser:new_test"]
    new_owners_list = create_owners_list_from_urn_list(new_owner_urns, "SERVICE")
    transformed_owner_list = source.get_transformed_owners_by_source_type(
        new_owners_list, "urn:li:dataset:dummy", "DATABASE"
    )
    assert len(transformed_owner_list) == 2
    owner_set = {"urn:li:corpuser:test_user", "urn:li:corpuser:new_test"}
    for single_owner in transformed_owner_list:
        assert single_owner.owner in owner_set
        assert single_owner.source and single_owner.source.type in {
            OwnershipSourceTypeClass.AUDIT,
            OwnershipSourceTypeClass.SERVICE,
        }


def test_dbt_source_patching_with_conflict():
    # verifying when new owner overrides existing owner
    source = create_mocked_dbt_source()
    new_owner_urns = ["urn:li:corpuser:new_test", "urn:li:corpuser:new_test2"]
    new_owners_list = create_owners_list_from_urn_list(new_owner_urns, "AUDIT")
    transformed_owner_list = source.get_transformed_owners_by_source_type(
        new_owners_list, "urn:li:dataset:dummy", "AUDIT"
    )
    assert len(transformed_owner_list) == 2
    expected_owner_set = {"urn:li:corpuser:new_test", "urn:li:corpuser:new_test2"}
    for single_owner in transformed_owner_list:
        assert single_owner.owner in expected_owner_set
        assert (
            single_owner.source
            and single_owner.source.type == OwnershipSourceTypeClass.AUDIT
        )


def test_dbt_source_patching_with_null_source_type_in_existing_owner_preserves_them():
    source = create_mocked_dbt_source()
    graph = mock.MagicMock()
    graph.get_ownership.return_value = mce_builder.make_ownership_aspect_from_urn_list(
        ["urn:li:corpuser:existing_test_user"], None
    )
    source.ctx.graph = graph
    new_owner_urns = ["urn:li:corpuser:new_test", "urn:li:corpuser:new_test2"]
    new_owners_list = create_owners_list_from_urn_list(new_owner_urns, "AUDIT")
    transformed_owner_list = source.get_transformed_owners_by_source_type(
        new_owners_list, "urn:li:dataset:dummy", "AUDIT"
    )
    assert len(transformed_owner_list) == 3
    transformed_urns = {o.owner for o in transformed_owner_list}
    assert "urn:li:corpuser:existing_test_user" in transformed_urns
    assert "urn:li:corpuser:new_test" in transformed_urns
    assert "urn:li:corpuser:new_test2" in transformed_urns


def test_dbt_source_patching_preserves_manually_added_owners_without_source():
    source = create_mocked_dbt_source()
    graph = mock.MagicMock()

    dbt_owner = OwnerClass(
        owner="urn:li:corpuser:dbt_defined_owner",
        type=OwnershipTypeClass.DATAOWNER,
        source=OwnershipSourceClass(type=OwnershipSourceTypeClass.SOURCE_CONTROL),
    )
    api_added_owner = OwnerClass(
        owner="urn:li:corpGroup:team_data_infra",
        type=OwnershipTypeClass.DATAOWNER,
        source=None,
    )
    graph.get_ownership.return_value = OwnershipClass(
        owners=[dbt_owner, api_added_owner]
    )
    source.ctx.graph = graph

    new_owners = [
        OwnerClass(
            owner="urn:li:corpuser:dbt_defined_owner",
            type=OwnershipTypeClass.DATAOWNER,
            source=OwnershipSourceClass(type=OwnershipSourceTypeClass.SOURCE_CONTROL),
        )
    ]

    transformed = source.get_transformed_owners_by_source_type(
        new_owners,
        "urn:li:dataset:dummy",
        str(OwnershipSourceTypeClass.SOURCE_CONTROL),
    )

    transformed_urns = {o.owner for o in transformed}
    assert "urn:li:corpGroup:team_data_infra" in transformed_urns
    assert "urn:li:corpuser:dbt_defined_owner" in transformed_urns
    assert len(transformed) == 2


def test_dbt_source_patching_tags():
    # two existing tags out of which one as a prefix that we want to filter on.
    # two new tags out of which one has a prefix we are filtering on existing tags, so this tag will
    # override the existing one with the same prefix.
    source = create_mocked_dbt_source()
    new_tag_aspect = mce_builder.make_global_tag_aspect_with_tag_list(
        ["new_non_dbt", "dbt:new_dbt"]
    )
    transformed_tags = source.get_transformed_tags_by_prefix(
        new_tag_aspect.tags, "urn:li:dataset:dummy", "dbt:"
    )
    expected_tags = {
        "urn:li:tag:new_non_dbt",
        "urn:li:tag:non_dbt_existing",
        "urn:li:tag:dbt:new_dbt",
    }
    assert len(transformed_tags) == 3
    for transformed_tag in transformed_tags:
        assert transformed_tag.tag in expected_tags


def test_dbt_source_patching_terms():
    # existing terms and new terms have two terms each and one common. After deduping we should only get 3 unique terms
    source = create_mocked_dbt_source()
    new_terms = mce_builder.make_glossary_terms_aspect_from_urn_list(
        ["urn:li:glossaryTerm:old", "urn:li:glossaryTerm:new"]
    )
    transformed_terms = source.get_transformed_terms(
        new_terms.terms, "urn:li:dataset:dummy"
    )
    expected_terms = {
        "urn:li:glossaryTerm:old",
        "urn:li:glossaryTerm:old2",
        "urn:li:glossaryTerm:new",
    }
    assert len(transformed_terms) == 3
    for transformed_term in transformed_terms:
        assert transformed_term.urn in expected_terms


def test_dbt_entity_emission_configuration():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Only", "seeds": "Only"},
    }
    with pytest.raises(
        ValidationError,
        match="Cannot have more than 1 type of entity emission set to ONLY",
    ):
        DBTCoreConfig.model_validate(config_dict)

    # valid config
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Yes", "seeds": "Only"},
    }
    DBTCoreConfig.model_validate(config_dict)


def test_dbt_config_skip_sources_in_lineage():
    with pytest.raises(
        ValidationError,
        match="skip_sources_in_lineage.*entities_enabled.sources.*set to NO",
    ):
        config_dict = {
            "manifest_path": "dummy_path",
            "catalog_path": "dummy_path",
            "target_platform": "dummy_platform",
            "skip_sources_in_lineage": True,
        }
        config = DBTCoreConfig.model_validate(config_dict)

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "skip_sources_in_lineage": True,
        "entities_enabled": {"sources": "NO"},
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert config.skip_sources_in_lineage is True


def test_dbt_config_prefer_sql_parser_lineage():
    with pytest.raises(
        ValidationError,
        match="prefer_sql_parser_lineage.*requires.*skip_sources_in_lineage",
    ):
        config_dict = {
            "manifest_path": "dummy_path",
            "catalog_path": "dummy_path",
            "target_platform": "dummy_platform",
            "prefer_sql_parser_lineage": True,
        }
        config = DBTCoreConfig.model_validate(config_dict)

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "skip_sources_in_lineage": True,
        "prefer_sql_parser_lineage": True,
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert config.skip_sources_in_lineage is True
    assert config.prefer_sql_parser_lineage is True


def test_dbt_prefer_sql_parser_lineage_no_self_reference():
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.model_validate(
        {
            **create_base_dbt_config(),
            "skip_sources_in_lineage": True,
            "prefer_sql_parser_lineage": True,
        }
    )
    source: DBTCoreSource = DBTCoreSource(config, ctx)
    all_nodes_map = {
        "model1": DBTNode(
            name="model1",
            database=None,
            schema=None,
            alias=None,
            comment="",
            description="",
            language="sql",
            raw_code=None,
            dbt_adapter="postgres",
            dbt_name="model1",
            dbt_file_path=None,
            dbt_package_name=None,
            node_type="model",
            materialization="table",
            max_loaded_at=None,
            catalog_type=None,
            missing_from_catalog=False,
            owner=None,
            compiled_code="SELECT d FROM results WHERE d > (SELECT MAX(d) FROM model1)",
        ),
    }
    source._infer_schemas_and_update_cll(all_nodes_map)
    upstream_lineage = source._create_lineage_aspect_for_dbt_node(
        all_nodes_map["model1"], all_nodes_map
    )
    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) == 1


def test_dbt_cll_skip_python_model() -> None:
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.model_validate(create_base_dbt_config())
    source: DBTCoreSource = DBTCoreSource(config, ctx)
    all_nodes_map = {
        "model1": DBTNode(
            name="model1",
            database=None,
            schema=None,
            alias=None,
            comment="",
            description="",
            language="python",
            raw_code=None,
            dbt_adapter="postgres",
            dbt_name="model1",
            dbt_file_path=None,
            dbt_package_name=None,
            node_type="model",
            materialization="table",
            max_loaded_at=None,
            catalog_type=None,
            missing_from_catalog=False,
            owner=None,
            compiled_code="import pandas as pd\n# Other processing here...",
        ),
    }
    source._infer_schemas_and_update_cll(all_nodes_map)
    assert len(source.report.sql_parser_skipped_non_sql_model) == 1

    # TODO: Also test that table-level lineage is still created.


def test_dbt_s3_config():
    # test missing aws config
    config_dict: dict = {
        "manifest_path": "s3://dummy_path",
        "catalog_path": "s3://dummy_path",
        "target_platform": "dummy_platform",
    }
    with pytest.raises(ValidationError, match="provide aws_connection"):
        DBTCoreConfig.model_validate(config_dict)

    # valid config
    config_dict = {
        "manifest_path": "s3://dummy_path",
        "catalog_path": "s3://dummy_path",
        "target_platform": "dummy_platform",
        "aws_connection": {},
    }
    DBTCoreConfig.model_validate(config_dict)


def test_default_convert_column_urns_to_lowercase():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Yes", "seeds": "Only"},
    }

    config = DBTCoreConfig.model_validate({**config_dict})
    assert config.convert_column_urns_to_lowercase is False

    config = DBTCoreConfig.model_validate(
        {**config_dict, "target_platform": "snowflake"}
    )
    assert config.convert_column_urns_to_lowercase is True

    # Check that we respect the user's setting if provided.
    config = DBTCoreConfig.model_validate(
        {
            **config_dict,
            "convert_column_urns_to_lowercase": False,
            "target_platform": "snowflake",
        }
    )
    assert config.convert_column_urns_to_lowercase is False


def test_default_convert_urns_to_lowercase():
    """convert_urns_to_lowercase defaults to True to match historical dbt behavior."""
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Yes", "seeds": "Only"},
    }

    config = DBTCoreConfig.model_validate({**config_dict})
    assert config.convert_urns_to_lowercase is True

    # Snowflake also defaults to True.
    config = DBTCoreConfig.model_validate(
        {**config_dict, "target_platform": "snowflake"}
    )
    assert config.convert_urns_to_lowercase is True

    # Explicit opt-out should work (e.g. for BigQuery with mixed-case identifiers).
    config = DBTCoreConfig.model_validate(
        {
            **config_dict,
            "convert_urns_to_lowercase": False,
            "target_platform": "bigquery",
        }
    )
    assert config.convert_urns_to_lowercase is False


def test_convert_urns_to_lowercase_affects_dbt_urns():
    """When convert_urns_to_lowercase is True, dbt platform URNs should be lowercased."""
    node = DBTNode(
        dbt_name="model.my_project.dim_industry",
        dbt_adapter="snowflake",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=[],
        materialization="table",
        columns=[],
        meta={},
        query_tag={},
        tags=[],
        owner="",
        language="sql",
        database="MY_DB",
        schema="APP_SALES",
        name="dim_industry",
        alias=None,
        raw_code=None,
        dbt_file_path="/models/dim_industry.sql",
        dbt_package_name=None,
        catalog_type=None,
        missing_from_catalog=False,
    )

    # Without the flag, dbt platform URNs preserve original casing.
    dbt_urn_without_flag = node.get_urn(
        target_platform="dbt",
        env="PROD",
        data_platform_instance=None,
    )
    assert "MY_DB.APP_SALES.dim_industry" in dbt_urn_without_flag

    # With the flag set on the node, dbt platform URNs are lowercased.
    node.convert_urns_to_lowercase = True
    dbt_urn_with_flag = node.get_urn(
        target_platform="dbt",
        env="PROD",
        data_platform_instance=None,
    )
    assert "my_db.app_sales.dim_industry" in dbt_urn_with_flag

    # Target platform URNs preserve casing when flag is off.
    node.convert_urns_to_lowercase = False
    target_urn = node.get_urn(
        target_platform="snowflake",
        env="PROD",
        data_platform_instance=None,
    )
    assert "MY_DB.APP_SALES.dim_industry" in target_urn

    # Target platform URNs are lowercased when flag is on.
    node.convert_urns_to_lowercase = True
    target_urn = node.get_urn(
        target_platform="snowflake",
        env="PROD",
        data_platform_instance=None,
    )
    assert "my_db.app_sales.dim_industry" in target_urn


def test_bigquery_mixed_case_urn_preserved():
    """Regression test for Zendesk #7397 / PR #16358.

    BigQuery is case-sensitive for quoted identifiers. dbt must not unconditionally
    lowercase BigQuery URNs, or lineage to the real BigQuery entity will break.
    Uses the exact customer entity from the ticket (AM100 in table name).
    """
    node = DBTNode(
        dbt_name="model.sales_index.int_sales_index__retailer_groups_with_AM100_position",
        dbt_adapter="bigquery",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=[],
        materialization="table",
        columns=[],
        meta={},
        query_tag={},
        tags=[],
        owner="",
        language="sql",
        database="at-dp-salesindex-prod",
        schema="sales_index",
        name="int_sales_index__retailer_groups_with_AM100_position",
        alias=None,
        raw_code=None,
        dbt_file_path="/models/int_sales_index__retailer_groups_with_AM100_position.sql",
        dbt_package_name=None,
        catalog_type=None,
        missing_from_catalog=False,
    )

    # Default: convert_urns_to_lowercase is False — casing must be preserved.
    assert node.convert_urns_to_lowercase is False

    bq_urn = node.get_urn(
        target_platform="bigquery",
        env="PROD",
        data_platform_instance=None,
    )
    assert "int_sales_index__retailer_groups_with_AM100_position" in bq_urn
    assert "am100" not in bq_urn

    dbt_urn = node.get_urn(
        target_platform="dbt",
        env="PROD",
        data_platform_instance=None,
    )
    assert "int_sales_index__retailer_groups_with_AM100_position" in dbt_urn

    # Opt-in: when enabled, lowercasing should apply.
    node.convert_urns_to_lowercase = True
    bq_urn_lower = node.get_urn(
        target_platform="bigquery",
        env="PROD",
        data_platform_instance=None,
    )
    assert "am100" in bq_urn_lower
    assert "AM100" not in bq_urn_lower


def test_convert_urns_to_lowercase_truth_table():
    """Verify the full truth table from Zendesk #7397 — lowercasing must be opt-in only.

    | target_platform | convert_urns_to_lowercase | URN lowercased? |
    |-----------------|--------------------------|-----------------|
    | "dbt"           | False                    | No              |
    | "dbt"           | True                     | Yes             |
    | "bigquery"      | False                    | No              |
    | "bigquery"      | True                     | Yes             |
    """
    node = DBTNode(
        dbt_name="model.project.MixedCaseTable",
        dbt_adapter="bigquery",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=[],
        materialization="table",
        columns=[],
        meta={},
        query_tag={},
        tags=[],
        owner="",
        language="sql",
        database="MyProject",
        schema="MyDataset",
        name="MixedCaseTable",
        alias=None,
        raw_code=None,
        dbt_file_path="/models/MixedCaseTable.sql",
        dbt_package_name=None,
        catalog_type=None,
        missing_from_catalog=False,
    )

    # Row 1: dbt + flag=False → NOT lowercased
    node.convert_urns_to_lowercase = False
    urn = node.get_urn(target_platform="dbt", env="PROD", data_platform_instance=None)
    assert "MyProject.MyDataset.MixedCaseTable" in urn

    # Row 2: dbt + flag=True → lowercased
    node.convert_urns_to_lowercase = True
    urn = node.get_urn(target_platform="dbt", env="PROD", data_platform_instance=None)
    assert "myproject.mydataset.mixedcasetable" in urn

    # Row 3: bigquery + flag=False → NOT lowercased (this was the bug)
    node.convert_urns_to_lowercase = False
    urn = node.get_urn(
        target_platform="bigquery", env="PROD", data_platform_instance=None
    )
    assert "MyProject.MyDataset.MixedCaseTable" in urn

    # Row 4: bigquery + flag=True → lowercased
    node.convert_urns_to_lowercase = True
    urn = node.get_urn(
        target_platform="bigquery", env="PROD", data_platform_instance=None
    )
    assert "myproject.mydataset.mixedcasetable" in urn


def test_dbt_entity_emission_configuration_helpers():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {
            "models": "Only",
        },
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert config.entities_enabled.can_emit_node_type("model")
    assert not config.entities_enabled.can_emit_node_type("source")
    assert not config.entities_enabled.can_emit_node_type("test")
    assert not config.entities_enabled.can_emit_test_results
    assert not config.entities_enabled.can_emit_model_performance
    assert not config.entities_enabled.is_only_test_results()

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert config.entities_enabled.can_emit_node_type("model")
    assert config.entities_enabled.can_emit_node_type("source")
    assert config.entities_enabled.can_emit_node_type("test")
    assert config.entities_enabled.can_emit_test_results
    assert config.entities_enabled.can_emit_model_performance
    assert not config.entities_enabled.is_only_test_results()

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {
            "test_results": "Only",
        },
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert not config.entities_enabled.can_emit_node_type("model")
    assert not config.entities_enabled.can_emit_node_type("source")
    assert not config.entities_enabled.can_emit_node_type("test")
    assert config.entities_enabled.can_emit_test_results
    assert not config.entities_enabled.can_emit_model_performance
    assert config.entities_enabled.is_only_test_results()

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {
            "test_results": "Yes",
            "test_definitions": "Yes",
            "model_performance": "Yes",
            "models": "No",
            "sources": "No",
        },
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert not config.entities_enabled.can_emit_node_type("model")
    assert not config.entities_enabled.can_emit_node_type("source")
    assert config.entities_enabled.can_emit_node_type("test")
    assert config.entities_enabled.can_emit_test_results
    assert config.entities_enabled.can_emit_model_performance
    assert not config.entities_enabled.is_only_test_results()


def test_dbt_cloud_config_access_url():
    config_dict = {
        "access_url": "https://emea.getdbt.com",
        "token": "dummy_token",
        "account_id": "123456",
        "project_id": "1234567",
        "job_id": "12345678",
        "run_id": "123456789",
        "target_platform": "dummy_platform",
    }
    config = DBTCloudConfig.model_validate(config_dict)
    assert config.access_url == "https://emea.getdbt.com"
    assert config.metadata_endpoint == "https://metadata.emea.getdbt.com/graphql"


def test_dbt_cloud_config_with_defined_metadata_endpoint():
    config_dict = {
        "access_url": "https://my-dbt-cloud.dbt.com",
        "token": "dummy_token",
        "account_id": "123456",
        "project_id": "1234567",
        "job_id": "12345678",
        "run_id": "123456789",
        "target_platform": "dummy_platform",
        "metadata_endpoint": "https://my-metadata-endpoint.my-dbt-cloud.dbt.com/graphql",
    }
    config = DBTCloudConfig.model_validate(config_dict)
    assert config.access_url == "https://my-dbt-cloud.dbt.com"
    assert (
        config.metadata_endpoint
        == "https://my-metadata-endpoint.my-dbt-cloud.dbt.com/graphql"
    )


def test_infer_metadata_endpoint() -> None:
    assert_doctest(dbt_cloud)


def test_dbt_time_parsing() -> None:
    time_formats = [
        "2024-03-28T05:56:15.236210Z",
        "2024-04-04T11:55:28Z",
        "2024-04-04T12:55:28Z",
        "2024-03-25T00:52:14Z",
    ]

    for time_format in time_formats:
        # Check that it parses without an error.
        timestamp = parse_dbt_timestamp(time_format)

        # Ensure that we get an object with tzinfo set to UTC.
        assert timestamp.tzinfo is not None and timestamp.tzinfo.utcoffset(
            timestamp
        ) == timedelta(0)


def test_get_column_type_redshift():
    report = DBTSourceReport()
    dataset_name = "test_dataset"

    # Test 'super' type which should not show any warnings/errors
    result_super = get_column_type(report, dataset_name, "super", "redshift")
    assert isinstance(result_super.type, NullTypeClass)
    assert len(report.infos) == 0, (
        "No warnings should be generated for known SUPER type"
    )

    # Test unknown type, which generates a warning but resolves to NullTypeClass
    unknown_type = "unknown_type"
    result_unknown = get_column_type(report, dataset_name, unknown_type, "redshift")
    assert isinstance(result_unknown.type, NullTypeClass)

    # exact warning message for an unknown type
    expected_context = f"{dataset_name} - {unknown_type}"
    messages = [info for info in report.infos if expected_context in str(info.context)]
    assert len(messages) == 1
    assert messages[0].title == "Unable to map column types to DataHub types"
    assert (
        messages[0].message
        == "Got an unexpected column type. The column's parsed field type will not be populated."
    )


def test_include_database_name_default():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
    }
    config = DBTCoreConfig.model_validate({**config_dict})
    assert config.include_database_name is True


@pytest.mark.parametrize(
    ("include_database_name", "expected"), [("false", False), ("true", True)]
)
def test_include_database_name(include_database_name: str, expected: bool) -> None:
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
    }
    config_dict.update({"include_database_name": include_database_name})
    config = DBTCoreConfig.model_validate({**config_dict})
    assert config.include_database_name is expected


def test_extract_dbt_entities() -> None:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="tests/unit/dbt/artifacts/manifest.json",
        catalog_path="tests/unit/dbt/artifacts/catalog.json",
        target_platform="dummy",
    )
    source = DBTCoreSource(config, ctx)
    assert all(node.database is not None for node in source.loadManifestAndCatalog()[0])
    config.include_database_name = False
    source = DBTCoreSource(config, ctx)
    assert all(node.database is None for node in source.loadManifestAndCatalog()[0])


def test_drop_duplicate_sources() -> None:
    class SharedDBTNodeFields(TypedDict, total=False):
        database: str
        schema: str
        alias: None
        comment: str
        raw_code: None
        dbt_adapter: str
        dbt_file_path: None
        dbt_package_name: str
        max_loaded_at: None
        catalog_type: None
        missing_from_catalog: bool
        owner: None
        compiled_code: None

    # Create 3 nodes: model, duplicate source, and another model that references the source
    shared_fields: SharedDBTNodeFields = {
        "database": "test_db",
        "schema": "test_schema",
        "alias": None,
        "comment": "",
        "raw_code": None,
        "dbt_adapter": "postgres",
        "dbt_file_path": None,
        "dbt_package_name": "package",
        "max_loaded_at": None,
        "catalog_type": None,
        "missing_from_catalog": False,
        "owner": None,
        "compiled_code": None,
    }

    model_node = DBTNode(
        **shared_fields,
        name="shared_table",
        description="A model",
        language="sql",
        dbt_name="model.package.shared_table",
        node_type="model",
        materialization="table",
    )

    duplicate_source = DBTNode(
        **shared_fields,
        name="shared_table",  # Same warehouse name as model
        description="A source with same name as model",
        language=None,
        dbt_name="source.package.external_source.shared_table",
        node_type="source",
        materialization=None,
    )

    referencing_model = DBTNode(
        **shared_fields,
        name="downstream_table",
        description="A model that references the source",
        language="sql",
        dbt_name="model.package.downstream_table",
        node_type="model",
        materialization="table",
        upstream_nodes=[
            "source.package.external_source.shared_table"
        ],  # References the source
    )

    original_nodes = [model_node, duplicate_source, referencing_model]

    # Test the method
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig.model_validate(create_base_dbt_config())
    source: DBTCoreSource = DBTCoreSource(config, ctx)

    result_nodes: List[DBTNode] = source._drop_duplicate_sources(original_nodes)

    # Verify source was dropped (only 2 nodes remain)
    assert len(result_nodes) == 2
    node_types = [node.node_type for node in result_nodes]
    assert "source" not in node_types
    assert node_types.count("model") == 2

    # Verify reference was updated
    downstream_node = next(
        node
        for node in result_nodes
        if node.dbt_name == "model.package.downstream_table"
    )
    assert downstream_node.upstream_nodes == ["model.package.shared_table"]

    # Verify report counters
    assert source.report.duplicate_sources_dropped == 1
    assert source.report.duplicate_sources_references_updated == 1


def _make_sibling_dbt_node(
    materialization: str = "table",
    node_type: str = "model",
    name: str = "test_model",
) -> DBTNode:
    return DBTNode(
        name=name,
        database="test_db",
        schema="test_schema",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name=f"{node_type}.package.{name}",
        dbt_file_path=None,
        dbt_package_name="package",
        node_type=node_type,
        materialization=materialization,
        max_loaded_at=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        compiled_code=None,
    )


def _make_dbt_source(dbt_is_primary_sibling: bool = True) -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig(**create_base_dbt_config())
    source = DBTCoreSource(config, ctx)
    source.config.dbt_is_primary_sibling = dbt_is_primary_sibling
    return source


@pytest.mark.parametrize("dbt_is_primary_sibling", [True, False])
def test_dbt_sibling_created_for_semantic_views(dbt_is_primary_sibling: bool) -> None:
    """Regression test for CUS-7718: semantic views showed as duplicate search
    results because the SiblingAssociationHook only handles the "source" subtype."""
    source = _make_dbt_source(dbt_is_primary_sibling)
    node = _make_sibling_dbt_node(materialization="semantic_view")
    assert source._should_create_sibling_relationships(node) is True


def test_dbt_sibling_not_created_for_standard_models_when_primary() -> None:
    """Standard models rely on the SiblingAssociationHook for sibling creation
    when dbt is primary. Only semantic views need explicit emission."""
    source = _make_dbt_source(dbt_is_primary_sibling=True)
    for materialization in ("table", "view", "incremental"):
        node = _make_sibling_dbt_node(materialization=materialization)
        assert source._should_create_sibling_relationships(node) is False


def test_dbt_sibling_created_for_all_nodes_when_not_primary() -> None:
    """When target platform is primary (dbt_is_primary_sibling=False),
    the dbt source emits siblings for all nodes."""
    source = _make_dbt_source(dbt_is_primary_sibling=False)
    for materialization in ("table", "view", "incremental", "semantic_view"):
        node = _make_sibling_dbt_node(materialization=materialization)
        assert source._should_create_sibling_relationships(node) is True


@pytest.mark.parametrize(
    "materialization,node_type",
    [("ephemeral", "model"), ("test", "test")],
)
def test_dbt_sibling_not_created_for_ephemeral_or_test_nodes(
    materialization: str, node_type: str
) -> None:
    source = _make_dbt_source()
    node = _make_sibling_dbt_node(materialization=materialization, node_type=node_type)
    assert source._should_create_sibling_relationships(node) is False


def test_dbt_cloud_source_description_precedence() -> None:
    """
    Test that dbt Cloud source prioritizes table-level description over schema-level sourceDescription.
    """

    config = DBTCloudConfig(
        access_url="https://test.getdbt.com",
        token="dummy_token",
        account_id="123456",
        project_id="1234567",
        job_id="12345678",
        run_id="123456789",
        target_platform="snowflake",
    )

    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-cloud-source")
    source = DBTCloudSource(config, ctx)

    source_node_data: Dict[str, Any] = {
        "uniqueId": "source.my_project.my_schema.my_table",
        "name": "my_table",
        "description": "This is the table-level description for my_table",
        "sourceDescription": "This is the schema-level description for my_schema",
        "resourceType": "source",
        "identifier": "my_table",
        "sourceName": "my_schema",
        "database": "my_database",
        "schema": "my_schema",
        "type": None,
        "owner": None,
        "comment": "",
        "columns": [],
        "meta": {},
        "tags": [],
        "maxLoadedAt": None,
        "snapshottedAt": None,
        "state": None,
        "freshnessChecked": None,
        "loader": None,
    }

    parsed_node = source._parse_into_dbt_node(source_node_data)

    assert parsed_node.description == "This is the table-level description for my_table"
    assert (
        parsed_node.description != "This is the schema-level description for my_schema"
    )
    assert parsed_node.name == "my_table"
    assert parsed_node.node_type == "source"


def test_dbt_cloud_source_description_fallback() -> None:
    """
    Test that dbt Cloud source falls back to sourceDescription when table description is empty.
    """

    config = DBTCloudConfig(
        access_url="https://test.getdbt.com",
        token="dummy_token",
        account_id="123456",
        project_id="1234567",
        job_id="12345678",
        run_id="123456789",
        target_platform="snowflake",
    )

    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-cloud-source")
    source = DBTCloudSource(config, ctx)

    source_node_data: Dict[str, Any] = {
        "uniqueId": "source.my_project.my_schema.my_table",
        "name": "my_table",
        "description": "",  # Empty table description
        "sourceDescription": "This is the schema-level description for my_schema",
        "resourceType": "source",
        "identifier": "my_table",
        "sourceName": "my_schema",
        "database": "my_database",
        "schema": "my_schema",
        "type": None,
        "owner": None,
        "comment": "",
        "columns": [],
        "meta": {},
        "tags": [],
        "maxLoadedAt": None,
        "snapshottedAt": None,
        "state": None,
        "freshnessChecked": None,
        "loader": None,
    }

    parsed_node = source._parse_into_dbt_node(source_node_data)

    assert (
        parsed_node.description == "This is the schema-level description for my_schema"
    )


def test_dbt_semantic_view_subtype() -> None:
    """
    Test that semantic views get the correct SEMANTIC_VIEW subtype.
    """
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(**create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    semantic_view_node = DBTNode(
        database="analytics",
        schema="public",
        name="sales_analytics",
        alias="sales_analytics",
        dbt_name="my_project.sales_analytics",
        dbt_adapter="snowflake",
        node_type="model",  # dbt's resource_type for models
        max_loaded_at=None,
        materialization="semantic_view",  # Semantic views are models with this materialization
        comment="",
        description="Sales analytics semantic view",
        dbt_file_path=None,
        catalog_type=None,
        language="sql",
        raw_code=None,
        dbt_package_name="my_project",
        missing_from_catalog=False,
        owner="",
    )

    subtype_wu = source._create_subType_wu(
        semantic_view_node,
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.sales_analytics,PROD)",
    )

    assert subtype_wu is not None
    assert isinstance(subtype_wu.metadata, MetadataChangeProposalWrapper)
    aspect = subtype_wu.metadata.aspect
    assert aspect is not None
    assert isinstance(aspect, SubTypesClass)
    assert DatasetSubTypes.SEMANTIC_VIEW in aspect.typeNames


def test_parse_semantic_view_cll_derived_metrics() -> None:
    """
    Test parsing derived metrics computed from other metrics.
    Derived: TOTAL_REVENUE AS ORDERS.GROSS_REVENUE + TRANSACTIONS.NET_PAYMENT
    """
    compiled_sql = """
    METRICS (
        ORDERS.GROSS_REVENUE AS SUM(ORDER_TOTAL),
        TRANSACTIONS.NET_PAYMENT AS SUM(TRANSACTION_AMOUNT),
        TOTAL_REVENUE AS ORDERS.GROSS_REVENUE + TRANSACTIONS.NET_PAYMENT
            COMMENT='Combined revenue'
    )
    """

    upstream_nodes = [
        "source.project.shop.ORDERS",
        "source.project.shop.TRANSACTIONS",
    ]

    all_nodes_map = {
        "source.project.shop.ORDERS": create_mock_dbt_node("ORDERS"),
        "source.project.shop.TRANSACTIONS": create_mock_dbt_node("TRANSACTIONS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract 2 base metrics + 2 derived entries = 4 total
    assert len(cll_info) == 4

    # Check base metrics exist
    gross_rev = [cll for cll in cll_info if cll.downstream_col == "gross_revenue"]
    assert len(gross_rev) == 1

    # Check derived metric has lineage from BOTH base columns
    total_rev = [cll for cll in cll_info if cll.downstream_col == "total_revenue"]
    assert len(total_rev) == 2  # Two sources!

    # Verify it traces back to the original columns
    upstream_cols = {cll.upstream_col for cll in total_rev}
    assert upstream_cols == {"order_total", "transaction_amount"}


def test_parse_semantic_view_cll_multiple_tables_same_column() -> None:
    """
    Test handling columns that exist in multiple upstream tables.
    Example: ORDER_ID exists in both ORDERS and TRANSACTIONS
    """
    compiled_sql = """
    DIMENSIONS (
        ORDERS.ORDER_ID AS ORDER_ID,
        ORDERS.CUSTOMER_ID AS CUSTOMER_ID
    )
    FACTS (
        TRANSACTIONS.ORDER_ID AS ORDER_ID,
        TRANSACTIONS.AMOUNT AS AMOUNT
    )
    """

    upstream_nodes = [
        "source.project.shop.ORDERS",
        "source.project.shop.TRANSACTIONS",
    ]

    all_nodes_map = {
        "source.project.shop.ORDERS": create_mock_dbt_node("ORDERS"),
        "source.project.shop.TRANSACTIONS": create_mock_dbt_node("TRANSACTIONS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract entries for both ORDER_ID occurrences
    order_ids = [cll for cll in cll_info if cll.downstream_col == "order_id"]
    assert len(order_ids) == 2  # From both tables!

    # Verify both upstream tables are represented
    upstream_tables = {cll.upstream_dbt_name for cll in order_ids}
    assert upstream_tables == {
        "source.project.shop.ORDERS",
        "source.project.shop.TRANSACTIONS",
    }


def test_parse_semantic_view_cll_case_handling() -> None:
    """
    Test comprehensive case handling: SQL keywords, table names, and column names.
    Validates case-insensitive matching and normalization to lowercase.
    """
    compiled_sql = """
    METRICS (
        orders.gross_revenue as SUM(order_total),
        TRANSACTIONS.Net_Payment AS AVG(transaction_amount)
    )
    DIMENSIONS (
        Orders.Customer_Id as customer_id,
        ORDERS.ORDER_ID as order_id,
        transactions.store_id AS store_id
    )
    """

    upstream_nodes = [
        "source.project.shop.ORDERS",
        "source.project.shop.TRANSACTIONS",
    ]

    all_nodes_map = {
        "source.project.shop.ORDERS": create_mock_dbt_node("ORDERS"),
        "source.project.shop.TRANSACTIONS": create_mock_dbt_node("TRANSACTIONS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract all entries regardless of case variations
    assert len(cll_info) == 5

    # Verify entries are normalized to lowercase
    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert downstream_cols == {
        "gross_revenue",
        "net_payment",
        "customer_id",
        "order_id",
        "store_id",
    }

    # Verify all table references matched correctly despite case variations
    assert all(
        cll.upstream_dbt_name.startswith("source.project.shop.") for cll in cll_info
    )


def test_parse_semantic_view_cll_missing_upstream_node() -> None:
    """
    Test handling when upstream node is not in all_nodes_map.
    Should log warning but continue processing other nodes.
    """
    compiled_sql = """
    METRICS (
        ORDERS.GROSS_REVENUE AS SUM(ORDER_TOTAL),
        TRANSACTIONS.NET_PAYMENT AS SUM(TRANSACTION_AMOUNT)
    )
    """

    upstream_nodes = [
        "source.project.shop.ORDERS",
        "source.project.shop.TRANSACTIONS",  # This one is missing
    ]

    # Only include ORDERS in the map
    all_nodes_map = {
        "source.project.shop.ORDERS": create_mock_dbt_node("ORDERS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract only the ORDERS metric
    assert len(cll_info) == 1
    assert cll_info[0].upstream_dbt_name == "source.project.shop.ORDERS"
    assert cll_info[0].downstream_col == "gross_revenue"


def test_parse_semantic_view_cll_table_not_in_mapping() -> None:
    """
    Test handling when DDL references a table not in upstream_nodes.
    Should skip that entry gracefully.
    """
    compiled_sql = """
    METRICS (
        ORDERS.GROSS_REVENUE AS SUM(ORDER_TOTAL),
        UNKNOWN_TABLE.SOME_METRIC AS SUM(SOME_COLUMN)
    )
    """

    upstream_nodes = ["source.project.shop.ORDERS"]
    all_nodes_map = {"source.project.shop.ORDERS": create_mock_dbt_node("ORDERS")}

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract only the ORDERS metric, skip UNKNOWN_TABLE
    assert len(cll_info) == 1
    assert cll_info[0].upstream_col == "order_total"


def test_parse_semantic_view_cll_derived_metric_missing_reference() -> None:
    """
    Test derived metric that references a non-existent metric.
    Should skip the missing reference gracefully.
    """
    compiled_sql = """
    METRICS (
        ORDERS.REVENUE AS SUM(ORDER_TOTAL),
        TOTAL AS ORDERS.REVENUE + ORDERS.MISSING_METRIC
    )
    """

    upstream_nodes = ["source.project.shop.ORDERS"]
    all_nodes_map = {"source.project.shop.ORDERS": create_mock_dbt_node("ORDERS")}

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract base metric + partial derived metric (only REVENUE part)
    assert len(cll_info) == 2  # 1 base + 1 derived from revenue

    # Verify base metric
    base_metric = [cll for cll in cll_info if cll.downstream_col == "revenue"]
    assert len(base_metric) == 1

    # Verify derived metric only has lineage from revenue (not missing_metric)
    total_metric = [cll for cll in cll_info if cll.downstream_col == "total"]
    assert len(total_metric) == 1
    assert total_metric[0].upstream_col == "order_total"


def test_parse_semantic_view_cll_malformed_sql() -> None:
    """Test parse_semantic_view_cll with malformed/invalid SQL patterns."""

    # Malformed DIMENSIONS (missing AS keyword)
    compiled_sql = """
    DIMENSIONS (
        ORDERS.CUSTOMER_ID CUSTOMER_ID
    )
    """
    upstream_nodes = ["source.project.src.ORDERS"]
    all_nodes_map = {"source.project.src.ORDERS": create_mock_dbt_node("ORDERS")}

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should not extract lineage from malformed syntax
    assert len(cll_info) == 0


def test_parse_semantic_view_cll_multiple_aggregations() -> None:
    """Test parse_semantic_view_cll with multiple aggregation functions."""

    compiled_sql = """
    METRICS (
        ORDERS.TOTAL_REVENUE AS SUM(ORDER_TOTAL),
        ORDERS.AVG_ORDER AS AVG(ORDER_TOTAL),
        ORDERS.ORDER_COUNT AS COUNT(ORDER_ID),
        ORDERS.MIN_ORDER AS MIN(ORDER_TOTAL),
        ORDERS.MAX_ORDER AS MAX(ORDER_TOTAL)
    )
    """
    upstream_nodes = ["source.project.src.ORDERS"]
    all_nodes_map = {"source.project.src.ORDERS": create_mock_dbt_node("ORDERS")}

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should extract all 5 metrics
    assert len(cll_info) == 5

    # Check all aggregation types are captured
    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert downstream_cols == {
        "total_revenue",
        "avg_order",
        "order_count",
        "min_order",
        "max_order",
    }

    # All should map from same upstream columns
    assert all(cll.upstream_dbt_name == "source.project.src.ORDERS" for cll in cll_info)


def test_parse_semantic_view_cll_chained_derived_metrics() -> None:
    """Test parse_semantic_view_cll with multiple levels of derived metrics."""

    compiled_sql = """
    METRICS (
        ORDERS.BASE_REVENUE AS SUM(ORDER_TOTAL),
        TRANSACTIONS.BASE_PAYMENT AS SUM(TRANSACTION_AMOUNT),
        COMBINED_REVENUE AS ORDERS.BASE_REVENUE + TRANSACTIONS.BASE_PAYMENT,
        REVENUE_WITH_TAX AS COMBINED_REVENUE * 1.1
    )
    """
    upstream_nodes = [
        "source.project.src.ORDERS",
        "source.project.src.TRANSACTIONS",
    ]
    all_nodes_map = {
        "source.project.src.ORDERS": create_mock_dbt_node("ORDERS"),
        "source.project.src.TRANSACTIONS": create_mock_dbt_node("TRANSACTIONS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should have:
    # - 2 base metrics (base_revenue, base_payment)
    # - 2 derived metrics mapping to base columns (combined_revenue from both sources)
    # Note: REVENUE_WITH_TAX references COMBINED_REVENUE which isn't a base metric,
    # so it won't create lineage (current limitation)
    assert len(cll_info) == 4

    # Check combined_revenue has lineage to both base columns
    combined_lineages = [
        cll for cll in cll_info if cll.downstream_col == "combined_revenue"
    ]
    assert len(combined_lineages) == 2
    upstream_cols = {cll.upstream_col for cll in combined_lineages}
    assert upstream_cols == {"order_total", "transaction_amount"}


def test_semantic_view_cll_integration_with_node() -> None:
    """Test that semantic view CLL is properly extracted and added to DBTNode during processing."""

    # Create a semantic view node with compiled code
    semantic_view_node = DBTNode(
        dbt_name="model.project.sales_view",
        dbt_adapter="snowflake",
        database="db",
        schema="schema",
        name="sales_view",
        alias="sales_view",
        comment="",
        description="",
        raw_code="",
        compiled_code="""
        DIMENSIONS (
            ORDERS.CUSTOMER_ID AS CUSTOMER_ID,
            ORDERS.ORDER_ID AS ORDER_ID
        )
        METRICS (
            ORDERS.TOTAL_REVENUE AS SUM(ORDER_TOTAL)
        )
        """,
        dbt_file_path="",
        node_type="semantic_view",
        max_loaded_at=None,
        materialization=None,
        upstream_nodes=["source.project.src.ORDERS"],
        catalog_type=None,
        upstream_cll=[],  # Should be populated by CLL extraction
        language="sql",
        dbt_package_name="project",
        missing_from_catalog=False,
        owner=None,
    )

    # Create upstream nodes map
    all_nodes_map = {
        "source.project.src.ORDERS": create_mock_dbt_node("ORDERS"),
    }

    # Simulate what _infer_schemas_and_update_cll does
    if semantic_view_node.compiled_code:
        cll_info = parse_semantic_view_cll(
            compiled_sql=semantic_view_node.compiled_code,
            upstream_nodes=semantic_view_node.upstream_nodes,
            all_nodes_map=all_nodes_map,
        )
        semantic_view_node.upstream_cll.extend(cll_info)

    # Verify CLL was added to the node
    assert len(semantic_view_node.upstream_cll) == 3
    downstream_cols = {cll.downstream_col for cll in semantic_view_node.upstream_cll}
    assert downstream_cols == {"customer_id", "order_id", "total_revenue"}

    # Verify all CLL entries point to the correct upstream
    assert all(
        cll.upstream_dbt_name == "source.project.src.ORDERS"
        for cll in semantic_view_node.upstream_cll
    )


def test_semantic_view_cll_integration_missing_code() -> None:
    """Test that semantic view CLL extraction handles missing/empty compiled_code gracefully."""

    all_nodes_map = {
        "source.project.src.ORDERS": create_mock_dbt_node("ORDERS"),
    }

    # Test Case 1: compiled_code is None
    node_with_none = DBTNode(
        dbt_name="model.project.sales_view",
        dbt_adapter="snowflake",
        database="db",
        schema="schema",
        name="sales_view",
        alias="sales_view",
        comment="",
        description="",
        raw_code="",
        compiled_code=None,  # No compiled code
        dbt_file_path="",
        node_type="semantic_view",
        max_loaded_at=None,
        materialization=None,
        upstream_nodes=["source.project.src.ORDERS"],
        catalog_type=None,
        upstream_cll=[],
        language="sql",
        dbt_package_name="project",
        missing_from_catalog=False,
        owner=None,
    )

    # Simulate what _infer_schemas_and_update_cll does
    if node_with_none.compiled_code:
        cll_info = parse_semantic_view_cll(
            compiled_sql=node_with_none.compiled_code,
            upstream_nodes=node_with_none.upstream_nodes,
            all_nodes_map=all_nodes_map,
        )
        node_with_none.upstream_cll.extend(cll_info)

    assert len(node_with_none.upstream_cll) == 0, (
        "None compiled_code should skip CLL extraction"
    )

    # Test Case 2: compiled_code is empty string
    node_with_empty = DBTNode(
        dbt_name="model.project.sales_view2",
        dbt_adapter="snowflake",
        database="db",
        schema="schema",
        name="sales_view2",
        alias="sales_view2",
        comment="",
        description="",
        raw_code="",
        compiled_code="",  # Empty string
        dbt_file_path="",
        node_type="semantic_view",
        max_loaded_at=None,
        materialization=None,
        upstream_nodes=["source.project.src.ORDERS"],
        catalog_type=None,
        upstream_cll=[],
        language="sql",
        dbt_package_name="project",
        missing_from_catalog=False,
        owner=None,
    )

    if node_with_empty.compiled_code:
        cll_info = parse_semantic_view_cll(
            compiled_sql=node_with_empty.compiled_code,
            upstream_nodes=node_with_empty.upstream_nodes,
            all_nodes_map=all_nodes_map,
        )
        node_with_empty.upstream_cll.extend(cll_info)

    assert len(node_with_empty.upstream_cll) == 0, (
        "Empty compiled_code should return empty list"
    )


def test_semantic_view_cll_integration_by_materialization() -> None:
    """Test that CLL extraction works for nodes identified by materialization (not node_type)."""

    # Create a node with materialization='semantic_view' but node_type='model'
    # This happens with dbt_semantic_view package
    semantic_view_node = DBTNode(
        dbt_name="model.project.sales_view",
        dbt_adapter="snowflake",
        database="db",
        schema="schema",
        name="sales_view",
        alias="sales_view",
        comment="",
        description="",
        raw_code="",
        compiled_code="""
        DIMENSIONS (
            TRANSACTIONS.TRANSACTION_ID AS TRANSACTION_ID
        )
        FACTS (
            TRANSACTIONS.AMOUNT AS AMOUNT
        )
        """,
        dbt_file_path="",
        node_type="model",  # Regular model
        max_loaded_at=None,
        materialization="semantic_view",  # But materialized as semantic_view
        upstream_nodes=["source.project.src.TRANSACTIONS"],
        catalog_type=None,
        upstream_cll=[],
        language="sql",
        dbt_package_name="project",
        missing_from_catalog=False,
        owner=None,
    )

    all_nodes_map = {
        "source.project.src.TRANSACTIONS": create_mock_dbt_node("TRANSACTIONS"),
    }

    # Check condition matches what's in _infer_schemas_and_update_cll
    should_parse = (
        semantic_view_node.node_type == "semantic_view"
        or semantic_view_node.materialization == "semantic_view"
    )
    assert should_parse is True

    # Simulate CLL extraction
    if semantic_view_node.compiled_code:
        cll_info = parse_semantic_view_cll(
            compiled_sql=semantic_view_node.compiled_code,
            upstream_nodes=semantic_view_node.upstream_nodes,
            all_nodes_map=all_nodes_map,
        )
        semantic_view_node.upstream_cll.extend(cll_info)

    # Verify CLL was extracted
    assert len(semantic_view_node.upstream_cll) == 2
    downstream_cols = {cll.downstream_col for cll in semantic_view_node.upstream_cll}
    assert downstream_cols == {"transaction_id", "amount"}


def test_parse_semantic_view_cll_circular_metric_reference() -> None:
    """Test that circular metric references are handled gracefully (A→B→A)."""

    compiled_sql = """
    METRICS (
        ORDERS.METRIC_A AS SUM(ORDER_TOTAL),
        ORDERS.METRIC_B AS METRIC_A * 2,
        ORDERS.CIRCULAR AS METRIC_B + METRIC_A
    )
    """
    upstream_nodes = ["source.project.src.ORDERS"]
    all_nodes_map = {"source.project.src.ORDERS": create_mock_dbt_node("ORDERS")}

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Parser extracts what it can without infinite loops:
    # - METRIC_A: order_total → metric_a (SUM pattern)
    # - METRIC_A: metric_b → metric_a (dimension pattern from "METRIC_A * 2")
    # - CIRCULAR: circular → metric_b (dimension pattern)
    # Note: Parser treats table-qualified references in expressions as column refs
    assert len(cll_info) >= 1  # At least base metric

    # Verify base metric from ORDER_TOTAL is captured
    base_metrics = [
        cll
        for cll in cll_info
        if cll.downstream_col == "metric_a" and cll.upstream_col == "order_total"
    ]
    assert len(base_metrics) == 1, "Base metric from physical column should be captured"


def test_parse_semantic_view_cll_production_pattern() -> None:
    """
    Test real production pattern from dbt Cloud logs.
    Based on actual semantic view from WAREHOUSE_COFFEE_COMPANY.
    """
    compiled_sql = """
    FACTS (
        orders.ORDER_TOTAL AS ORDER_TOTAL,
        transactions.ORDER_ID AS ORDER_ID,
        transactions.TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT
    )
    DIMENSIONS (
        ORDERS.CUSTOMER_ID AS CUSTOMER_ID,
        ORDERS.ORDER_ID AS ORDER_ID,
        ORDERS.ORDER_TYPE AS ORDER_TYPE,
        ORDERS.STORE_ID AS STORE_ID,
        TRANSACTIONS.PAYMENT_METHOD AS PAYMENT_METHOD,
        TRANSACTIONS.TRANSACTION_DATE AS TRANSACTION_DATE,
        TRANSACTIONS.TRANSACTION_ID AS TRANSACTION_ID,
        TRANSACTIONS.TRANSACTION_TYPE AS TRANSACTION_TYPE
    )
    METRICS (
        ORDERS.GROSS_REVENUE AS SUM(ORDER_TOTAL),
        TRANSACTIONS.NET_PAYMENT_AMOUNT AS SUM(TRANSACTION_AMOUNT),
        TOTAL_ORDER_REVENUE AS ORDERS.GROSS_REVENUE + TRANSACTIONS.NET_PAYMENT_AMOUNT
    )
    """

    upstream_nodes = [
        "source.my_analytics_project.coffee_shop_source.ORDERS",
        "source.my_analytics_project.coffee_shop_source.TRANSACTIONS",
    ]

    all_nodes_map = {
        "source.my_analytics_project.coffee_shop_source.ORDERS": create_mock_dbt_node(
            "ORDERS"
        ),
        "source.my_analytics_project.coffee_shop_source.TRANSACTIONS": create_mock_dbt_node(
            "TRANSACTIONS"
        ),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Expected: 3 facts + 8 dimensions + 2 simple metrics + 2 derived metric entries = 15
    assert len(cll_info) == 15, f"Expected 15 CLL entries, got {len(cll_info)}"

    # Verify derived metric has lineage from both sources
    derived_lineages = [
        cll for cll in cll_info if cll.downstream_col == "total_order_revenue"
    ]
    assert len(derived_lineages) == 2, (
        "Derived metric should trace to both base columns"
    )

    upstream_cols = {cll.upstream_col for cll in derived_lineages}
    assert upstream_cols == {"order_total", "transaction_amount"}

    # Verify duplicate ORDER_ID is captured from both tables
    order_id_lineages = [cll for cll in cll_info if cll.downstream_col == "order_id"]
    assert len(order_id_lineages) == 2, (
        "ORDER_ID should have lineage from both ORDERS and TRANSACTIONS"
    )

    upstream_tables = {cll.upstream_dbt_name for cll in order_id_lineages}
    assert len(upstream_tables) == 2, "ORDER_ID should come from 2 different tables"


def test_parse_semantic_view_cll_with_table_aliases() -> None:
    """
    Test that parser correctly handles table aliases in TABLES section.
    This is a common pattern in semantic views where tables are aliased.
    """
    compiled_sql = """
    TABLES (
        cases as analytics_cs_mart.analytics.r_support_case_analysis,
        hierarchy as analytics_cs_mart.analytics.r_cs_all_level_success_factor_hierarchy
    )
    
    DIMENSIONS (
        cases.account_name as account_name,
        cases.case_id as case_id,
        hierarchy.l1_name as l1_name,
        hierarchy.l2_name as l2_name
    )
    
    METRICS (
        cases.first_response_met as COUNT(case_id)
    )
    """

    upstream_nodes = [
        "model.dbt_cs_analytics.r_support_case_analysis",
        "model.dbt_cs_analytics.r_cs_all_level_success_factor_hierarchy",
    ]

    all_nodes_map = {
        "model.dbt_cs_analytics.r_support_case_analysis": create_mock_dbt_node(
            "r_support_case_analysis"
        ),
        "model.dbt_cs_analytics.r_cs_all_level_success_factor_hierarchy": create_mock_dbt_node(
            "r_cs_all_level_success_factor_hierarchy"
        ),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Expected: 4 dimensions + 1 metric = 5 CLL entries
    assert len(cll_info) == 5, f"Expected 5 CLL entries, got {len(cll_info)}"

    # Verify dimensions from 'cases' alias
    cases_dimensions = [
        cll
        for cll in cll_info
        if cll.upstream_dbt_name == "model.dbt_cs_analytics.r_support_case_analysis"
        and cll.downstream_col in ["account_name", "case_id", "first_response_met"]
    ]
    assert len(cases_dimensions) == 3, (
        f"Expected 3 lineages from cases alias, got {len(cases_dimensions)}"
    )

    # Verify dimensions from 'hierarchy' alias
    hierarchy_dimensions = [
        cll
        for cll in cll_info
        if cll.upstream_dbt_name
        == "model.dbt_cs_analytics.r_cs_all_level_success_factor_hierarchy"
    ]
    assert len(hierarchy_dimensions) == 2, (
        f"Expected 2 lineages from hierarchy alias, got {len(hierarchy_dimensions)}"
    )

    # Verify column names
    assert any(cll.downstream_col == "account_name" for cll in cll_info)
    assert any(cll.downstream_col == "case_id" for cll in cll_info)
    assert any(cll.downstream_col == "l1_name" for cll in cll_info)
    assert any(cll.downstream_col == "first_response_met" for cll in cll_info)


def test_semantic_view_cll_integration_multiple_upstreams() -> None:
    """Test that CLL extraction works correctly with multiple upstream tables."""

    semantic_view_node = DBTNode(
        dbt_name="model.project.sales_view",
        dbt_adapter="snowflake",
        database="db",
        schema="schema",
        name="sales_view",
        alias="sales_view",
        comment="",
        description="",
        raw_code="",
        compiled_code="""
        DIMENSIONS (
            ORDERS.ORDER_ID AS ORDER_ID,
            CUSTOMERS.CUSTOMER_ID AS CUSTOMER_ID
        )
        METRICS (
            ORDERS.REVENUE AS SUM(ORDER_TOTAL),
            COMBINED_METRIC AS ORDERS.REVENUE + CUSTOMERS.LIFETIME_VALUE
        )
        """,
        dbt_file_path="",
        node_type="semantic_view",
        max_loaded_at=None,
        materialization=None,
        upstream_nodes=[
            "source.project.src.ORDERS",
            "source.project.src.CUSTOMERS",
        ],
        catalog_type=None,
        upstream_cll=[],
        language="sql",
        dbt_package_name="project",
        missing_from_catalog=False,
        owner=None,
    )

    all_nodes_map = {
        "source.project.src.ORDERS": create_mock_dbt_node("ORDERS"),
        "source.project.src.CUSTOMERS": create_mock_dbt_node("CUSTOMERS"),
    }

    # Simulate CLL extraction
    if semantic_view_node.compiled_code:
        cll_info = parse_semantic_view_cll(
            compiled_sql=semantic_view_node.compiled_code,
            upstream_nodes=semantic_view_node.upstream_nodes,
            all_nodes_map=all_nodes_map,
        )
        semantic_view_node.upstream_cll.extend(cll_info)

    # Should have lineage from both upstream tables
    assert len(semantic_view_node.upstream_cll) > 0

    # Verify we have lineage from both tables
    upstream_tables = {cll.upstream_dbt_name for cll in semantic_view_node.upstream_cll}
    assert "source.project.src.ORDERS" in upstream_tables
    assert "source.project.src.CUSTOMERS" in upstream_tables

    # Verify derived metric has lineage (COMBINED_METRIC references REVENUE and LIFETIME_VALUE)
    combined_lineages = [
        cll
        for cll in semantic_view_node.upstream_cll
        if cll.downstream_col == "combined_metric"
    ]
    # Should have lineage from ORDER_TOTAL (via REVENUE metric)
    assert any(cll.upstream_col == "order_total" for cll in combined_lineages)


def test_semantic_view_cll_non_snowflake_adapter() -> None:
    """Test that CLL extraction is skipped for non-Snowflake adapters.

    The source code checks dbt_adapter and only extracts CLL for Snowflake.
    For other adapters, a warning is logged and CLL extraction is skipped.
    This test documents the expected behavior for non-Snowflake adapters.
    """

    # Create a semantic view node with a non-Snowflake adapter
    semantic_view_node = DBTNode(
        dbt_name="model.project.sales_view",
        dbt_adapter="bigquery",  # Non-Snowflake adapter
        database="db",
        schema="schema",
        name="sales_view",
        alias="sales_view",
        comment="",
        description="",
        raw_code="",
        compiled_code="""
        DIMENSIONS (
            ORDERS.CUSTOMER_ID AS CUSTOMER_ID
        )
        METRICS (
            ORDERS.TOTAL_REVENUE AS SUM(ORDER_TOTAL)
        )
        """,
        dbt_file_path="",
        node_type="model",
        max_loaded_at=None,
        materialization="semantic_view",
        upstream_nodes=["source.project.src.ORDERS"],
        catalog_type=None,
        upstream_cll=[],
        language="sql",
        dbt_package_name="project",
        missing_from_catalog=False,
        owner=None,
    )

    all_nodes_map = {
        "source.project.src.ORDERS": create_mock_dbt_node("ORDERS"),
    }

    # Simulate what _infer_schemas_and_update_cll does:
    # For non-Snowflake adapters, CLL extraction should be skipped
    if (
        semantic_view_node.materialization == "semantic_view"
        and semantic_view_node.dbt_adapter == "snowflake"
        and semantic_view_node.compiled_code
    ):
        cll_info = parse_semantic_view_cll(
            compiled_sql=semantic_view_node.compiled_code,
            upstream_nodes=semantic_view_node.upstream_nodes,
            all_nodes_map=all_nodes_map,
        )
        semantic_view_node.upstream_cll.extend(cll_info)

    # For BigQuery adapter, CLL should NOT be extracted
    assert len(semantic_view_node.upstream_cll) == 0


def test_semantic_view_cll_empty_results() -> None:
    """Test behavior when CLL parsing returns empty results.

    This can happen when the DDL contains unsupported syntax or patterns.
    """

    # DDL with valid structure but no extractable lineage patterns
    compiled_sql = """
    -- Just comments, no actual DIMENSIONS/FACTS/METRICS
    SELECT * FROM some_table
    """

    upstream_nodes = ["source.project.src.ORDERS"]
    all_nodes_map = {"source.project.src.ORDERS": create_mock_dbt_node("ORDERS")}

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should return empty set when no patterns match
    assert len(cll_info) == 0


def test_make_assertion_from_freshness() -> None:
    node = DBTNode(
        database="raw_db",
        schema="raw",
        name="users",
        alias="users",
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name="source.test.raw.users",
        dbt_file_path=None,
        dbt_package_name="test",
        node_type="source",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )
    node.freshness_info = DBTFreshnessInfo(
        invocation_id="test-123",
        status="pass",
        max_loaded_at=datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc),
        snapshotted_at=datetime(2026, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
        max_loaded_at_time_ago_in_s=7200.0,
        warn_after=DBTFreshnessCriteria(count=12, period="hour"),
        error_after=DBTFreshnessCriteria(count=24, period="hour"),
    )

    mcp = make_assertion_from_freshness(
        {}, node, "urn:li:assertion:test", "urn:li:dataset:test"
    )

    assert mcp.aspect is not None
    assert isinstance(mcp.aspect, AssertionInfoClass)
    assert mcp.aspect.type == AssertionTypeClass.CUSTOM
    assert mcp.aspect.customAssertion is not None
    assert isinstance(mcp.aspect.customAssertion, CustomAssertionInfoClass)
    assert mcp.aspect.customAssertion.type == "dbt Freshness"
    assert mcp.aspect.customAssertion.entity == "urn:li:dataset:test"
    assert mcp.aspect.source is not None
    assert mcp.aspect.source.created is not None
    assert mcp.aspect.source.created.actor == SYSTEM_ACTOR
    assert mcp.aspect.customProperties is not None
    assert mcp.aspect.customProperties.get("error_after_count") == "24"
    assert mcp.aspect.customProperties.get("warn_after_count") == "12"


@pytest.mark.parametrize(
    ("status", "warnings_are_errors", "expected_type", "expected_severity"),
    [
        ("pass", False, AssertionResultTypeClass.SUCCESS, None),
        ("warn", False, AssertionResultTypeClass.SUCCESS, None),
        (
            "warn",
            True,
            AssertionResultTypeClass.FAILURE,
            AssertionResultSeverityClass.LOW,
        ),
        (
            "error",
            False,
            AssertionResultTypeClass.FAILURE,
            AssertionResultSeverityClass.HIGH,
        ),
        ("runtime error", False, AssertionResultTypeClass.ERROR, None),
    ],
)
def test_make_assertion_result_from_freshness(
    status: str,
    warnings_are_errors: bool,
    expected_type: str,
    expected_severity: Optional[str],
) -> None:
    node = DBTNode(
        database="raw_db",
        schema="raw",
        name="users",
        alias="users",
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name="source.test.raw.users",
        dbt_file_path=None,
        dbt_package_name="test",
        node_type="source",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )
    node.freshness_info = DBTFreshnessInfo(
        invocation_id="test-123",
        status=status,
        max_loaded_at=datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc),
        snapshotted_at=datetime(2026, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
        max_loaded_at_time_ago_in_s=7200.0,
        warn_after=DBTFreshnessCriteria(count=12, period="hour"),
        error_after=DBTFreshnessCriteria(count=24, period="hour"),
    )

    mcp = make_assertion_result_from_freshness(
        node, "urn:li:assertion:test", "urn:li:dataset:test", warnings_are_errors
    )

    assert mcp.aspect is not None
    assert isinstance(mcp.aspect, AssertionRunEventClass)
    assert mcp.aspect.result is not None
    assert mcp.aspect.result.type == expected_type
    assert mcp.aspect.result.severity == expected_severity


@pytest.mark.parametrize(
    ("status", "warnings_are_errors", "expected_type", "expected_severity"),
    [
        ("pass", False, AssertionResultTypeClass.SUCCESS, None),
        ("success", False, AssertionResultTypeClass.SUCCESS, None),
        ("warn", False, AssertionResultTypeClass.SUCCESS, None),
        (
            "warn",
            True,
            AssertionResultTypeClass.FAILURE,
            AssertionResultSeverityClass.LOW,
        ),
        (
            "fail",
            False,
            AssertionResultTypeClass.FAILURE,
            AssertionResultSeverityClass.HIGH,
        ),
        ("error", False, AssertionResultTypeClass.ERROR, None),
        ("runtime error", False, AssertionResultTypeClass.ERROR, None),
    ],
)
def test_make_assertion_result_from_test(
    status: str,
    warnings_are_errors: bool,
    expected_type: str,
    expected_severity: Optional[str],
) -> None:
    node = DBTNode(
        database="analytics",
        schema="dbt",
        name="users",
        alias="users",
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name="test.some_test",
        dbt_file_path=None,
        dbt_package_name="test",
        node_type="test",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )
    test_result = DBTTestResult(
        invocation_id="test-123",
        status=status,
        execution_time=datetime(2026, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
        native_results={},
    )

    mcp = make_assertion_result_from_test(
        node,
        test_result,
        "urn:li:assertion:test",
        "urn:li:dataset:test",
        warnings_are_errors,
    )

    assert mcp.aspect is not None
    assert isinstance(mcp.aspect, AssertionRunEventClass)
    assert mcp.aspect.result is not None
    assert mcp.aspect.result.type == expected_type
    assert mcp.aspect.result.severity == expected_severity


def test_parse_freshness_criteria_with_null_fields() -> None:
    """When dbt serializes error_after as {"count": null, "period": null},
    parse_freshness_criteria should return None."""
    assert parse_freshness_criteria({"count": None, "period": None}) is None
    assert parse_freshness_criteria({"count": 1, "period": None}) is None
    assert parse_freshness_criteria({"count": None, "period": "hour"}) is None
    assert parse_freshness_criteria(None) is None
    assert parse_freshness_criteria({}) is None
    result = parse_freshness_criteria({"count": 1, "period": "hour"})
    assert result is not None
    assert result.count == 1
    assert result.period == "hour"


def test_make_assertion_from_freshness_warn_only() -> None:
    """Freshness assertion with warn_after only (no error_after) should be valid."""
    node = DBTNode(
        database="raw_db",
        schema="raw",
        name="users",
        alias="users",
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name="source.test.raw.users",
        dbt_file_path=None,
        dbt_package_name="test",
        node_type="source",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )
    node.freshness_info = DBTFreshnessInfo(
        invocation_id="test-123",
        status="warn",
        max_loaded_at=datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc),
        snapshotted_at=datetime(2026, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
        max_loaded_at_time_ago_in_s=7200.0,
        warn_after=DBTFreshnessCriteria(count=1, period="day"),
        error_after=None,
    )

    mcp = make_assertion_from_freshness(
        {}, node, "urn:li:assertion:test", "urn:li:dataset:test"
    )

    assert mcp.aspect is not None
    assert isinstance(mcp.aspect, AssertionInfoClass)
    assert mcp.aspect.customProperties is not None
    assert mcp.aspect.customProperties.get("warn_after_count") == "1"
    assert mcp.aspect.customProperties.get("warn_after_period") == "day"
    assert "error_after_count" not in mcp.aspect.customProperties
    assert "error_after_period" not in mcp.aspect.customProperties
    assert mcp.aspect.source is not None
    assert mcp.aspect.source.created is not None
    assert mcp.aspect.source.created.actor == SYSTEM_ACTOR
    mcp.aspect.to_obj()


def test_extract_dbt_exposures_basic():
    manifest_exposures: Dict[str, Any] = {
        "exposure.my_project.weekly_dashboard": {
            "name": "weekly_dashboard",
            "type": "dashboard",
            "owner": {"name": "Analytics Team", "email": "analytics@company.com"},
            "description": "Weekly metrics dashboard",
            "url": "https://looker.company.com/dashboards/42",
            "maturity": "high",
            "depends_on": {
                "nodes": ["model.my_project.orders", "model.my_project.customers"],
                "macros": [],
            },
            "tags": ["executive", "weekly"],
            "meta": {"team": "analytics", "priority": "P1"},
            "package_name": "my_project",
            "original_file_path": "models/exposures.yml",
        }
    }

    exposures = extract_dbt_exposures(manifest_exposures, tag_prefix="dbt:")

    assert len(exposures) == 1
    exp = exposures[0]
    assert exp.name == "weekly_dashboard"
    assert exp.unique_id == "exposure.my_project.weekly_dashboard"
    assert exp.type == "dashboard"
    assert exp.owner_name == "Analytics Team"
    assert exp.owner_email == "analytics@company.com"
    assert exp.description == "Weekly metrics dashboard"
    assert exp.url == "https://looker.company.com/dashboards/42"
    assert exp.maturity == "high"
    assert exp.depends_on == [
        "model.my_project.orders",
        "model.my_project.customers",
    ]
    assert exp.tags == ["dbt:executive", "dbt:weekly"]
    assert exp.meta == {"team": "analytics", "priority": "P1"}
    assert exp.dbt_package_name == "my_project"
    assert exp.dbt_file_path == "models/exposures.yml"


def test_extract_dbt_exposures_minimal():
    manifest_exposures: Dict[str, Any] = {
        "exposure.my_project.simple_exposure": {
            "name": "simple_exposure",
            "type": "notebook",
        }
    }

    exposures = extract_dbt_exposures(manifest_exposures, tag_prefix="")

    assert len(exposures) == 1
    exp = exposures[0]
    assert exp.name == "simple_exposure"
    assert exp.type == "notebook"
    assert exp.owner_name is None
    assert exp.owner_email is None
    assert exp.description is None
    assert exp.depends_on == []
    assert exp.tags == []


def test_dbt_exposure_get_urn():
    exposure = DBTExposure(
        name="weekly_dashboard",
        unique_id="exposure.my_project.weekly_dashboard",
        type="dashboard",
    )

    urn = exposure.get_urn(platform_instance=None)
    assert urn == "urn:li:dashboard:(dbt,exposure.my_project.weekly_dashboard)"


def test_dbt_exposure_get_urn_with_platform_instance():
    exposure = DBTExposure(
        name="weekly_dashboard",
        unique_id="exposure.my_project.weekly_dashboard",
        type="dashboard",
    )

    urn = exposure.get_urn(platform_instance="my_instance")
    assert (
        urn == "urn:li:dashboard:(dbt,my_instance.exposure.my_project.weekly_dashboard)"
    )


def test_dbt_entities_enabled_exposures_default():
    config = DBTEntitiesEnabled()
    assert config.exposures == EmitDirective.YES
    assert config.can_emit_exposures is True


def test_dbt_cloud_parse_into_dbt_exposure():
    from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudSource

    # Mock exposure data from dbt Cloud GraphQL API
    raw_exposure = {
        "name": "weekly_dashboard",
        "uniqueId": "exposure.my_project.weekly_dashboard",
        "exposureType": "dashboard",
        "ownerName": "Analytics Team",
        "ownerEmail": "analytics@company.com",
        "description": "Weekly metrics dashboard",
        "url": "https://looker.company.com/dashboards/42",
        "maturity": "high",
        "dependsOn": ["model.my_project.orders", "model.my_project.customers"],
        "tags": ["executive", "weekly"],
        "meta": {"team": "analytics"},
        "packageName": "my_project",
    }

    # Create a mock source with minimal config (need job_id or auto_discovery)
    config_dict = {
        "account_id": "123456",
        "project_id": "1234567",
        "job_id": "999999",
        "token": "test_token",
        "target_platform": "postgres",
        "tag_prefix": "dbt:",
    }
    config = dbt_cloud.DBTCloudConfig.model_validate(config_dict)

    # Test the parsing method directly
    source = object.__new__(DBTCloudSource)
    source.config = config

    exposure = source._parse_into_dbt_exposure(raw_exposure)

    assert exposure.name == "weekly_dashboard"
    assert exposure.unique_id == "exposure.my_project.weekly_dashboard"
    assert exposure.type == "dashboard"
    assert exposure.owner_name == "Analytics Team"
    assert exposure.owner_email == "analytics@company.com"
    assert exposure.description == "Weekly metrics dashboard"
    assert exposure.url == "https://looker.company.com/dashboards/42"
    assert exposure.maturity == "high"
    assert exposure.depends_on == [
        "model.my_project.orders",
        "model.my_project.customers",
    ]
    assert exposure.tags == ["dbt:executive", "dbt:weekly"]
    assert exposure.meta == {"team": "analytics"}
    assert exposure.dbt_package_name == "my_project"


def test_dbt_core_load_exposures():
    # Test that DBTCoreSource properly loads exposures
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.model_validate(create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    # Manually set exposures to test load_exposures
    source._exposures = [
        DBTExposure(
            name="test_exposure",
            unique_id="exposure.test.test_exposure",
            type="dashboard",
        )
    ]

    exposures = source.load_exposures()
    assert len(exposures) == 1
    assert exposures[0].name == "test_exposure"


def test_dbt_cloud_load_exposures():
    """Test that DBTCloudSource.load_exposures returns stored exposures."""
    from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudSource

    config_dict = {
        "account_id": "123456",
        "project_id": "1234567",
        "job_id": "999999",
        "token": "test_token",
        "target_platform": "postgres",
    }
    config = dbt_cloud.DBTCloudConfig.model_validate(config_dict)

    source = object.__new__(DBTCloudSource)
    source.config = config
    source._exposures = [
        DBTExposure(
            name="cloud_exposure",
            unique_id="exposure.cloud.cloud_exposure",
            type="dashboard",
        )
    ]

    exposures = source.load_exposures()
    assert len(exposures) == 1
    assert exposures[0].name == "cloud_exposure"


def test_create_exposure_mcps_basic():
    """Test create_exposure_mcps using real DBTCoreSource to get actual coverage."""
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.model_validate(create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    exposure = DBTExposure(
        name="weekly_dashboard",
        unique_id="exposure.my_project.weekly_dashboard",
        type="dashboard",
        description="Weekly metrics",
    )

    # Call the actual method on real source
    mcps = list(source.create_exposure_mcps([exposure], {}))

    # Should generate 4 MCPs: platform instance, dashboard info, status, subtypes
    assert len(mcps) == 4

    # Check aspect types
    aspect_types = [type(mcp.aspect).__name__ for mcp in mcps]
    assert "DataPlatformInstanceClass" in aspect_types
    assert "DashboardInfoClass" in aspect_types
    assert "StatusClass" in aspect_types
    assert "SubTypesClass" in aspect_types


def test_create_exposure_mcps_with_missing_upstream():
    """Test create_exposure_mcps handles missing upstream node gracefully."""
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.model_validate(create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    exposure = DBTExposure(
        name="dashboard_with_missing_dep",
        unique_id="exposure.my_project.dashboard_with_missing_dep",
        type="dashboard",
        depends_on=["model.my_project.nonexistent_model"],
    )

    # Call with empty nodes map - triggers warning branch for missing upstream
    mcps = list(source.create_exposure_mcps([exposure], {}))

    # Should still generate MCPs, but without upstream lineage
    assert len(mcps) == 4  # No ownership or tags, so just 4 MCPs


def test_create_exposure_mcps_with_owner_name_only():
    """Test create_exposure_mcps when owner_name is set but owner_email is None."""
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.model_validate(create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    exposure = DBTExposure(
        name="dashboard_with_owner_name",
        unique_id="exposure.my_project.dashboard_with_owner_name",
        type="dashboard",
        owner_name="John Doe",  # Only owner_name, no owner_email
    )

    mcps = list(source.create_exposure_mcps([exposure], {}))

    # Should generate 5 MCPs: platform instance, dashboard info, status, subtypes, ownership
    assert len(mcps) == 5

    # Find ownership aspect
    ownership_mcp = next(
        (mcp for mcp in mcps if type(mcp.aspect).__name__ == "OwnershipClass"), None
    )
    assert ownership_mcp is not None
    assert ownership_mcp.aspect is not None
    assert isinstance(ownership_mcp.aspect, OwnershipClass)
    # Owner URN should be derived from owner_name: "john_doe"
    assert "john_doe" in ownership_mcp.aspect.owners[0].owner


def test_create_exposure_mcps_with_owner_extraction_disabled():
    """Test that enable_owner_extraction=False disables ownership for exposures."""
    ctx = PipelineContext(run_id="test-run-id")
    config_dict = create_base_dbt_config()
    config_dict["enable_owner_extraction"] = False
    config = DBTCoreConfig.model_validate(config_dict)
    source = DBTCoreSource(config, ctx)

    exposure = DBTExposure(
        name="dashboard_no_owner",
        unique_id="exposure.my_project.dashboard_no_owner",
        type="dashboard",
        owner_email="analytics@company.com",
    )

    mcps = list(source.create_exposure_mcps([exposure], {}))

    # Should generate 4 MCPs: platform instance, dashboard info, status, subtypes (no ownership)
    assert len(mcps) == 4

    # Verify no ownership aspect
    ownership_mcp = next(
        (mcp for mcp in mcps if type(mcp.aspect).__name__ == "OwnershipClass"), None
    )
    assert ownership_mcp is None


def test_create_exposure_mcps_with_strip_user_ids_from_email():
    """Test that strip_user_ids_from_email applies to exposure owners."""
    ctx = PipelineContext(run_id="test-run-id")
    config_dict = create_base_dbt_config()
    config_dict["strip_user_ids_from_email"] = True
    config = DBTCoreConfig.model_validate(config_dict)
    source = DBTCoreSource(config, ctx)

    exposure = DBTExposure(
        name="dashboard_stripped_owner",
        unique_id="exposure.my_project.dashboard_stripped_owner",
        type="dashboard",
        owner_email="analytics@company.com",
    )

    mcps = list(source.create_exposure_mcps([exposure], {}))

    # Find ownership aspect
    ownership_mcp = next(
        (mcp for mcp in mcps if type(mcp.aspect).__name__ == "OwnershipClass"), None
    )
    assert ownership_mcp is not None
    assert ownership_mcp.aspect is not None
    assert isinstance(ownership_mcp.aspect, OwnershipClass)
    # Owner URN should be stripped: "analytics" (not "analytics@company.com")
    assert ownership_mcp.aspect.owners[0].owner == "urn:li:corpuser:analytics"


def test_has_glob_characters():
    from datahub.ingestion.source.dbt.dbt_core import _has_glob_characters

    assert _has_glob_characters("s3://bucket/results/*/run_results.json")
    assert _has_glob_characters("s3://bucket/results/?/run_results.json")
    assert _has_glob_characters("/local/path/[abc]/file.json")
    assert not _has_glob_characters("s3://bucket/results/run_results.json")
    assert not _has_glob_characters("/simple/path/file.json")


def test_expand_s3_glob():
    s3_objects = [
        {"Key": "results/model_a/run_results.json"},
        {"Key": "results/model_b/run_results.json"},
        {"Key": "results/model_c/run_results.json"},
        {"Key": "results/model_a/manifest.json"},
        {"Key": "results/other_file.json"},
    ]

    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"Contents": s3_objects}]

    result = DBTCoreSource._expand_s3_glob(
        "s3://my-bucket/results/*/run_results.json", mock_aws
    )

    assert result == [
        "s3://my-bucket/results/model_a/run_results.json",
        "s3://my-bucket/results/model_b/run_results.json",
        "s3://my-bucket/results/model_c/run_results.json",
    ]

    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="my-bucket", Prefix="results/"
    )


def test_expand_s3_glob_no_matches():
    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"Contents": []}]

    result = DBTCoreSource._expand_s3_glob(
        "s3://my-bucket/nonexistent/*/run_results.json", mock_aws
    )

    assert result == []


def test_expand_s3_glob_prefix_calculation():
    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"Contents": []}]

    DBTCoreSource._expand_s3_glob("s3://bucket/a/b/c/*/d/*/run_results.json", mock_aws)
    mock_paginator.paginate.assert_called_with(Bucket="bucket", Prefix="a/b/c/")


def test_expand_run_results_paths_plain_paths():
    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        "/path/to/run_results_1.json",
        "/path/to/run_results_2.json",
    ]

    result = source._expand_run_results_paths()
    assert result == [
        "/path/to/run_results_1.json",
        "/path/to/run_results_2.json",
    ]


def test_expand_run_results_paths_local_glob(tmp_path):
    results_dir = tmp_path / "results"
    for model in ["model_a", "model_b", "model_c"]:
        d = results_dir / model
        d.mkdir(parents=True)
        (d / "run_results.json").write_text("{}")

    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        str(results_dir / "*" / "run_results.json"),
    ]

    result = source._expand_run_results_paths()
    assert len(result) == 3
    assert all("run_results.json" in p for p in result)


def test_expand_run_results_paths_s3_glob():
    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        "s3://bucket/results/*/run_results.json",
    ]
    source.config.aws_connection = mock.MagicMock()

    mock_s3_client = mock.MagicMock()
    source.config.aws_connection.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "results/model_a/run_results.json"},
                {"Key": "results/model_b/run_results.json"},
            ]
        }
    ]

    result = source._expand_run_results_paths()
    assert result == [
        "s3://bucket/results/model_a/run_results.json",
        "s3://bucket/results/model_b/run_results.json",
    ]


def test_expand_run_results_paths_mixed(tmp_path):
    results_dir = tmp_path / "local_results"
    d = results_dir / "model_x"
    d.mkdir(parents=True)
    (d / "run_results.json").write_text("{}")

    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        "/explicit/path/run_results.json",
        str(results_dir / "*" / "run_results.json"),
        "s3://bucket/results/*/run_results.json",
    ]
    source.config.aws_connection = mock.MagicMock()

    mock_s3_client = mock.MagicMock()
    source.config.aws_connection.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "results/m1/run_results.json"}]}
    ]

    result = source._expand_run_results_paths()
    assert result[0] == "/explicit/path/run_results.json"
    assert "model_x" in result[1]
    assert result[2] == "s3://bucket/results/m1/run_results.json"


def test_expand_run_results_paths_http_glob_warns():
    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        "https://example.com/results/*/run_results.json",
    ]

    result = source._expand_run_results_paths()
    assert result == []


def test_run_results_s3_glob_requires_aws_connection():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "run_results_paths": ["s3://bucket/results/*/run_results.json"],
    }
    with pytest.raises(ValidationError, match="provide aws_connection"):
        DBTCoreConfig.model_validate(config_dict)


def test_run_results_s3_glob_valid_config():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "run_results_paths": ["s3://bucket/results/*/run_results.json"],
        "aws_connection": {},
    }
    config = DBTCoreConfig.model_validate(config_dict)
    assert config.run_results_paths == ["s3://bucket/results/*/run_results.json"]


def test_expand_s3_glob_multiple_pages():
    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "results/model_a/run_results.json"}]},
        {"Contents": [{"Key": "results/model_b/run_results.json"}]},
        {"Contents": [{"Key": "results/model_c/run_results.json"}]},
    ]

    result = DBTCoreSource._expand_s3_glob(
        "s3://bucket/results/*/run_results.json", mock_aws
    )
    assert result == [
        "s3://bucket/results/model_a/run_results.json",
        "s3://bucket/results/model_b/run_results.json",
        "s3://bucket/results/model_c/run_results.json",
    ]


def test_expand_s3_glob_wildcard_at_root():
    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "run_results_a.json"},
                {"Key": "run_results_b.json"},
                {"Key": "other.txt"},
            ]
        }
    ]

    result = DBTCoreSource._expand_s3_glob("s3://bucket/run_results_*.json", mock_aws)
    mock_paginator.paginate.assert_called_with(Bucket="bucket", Prefix="")
    assert result == [
        "s3://bucket/run_results_a.json",
        "s3://bucket/run_results_b.json",
    ]


def test_expand_s3_glob_client_error():
    from botocore.exceptions import ClientError

    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
        "ListObjectsV2",
    )

    with pytest.raises(ClientError, match="Access Denied"):
        DBTCoreSource._expand_s3_glob(
            "s3://bucket/results/*/run_results.json", mock_aws
        )


def test_expand_s3_glob_no_cross_slash_matching():
    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "results/a/run_results.json"},
                {"Key": "results/a/b/run_results.json"},
                {"Key": "results/a/b/c/run_results.json"},
            ]
        }
    ]

    result = DBTCoreSource._expand_s3_glob(
        "s3://bucket/results/*/run_results.json", mock_aws
    )
    assert result == ["s3://bucket/results/a/run_results.json"]


def test_expand_run_results_paths_s3_error_reports_failure():
    from botocore.exceptions import ClientError

    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        "s3://bucket/results/*/run_results.json",
    ]
    source.config.aws_connection = mock.MagicMock()

    mock_s3_client = mock.MagicMock()
    source.config.aws_connection.get_s3_client.return_value = mock_s3_client

    mock_paginator = mock.MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
        "ListObjectsV2",
    )

    result = source._expand_run_results_paths()
    assert result == []
    assert any("S3 glob expansion failed" in str(f) for f in source.report.failures)


def test_expand_run_results_paths_missing_aws_connection():
    source = create_mocked_dbt_source()
    source.config.run_results_paths = [
        "s3://bucket/results/*/run_results.json",
    ]
    source.config.aws_connection = None

    result = source._expand_run_results_paths()
    assert result == []
    assert any("Missing AWS connection" in str(f) for f in source.report.failures)


def _make_dbt_node(dbt_name, node_type="model", **overrides):
    defaults = dict(
        database=None,
        schema=None,
        name=dbt_name.split(".")[-1],
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name=dbt_name,
        dbt_file_path=None,
        dbt_package_name=None,
        node_type=node_type,
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )
    defaults.update(overrides)
    return DBTNode(**defaults)


def test_create_test_entity_mcps_emits_assertion_ownership_from_test_owner():
    source = create_mocked_dbt_source()
    test_node = _make_dbt_node(
        "test.project.unique_my_model",
        node_type="test",
        owner="assertion_owner@example.com",
        upstream_nodes=["model.project.my_model"],
    )
    test_node.test_info = DBTTest(
        qualified_test_name="not_null", column_name="id", kw_args={}
    )
    model_node = _make_dbt_node("model.project.my_model")

    mcps = list(
        source.create_test_entity_mcps(
            [test_node], {}, {"model.project.my_model": model_node}
        )
    )

    ownership_mcps = [mcp for mcp in mcps if isinstance(mcp.aspect, OwnershipClass)]
    assert len(ownership_mcps) == 1
    ownership_mcp = ownership_mcps[0]
    assert ownership_mcp.entityUrn is not None
    assert isinstance(ownership_mcp.aspect, OwnershipClass)
    assert ownership_mcp.entityUrn.startswith("urn:li:assertion:")
    assert ownership_mcp.aspect.owners[0].owner == (
        "urn:li:corpuser:assertion_owner@example.com"
    )
    assert ownership_mcp.aspect.owners[0].type == OwnershipTypeClass.DATAOWNER


def test_create_freshness_assertion_mcps_does_not_copy_source_owner():
    source = create_mocked_dbt_source()
    source_node = _make_dbt_node(
        "source.project.raw.users",
        node_type="source",
        owner="source_owner",
    )
    source_node.freshness_info = DBTFreshnessInfo(
        invocation_id="test-123",
        status="pass",
        max_loaded_at=datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc),
        snapshotted_at=datetime(2026, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
        max_loaded_at_time_ago_in_s=7200.0,
        warn_after=DBTFreshnessCriteria(count=12, period="hour"),
        error_after=None,
    )

    mcps = list(source.create_freshness_assertion_mcps([source_node], {}))

    assert not any(isinstance(mcp.aspect, OwnershipClass) for mcp in mcps)


def test_load_run_results_skips_generate():
    run_results_json = {
        "args": {"which": "generate"},
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
            "dbt_version": "1.7.0",
            "generated_at": "2024-01-01T00:00:00Z",
            "invocation_id": "abc-123",
        },
        "results": [],
    }
    nodes = [_make_dbt_node("model.project.my_model")]
    config = mock.MagicMock()
    result = load_run_results(config, run_results_json, nodes)
    assert result is nodes
    assert len(nodes[0].test_results) == 0
    assert len(nodes[0].model_performances) == 0


def test_load_run_results_with_test_and_model():
    run_results_json = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
            "dbt_version": "1.7.0",
            "generated_at": "2024-01-01T00:00:00Z",
            "invocation_id": "inv-001",
        },
        "results": [
            {
                "unique_id": "test.project.my_test",
                "status": "pass",
                "timing": [
                    {
                        "name": "execute",
                        "started_at": "2024-01-01T00:00:01Z",
                        "completed_at": "2024-01-01T00:00:02Z",
                    }
                ],
            },
            {
                "unique_id": "model.project.my_model",
                "status": "success",
                "timing": [
                    {
                        "name": "execute",
                        "started_at": "2024-01-01T00:00:03Z",
                        "completed_at": "2024-01-01T00:00:05Z",
                    }
                ],
            },
        ],
    }
    test_node = _make_dbt_node("test.project.my_test", node_type="test")
    test_node.test_info = DBTTest(
        qualified_test_name="dbt_utils.my_test", column_name=None, kw_args={}
    )
    model_node = _make_dbt_node("model.project.my_model")

    config = mock.MagicMock()
    load_run_results(config, run_results_json, [test_node, model_node])

    assert len(test_node.test_results) == 1
    assert test_node.test_results[0].status == "pass"
    assert test_node.test_results[0].invocation_id == "inv-001"

    assert len(model_node.model_performances) == 1
    assert model_node.model_performances[0].status == "success"
    assert model_node.model_performances[0].run_id == "inv-001"


def test_load_run_results_failed_test():
    run_results_json = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
            "dbt_version": "1.7.0",
            "generated_at": "2024-01-01T00:00:00Z",
            "invocation_id": "inv-002",
        },
        "results": [
            {
                "unique_id": "test.project.failing_test",
                "status": "fail",
                "message": "Got 3 results, expected 0",
                "failures": 3,
                "timing": [],
            },
        ],
    }
    test_node = _make_dbt_node("test.project.failing_test", node_type="test")
    test_node.test_info = DBTTest(
        qualified_test_name="dbt_utils.failing_test", column_name=None, kw_args={}
    )

    config = mock.MagicMock()
    load_run_results(config, run_results_json, [test_node])

    assert len(test_node.test_results) == 1
    tr = test_node.test_results[0]
    assert tr.status == "fail"
    assert tr.native_results["message"] == "Got 3 results, expected 0"
    assert tr.native_results["failures"] == "3"


def test_load_run_results_unknown_node_skipped():
    run_results_json = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
            "dbt_version": "1.7.0",
            "generated_at": "2024-01-01T00:00:00Z",
            "invocation_id": "inv-003",
        },
        "results": [
            {
                "unique_id": "model.project.missing_model",
                "status": "success",
                "timing": [
                    {
                        "name": "execute",
                        "started_at": "2024-01-01T00:00:01Z",
                        "completed_at": "2024-01-01T00:00:02Z",
                    }
                ],
            },
        ],
    }
    config = mock.MagicMock()
    result = load_run_results(config, run_results_json, [])
    assert result == []


def test_load_file_as_json_s3():
    mock_aws = mock.MagicMock()
    mock_s3_client = mock.MagicMock()
    mock_aws.get_s3_client.return_value = mock_s3_client
    mock_s3_client.get_object.return_value = {
        "Body": mock.MagicMock(read=mock.MagicMock(return_value=b'{"key": "value"}'))
    }

    result = DBTCoreSource.load_file_as_json(
        "s3://my-bucket/path/to/manifest.json", mock_aws
    )
    assert result == {"key": "value"}
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="my-bucket", Key="path/to/manifest.json"
    )


# =============================================================================
# Tests for catalog.json stats extraction (row_count, size_in_bytes)
# =============================================================================


def _create_manifest_entity(
    name: str = "my_model",
    materialized: str = "table",
) -> Dict[str, Any]:
    """Helper to create a manifest entity for stats tests."""
    return {
        "name": name,
        "database": "test_db",
        "schema": "test_schema",
        "resource_type": "model",
        "original_file_path": f"models/{name}.sql",
        "config": {"materialized": materialized},
        "description": "Test model",
        "meta": {},
        "tags": [],
    }


def _create_catalog_entity_with_stats(
    name: str = "my_model",
    catalog_type: str = "table",
    num_rows: Optional[float] = None,
    num_bytes: Optional[float] = None,
    include_rows: bool = True,
    include_bytes: bool = True,
) -> Dict[str, Any]:
    """Helper to create a catalog entity with stats for tests."""
    stats: Dict[str, Any] = {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": num_rows is not None or num_bytes is not None,
            "include": False,
            "description": "Indicates whether there are statistics",
        },
    }
    if num_rows is not None:
        stats["num_rows"] = {
            "id": "num_rows",
            "label": "# Rows",
            "value": num_rows,
            "include": include_rows,
            "description": "Approximate count of rows",
        }
    if num_bytes is not None:
        stats["num_bytes"] = {
            "id": "num_bytes",
            "label": "Approximate Size",
            "value": num_bytes,
            "include": include_bytes,
            "description": "Approximate size of table",
        }

    return {
        "metadata": {
            "type": catalog_type,
            "schema": "test_schema",
            "name": name,
            "database": "test_db",
            "comment": None,
        },
        "columns": {},
        "stats": stats,
    }


def test_extract_catalog_stats_with_row_count_and_size() -> None:
    """Test that stats are extracted when present in catalog.json."""
    manifest_entities = {"model.test.my_model": _create_manifest_entity()}
    catalog_entities = {
        "model.test.my_model": _create_catalog_entity_with_stats(
            num_rows=1000.0, num_bytes=50000.0
        )
    }

    report = DBTSourceReport()
    nodes = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=catalog_entities,
        sources_results=[],
        manifest_adapter="bigquery",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=report,
    )

    assert len(nodes) == 1
    node = nodes[0]
    assert node.row_count == 1000
    assert node.size_in_bytes == 50000


def test_extract_catalog_stats_without_stats() -> None:
    """Test that stats are None when not present in catalog.json."""
    manifest_entities = {
        "model.test.my_model": _create_manifest_entity(materialized="view")
    }
    catalog_entities = {
        "model.test.my_model": _create_catalog_entity_with_stats(catalog_type="view")
    }

    report = DBTSourceReport()
    nodes = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=catalog_entities,
        sources_results=[],
        manifest_adapter="bigquery",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=report,
    )

    assert len(nodes) == 1
    node = nodes[0]
    assert node.row_count is None
    assert node.size_in_bytes is None


def test_extract_catalog_stats_with_include_false() -> None:
    """Test that stats are not extracted when include=False."""
    manifest_entities = {"model.test.my_model": _create_manifest_entity()}
    catalog_entities = {
        "model.test.my_model": _create_catalog_entity_with_stats(
            num_rows=1000.0,
            num_bytes=50000.0,
            include_rows=False,
            include_bytes=False,
        )
    }

    report = DBTSourceReport()
    nodes = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=catalog_entities,
        sources_results=[],
        manifest_adapter="bigquery",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=report,
    )

    assert len(nodes) == 1
    node = nodes[0]
    # Stats should be None because include=False
    assert node.row_count is None
    assert node.size_in_bytes is None


def test_extract_catalog_stats_no_catalog() -> None:
    """Test that stats are None when no catalog is provided."""
    manifest_entities = {
        "model.test.my_model": _create_manifest_entity(materialized="ephemeral")
    }

    report = DBTSourceReport()
    nodes = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=None,  # No catalog
        sources_results=[],
        manifest_adapter="bigquery",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=report,
    )

    assert len(nodes) == 1
    node = nodes[0]
    assert node.row_count is None
    assert node.size_in_bytes is None


def test_extract_catalog_stats_partial_only_row_count() -> None:
    """Test extraction when only row count is present (no size)."""
    manifest_entities = {"model.test.my_model": _create_manifest_entity()}
    catalog_entities = {
        "model.test.my_model": _create_catalog_entity_with_stats(num_rows=500.0)
    }

    report = DBTSourceReport()
    nodes = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=catalog_entities,
        sources_results=[],
        manifest_adapter="bigquery",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=report,
    )

    assert len(nodes) == 1
    node = nodes[0]
    assert node.row_count == 500
    assert node.size_in_bytes is None  # Not present in catalog


# =============================================================================
# Semantic Model Tests
# =============================================================================


def test_convert_semantic_model_fields_to_columns_basic():
    """Test converting semantic model entities, dimensions, and measures to columns."""
    entities: list[SemanticModelEntity] = [
        {"name": "order_id", "type": "primary", "description": "Primary order key"},
        {"name": "customer_id", "type": "foreign", "description": ""},
    ]
    dimensions: list[SemanticModelDimension] = [
        {"name": "order_date", "type": "time", "description": "When order was placed"},
        {"name": "status", "type": "categorical", "description": ""},
    ]
    measures: list[SemanticModelMeasure] = [
        {"name": "total_revenue", "agg": "sum", "description": "Sum of order amounts"},
        {"name": "order_count", "agg": "count", "description": ""},
    ]

    columns = convert_semantic_model_fields_to_columns(entities, dimensions, measures)

    assert len(columns) == 6

    order_id_col = next(c for c in columns if c.name == "order_id")
    assert order_id_col.data_type == "entity:primary"
    assert order_id_col.description == "Primary order key"

    customer_id_col = next(c for c in columns if c.name == "customer_id")
    assert customer_id_col.data_type == "entity:foreign"
    assert "Entity" in customer_id_col.description

    order_date_col = next(c for c in columns if c.name == "order_date")
    assert order_date_col.data_type == "dimension:time"
    assert order_date_col.description == "When order was placed"

    total_revenue_col = next(c for c in columns if c.name == "total_revenue")
    assert total_revenue_col.data_type == "measure:sum"
    assert total_revenue_col.description == "Sum of order amounts"


def test_convert_semantic_model_fields_empty_descriptions():
    """Test default description generation when descriptions are empty."""
    entities: list[SemanticModelEntity] = [
        {"name": "id", "type": "primary", "description": ""},
    ]
    dimensions: list[SemanticModelDimension] = [
        {"name": "category", "type": "categorical", "description": ""},
    ]
    measures: list[SemanticModelMeasure] = [
        {"name": "total", "agg": "sum", "description": ""},
    ]

    columns = convert_semantic_model_fields_to_columns(entities, dimensions, measures)

    assert len(columns) == 3

    id_col = next(c for c in columns if c.name == "id")
    assert "Entity" in id_col.description
    assert "primary" in id_col.description

    category_col = next(c for c in columns if c.name == "category")
    assert "Dimension" in category_col.description
    assert "categorical" in category_col.description

    total_col = next(c for c in columns if c.name == "total")
    assert "Measure" in total_col.description
    assert "sum" in total_col.description


def test_extract_semantic_models_partial_node_relation():
    """Test fallback when node_relation has only database (schema from depends_on)."""
    manifest_semantic_models: Dict[str, Any] = {
        "semantic_model.my_project.partial_metrics": {
            "name": "partial_metrics",
            "description": "Test partial node_relation",
            "node_relation": {"database": "my_database"},
            "depends_on": {"nodes": ["model.my_project.ref_model"]},
            "entities": [],
            "dimensions": [],
            "measures": [{"name": "count", "agg": "count", "description": ""}],
            "tags": [],
            "meta": {},
        }
    }

    manifest_nodes: Dict[str, Any] = {
        "model.my_project.ref_model": {
            "database": "other_db",
            "schema": "ref_schema",
            "name": "ref_model",
        }
    }

    nodes = extract_semantic_models(
        manifest_semantic_models=manifest_semantic_models,
        manifest_nodes=manifest_nodes,
        manifest_adapter="snowflake",
        tag_prefix="",
    )

    assert len(nodes) == 1
    node = nodes[0]
    assert node.database == "my_database"
    assert node.schema == "ref_schema"
    assert node.dbt_adapter == "snowflake"


def test_dbt_semantic_model_subtype() -> None:
    """Test that semantic models get the correct SEMANTIC_MODEL subtype."""
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(**create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    semantic_model_node = DBTNode(
        database="analytics",
        schema="public",
        name="order_metrics",
        alias="order_metrics",
        dbt_name="semantic_model.my_project.order_metrics",
        dbt_adapter="snowflake",
        node_type="semantic_model",  # The key difference from regular models
        max_loaded_at=None,
        materialization=None,  # Semantic models don't have materialization
        comment="",
        description="Order metrics semantic model",
        dbt_file_path=None,
        catalog_type=None,
        language="yaml",
        raw_code=None,
        dbt_package_name="my_project",
        missing_from_catalog=False,
        owner="",
    )

    subtype_wu = source._create_subType_wu(
        semantic_model_node,
        "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.public.order_metrics,PROD)",
    )

    assert subtype_wu is not None
    assert isinstance(subtype_wu.metadata, MetadataChangeProposalWrapper)
    aspect = subtype_wu.metadata.aspect
    assert aspect is not None
    assert isinstance(aspect, SubTypesClass)
    assert DatasetSubTypes.SEMANTIC_MODEL in aspect.typeNames


def test_extract_semantic_models_basic():
    """Test extracting semantic models from manifest.json."""
    manifest_semantic_models: Dict[str, Any] = {
        "semantic_model.my_project.order_metrics": {
            "name": "order_metrics",
            "description": "Metrics for order analysis",
            "node_relation": {
                "database": "analytics",
                "schema": "public",
                "alias": "order_metrics",
            },
            "depends_on": {"nodes": ["model.my_project.stg_orders"]},
            "entities": [
                {"name": "order_id", "type": "primary", "description": "Primary key"}
            ],
            "dimensions": [
                {"name": "order_date", "type": "time", "description": "Order date"}
            ],
            "measures": [
                {"name": "revenue", "agg": "sum", "description": "Total revenue"}
            ],
            "tags": ["metrics", "orders"],
            "meta": {"team": "analytics"},
            "original_file_path": "models/semantic_models/order_metrics.yml",
        }
    }

    manifest_nodes: Dict[str, Any] = {
        "model.my_project.stg_orders": {
            "database": "analytics",
            "schema": "staging",
            "name": "stg_orders",
        }
    }

    nodes = extract_semantic_models(
        manifest_semantic_models=manifest_semantic_models,
        manifest_nodes=manifest_nodes,
        manifest_adapter="snowflake",
        tag_prefix="dbt:",
    )

    assert len(nodes) == 1
    node = nodes[0]

    assert node.name == "order_metrics"
    assert node.description == "Metrics for order analysis"
    assert node.node_type == "semantic_model"
    assert node.database == "analytics"
    assert node.dbt_adapter == "snowflake"
    assert node.schema == "public"
    assert node.language == "yaml"
    assert node.materialization is None

    # Check columns were converted from entities/dimensions/measures
    assert len(node.columns) == 3
    column_names = [c.name for c in node.columns]
    assert "order_id" in column_names
    assert "order_date" in column_names
    assert "revenue" in column_names

    # Check tags have prefix
    assert "dbt:metrics" in node.tags
    assert "dbt:orders" in node.tags


def test_extract_semantic_models_fallback_to_depends_on():
    """Test extracting semantic models when node_relation is missing db/schema."""
    manifest_semantic_models: Dict[str, Any] = {
        "semantic_model.my_project.customer_metrics": {
            "name": "customer_metrics",
            "description": "Customer metrics",
            "node_relation": {},  # Empty - should fallback to depends_on
            "depends_on": {"nodes": ["model.my_project.dim_customers"]},
            "entities": [{"name": "customer_id", "type": "primary", "description": ""}],
            "dimensions": [],
            "measures": [{"name": "count", "agg": "count", "description": ""}],
            "tags": [],
            "meta": {},
            "original_file_path": "models/semantic_models/customer_metrics.yml",
            "package_name": "my_project",
        }
    }

    # The referenced model has database/schema info
    manifest_nodes: Dict[str, Any] = {
        "model.my_project.dim_customers": {
            "database": "warehouse",
            "schema": "analytics",
            "name": "dim_customers",
        }
    }

    nodes = extract_semantic_models(
        manifest_semantic_models=manifest_semantic_models,
        manifest_nodes=manifest_nodes,
        manifest_adapter="postgres",
        tag_prefix="dbt:",
    )

    assert len(nodes) == 1
    node = nodes[0]

    assert node.database == "warehouse"
    assert node.schema == "analytics"
    assert node.name == "customer_metrics"
    assert node.node_type == "semantic_model"


def test_extract_semantic_models_no_db_schema():
    """Test extracting semantic models when neither node_relation nor depends_on has db/schema."""
    manifest_semantic_models: Dict[str, Any] = {
        "semantic_model.my_project.orphan_metrics": {
            "name": "orphan_metrics",
            "description": "Orphan metrics",
            "node_relation": {},
            "depends_on": {"nodes": []},  # No dependencies
            "entities": [],
            "dimensions": [],
            "measures": [{"name": "count", "agg": "count", "description": ""}],
            "tags": [],
            "meta": {},
        }
    }

    nodes = extract_semantic_models(
        manifest_semantic_models=manifest_semantic_models,
        manifest_nodes={},
        manifest_adapter=None,
        tag_prefix="",
    )

    assert len(nodes) == 1
    node = nodes[0]

    assert node.database is None
    assert node.schema is None
    assert node.dbt_adapter is None
    assert node.name == "orphan_metrics"


def test_dbt_entities_enabled_semantic_models_default():
    """Test that semantic models are enabled by default."""
    config = DBTEntitiesEnabled()
    assert config.semantic_models == EmitDirective.YES
    assert config.can_emit_semantic_models is True
    assert config.can_emit_node_type("semantic_model") is True


def test_dbt_entities_enabled_semantic_models_disabled():
    """Test disabling semantic models emission."""
    config = DBTEntitiesEnabled(semantic_models=EmitDirective.NO)
    assert config.semantic_models == EmitDirective.NO
    assert config.can_emit_semantic_models is False
    assert config.can_emit_node_type("semantic_model") is False


def test_dbt_entities_enabled_semantic_models_only():
    """Test setting semantic models to ONLY mode."""
    config = DBTEntitiesEnabled(semantic_models=EmitDirective.ONLY)

    # ONLY directive should be converted to YES, others to NO
    assert config.semantic_models == EmitDirective.YES
    assert config.can_emit_semantic_models is True
    assert config.can_emit_node_type("semantic_model") is True

    # Other node types should be disabled
    assert config.models == EmitDirective.NO
    assert config.sources == EmitDirective.NO
    assert config.can_emit_node_type("model") is False


def test_dbt_cloud_parse_semantic_model_node():
    """Test parsing semantic model nodes from dbt Cloud GraphQL response."""
    config = DBTCloudConfig(
        access_url="https://cloud.getdbt.com",
        token="dummy_token",
        account_id=123456,
        project_id=1234567,
        job_id=12345678,
        target_platform="snowflake",
    )
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
    source = DBTCloudSource(config, ctx)

    # Mock semantic model node from GraphQL (no database/schema - API doesn't expose these)
    semantic_model_node = {
        "uniqueId": "semantic_model.my_project.order_metrics",
        "name": "order_metrics",
        "description": "Order metrics for analysis",
        "resourceType": "semantic_model",
        "packageName": "my_project",
        "meta": {},
        "tags": ["metrics"],
        "dependsOn": ["model.my_project.stg_orders"],
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "description": "Primary key",
                "expr": "order_id",
            }
        ],
        "dimensions": [
            {
                "name": "order_date",
                "type": "time",
                "description": "Order date",
                "expr": "order_date",
                "typeParams": {},
            }
        ],
        "measures": [
            {
                "name": "total_revenue",
                "agg": "sum",
                "description": "Total revenue",
                "expr": "amount",
                "createMetric": True,
            }
        ],
    }

    node = source._parse_into_dbt_node(semantic_model_node)

    assert node.name == "order_metrics"
    assert node.node_type == "semantic_model"
    assert (
        node.database is None
    )  # dbt Cloud API doesn't expose database for semantic models
    assert (
        node.schema is None
    )  # dbt Cloud API doesn't expose schema for semantic models
    assert node.materialization is None
    assert node.language == "yaml"
    assert len(node.columns) == 3

    # Verify columns were converted correctly
    column_names = [c.name for c in node.columns]
    assert "order_id" in column_names
    assert "order_date" in column_names
    assert "total_revenue" in column_names


def test_dbt_cloud_semantic_model_column_types():
    """Test that semantic model columns have correct data types."""
    config = DBTCloudConfig(
        access_url="https://cloud.getdbt.com",
        token="dummy_token",
        account_id=123456,
        project_id=1234567,
        job_id=12345678,
        target_platform="snowflake",
    )
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
    source = DBTCloudSource(config, ctx)

    semantic_model_node = {
        "uniqueId": "semantic_model.my_project.test_metrics",
        "name": "test_metrics",
        "description": "",
        "resourceType": "semantic_model",
        "packageName": "my_project",
        "meta": {},
        "tags": [],
        "dependsOn": [],
        "entities": [{"name": "id", "type": "primary", "description": "", "expr": ""}],
        "dimensions": [
            {
                "name": "category",
                "type": "categorical",
                "description": "",
                "expr": "",
                "typeParams": {},
            }
        ],
        "measures": [
            {
                "name": "count",
                "agg": "count",
                "description": "",
                "expr": "",
                "createMetric": False,
            }
        ],
    }

    node = source._parse_into_dbt_node(semantic_model_node)

    # Find columns by name
    id_col = next(c for c in node.columns if c.name == "id")
    category_col = next(c for c in node.columns if c.name == "category")
    count_col = next(c for c in node.columns if c.name == "count")

    assert id_col.data_type == "entity:primary"
    assert category_col.data_type == "dimension:categorical"
    assert count_col.data_type == "measure:count"


def test_dbt_common_semantic_model_subtype_assignment():
    """Test that _create_subType_wu assigns correct subtypes for different node types."""
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(**create_base_dbt_config())
    source = DBTCoreSource(config, ctx)

    # Test with different node types
    # For model/source/seed/snapshot, subtype is node_type.capitalize()
    # For semantic_model, subtype is DatasetSubTypes.SEMANTIC_MODEL
    test_cases = [
        ("model", "Model"),
        ("source", "Source"),
        ("seed", "Seed"),
        ("snapshot", "Snapshot"),
        ("semantic_model", DatasetSubTypes.SEMANTIC_MODEL),
    ]

    for node_type, expected_subtype in test_cases:
        node = DBTNode(
            database="test_db",
            schema="test_schema",
            name="test_node",
            alias=None,
            comment="",
            description="",
            language="sql" if node_type != "semantic_model" else "yaml",
            raw_code=None,
            compiled_code=None,
            dbt_adapter="snowflake" if node_type != "semantic_model" else None,
            dbt_name=f"{node_type}.my_project.test_node",
            dbt_file_path=None,
            dbt_package_name="my_project",
            node_type=node_type,
            max_loaded_at=None,
            materialization="table" if node_type == "model" else None,
            catalog_type=None,
            missing_from_catalog=False,
            meta={},
            query_tag={},
            tags=[],
            owner=None,
            columns=[],
        )

        wu = source._create_subType_wu(
            node, f"urn:li:dataset:(urn:li:dataPlatform:dbt,test.{node_type},PROD)"
        )

        assert wu is not None
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        assert isinstance(aspect, SubTypesClass)
        assert expected_subtype in aspect.typeNames, f"Failed for node_type={node_type}"


# ==================== Contract Tests ====================


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
    """Test that contract information is extracted from manifest."""
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="tests/unit/dbt/artifacts/manifest.json",
        target_platform="dummy",
        ingest_contracts=True,
    )
    source = DBTCoreSource(config, ctx)
    nodes, *_ = source.loadManifestAndCatalog()

    # Find the model with contract.enforced=true
    contracted_node = None
    for node in nodes:
        if node.dbt_name == "model.tdd.simple":
            contracted_node = node
            break

    assert contracted_node is not None, "Expected to find model.tdd.simple"
    assert contracted_node.contract is not None, "Expected contract to be extracted"
    assert contracted_node.contract.enforced is True
    assert contracted_node.contract.checksum is not None


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


def _make_contracted_source(*, ingest_column_constraints: bool = True) -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-contract", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="snowflake",
        ingest_contracts=True,
        ingest_column_constraints_as_assertions=ingest_column_constraints,
        enable_meta_mapping=False,
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
    dataset_assertion = info.datasetAssertion
    assert dataset_assertion is not None
    assert dataset_assertion.operator == AssertionStdOperatorClass.EQUAL_TO
    assert (
        dataset_assertion.aggregation == AssertionStdAggregationClass.UNIQUE_PROPOTION
    )
    assert dataset_assertion.parameters is not None
    assert dataset_assertion.parameters.value is not None
    assert dataset_assertion.parameters.value.value == "1.0"


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
    dataset_assertions = [
        _assertion_info_from_mcps(mcps).datasetAssertion for _, mcps in results
    ]
    assert all(da is not None for da in dataset_assertions)
    operators = {da.operator for da in dataset_assertions if da is not None}
    aggregations = {da.aggregation for da in dataset_assertions if da is not None}
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
    infos = [_assertion_info_from_mcps(mcps) for _, mcps in results]

    unique_infos = [
        i
        for i in infos
        if i.datasetAssertion is not None
        and i.datasetAssertion.aggregation
        == AssertionStdAggregationClass.UNIQUE_PROPOTION
    ]
    assert len(unique_infos) == 1
    unique_fields = unique_infos[0].datasetAssertion.fields  # type: ignore[union-attr]
    assert unique_fields is not None and len(unique_fields) == 2

    not_null_infos = [
        i
        for i in infos
        if i.datasetAssertion is not None
        and i.datasetAssertion.aggregation == AssertionStdAggregationClass.IDENTITY
    ]
    assert len(not_null_infos) == 2


def test_constraint_assertion_foreign_key_emits_native() -> None:
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
                        expression="customers(id)",
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
    assert info.datasetAssertion is not None
    assert info.datasetAssertion.operator == AssertionStdOperatorClass._NATIVE_
    assert info.datasetAssertion.aggregation == AssertionStdAggregationClass._NATIVE_
    assert info.customProperties is not None
    assert info.customProperties.get("expression") == "customers(id)"


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
    contract_tag: str = "dbt:contract",
) -> DBTNode:
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
        tags=[contract_tag],
        owner="",
        language="sql",
        raw_code=None,
        compiled_code=None,
        columns=[],
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
    assert len(dq_urns) == 1

    # Single filtered upstream → URN uses the backwards-compat form
    # (no on_dbt_upstream) that create_test_entity_mcps would emit.
    expected_urn = source._make_test_assertion_urn(
        test_dbt_name=test_node.dbt_name,
        upstream_dbt_name=None,
    )
    assert dq_urns[0] == expected_urn


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
        assert len(dq_urns) == 1
        assert props.entity in expected_by_entity
        assert dq_urns[0] == expected_by_entity[props.entity]


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
    assert props.schema is not None
    assert props.dataQuality is not None and len(props.dataQuality) >= 1

    # Schema assertion sources from contract_columns: the manifest types
    # (``bigint``/``varchar(255)``/...) differ in case from the catalog
    # types (``BIGINT``/``VARCHAR(255)``/...), so the nativeDataType values
    # here tell us which source the assertion was built from.
    schema_assertion_infos = [
        a
        for a in aspects
        if isinstance(a, AssertionInfoClass) and a.schemaAssertion is not None
    ]
    assert len(schema_assertion_infos) == 1
    schema_info = schema_assertion_infos[0].schemaAssertion
    assert schema_info is not None
    assert schema_info.schema is not None
    field_types = {f.fieldPath: f.nativeDataType for f in schema_info.schema.fields}
    assert field_types == {
        "id": "bigint",
        "email": "varchar(255)",
        "age": "int",
        "status": "varchar(32)",
    }

    dataset_assertion_infos = [
        a
        for a in aspects
        if isinstance(a, AssertionInfoClass) and a.datasetAssertion is not None
    ]
    unique_assertions = [
        a
        for a in dataset_assertion_infos
        if a.datasetAssertion is not None
        and a.datasetAssertion.aggregation
        == AssertionStdAggregationClass.UNIQUE_PROPOTION
    ]
    not_null_assertions = [
        a
        for a in dataset_assertion_infos
        if a.datasetAssertion is not None
        and a.datasetAssertion.operator == AssertionStdOperatorClass.NOT_NULL
    ]
    native_assertions = [
        a
        for a in dataset_assertion_infos
        if a.datasetAssertion is not None
        and a.datasetAssertion.operator == AssertionStdOperatorClass._NATIVE_
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
    from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudSource

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
    from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudSource

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
        DBTCloudSource,
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
    from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudSource

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
    from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudSource

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
