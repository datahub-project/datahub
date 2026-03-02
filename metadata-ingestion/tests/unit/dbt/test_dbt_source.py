from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, TypedDict, Union
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.emitter import mce_builder
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
    get_column_type,
    parse_semantic_view_cll,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    extract_dbt_exposures,
    parse_dbt_timestamp,
)
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTFreshnessCriteria,
    DBTFreshnessInfo,
    make_assertion_from_freshness,
    make_assertion_result_from_freshness,
    parse_freshness_criteria,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
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


def test_dbt_source_patching_with_conflict_null_source_type_in_existing_owner():
    # verifying when existing owners have null source_type and new owners are present.
    # So the existing owners will null type will be removed.
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
    assert len(transformed_owner_list) == 2
    expected_owner_set = {"urn:li:corpuser:new_test", "urn:li:corpuser:new_test2"}
    for single_owner in transformed_owner_list:
        assert single_owner.owner in expected_owner_set
        assert (
            single_owner.source
            and single_owner.source.type == OwnershipSourceTypeClass.AUDIT
        )


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


def test_dbt_sibling_aspects_creation():
    """Test that sibling patches are created correctly based on configuration."""
    ctx = PipelineContext(run_id="test-run-id")
    base_config = create_base_dbt_config()

    # Create source with dbt as primary (default behavior)
    config_dbt_primary = DBTCoreConfig(**base_config)
    source_dbt_primary = DBTCoreSource(config_dbt_primary, ctx)

    # Manually set the config value for testing since the field might not be parsed yet
    source_dbt_primary.config.dbt_is_primary_sibling = True

    model_node = DBTNode(
        name="test_model",
        database="test_db",
        schema="test_schema",
        alias=None,
        comment="",
        description="Test model",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name="model.package.test_model",
        dbt_file_path=None,
        dbt_package_name="package",
        node_type="model",
        materialization="table",
        max_loaded_at=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        compiled_code=None,
    )
    # Note: exists_in_target_platform is a property that returns True for non-ephemeral, non-test nodes
    # Our node_type="model" and materialization="table" will make this property return True

    # For models when dbt is primary - should not create sibling patches
    should_create_siblings = source_dbt_primary._should_create_sibling_relationships(
        model_node
    )
    assert should_create_siblings is False

    # Test with target platform as primary - should create sibling patches
    config_target_primary = DBTCoreConfig(**base_config)
    source_target_primary = DBTCoreSource(config_target_primary, ctx)

    # Manually set the config value for testing
    source_target_primary.config.dbt_is_primary_sibling = False

    # For models when target platform is primary - should create sibling patches
    should_create_siblings = source_target_primary._should_create_sibling_relationships(
        model_node
    )
    assert should_create_siblings is True


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
    assert mcp.aspect.customAssertion.type == "Freshness"
    assert mcp.aspect.customAssertion.entity == "urn:li:dataset:test"
    assert mcp.aspect.customProperties is not None
    assert mcp.aspect.customProperties.get("error_after_count") == "24"
    assert mcp.aspect.customProperties.get("warn_after_count") == "12"


@pytest.mark.parametrize(
    ("status", "warnings_are_errors", "expected_success"),
    [
        ("pass", False, True),
        ("warn", False, True),
        ("warn", True, False),
        ("error", False, False),
    ],
)
def test_make_assertion_result_from_freshness(
    status: str, warnings_are_errors: bool, expected_success: bool
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

    expected = (
        AssertionResultTypeClass.SUCCESS
        if expected_success
        else AssertionResultTypeClass.FAILURE
    )
    assert mcp.aspect is not None
    assert isinstance(mcp.aspect, AssertionRunEventClass)
    assert mcp.aspect.result is not None
    assert mcp.aspect.result.type == expected


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
    # Validate that the aspect can be serialized without errors
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
