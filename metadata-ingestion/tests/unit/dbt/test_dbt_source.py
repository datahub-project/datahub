from datetime import timedelta
from typing import Dict, List, Union
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.emitter import mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt import dbt_cloud
from datahub.ingestion.source.dbt.dbt_cloud import DBTCloudConfig
from datahub.ingestion.source.dbt.dbt_common import (
    DBTNode,
    DBTSourceReport,
    NullTypeClass,
    get_column_type,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    parse_dbt_timestamp,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)
from datahub.testing.doctest import assert_doctest


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
    return DBTCoreSource(DBTCoreConfig(**create_base_dbt_config()), ctx, "dbt")


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
        DBTCoreConfig.parse_obj(config_dict)

    # valid config
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Yes", "seeds": "Only"},
    }
    DBTCoreConfig.parse_obj(config_dict)


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
        config = DBTCoreConfig.parse_obj(config_dict)

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "skip_sources_in_lineage": True,
        "entities_enabled": {"sources": "NO"},
    }
    config = DBTCoreConfig.parse_obj(config_dict)
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
        config = DBTCoreConfig.parse_obj(config_dict)

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "skip_sources_in_lineage": True,
        "prefer_sql_parser_lineage": True,
    }
    config = DBTCoreConfig.parse_obj(config_dict)
    assert config.skip_sources_in_lineage is True
    assert config.prefer_sql_parser_lineage is True


def test_dbt_prefer_sql_parser_lineage_no_self_reference():
    ctx = PipelineContext(run_id="test-run-id")
    config = DBTCoreConfig.parse_obj(
        {
            **create_base_dbt_config(),
            "skip_sources_in_lineage": True,
            "prefer_sql_parser_lineage": True,
        }
    )
    source: DBTCoreSource = DBTCoreSource(config, ctx, "dbt")
    all_nodes_map = {
        "model1": DBTNode(
            name="model1",
            database=None,
            schema=None,
            alias=None,
            comment="",
            description="",
            language=None,
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


def test_dbt_s3_config():
    # test missing aws config
    config_dict: dict = {
        "manifest_path": "s3://dummy_path",
        "catalog_path": "s3://dummy_path",
        "target_platform": "dummy_platform",
    }
    with pytest.raises(ValidationError, match="provide aws_connection"):
        DBTCoreConfig.parse_obj(config_dict)

    # valid config
    config_dict = {
        "manifest_path": "s3://dummy_path",
        "catalog_path": "s3://dummy_path",
        "target_platform": "dummy_platform",
        "aws_connection": {},
    }
    DBTCoreConfig.parse_obj(config_dict)


def test_default_convert_column_urns_to_lowercase():
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Yes", "seeds": "Only"},
    }

    config = DBTCoreConfig.parse_obj({**config_dict})
    assert config.convert_column_urns_to_lowercase is False

    config = DBTCoreConfig.parse_obj({**config_dict, "target_platform": "snowflake"})
    assert config.convert_column_urns_to_lowercase is True

    # Check that we respect the user's setting if provided.
    config = DBTCoreConfig.parse_obj(
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
    config = DBTCoreConfig.parse_obj(config_dict)
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
    config = DBTCoreConfig.parse_obj(config_dict)
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
    config = DBTCoreConfig.parse_obj(config_dict)
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
    config = DBTCoreConfig.parse_obj(config_dict)
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
    config = DBTCloudConfig.parse_obj(config_dict)
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
    config = DBTCloudConfig.parse_obj(config_dict)
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
    config = DBTCoreConfig.parse_obj({**config_dict})
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
    config = DBTCoreConfig.parse_obj({**config_dict})
    assert config.include_database_name is expected


def test_extract_dbt_entities():
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="tests/unit/dbt/artifacts/manifest.json",
        catalog_path="tests/unit/dbt/artifacts/catalog.json",
        target_platform="dummy",
    )
    source = DBTCoreSource(config, ctx, "dbt")
    assert all(node.database is not None for node in source.loadManifestAndCatalog()[0])
    config.include_database_name = False
    source = DBTCoreSource(config, ctx, "dbt")
    assert all(node.database is None for node in source.loadManifestAndCatalog()[0])
