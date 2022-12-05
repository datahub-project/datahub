from typing import Dict, List, Union
from unittest import mock

from pydantic import ValidationError

from datahub.emitter import mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
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
    ctx = PipelineContext("test-run-id")
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
        new_tag_aspect.tags, "urn:li:dataset:dummy", "urn:li:tag:dbt:"
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
    try:
        DBTCoreConfig.parse_obj(config_dict)
    except ValidationError as ve:
        assert len(ve.errors()) == 1
        assert (
            "Cannot have more than 1 type of entity emission set to ONLY"
            in ve.errors()[0]["msg"]
        )
    # valid config
    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {"models": "Yes", "seeds": "Only"},
    }
    DBTCoreConfig.parse_obj(config_dict)


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

    config_dict = {
        "manifest_path": "dummy_path",
        "catalog_path": "dummy_path",
        "target_platform": "dummy_platform",
        "entities_enabled": {
            "test_results": "Yes",
            "test_definitions": "Yes",
            "models": "No",
            "sources": "No",
        },
    }
    config = DBTCoreConfig.parse_obj(config_dict)
    assert not config.entities_enabled.can_emit_node_type("model")
    assert not config.entities_enabled.can_emit_node_type("source")
    assert config.entities_enabled.can_emit_node_type("test")
    assert config.entities_enabled.can_emit_test_results
