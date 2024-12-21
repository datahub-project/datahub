import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, MutableSequence, Optional, Type, Union, cast
from unittest import mock
from uuid import uuid4

import pytest

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
import tests.test_helpers.mce_helpers
from datahub.configuration.common import TransformerSemantics
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api import workunit
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.transformer.add_dataset_browse_path import (
    AddDatasetBrowsePathTransformer,
)
from datahub.ingestion.transformer.add_dataset_dataproduct import (
    AddDatasetDataProduct,
    PatternAddDatasetDataProduct,
    SimpleAddDatasetDataProduct,
)
from datahub.ingestion.transformer.add_dataset_ownership import (
    AddDatasetOwnership,
    PatternAddDatasetOwnership,
    SimpleAddDatasetOwnership,
)
from datahub.ingestion.transformer.add_dataset_properties import (
    AddDatasetProperties,
    AddDatasetPropertiesResolverBase,
    SimpleAddDatasetProperties,
)
from datahub.ingestion.transformer.add_dataset_schema_tags import (
    PatternAddDatasetSchemaTags,
)
from datahub.ingestion.transformer.add_dataset_schema_terms import (
    PatternAddDatasetSchemaTerms,
)
from datahub.ingestion.transformer.add_dataset_tags import (
    AddDatasetTags,
    PatternAddDatasetTags,
    SimpleAddDatasetTags,
)
from datahub.ingestion.transformer.add_dataset_terms import (
    PatternAddDatasetTerms,
    SimpleAddDatasetTerms,
)
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.ingestion.transformer.dataset_domain import (
    PatternAddDatasetDomain,
    SimpleAddDatasetDomain,
    TransformerOnConflict,
)
from datahub.ingestion.transformer.dataset_domain_based_on_tags import (
    DatasetTagDomainMapper,
)
from datahub.ingestion.transformer.dataset_transformer import (
    ContainerTransformer,
    DatasetTransformer,
    TagTransformer,
)
from datahub.ingestion.transformer.extract_dataset_tags import ExtractDatasetTags
from datahub.ingestion.transformer.extract_ownership_from_tags import (
    ExtractOwnersFromTagsTransformer,
)
from datahub.ingestion.transformer.mark_dataset_status import MarkDatasetStatus
from datahub.ingestion.transformer.pattern_cleanup_dataset_usage_user import (
    PatternCleanupDatasetUsageUser,
)
from datahub.ingestion.transformer.pattern_cleanup_ownership import (
    PatternCleanUpOwnership,
)
from datahub.ingestion.transformer.remove_dataset_ownership import (
    SimpleRemoveDatasetOwnership,
)
from datahub.ingestion.transformer.replace_external_url import (
    ReplaceExternalUrlContainer,
    ReplaceExternalUrlDataset,
)
from datahub.ingestion.transformer.tags_to_terms import TagsToTermMapper
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DatasetPropertiesClass,
    DatasetUserUsageCountsClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn


def make_generic_dataset(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
    aspects: Optional[List[Any]] = None,
) -> models.MetadataChangeEventClass:
    if aspects is None:
        # Default to a status aspect if none is provided.
        aspects = [models.StatusClass(removed=False)]
    return models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn=entity_urn,
            aspects=aspects,
        ),
    )


def make_generic_dataset_mcp(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
    aspect_name: str = "status",
    aspect: Any = models.StatusClass(removed=False),
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.from_string(entity_urn).get_type(),
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )


def make_generic_container_mcp(
    entity_urn: str = "urn:li:container:6338f55439c7ae58243a62c4d6fbffeee",
    aspect_name: str = "status",
    aspect: Any = None,
) -> MetadataChangeProposalWrapper:
    if aspect is None:
        aspect = models.StatusClass(removed=False)
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.from_string(entity_urn).get_type(),
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )


def create_and_run_test_pipeline(
    events: List[Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]],
    transformers: List[Dict[str, Any]],
    path: str,
) -> str:
    with mock.patch(
        "tests.unit.test_source.FakeSource.get_workunits"
    ) as mock_getworkunits:
        mock_getworkunits.return_value = [
            (
                workunit.MetadataWorkUnit(
                    id=f"test-workunit-mce-{e.proposedSnapshot.urn}", mce=e
                )
                if isinstance(e, MetadataChangeEventClass)
                else workunit.MetadataWorkUnit(
                    id=f"test-workunit-mcp-{e.entityUrn}-{e.aspectName}", mcp=e
                )
            )
            for e in events
        ]
        events_file = f"{path}/{str(uuid4())}.json"
        pipeline = Pipeline.create(
            config_dict={
                "source": {
                    "type": "tests.unit.test_source.FakeSource",
                    "config": {},
                },
                "transformers": transformers,
                "sink": {"type": "file", "config": {"filename": events_file}},
            }
        )

        pipeline.run()
        pipeline.raise_from_status()
    return events_file


def make_dataset_with_owner() -> models.MetadataChangeEventClass:
    return models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)",
            aspects=[
                models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner=builder.make_user_urn("fake_owner"),
                            type=models.OwnershipTypeClass.DATAOWNER,
                        ),
                    ],
                    lastModified=models.AuditStampClass(
                        time=1625266033123, actor="urn:li:corpuser:datahub"
                    ),
                )
            ],
        ),
    )


EXISTING_PROPERTIES = {"my_existing_property": "existing property value"}


def make_dataset_with_properties() -> models.MetadataChangeEventClass:
    return models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
            aspects=[
                models.StatusClass(removed=False),
                models.DatasetPropertiesClass(
                    customProperties=EXISTING_PROPERTIES.copy()
                ),
            ],
        ),
    )


def test_dataset_ownership_transformation(mock_time):
    no_owner_aspect = make_generic_dataset()

    with_owner_aspect = make_dataset_with_owner()

    not_a_dataset = models.MetadataChangeEventClass(
        proposedSnapshot=models.DataJobSnapshotClass(
            urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
            aspects=[
                models.DataJobInfoClass(
                    name="User Deletions",
                    description="Constructs the fct_users_deleted from logging_events",
                    type=models.AzkabanJobTypeClass.SQL,
                )
            ],
        )
    )

    inputs = [no_owner_aspect, with_owner_aspect, not_a_dataset, EndOfStream()]

    transformer = SimpleAddDatasetOwnership.create(
        {
            "owner_urns": [
                builder.make_user_urn("person1"),
                builder.make_user_urn("person2"),
            ]
        },
        PipelineContext(run_id="test"),
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert len(outputs) == len(inputs) + 2

    # Check the first entry.
    first_ownership_aspect = builder.get_aspect_if_available(
        outputs[0].record, models.OwnershipClass
    )
    assert first_ownership_aspect is None

    last_event = outputs[3].record
    assert isinstance(last_event, MetadataChangeProposalWrapper)
    assert isinstance(last_event.aspect, OwnershipClass)
    assert len(last_event.aspect.owners) == 2
    assert last_event.entityUrn == outputs[0].record.proposedSnapshot.urn
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER and owner.typeUrn is None
            for owner in last_event.aspect.owners
        ]
    )

    # Check the second entry.
    second_ownership_aspect = builder.get_aspect_if_available(
        outputs[1].record, models.OwnershipClass
    )
    assert second_ownership_aspect
    assert len(second_ownership_aspect.owners) == 3
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER and owner.typeUrn is None
            for owner in second_ownership_aspect.owners
        ]
    )

    third_ownership_aspect = outputs[4].record.aspect
    assert third_ownership_aspect
    assert len(third_ownership_aspect.owners) == 2
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER and owner.typeUrn is None
            for owner in second_ownership_aspect.owners
        ]
    )

    # Verify that the third entry is unchanged.
    assert inputs[2] == outputs[2].record

    # Verify that the last entry is EndOfStream
    assert inputs[-1] == outputs[-1].record


def test_simple_dataset_ownership_with_type_transformation(mock_time):
    input = make_generic_dataset()

    transformer = SimpleAddDatasetOwnership.create(
        {
            "owner_urns": [
                builder.make_user_urn("person1"),
            ],
            "ownership_type": "PRODUCER",
        },
        PipelineContext(run_id="test"),
    )

    output = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )

    assert len(output) == 3

    # original MCE is unchanged
    assert input == output[0].record

    ownership_aspect = output[1].record.aspect

    assert isinstance(ownership_aspect, OwnershipClass)
    assert len(ownership_aspect.owners) == 1
    assert ownership_aspect.owners[0].type == models.OwnershipTypeClass.PRODUCER


def test_simple_dataset_ownership_with_type_urn_transformation(mock_time):
    input = make_generic_dataset()

    transformer = SimpleAddDatasetOwnership.create(
        {
            "owner_urns": [
                builder.make_user_urn("person1"),
            ],
            "ownership_type": "urn:li:ownershipType:__system__technical_owner",
        },
        PipelineContext(run_id="test"),
    )

    output = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )

    assert len(output) == 3

    # original MCE is unchanged
    assert input == output[0].record

    ownership_aspect = output[1].record.aspect

    assert isinstance(ownership_aspect, OwnershipClass)
    assert len(ownership_aspect.owners) == 1
    assert ownership_aspect.owners[0].type == OwnershipTypeClass.CUSTOM
    assert (
        ownership_aspect.owners[0].typeUrn
        == "urn:li:ownershipType:__system__technical_owner"
    )


def _test_extract_tags(in_urn: str, regex_str: str, out_tag: str) -> None:
    input = make_generic_dataset(entity_urn=in_urn)
    transformer = ExtractDatasetTags.create(
        {
            "extract_tags_from": "urn",
            "extract_tags_regex": regex_str,
            "semantics": "overwrite",
        },
        PipelineContext(run_id="test"),
    )
    output = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )

    assert len(output) == 3
    assert output[0].record == input
    tags_aspect = output[1].record.aspect
    assert isinstance(tags_aspect, GlobalTagsClass)
    assert len(tags_aspect.tags) == 1
    assert tags_aspect.tags[0].tag == out_tag


def test_extract_dataset_tags(mock_time):
    _test_extract_tags(
        in_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,clusterid.part1-part2-part3_part4,PROD)",
        regex_str="(.*)",
        out_tag="urn:li:tag:clusterid.part1-part2-part3_part4",
    )
    _test_extract_tags(
        in_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,clusterid.USA-ops-team_table1,PROD)",
        regex_str=".([^._]*)_",
        out_tag="urn:li:tag:USA-ops-team",
    )
    _test_extract_tags(
        in_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,clusterid.Canada-marketing_table1,PROD)",
        regex_str=".([^._]*)_",
        out_tag="urn:li:tag:Canada-marketing",
    )
    _test_extract_tags(
        in_urn="urn:li:dataset:(urn:li:dataPlatform:elasticsearch,abcdef-prefix_datahub_usage_event-000027,PROD)",
        regex_str="([^._]*)_",
        out_tag="urn:li:tag:abcdef-prefix",
    )


def test_simple_dataset_ownership_with_invalid_type_transformation(mock_time):
    with pytest.raises(ValueError):
        SimpleAddDatasetOwnership.create(
            {
                "owner_urns": [
                    builder.make_user_urn("person1"),
                ],
                "ownership_type": "INVALID_TYPE",
            },
            PipelineContext(run_id="test"),
        )


def test_simple_remove_dataset_ownership():
    with_owner_aspect = make_dataset_with_owner()

    transformer = SimpleRemoveDatasetOwnership.create(
        {},
        PipelineContext(run_id="test"),
    )
    outputs = list(
        transformer.transform([RecordEnvelope(with_owner_aspect, metadata={})])
    )

    ownership_aspect = builder.get_aspect_if_available(
        outputs[0].record, models.OwnershipClass
    )
    assert ownership_aspect
    assert len(ownership_aspect.owners) == 0


def test_mark_status_dataset(tmp_path):
    dataset = make_generic_dataset()

    transformer = MarkDatasetStatus.create(
        {"removed": True},
        PipelineContext(run_id="test"),
    )
    removed = list(
        transformer.transform(
            [
                RecordEnvelope(dataset, metadata={}),
            ]
        )
    )
    assert len(removed) == 1
    status_aspect = builder.get_aspect_if_available(
        removed[0].record, models.StatusClass
    )
    assert status_aspect
    assert status_aspect.removed is True

    transformer = MarkDatasetStatus.create(
        {"removed": False},
        PipelineContext(run_id="test"),
    )
    not_removed = list(
        transformer.transform(
            [
                RecordEnvelope(dataset, metadata={}),
            ]
        )
    )
    assert len(not_removed) == 1
    status_aspect = builder.get_aspect_if_available(
        not_removed[0].record, models.StatusClass
    )
    assert status_aspect
    assert status_aspect.removed is False

    mcp = make_generic_dataset_mcp(
        aspect_name="datasetProperties",
        aspect=DatasetPropertiesClass(description="Test dataset"),
    )
    events_file = create_and_run_test_pipeline(
        events=[mcp],
        transformers=[{"type": "mark_dataset_status", "config": {"removed": True}}],
        path=tmp_path,
    )

    # assert dataset properties aspect was preserved
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="datasetProperties",
            aspect_field_matcher={"description": "Test dataset"},
            file=events_file,
        )
        == 1
    )

    # assert Status aspect was generated
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="status",
            aspect_field_matcher={"removed": True},
            file=events_file,
        )
        == 1
    )

    # MCE only
    test_aspect = DatasetPropertiesClass(description="Test dataset")
    events_file = create_and_run_test_pipeline(
        events=[make_generic_dataset(aspects=[test_aspect])],
        transformers=[{"type": "mark_dataset_status", "config": {"removed": True}}],
        path=tmp_path,
    )

    # assert dataset properties aspect was preserved
    assert (
        tests.test_helpers.mce_helpers.assert_entity_mce_aspect(
            entity_urn=mcp.entityUrn or "",
            aspect=test_aspect,
            aspect_type=DatasetPropertiesClass,
            file=events_file,
        )
        == 1
    )

    # assert Status aspect was generated
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="status",
            aspect_field_matcher={"removed": True},
            file=events_file,
        )
        == 1
    )

    # MCE (non-matching) + MCP (matching)
    test_aspect = DatasetPropertiesClass(description="Test dataset")
    events_file = create_and_run_test_pipeline(
        events=[
            make_generic_dataset(aspects=[test_aspect]),
            make_generic_dataset_mcp(),
        ],
        transformers=[{"type": "mark_dataset_status", "config": {"removed": True}}],
        path=tmp_path,
    )

    # assert dataset properties aspect was preserved
    assert (
        tests.test_helpers.mce_helpers.assert_entity_mce_aspect(
            entity_urn=mcp.entityUrn or "",
            aspect=test_aspect,
            aspect_type=DatasetPropertiesClass,
            file=events_file,
        )
        == 1
    )

    # assert Status aspect was generated
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="status",
            aspect_field_matcher={"removed": True},
            file=events_file,
        )
        == 1
    )

    # MCE (matching) + MCP (non-matching)
    test_status_aspect = StatusClass(removed=False)
    events_file = create_and_run_test_pipeline(
        events=[
            make_generic_dataset(aspects=[test_status_aspect]),
            make_generic_dataset_mcp(
                aspect_name="datasetProperties",
                aspect=DatasetPropertiesClass(description="test dataset"),
            ),
        ],
        transformers=[{"type": "mark_dataset_status", "config": {"removed": True}}],
        path=tmp_path,
    )

    # assert MCE was transformed
    assert (
        tests.test_helpers.mce_helpers.assert_entity_mce_aspect(
            entity_urn=mcp.entityUrn or "",
            aspect=StatusClass(removed=True),
            aspect_type=StatusClass,
            file=events_file,
        )
        == 1
    )

    # assert MCP aspect was preserved
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="datasetProperties",
            aspect_field_matcher={"description": "test dataset"},
            file=events_file,
        )
        == 1
    )

    # MCE (non-matching) + MCP (non-matching)
    test_mcp_aspect = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:test")])
    test_dataset_props_aspect = DatasetPropertiesClass(description="Test dataset")
    events_file = create_and_run_test_pipeline(
        events=[
            make_generic_dataset(aspects=[test_dataset_props_aspect]),
            make_generic_dataset_mcp(aspect_name="globalTags", aspect=test_mcp_aspect),
        ],
        transformers=[{"type": "mark_dataset_status", "config": {"removed": True}}],
        path=tmp_path,
    )

    # assert MCE was preserved
    assert (
        tests.test_helpers.mce_helpers.assert_entity_mce_aspect(
            entity_urn=mcp.entityUrn or "",
            aspect=test_dataset_props_aspect,
            aspect_type=DatasetPropertiesClass,
            file=events_file,
        )
        == 1
    )

    # assert MCP aspect was preserved
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="globalTags",
            aspect_field_matcher={"tags": [{"tag": "urn:li:tag:test"}]},
            file=events_file,
        )
        == 1
    )

    # assert MCP Status aspect was generated
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="status",
            aspect_field_matcher={"removed": True},
            file=events_file,
        )
        == 1
    )


def test_extract_owners_from_tags():
    def _test_owner(
        tag: str,
        config: Dict,
        expected_owner: str,
        expected_owner_type: Optional[str] = None,
        expected_owner_type_urn: Optional[str] = None,
    ) -> None:
        dataset = make_generic_dataset(
            aspects=[
                models.GlobalTagsClass(
                    tags=[TagAssociationClass(tag=builder.make_tag_urn(tag))]
                )
            ]
        )

        transformer = ExtractOwnersFromTagsTransformer.create(
            config,
            PipelineContext(run_id="test"),
        )

        record_envelops: List[RecordEnvelope] = list(
            transformer.transform(
                [
                    RecordEnvelope(dataset, metadata={}),
                    RecordEnvelope(record=EndOfStream(), metadata={}),
                ]
            )
        )

        assert len(record_envelops) == 3

        mcp: MetadataChangeProposalWrapper = record_envelops[1].record

        owners_aspect = cast(OwnershipClass, mcp.aspect)

        owners = owners_aspect.owners

        owner = owners[0]

        assert expected_owner_type is not None

        assert owner.type == expected_owner_type

        assert owner.owner == expected_owner

        assert owner.typeUrn == expected_owner_type_urn

    _test_owner(
        tag="owner:foo",
        config={
            "tag_prefix": "owner:",
        },
        expected_owner="urn:li:corpuser:foo",
        expected_owner_type=OwnershipTypeClass.TECHNICAL_OWNER,
    )
    _test_owner(
        tag="abcdef-owner:foo",
        config={
            "tag_prefix": ".*owner:",
        },
        expected_owner="urn:li:corpuser:foo",
        expected_owner_type=OwnershipTypeClass.TECHNICAL_OWNER,
    )
    _test_owner(
        tag="owner:foo",
        config={
            "tag_prefix": "owner:",
            "is_user": False,
        },
        expected_owner="urn:li:corpGroup:foo",
        expected_owner_type=OwnershipTypeClass.TECHNICAL_OWNER,
    )
    _test_owner(
        tag="owner:foo",
        config={
            "tag_prefix": "owner:",
            "email_domain": "example.com",
        },
        expected_owner="urn:li:corpuser:foo@example.com",
        expected_owner_type=OwnershipTypeClass.TECHNICAL_OWNER,
    )
    _test_owner(
        tag="owner:foo",
        config={
            "tag_prefix": "owner:",
            "email_domain": "example.com",
            "owner_type": "TECHNICAL_OWNER",
        },
        expected_owner="urn:li:corpuser:foo@example.com",
        expected_owner_type=OwnershipTypeClass.TECHNICAL_OWNER,
    )
    _test_owner(
        tag="owner:foo",
        config={
            "tag_prefix": "owner:",
            "email_domain": "example.com",
            "owner_type": "AUTHOR",
            "owner_type_urn": "urn:li:ownershipType:ad8557d6-dcb9-4d2a-83fc-b7d0d54f3e0f",
        },
        expected_owner="urn:li:corpuser:foo@example.com",
        expected_owner_type=OwnershipTypeClass.CUSTOM,
        expected_owner_type_urn="urn:li:ownershipType:ad8557d6-dcb9-4d2a-83fc-b7d0d54f3e0f",
    )
    _test_owner(
        tag="data__producer__owner__email:abc--xyz-email_com",
        config={
            "tag_pattern": "(.*)_owner_email:",
            "tag_character_mapping": {
                "_": ".",
                "-": "@",
                "__": "_",
                "--": "-",
            },
            "extract_owner_type_from_tag_pattern": True,
        },
        expected_owner="urn:li:corpuser:abc-xyz@email.com",
        expected_owner_type=OwnershipTypeClass.CUSTOM,
        expected_owner_type_urn="urn:li:ownershipType:data_producer",
    )


def test_add_dataset_browse_paths():
    dataset = make_generic_dataset()

    transformer = AddDatasetBrowsePathTransformer.create(
        {"path_templates": ["/abc"]},
        PipelineContext(run_id="test"),
    )
    transformed = list(
        transformer.transform(
            [
                RecordEnvelope(dataset, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )
    browse_path_aspect = transformed[1].record.aspect
    assert browse_path_aspect
    assert browse_path_aspect.paths == ["/abc"]

    # use an mce with a pre-existing browse path
    dataset_mce = make_generic_dataset(
        aspects=[StatusClass(removed=False), browse_path_aspect]
    )

    transformer = AddDatasetBrowsePathTransformer.create(
        {
            "path_templates": [
                "/PLATFORM/foo/DATASET_PARTS/ENV",
                "/ENV/PLATFORM/bar/DATASET_PARTS/",
            ]
        },
        PipelineContext(run_id="test"),
    )
    transformed = list(
        transformer.transform(
            [
                RecordEnvelope(dataset_mce, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )
    assert len(transformed) == 2
    browse_path_aspect = builder.get_aspect_if_available(
        transformed[0].record, BrowsePathsClass
    )
    assert browse_path_aspect
    assert browse_path_aspect.paths == [
        "/abc",
        "/bigquery/foo/example1/prod",
        "/prod/bigquery/bar/example1/",
    ]

    transformer = AddDatasetBrowsePathTransformer.create(
        {
            "path_templates": [
                "/xyz",
            ],
            "replace_existing": True,
        },
        PipelineContext(run_id="test"),
    )
    transformed = list(
        transformer.transform(
            [
                RecordEnvelope(dataset_mce, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )
    assert len(transformed) == 2
    browse_path_aspect = builder.get_aspect_if_available(
        transformed[0].record, BrowsePathsClass
    )
    assert browse_path_aspect
    assert browse_path_aspect.paths == [
        "/xyz",
    ]


def test_simple_dataset_tags_transformation(mock_time):
    dataset_mce = make_generic_dataset()

    transformer = SimpleAddDatasetTags.create(
        {
            "tag_urns": [
                builder.make_tag_urn("NeedsDocumentation"),
                builder.make_tag_urn("Legacy"),
            ]
        },
        PipelineContext(run_id="test-tags"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, EndOfStream()]
            ]
        )
    )

    assert len(outputs) == 5

    # Check that tags were added.
    tags_aspect = outputs[1].record.aspect
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("NeedsDocumentation")
    assert tags_aspect
    assert len(tags_aspect.tags) == 2

    # Check new tag entity should be there
    assert outputs[2].record.aspectName == "tagKey"
    assert outputs[2].record.aspect.name == "NeedsDocumentation"
    assert outputs[2].record.entityUrn == builder.make_tag_urn("NeedsDocumentation")

    assert outputs[3].record.aspectName == "tagKey"
    assert outputs[3].record.aspect.name == "Legacy"
    assert outputs[3].record.entityUrn == builder.make_tag_urn("Legacy")

    assert isinstance(outputs[4].record, EndOfStream)


def dummy_tag_resolver_method(dataset_snapshot):
    return []


def test_pattern_dataset_tags_transformation(mock_time):
    dataset_mce = make_generic_dataset()

    transformer = PatternAddDatasetTags.create(
        {
            "tag_pattern": {
                "rules": {
                    ".*example1.*": [
                        builder.make_tag_urn("Private"),
                        builder.make_tag_urn("Legacy"),
                    ],
                    ".*example2.*": [builder.make_term_urn("Needs Documentation")],
                }
            },
        },
        PipelineContext(run_id="test-tags"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, EndOfStream()]
            ]
        )
    )

    assert len(outputs) == 5
    tags_aspect = outputs[1].record.aspect
    assert tags_aspect
    assert len(tags_aspect.tags) == 2
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("Private")
    assert builder.make_tag_urn("Needs Documentation") not in tags_aspect.tags


def test_add_dataset_tags_transformation():
    transformer = AddDatasetTags.create(
        {
            "get_tags_to_add": "tests.unit.test_transform_dataset.dummy_tag_resolver_method"
        },
        PipelineContext(run_id="test-tags"),
    )
    output = list(
        transformer.transform(
            [RecordEnvelope(input, metadata={}) for input in [make_generic_dataset()]]
        )
    )
    assert output


def test_pattern_dataset_ownership_transformation(mock_time):
    no_owner_aspect = make_generic_dataset()

    with_owner_aspect = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)",
            aspects=[
                models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner=builder.make_user_urn("fake_owner"),
                            type=models.OwnershipTypeClass.DATAOWNER,
                        ),
                    ],
                    lastModified=models.AuditStampClass(
                        time=1625266033123, actor="urn:li:corpuser:datahub"
                    ),
                )
            ],
        ),
    )

    not_a_dataset = models.MetadataChangeEventClass(
        proposedSnapshot=models.DataJobSnapshotClass(
            urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
            aspects=[
                models.DataJobInfoClass(
                    name="User Deletions",
                    description="Constructs the fct_users_deleted from logging_events",
                    type=models.AzkabanJobTypeClass.SQL,
                )
            ],
        )
    )

    inputs = [no_owner_aspect, with_owner_aspect, not_a_dataset, EndOfStream()]

    transformer = PatternAddDatasetOwnership.create(
        {
            "owner_pattern": {
                "rules": {
                    ".*example1.*": [builder.make_user_urn("person1")],
                    ".*example2.*": [builder.make_user_urn("person2")],
                    ".*dag_abc.*": [builder.make_user_urn("person2")],
                }
            },
            "ownership_type": "DATAOWNER",
        },
        PipelineContext(run_id="test"),
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert (
        len(outputs) == len(inputs) + 2
    )  # additional MCP due to the no-owner MCE + datajob

    # Check the first entry.
    assert inputs[0] == outputs[0].record

    first_ownership_aspect = outputs[3].record.aspect
    assert first_ownership_aspect
    assert len(first_ownership_aspect.owners) == 1
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in first_ownership_aspect.owners
        ]
    )

    # Check the second entry.
    second_ownership_aspect = builder.get_aspect_if_available(
        outputs[1].record, models.OwnershipClass
    )
    assert second_ownership_aspect
    assert len(second_ownership_aspect.owners) == 2
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in second_ownership_aspect.owners
        ]
    )

    third_ownership_aspect = outputs[4].record.aspect
    assert third_ownership_aspect
    assert len(third_ownership_aspect.owners) == 1
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in third_ownership_aspect.owners
        ]
    )

    # Verify that the third entry is unchanged.
    assert inputs[2] == outputs[2].record

    # Verify that the last entry is unchanged (EOS)
    assert inputs[-1] == outputs[-1].record


def test_pattern_dataset_ownership_with_type_transformation(mock_time):
    input = make_generic_dataset()

    transformer = PatternAddDatasetOwnership.create(
        {
            "owner_pattern": {
                "rules": {
                    ".*example1.*": [builder.make_user_urn("person1")],
                }
            },
            "ownership_type": "PRODUCER",
        },
        PipelineContext(run_id="test"),
    )

    output = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={}),
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )

    assert len(output) == 3

    ownership_aspect = output[1].record.aspect
    assert ownership_aspect
    assert len(ownership_aspect.owners) == 1
    assert ownership_aspect.owners[0].type == models.OwnershipTypeClass.PRODUCER


def test_pattern_dataset_ownership_with_invalid_type_transformation(mock_time):
    with pytest.raises(ValueError):
        PatternAddDatasetOwnership.create(
            {
                "owner_pattern": {
                    "rules": {
                        ".*example1.*": [builder.make_user_urn("person1")],
                    }
                },
                "ownership_type": "INVALID_TYPE",
            },
            PipelineContext(run_id="test"),
        )


def test_pattern_container_and_dataset_ownership_transformation(
    mock_time, mock_datahub_graph_instance
):
    def fake_get_aspect(
        entity_urn: str,
        aspect_type: Type[models.BrowsePathsV2Class],
        version: int = 0,
    ) -> Optional[models.BrowsePathsV2Class]:
        return models.BrowsePathsV2Class(
            path=[
                models.BrowsePathEntryClass(
                    id="container_1", urn="urn:li:container:container_1"
                ),
                models.BrowsePathEntryClass(
                    id="container_2", urn="urn:li:container:container_2"
                ),
            ]
        )

    pipeline_context = PipelineContext(
        run_id="test_pattern_container_and_dataset_ownership_transformation"
    )
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_aspect = fake_get_aspect  # type: ignore

    # No owner aspect for the first dataset
    no_owner_aspect_dataset = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
            aspects=[models.StatusClass(removed=False)],
        ),
    )
    # Dataset with an existing owner
    with_owner_aspect_dataset = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)",
            aspects=[
                models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner=builder.make_user_urn("fake_owner"),
                            type=models.OwnershipTypeClass.DATAOWNER,
                        ),
                    ],
                    lastModified=models.AuditStampClass(
                        time=1625266033123, actor="urn:li:corpuser:datahub"
                    ),
                )
            ],
        ),
    )

    datajob = models.MetadataChangeEventClass(
        proposedSnapshot=models.DataJobSnapshotClass(
            urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
            aspects=[
                models.DataJobInfoClass(
                    name="User Deletions",
                    description="Constructs the fct_users_deleted from logging_events",
                    type=models.AzkabanJobTypeClass.SQL,
                )
            ],
        )
    )

    inputs = [
        no_owner_aspect_dataset,
        with_owner_aspect_dataset,
        datajob,
        EndOfStream(),
    ]

    # Initialize the transformer with container support
    transformer = PatternAddDatasetOwnership.create(
        {
            "owner_pattern": {
                "rules": {
                    ".*example1.*": [builder.make_user_urn("person1")],
                    ".*example2.*": [builder.make_user_urn("person2")],
                    ".*dag_abc.*": [builder.make_user_urn("person3")],
                }
            },
            "ownership_type": "DATAOWNER",
            "is_container": True,  # Enable container ownership handling
        },
        pipeline_context,
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert len(outputs) == len(inputs) + 4

    # Check that DatasetSnapshotClass has not changed
    assert inputs[0] == outputs[0].record

    # Check the ownership for the first dataset (example1)
    first_ownership_aspect = outputs[3].record.aspect
    assert first_ownership_aspect
    assert len(first_ownership_aspect.owners) == 1
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in first_ownership_aspect.owners
        ]
    )

    # Check the ownership for the second dataset (example2)
    second_ownership_aspect = builder.get_aspect_if_available(
        outputs[1].record, models.OwnershipClass
    )
    assert second_ownership_aspect
    assert len(second_ownership_aspect.owners) == 2  # One existing + one new
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in second_ownership_aspect.owners
        ]
    )

    third_ownership_aspect = outputs[4].record.aspect
    assert third_ownership_aspect
    assert len(third_ownership_aspect.owners) == 1  # new for datajob

    # Check container ownerships
    for i in range(2):
        container_ownership_aspect = outputs[i + 5].record.aspect
        assert container_ownership_aspect
        ownership = json.loads(container_ownership_aspect.value.decode("utf-8"))
        assert len(ownership) == 3
        assert ownership[0]["value"]["owner"] == builder.make_user_urn("person1")
        assert ownership[1]["value"]["owner"] == builder.make_user_urn("person2")

    # Verify that the third input (not a dataset) is unchanged
    assert inputs[2] == outputs[2].record


def test_pattern_container_and_dataset_ownership_with_no_container(
    mock_time, mock_datahub_graph_instance
):
    def fake_get_aspect(
        entity_urn: str,
        aspect_type: Type[models.BrowsePathsV2Class],
        version: int = 0,
    ) -> Optional[models.BrowsePathsV2Class]:
        return None

    pipeline_context = PipelineContext(
        run_id="test_pattern_container_and_dataset_ownership_with_no_container"
    )
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_aspect = fake_get_aspect  # type: ignore

    # No owner aspect for the first dataset
    no_owner_aspect = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
            aspects=[
                models.StatusClass(removed=False),
                models.BrowsePathsV2Class(
                    path=[
                        models.BrowsePathEntryClass(
                            id="container_1", urn="urn:li:container:container_1"
                        ),
                        models.BrowsePathEntryClass(
                            id="container_2", urn="urn:li:container:container_2"
                        ),
                    ]
                ),
            ],
        ),
    )
    # Dataset with an existing owner
    with_owner_aspect = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)",
            aspects=[
                models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner=builder.make_user_urn("fake_owner"),
                            type=models.OwnershipTypeClass.DATAOWNER,
                        ),
                    ],
                    lastModified=models.AuditStampClass(
                        time=1625266033123, actor="urn:li:corpuser:datahub"
                    ),
                ),
                models.BrowsePathsV2Class(
                    path=[
                        models.BrowsePathEntryClass(
                            id="container_1", urn="urn:li:container:container_1"
                        ),
                        models.BrowsePathEntryClass(
                            id="container_2", urn="urn:li:container:container_2"
                        ),
                    ]
                ),
            ],
        ),
    )

    inputs = [
        no_owner_aspect,
        with_owner_aspect,
        EndOfStream(),
    ]

    # Initialize the transformer with container support
    transformer = PatternAddDatasetOwnership.create(
        {
            "owner_pattern": {
                "rules": {
                    ".*example1.*": [builder.make_user_urn("person1")],
                    ".*example2.*": [builder.make_user_urn("person2")],
                }
            },
            "ownership_type": "DATAOWNER",
            "is_container": True,  # Enable container ownership handling
        },
        pipeline_context,
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert len(outputs) == len(inputs) + 1

    # Check the ownership for the first dataset (example1)
    first_ownership_aspect = outputs[2].record.aspect
    assert first_ownership_aspect
    assert len(first_ownership_aspect.owners) == 1
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in first_ownership_aspect.owners
        ]
    )

    # Check the ownership for the second dataset (example2)
    second_ownership_aspect = builder.get_aspect_if_available(
        outputs[1].record, models.OwnershipClass
    )
    assert second_ownership_aspect
    assert len(second_ownership_aspect.owners) == 2  # One existing + one new
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in second_ownership_aspect.owners
        ]
    )


def test_pattern_container_and_dataset_ownership_with_no_match(
    mock_time, mock_datahub_graph_instance
):
    def fake_get_aspect(
        entity_urn: str,
        aspect_type: Type[models.BrowsePathsV2Class],
        version: int = 0,
    ) -> models.BrowsePathsV2Class:
        return models.BrowsePathsV2Class(
            path=[
                models.BrowsePathEntryClass(
                    id="container_1", urn="urn:li:container:container_1"
                )
            ]
        )

    pipeline_context = PipelineContext(
        run_id="test_pattern_container_and_dataset_ownership_with_no_match"
    )
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_aspect = fake_get_aspect  # type: ignore

    # No owner aspect for the first dataset
    no_owner_aspect = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
            aspects=[
                models.StatusClass(removed=False),
            ],
        ),
    )
    # Dataset with an existing owner
    with_owner_aspect = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)",
            aspects=[
                models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner=builder.make_user_urn("fake_owner"),
                            type=models.OwnershipTypeClass.DATAOWNER,
                        ),
                    ],
                    lastModified=models.AuditStampClass(
                        time=1625266033123, actor="urn:li:corpuser:datahub"
                    ),
                )
            ],
        ),
    )

    inputs = [
        no_owner_aspect,
        with_owner_aspect,
        EndOfStream(),
    ]

    # Initialize the transformer with container support
    transformer = PatternAddDatasetOwnership.create(
        {
            "owner_pattern": {
                "rules": {
                    ".*example3.*": [builder.make_user_urn("person1")],
                    ".*example4.*": [builder.make_user_urn("person2")],
                }
            },
            "ownership_type": "DATAOWNER",
            "is_container": True,  # Enable container ownership handling
        },
        pipeline_context,
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert len(outputs) == len(inputs) + 1

    # Check the ownership for the first dataset (example1)
    first_ownership_aspect = outputs[2].record.aspect
    assert first_ownership_aspect
    assert builder.make_user_urn("person1") not in first_ownership_aspect.owners
    assert builder.make_user_urn("person2") not in first_ownership_aspect.owners

    # Check the ownership for the second dataset (example2)
    second_ownership_aspect = builder.get_aspect_if_available(
        outputs[1].record, models.OwnershipClass
    )
    assert second_ownership_aspect
    assert len(second_ownership_aspect.owners) == 1
    assert builder.make_user_urn("person1") not in second_ownership_aspect.owners
    assert builder.make_user_urn("person2") not in second_ownership_aspect.owners
    assert (
        builder.make_user_urn("fake_owner") == second_ownership_aspect.owners[0].owner
    )


def gen_owners(
    owners: List[str],
    ownership_type: Union[
        str, models.OwnershipTypeClass
    ] = models.OwnershipTypeClass.DATAOWNER,
) -> models.OwnershipClass:
    return models.OwnershipClass(
        owners=[models.OwnerClass(owner=owner, type=ownership_type) for owner in owners]
    )


def test_ownership_patching_intersect(mock_time):
    mock_graph = mock.MagicMock()
    server_ownership = gen_owners(["foo", "bar"])
    mce_ownership = gen_owners(["baz", "foo"])
    mock_graph.get_ownership.return_value = server_ownership

    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", mce_ownership
    )
    assert test_ownership and test_ownership.owners
    assert "foo" in [o.owner for o in test_ownership.owners]
    assert "bar" in [o.owner for o in test_ownership.owners]
    assert "baz" in [o.owner for o in test_ownership.owners]


def test_ownership_patching_with_nones(mock_time):
    mock_graph = mock.MagicMock()
    mce_ownership = gen_owners(["baz", "foo"])
    mock_graph.get_ownership.return_value = None
    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", mce_ownership
    )
    assert test_ownership and test_ownership.owners
    assert "foo" in [o.owner for o in test_ownership.owners]
    assert "baz" in [o.owner for o in test_ownership.owners]

    server_ownership = gen_owners(["baz", "foo"])
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", None
    )
    assert not test_ownership


def test_ownership_patching_with_empty_mce_none_server(mock_time):
    mock_graph = mock.MagicMock()
    mce_ownership = gen_owners([])
    mock_graph.get_ownership.return_value = None
    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", mce_ownership
    )
    # nothing to add, so we omit writing
    assert test_ownership is None


def test_ownership_patching_with_empty_mce_nonempty_server(mock_time):
    mock_graph = mock.MagicMock()
    server_ownership = gen_owners(["baz", "foo"])
    mce_ownership = gen_owners([])
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", mce_ownership
    )
    # nothing to add, so we omit writing
    assert test_ownership is None


def test_ownership_patching_with_different_types_1(mock_time):
    mock_graph = mock.MagicMock()
    server_ownership = gen_owners(["baz", "foo"], models.OwnershipTypeClass.PRODUCER)
    mce_ownership = gen_owners(["foo"], models.OwnershipTypeClass.DATAOWNER)
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", mce_ownership
    )
    assert test_ownership and test_ownership.owners
    # nothing to add, so we omit writing
    assert ("foo", models.OwnershipTypeClass.DATAOWNER) in [
        (o.owner, o.type) for o in test_ownership.owners
    ]
    assert ("baz", models.OwnershipTypeClass.PRODUCER) in [
        (o.owner, o.type) for o in test_ownership.owners
    ]


def test_ownership_patching_with_different_types_2(mock_time):
    mock_graph = mock.MagicMock()
    server_ownership = gen_owners(["baz", "foo"], models.OwnershipTypeClass.PRODUCER)
    mce_ownership = gen_owners(["foo", "baz"], models.OwnershipTypeClass.DATAOWNER)
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership._merge_with_server_ownership(
        mock_graph, "test_urn", mce_ownership
    )
    assert test_ownership and test_ownership.owners
    assert len(test_ownership.owners) == 2
    # nothing to add, so we omit writing
    assert ("foo", models.OwnershipTypeClass.DATAOWNER) in [
        (o.owner, o.type) for o in test_ownership.owners
    ]
    assert ("baz", models.OwnershipTypeClass.DATAOWNER) in [
        (o.owner, o.type) for o in test_ownership.owners
    ]


PROPERTIES_TO_ADD = {"my_new_property": "property value"}


class DummyPropertiesResolverClass(AddDatasetPropertiesResolverBase):
    def get_properties_to_add(self, entity_urn: str) -> Dict[str, str]:
        return PROPERTIES_TO_ADD


def test_add_dataset_properties(mock_time):
    dataset_mce = make_dataset_with_properties()

    transformer = AddDatasetProperties.create(
        {
            "add_properties_resolver_class": "tests.unit.test_transform_dataset.DummyPropertiesResolverClass"
        },
        PipelineContext(run_id="test-properties"),
    )

    outputs = list(
        transformer.transform(
            [RecordEnvelope(input, metadata={}) for input in [dataset_mce]]
        )
    )
    assert len(outputs) == 1

    custom_properties = builder.get_aspect_if_available(
        outputs[0].record, models.DatasetPropertiesClass
    )

    assert custom_properties is not None
    assert custom_properties.customProperties == {
        **EXISTING_PROPERTIES,
        **PROPERTIES_TO_ADD,
    }


def run_simple_add_dataset_properties_transformer_semantics(
    semantics: TransformerSemantics,
    new_properties: dict,
    server_properties: dict,
    mock_datahub_graph_instance: DataHubGraph,
) -> List[RecordEnvelope]:
    pipeline_context = PipelineContext(run_id="test_pattern_dataset_schema_terms")
    pipeline_context.graph = mock_datahub_graph_instance

    # fake the server response
    def fake_dataset_properties(entity_urn: str) -> models.DatasetPropertiesClass:
        return DatasetPropertiesClass(customProperties=server_properties)

    pipeline_context.graph.get_dataset_properties = fake_dataset_properties  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetProperties,
        pipeline_context=pipeline_context,
        aspect=models.DatasetPropertiesClass(
            customProperties=EXISTING_PROPERTIES.copy()
        ),
        config={
            "semantics": semantics,
            "properties": new_properties,
        },
    )

    return output


def test_simple_add_dataset_properties_overwrite(mock_datahub_graph_instance):
    new_properties = {"new-simple-property": "new-value"}
    server_properties = {"p1": "value1"}

    output = run_simple_add_dataset_properties_transformer_semantics(
        semantics=TransformerSemantics.OVERWRITE,
        new_properties=new_properties,
        server_properties=server_properties,
        mock_datahub_graph_instance=mock_datahub_graph_instance,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    custom_properties_aspect: models.DatasetPropertiesClass = cast(
        models.DatasetPropertiesClass, output[0].record.aspect
    )

    assert custom_properties_aspect.customProperties == {
        **EXISTING_PROPERTIES,
        **new_properties,
    }


def test_simple_add_dataset_properties_patch(mock_datahub_graph_instance):
    new_properties = {"new-simple-property": "new-value"}
    server_properties = {"p1": "value1"}

    output = run_simple_add_dataset_properties_transformer_semantics(
        semantics=TransformerSemantics.PATCH,
        new_properties=new_properties,
        server_properties=server_properties,
        mock_datahub_graph_instance=mock_datahub_graph_instance,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    custom_properties_aspect: models.DatasetPropertiesClass = cast(
        models.DatasetPropertiesClass, output[0].record.aspect
    )
    assert custom_properties_aspect.customProperties == {
        **EXISTING_PROPERTIES,
        **new_properties,
        **server_properties,
    }


def test_simple_add_dataset_properties(mock_time):
    new_properties = {"new-simple-property": "new-value"}
    outputs = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetProperties,
        aspect=models.DatasetPropertiesClass(
            customProperties=EXISTING_PROPERTIES.copy()
        ),
        config={
            "properties": new_properties,
        },
    )

    assert len(outputs) == 2
    assert outputs[0].record
    assert outputs[0].record.aspect
    custom_properties_aspect: models.DatasetPropertiesClass = cast(
        models.DatasetPropertiesClass, outputs[0].record.aspect
    )
    assert custom_properties_aspect.customProperties == {
        **EXISTING_PROPERTIES,
        **new_properties,
    }


def test_simple_add_dataset_properties_replace_existing(mock_time):
    new_properties = {"new-simple-property": "new-value"}
    outputs = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetProperties,
        aspect=models.DatasetPropertiesClass(
            customProperties=EXISTING_PROPERTIES.copy()
        ),
        config={
            "replace_existing": True,
            "properties": new_properties,
        },
    )

    assert len(outputs) == 2
    assert outputs[0].record
    assert outputs[0].record.aspect
    custom_properties_aspect: models.DatasetPropertiesClass = cast(
        models.DatasetPropertiesClass, outputs[0].record.aspect
    )

    assert custom_properties_aspect.customProperties == {
        **new_properties,
    }


def test_simple_dataset_terms_transformation(mock_time):
    dataset_mce = make_generic_dataset()

    transformer = SimpleAddDatasetTerms.create(
        {
            "term_urns": [
                builder.make_term_urn("Test"),
                builder.make_term_urn("Needs Review"),
            ]
        },
        PipelineContext(run_id="test-terms"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, EndOfStream()]
            ]
        )
    )
    assert len(outputs) == 3

    # Check that glossary terms were added.
    terms_aspect = outputs[1].record.aspect
    assert terms_aspect
    assert len(terms_aspect.terms) == 2
    assert terms_aspect.terms[0].urn == builder.make_term_urn("Test")


def test_pattern_dataset_terms_transformation(mock_time):
    dataset_mce = make_generic_dataset()

    transformer = PatternAddDatasetTerms.create(
        {
            "term_pattern": {
                "rules": {
                    ".*example1.*": [
                        builder.make_term_urn("AccountBalance"),
                        builder.make_term_urn("Email"),
                    ],
                    ".*example2.*": [builder.make_term_urn("Address")],
                }
            },
        },
        PipelineContext(run_id="test-terms"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, EndOfStream()]
            ]
        )
    )

    assert len(outputs) == 3
    # Check that glossary terms were added.
    terms_aspect = outputs[1].record.aspect
    assert terms_aspect
    assert len(terms_aspect.terms) == 2
    assert terms_aspect.terms[0].urn == builder.make_term_urn("AccountBalance")
    assert builder.make_term_urn("AccountBalance") not in terms_aspect.terms


def test_mcp_add_tags_missing(mock_time):
    dataset_mcp = make_generic_dataset_mcp()

    transformer = SimpleAddDatasetTags.create(
        {
            "tag_urns": [
                builder.make_tag_urn("NeedsDocumentation"),
                builder.make_tag_urn("Legacy"),
            ]
        },
        PipelineContext(run_id="test-tags"),
    )
    input_stream: List[RecordEnvelope] = [
        RecordEnvelope(input, metadata={}) for input in [dataset_mcp]
    ]
    input_stream.append(RecordEnvelope(record=EndOfStream(), metadata={}))
    outputs = list(transformer.transform(input_stream))
    assert len(outputs) == 5
    assert outputs[0].record == dataset_mcp
    # Check that tags were added, this will be the second result
    tags_aspect = outputs[1].record.aspect
    assert tags_aspect
    assert len(tags_aspect.tags) == 2
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("NeedsDocumentation")
    assert isinstance(outputs[-1].record, EndOfStream)


def test_mcp_add_tags_existing(mock_time):
    dataset_mcp = make_generic_dataset_mcp(
        aspect_name="globalTags",
        aspect=GlobalTagsClass(
            tags=[TagAssociationClass(tag=builder.make_tag_urn("Test"))]
        ),
    )

    transformer = SimpleAddDatasetTags.create(
        {
            "tag_urns": [
                builder.make_tag_urn("NeedsDocumentation"),
                builder.make_tag_urn("Legacy"),
            ]
        },
        PipelineContext(run_id="test-tags"),
    )
    input_stream: List[RecordEnvelope] = [
        RecordEnvelope(input, metadata={}) for input in [dataset_mcp]
    ]
    input_stream.append(RecordEnvelope(record=EndOfStream(), metadata={}))
    outputs = list(transformer.transform(input_stream))

    assert len(outputs) == 4

    # Check that tags were added, this will be the second result
    tags_aspect = outputs[0].record.aspect
    assert tags_aspect
    assert len(tags_aspect.tags) == 3
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("Test")
    assert tags_aspect.tags[1].tag == builder.make_tag_urn("NeedsDocumentation")
    assert tags_aspect.tags[2].tag == builder.make_tag_urn("Legacy")

    # Check tag entities got added
    assert outputs[1].record.entityType == "tag"
    assert outputs[1].record.entityUrn == builder.make_tag_urn("NeedsDocumentation")
    assert outputs[2].record.entityType == "tag"
    assert outputs[2].record.entityUrn == builder.make_tag_urn("Legacy")

    assert isinstance(outputs[-1].record, EndOfStream)


def test_mcp_multiple_transformers(mock_time, tmp_path):
    events_file = f"{tmp_path}/multi_transformer_test.json"

    pipeline = Pipeline.create(
        config_dict={
            "source": {
                "type": "tests.unit.test_source.FakeSource",
                "config": {},
            },
            "transformers": [
                {
                    "type": "set_dataset_browse_path",
                    "config": {
                        "path_templates": ["/ENV/PLATFORM/EsComments/DATASET_PARTS"]
                    },
                },
                {
                    "type": "simple_add_dataset_tags",
                    "config": {"tag_urns": ["urn:li:tag:EsComments"]},
                },
            ],
            "sink": {"type": "file", "config": {"filename": events_file}},
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    urn_pattern = "^" + re.escape(
        "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,fooIndex,PROD)"
    )
    assert (
        tests.test_helpers.mce_helpers.assert_mcp_entity_urn(
            filter="ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        == 3
    )

    # check on status aspect
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="status",
            aspect_field_matcher={"removed": False},
            file=events_file,
        )
        == 1
    )

    # check on globalTags aspect
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="globalTags",
            aspect_field_matcher={"tags": [{"tag": "urn:li:tag:EsComments"}]},
            file=events_file,
        )
        == 1
    )

    # check on globalTags aspect
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="browsePaths",
            aspect_field_matcher={"paths": ["/prod/elasticsearch/EsComments/fooIndex"]},
            file=events_file,
        )
        == 1
    )


def test_mcp_multiple_transformers_replace(mock_time, tmp_path):
    mcps: MutableSequence[
        Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]
    ] = [
        MetadataChangeProposalWrapper(
            entityUrn=str(
                DatasetUrn.create_from_ids(
                    platform_id="elasticsearch",
                    table_name=f"fooBarIndex{i}",
                    env="PROD",
                )
            ),
            aspect=GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Test")]),
        )
        for i in range(0, 10)
    ]
    mcps.extend(
        [
            MetadataChangeProposalWrapper(
                entityUrn=str(
                    DatasetUrn.create_from_ids(
                        platform_id="elasticsearch",
                        table_name=f"fooBarIndex{i}",
                        env="PROD",
                    )
                ),
                aspect=DatasetPropertiesClass(description="test dataset"),
            )
            for i in range(0, 10)
        ]
    )

    # shuffle the mcps
    import random

    random.shuffle(mcps)

    events_file = create_and_run_test_pipeline(
        events=list(mcps),
        transformers=[
            {
                "type": "set_dataset_browse_path",
                "config": {
                    "path_templates": ["/ENV/PLATFORM/EsComments/DATASET_PARTS"]
                },
            },
            {
                "type": "simple_add_dataset_tags",
                "config": {"tag_urns": ["urn:li:tag:EsComments"]},
            },
        ],
        path=tmp_path,
    )

    urn_pattern = "^" + re.escape(
        "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,fooBarIndex"
    )

    # there should be 30 MCP-s
    assert (
        tests.test_helpers.mce_helpers.assert_mcp_entity_urn(
            filter="ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        == 30
    )

    # 10 globalTags aspects with new tag attached
    assert (
        tests.test_helpers.mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="globalTags",
            aspect_field_matcher={
                "tags": [{"tag": "urn:li:tag:Test"}, {"tag": "urn:li:tag:EsComments"}]
            },
            file=events_file,
        )
        == 10
    )

    # check on browsePaths aspect
    for i in range(0, 10):
        assert (
            tests.test_helpers.mce_helpers.assert_entity_mcp_aspect(
                entity_urn=str(
                    DatasetUrn.create_from_ids(
                        platform_id="elasticsearch",
                        table_name=f"fooBarIndex{i}",
                        env="PROD",
                    )
                ),
                aspect_name="browsePaths",
                aspect_field_matcher={
                    "paths": [f"/prod/elasticsearch/EsComments/fooBarIndex{i}"]
                },
                file=events_file,
            )
            == 1
        )


class SuppressingTransformer(BaseTransformer, SingleAspectTransformer):
    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SuppressingTransformer":
        return SuppressingTransformer()

    def entity_types(self) -> List[str]:
        return super().entity_types()

    def aspect_name(self) -> str:
        return "datasetProperties"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        return None


def test_supression_works():
    dataset_mce = make_generic_dataset()
    dataset_mcp = make_generic_dataset_mcp(
        aspect_name="datasetProperties",
        aspect=DatasetPropertiesClass(description="supressable description"),
    )
    transformer = SuppressingTransformer.create(
        {},
        PipelineContext(run_id="test-suppress-transformer"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, dataset_mcp, EndOfStream()]
            ]
        )
    )

    assert len(outputs) == 2  # MCP will be dropped


def test_pattern_dataset_schema_terms_transformation(mock_time):
    dataset_mce = make_generic_dataset(
        aspects=[
            models.SchemaMetadataClass(
                schemaName="customer",  # not used
                platform=builder.make_data_platform_urn(
                    "hive"
                ),  # important <- platform must be an urn
                version=0,
                # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
                hash="",
                # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
                platformSchema=models.OtherSchemaClass(
                    rawSchema="__insert raw schema here__"
                ),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="address",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR(100)",
                        # use this to provide the type of the field in the source system's vernacular
                    ),
                    models.SchemaFieldClass(
                        fieldPath="first_name",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR(100)",
                        # use this to provide the type of the field in the source system's vernacular
                    ),
                    models.SchemaFieldClass(
                        fieldPath="last_name",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR(100)",
                        # use this to provide the type of the field in the source system's vernacular
                    ),
                ],
            )
        ]
    )

    transformer = PatternAddDatasetSchemaTerms.create(
        {
            "term_pattern": {
                "rules": {
                    ".*first_name.*": [
                        builder.make_term_urn("Name"),
                        builder.make_term_urn("FirstName"),
                    ],
                    ".*last_name.*": [
                        builder.make_term_urn("Name"),
                        builder.make_term_urn("LastName"),
                    ],
                }
            },
        },
        PipelineContext(run_id="test-schema-terms"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, EndOfStream()]
            ]
        )
    )

    assert len(outputs) == 2
    # Check that glossary terms were added.
    schema_aspect = outputs[0].record.proposedSnapshot.aspects[0]
    assert schema_aspect
    assert schema_aspect.fields[0].fieldPath == "address"
    assert schema_aspect.fields[0].glossaryTerms is None
    assert schema_aspect.fields[1].fieldPath == "first_name"
    assert schema_aspect.fields[1].glossaryTerms.terms[0].urn == builder.make_term_urn(
        "Name"
    )
    assert schema_aspect.fields[1].glossaryTerms.terms[1].urn == builder.make_term_urn(
        "FirstName"
    )
    assert schema_aspect.fields[2].fieldPath == "last_name"
    assert schema_aspect.fields[2].glossaryTerms.terms[0].urn == builder.make_term_urn(
        "Name"
    )
    assert schema_aspect.fields[2].glossaryTerms.terms[1].urn == builder.make_term_urn(
        "LastName"
    )


def test_pattern_dataset_schema_tags_transformation(mock_time):
    dataset_mce = make_generic_dataset(
        aspects=[
            models.SchemaMetadataClass(
                schemaName="customer",  # not used
                platform=builder.make_data_platform_urn(
                    "hive"
                ),  # important <- platform must be an urn
                version=0,
                # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
                hash="",
                # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
                platformSchema=models.OtherSchemaClass(
                    rawSchema="__insert raw schema here__"
                ),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="address",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR(100)",
                        # use this to provide the type of the field in the source system's vernacular
                    ),
                    models.SchemaFieldClass(
                        fieldPath="first_name",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR(100)",
                        # use this to provide the type of the field in the source system's vernacular
                    ),
                    models.SchemaFieldClass(
                        fieldPath="last_name",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR(100)",
                        # use this to provide the type of the field in the source system's vernacular
                    ),
                ],
            )
        ]
    )

    transformer = PatternAddDatasetSchemaTags.create(
        {
            "tag_pattern": {
                "rules": {
                    ".*first_name.*": [
                        builder.make_tag_urn("Name"),
                        builder.make_tag_urn("FirstName"),
                    ],
                    ".*last_name.*": [
                        builder.make_tag_urn("Name"),
                        builder.make_tag_urn("LastName"),
                    ],
                }
            },
        },
        PipelineContext(run_id="test-schema-tags"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [dataset_mce, EndOfStream()]
            ]
        )
    )

    assert len(outputs) == 2
    # Check that glossary terms were added.
    schema_aspect = outputs[0].record.proposedSnapshot.aspects[0]
    assert schema_aspect
    assert schema_aspect.fields[0].fieldPath == "address"
    assert schema_aspect.fields[0].globalTags is None
    assert schema_aspect.fields[1].fieldPath == "first_name"
    assert schema_aspect.fields[1].globalTags.tags[0].tag == builder.make_tag_urn(
        "Name"
    )
    assert schema_aspect.fields[1].globalTags.tags[1].tag == builder.make_tag_urn(
        "FirstName"
    )
    assert schema_aspect.fields[2].fieldPath == "last_name"
    assert schema_aspect.fields[2].globalTags.tags[0].tag == builder.make_tag_urn(
        "Name"
    )
    assert schema_aspect.fields[2].globalTags.tags[1].tag == builder.make_tag_urn(
        "LastName"
    )


def run_dataset_transformer_pipeline(
    transformer_type: Type[Union[DatasetTransformer, TagTransformer]],
    aspect: Optional[builder.Aspect],
    config: dict,
    pipeline_context: Optional[PipelineContext] = None,
    use_mce: bool = False,
) -> List[RecordEnvelope]:
    if pipeline_context is None:
        pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    transformer: DatasetTransformer = cast(
        DatasetTransformer, transformer_type.create(config, pipeline_context)
    )

    dataset: Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]
    if use_mce:
        dataset = MetadataChangeEventClass(
            proposedSnapshot=models.DatasetSnapshotClass(
                urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
                aspects=[],
            )
        )
    else:
        assert aspect
        dataset = make_generic_dataset_mcp(
            aspect=aspect, aspect_name=transformer.aspect_name()
        )

    outputs = list(
        transformer.transform(
            [RecordEnvelope(input, metadata={}) for input in [dataset, EndOfStream()]]
        )
    )
    return outputs


def run_container_transformer_pipeline(
    transformer_type: Type[ContainerTransformer],
    aspect: Optional[builder.Aspect],
    config: dict,
    pipeline_context: Optional[PipelineContext] = None,
    use_mce: bool = False,
) -> List[RecordEnvelope]:
    if pipeline_context is None:
        pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    transformer: ContainerTransformer = cast(
        ContainerTransformer, transformer_type.create(config, pipeline_context)
    )

    container: Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]
    if use_mce:
        container = MetadataChangeEventClass(
            proposedSnapshot=models.DatasetSnapshotClass(
                urn="urn:li:container:6338f55439c7ae58243a62c4d6fbffde",
                aspects=[],
            )
        )
    else:
        assert aspect
        container = make_generic_container_mcp(
            aspect=aspect, aspect_name=transformer.aspect_name()
        )

    outputs = list(
        transformer.transform(
            [RecordEnvelope(input, metadata={}) for input in [container, EndOfStream()]]
        )
    )
    return outputs


def test_simple_add_dataset_domain_aspect_name(mock_datahub_graph_instance):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    transformer = SimpleAddDatasetDomain.create({"domains": []}, pipeline_context)
    assert transformer.aspect_name() == models.DomainsClass.ASPECT_NAME


def test_simple_add_dataset_domain(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={"domains": [acryl_domain]},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 2
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_mce_support(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=None,
        config={"domains": [datahub_domain, acryl_domain]},
        pipeline_context=pipeline_context,
        use_mce=True,
    )

    assert len(output) == 3
    assert isinstance(output[0].record, MetadataChangeEventClass)
    assert isinstance(output[0].record.proposedSnapshot, models.DatasetSnapshotClass)
    assert len(output[0].record.proposedSnapshot.aspects) == 0

    assert isinstance(output[1].record, MetadataChangeProposalWrapper)
    assert output[1].record.aspect is not None
    assert isinstance(output[1].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[1].record.aspect)
    assert len(transformed_aspect.domains) == 2
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_replace_existing(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={"replace_existing": True, "domains": [acryl_domain]},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert datahub_domain not in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_semantics_overwrite(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    server_domain = builder.make_domain_urn("test.io")

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "semantics": TransformerSemantics.OVERWRITE,
            "domains": [acryl_domain],
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 2
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert server_domain not in transformed_aspect.domains


def test_simple_add_dataset_domain_semantics_patch(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph_instance
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    server_domain = builder.make_domain_urn("test.io")

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": False,
            "semantics": TransformerSemantics.PATCH,
            "domains": [acryl_domain],
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 3
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert server_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_on_conflict_do_nothing(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph_instance
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    server_domain = builder.make_domain_urn("test.io")

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": False,
            "semantics": TransformerSemantics.PATCH,
            "domains": [acryl_domain],
            "on_conflict": TransformerOnConflict.DO_NOTHING,
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 1
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, EndOfStream)


def test_simple_add_dataset_domain_on_conflict_do_nothing_no_conflict(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph_instance
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    irrelevant_domain = builder.make_domain_urn("test.io")

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": False,
            "semantics": TransformerSemantics.PATCH,
            "domains": [acryl_domain],
            "on_conflict": TransformerOnConflict.DO_NOTHING,
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 2
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert irrelevant_domain not in transformed_aspect.domains


def test_pattern_add_dataset_domain_aspect_name(mock_datahub_graph_instance):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    transformer = PatternAddDatasetDomain.create(
        {"domain_pattern": {"rules": {}}}, pipeline_context
    )
    assert transformer.aspect_name() == models.DomainsClass.ASPECT_NAME


def test_pattern_add_dataset_domain_match(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 2
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_pattern_add_dataset_domain_no_match(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:invalid,.*"

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain not in transformed_aspect.domains


def test_pattern_add_dataset_domain_replace_existing_match(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": True,
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert datahub_domain not in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_pattern_add_dataset_domain_replace_existing_no_match(
    mock_datahub_graph_instance,
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:invalid,.*"

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": True,
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 0


def test_pattern_add_dataset_domain_semantics_overwrite(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    server_domain = builder.make_domain_urn("test.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "semantics": TransformerSemantics.OVERWRITE,
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 2
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert server_domain not in transformed_aspect.domains


def test_pattern_add_dataset_domain_semantics_patch(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph_instance
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    server_domain = builder.make_domain_urn("test.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": False,
            "semantics": TransformerSemantics.PATCH,
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 3
    assert datahub_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert server_domain in transformed_aspect.domains


def test_simple_dataset_ownership_transformer_semantics_patch(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    server_owner: str = builder.make_owner_urn(
        "mohd@acryl.io", owner_type=builder.OwnerType.USER
    )
    owner1: str = builder.make_owner_urn(
        "john@acryl.io", owner_type=builder.OwnerType.USER
    )
    owner2: str = builder.make_owner_urn(
        "pedro@acryl.io", owner_type=builder.OwnerType.USER
    )

    # Return fake aspect to simulate server behaviour
    def fake_ownership_class(entity_urn: str) -> models.OwnershipClass:
        return models.OwnershipClass(
            owners=[
                models.OwnerClass(
                    owner=server_owner, type=models.OwnershipTypeClass.DATAOWNER
                )
            ]
        )

    pipeline_context.graph.get_ownership = fake_ownership_class  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetOwnership,
        aspect=models.OwnershipClass(
            owners=[
                models.OwnerClass(owner=owner1, type=models.OwnershipTypeClass.PRODUCER)
            ]
        ),
        config={
            "replace_existing": False,
            "semantics": TransformerSemantics.PATCH,
            "owner_urns": [owner2],
            "ownership_type": "DATAOWNER",
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.OwnershipClass)
    transformed_aspect: models.OwnershipClass = cast(
        models.OwnershipClass, output[0].record.aspect
    )
    assert len(transformed_aspect.owners) == 3
    owner_urns: List[str] = [
        owner_class.owner for owner_class in transformed_aspect.owners
    ]
    assert owner1 in owner_urns
    assert owner2 in owner_urns
    assert server_owner in owner_urns


def test_pattern_container_and_dataset_domain_transformation(
    mock_datahub_graph_instance,
):
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    acryl_domain = builder.make_domain_urn("acryl_domain")
    server_domain = builder.make_domain_urn("server_domain")

    def fake_get_aspect(
        entity_urn: str,
        aspect_type: Type[models.BrowsePathsV2Class],
        version: int = 0,
    ) -> models.BrowsePathsV2Class:
        return models.BrowsePathsV2Class(
            path=[
                models.BrowsePathEntryClass(
                    id="container_1", urn="urn:li:container:container_1"
                ),
                models.BrowsePathEntryClass(
                    id="container_2", urn="urn:li:container:container_2"
                ),
            ]
        )

    pipeline_context = PipelineContext(
        run_id="test_pattern_container_and_dataset_domain_transformation"
    )
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_aspect = fake_get_aspect  # type: ignore

    with_domain_aspect = make_generic_dataset_mcp(
        aspect=models.DomainsClass(domains=[datahub_domain]), aspect_name="domains"
    )
    no_domain_aspect = make_generic_dataset_mcp(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)"
    )

    # Not a dataset, should be ignored
    not_a_dataset = models.MetadataChangeEventClass(
        proposedSnapshot=models.DataJobSnapshotClass(
            urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
            aspects=[
                models.DataJobInfoClass(
                    name="User Deletions",
                    description="Constructs the fct_users_deleted from logging_events",
                    type=models.AzkabanJobTypeClass.SQL,
                )
            ],
        )
    )

    inputs = [
        with_domain_aspect,
        no_domain_aspect,
        not_a_dataset,
        EndOfStream(),
    ]

    # Initialize the transformer with container support for domains
    transformer = PatternAddDatasetDomain.create(
        {
            "domain_pattern": {
                "rules": {
                    ".*example1.*": [acryl_domain, server_domain],
                    ".*example2.*": [server_domain],
                }
            },
            "is_container": True,  # Enable container domain handling
        },
        pipeline_context,
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert (
        len(outputs) == len(inputs) + 3
    )  # MCPs for the dataset without domains and the containers

    first_domain_aspect = outputs[0].record.aspect
    assert first_domain_aspect
    assert len(first_domain_aspect.domains) == 3
    assert all(
        domain in first_domain_aspect.domains
        for domain in [datahub_domain, acryl_domain, server_domain]
    )

    second_domain_aspect = outputs[3].record.aspect
    assert second_domain_aspect
    assert len(second_domain_aspect.domains) == 1
    assert server_domain in second_domain_aspect.domains

    # Verify that the third input (not a dataset) is unchanged
    assert inputs[2] == outputs[2].record

    # Verify conainer 1 and container 2 should contain all domains
    container_1 = outputs[4].record.aspect
    assert len(container_1.domains) == 2
    assert acryl_domain in container_1.domains
    assert server_domain in container_1.domains

    container_2 = outputs[5].record.aspect
    assert len(container_2.domains) == 2
    assert acryl_domain in container_2.domains
    assert server_domain in container_2.domains


def test_pattern_container_and_dataset_domain_transformation_with_no_container(
    mock_datahub_graph_instance,
):
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    acryl_domain = builder.make_domain_urn("acryl_domain")
    server_domain = builder.make_domain_urn("server_domain")

    def fake_get_aspect(
        entity_urn: str,
        aspect_type: Type[models.BrowsePathsV2Class],
        version: int = 0,
    ) -> Optional[models.BrowsePathsV2Class]:
        return None

    pipeline_context = PipelineContext(
        run_id="test_pattern_container_and_dataset_domain_transformation_with_no_container"
    )
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_aspect = fake_get_aspect  # type: ignore

    with_domain_aspect = make_generic_dataset_mcp(
        aspect=models.DomainsClass(domains=[datahub_domain]), aspect_name="domains"
    )
    no_domain_aspect = make_generic_dataset_mcp(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)"
    )

    inputs = [
        with_domain_aspect,
        no_domain_aspect,
        EndOfStream(),
    ]

    # Initialize the transformer with container support for domains
    transformer = PatternAddDatasetDomain.create(
        {
            "domain_pattern": {
                "rules": {
                    ".*example1.*": [acryl_domain, server_domain],
                    ".*example2.*": [server_domain],
                }
            },
            "is_container": True,  # Enable container domain handling
        },
        pipeline_context,
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert len(outputs) == len(inputs) + 1

    first_domain_aspect = outputs[0].record.aspect
    assert first_domain_aspect
    assert len(first_domain_aspect.domains) == 3
    assert all(
        domain in first_domain_aspect.domains
        for domain in [datahub_domain, acryl_domain, server_domain]
    )

    second_domain_aspect = outputs[2].record.aspect
    assert second_domain_aspect
    assert len(second_domain_aspect.domains) == 1
    assert server_domain in second_domain_aspect.domains


def test_pattern_add_container_dataset_domain_no_match(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    datahub_domain = builder.make_domain_urn("datahubproject.io")
    pattern = "urn:li:dataset:\\(urn:li:dataPlatform:invalid,.*"

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    def fake_get_aspect(
        entity_urn: str,
        aspect_type: Type[models.BrowsePathsV2Class],
        version: int = 0,
    ) -> models.BrowsePathsV2Class:
        return models.BrowsePathsV2Class(
            path=[
                models.BrowsePathEntryClass(
                    id="container_1", urn="urn:li:container:container_1"
                )
            ]
        )

    pipeline_context.graph.get_aspect = fake_get_aspect  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetDomain,
        aspect=models.DomainsClass(domains=[datahub_domain]),
        config={
            "replace_existing": True,
            "domain_pattern": {"rules": {pattern: [acryl_domain]}},
            "is_container": True,
        },
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 0


def run_pattern_dataset_schema_terms_transformation_semantics(
    semantics: TransformerSemantics,
    mock_datahub_graph_instance: DataHubGraph,
) -> List[RecordEnvelope]:
    pipeline_context = PipelineContext(run_id="test_pattern_dataset_schema_terms")
    pipeline_context.graph = mock_datahub_graph_instance

    # fake the server response
    def fake_schema_metadata(entity_urn: str) -> models.SchemaMetadataClass:
        return models.SchemaMetadataClass(
            schemaName="customer",  # not used
            platform=builder.make_data_platform_urn(
                "hive"
            ),  # important <- platform must be an urn
            version=0,
            # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
            hash="",
            # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="first_name",
                    glossaryTerms=models.GlossaryTermsClass(
                        terms=[
                            models.GlossaryTermAssociationClass(
                                urn=builder.make_term_urn("pii")
                            )
                        ],
                        auditStamp=models.AuditStampClass._construct_with_defaults(),
                    ),
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="mobile_number",
                    glossaryTerms=models.GlossaryTermsClass(
                        terms=[
                            models.GlossaryTermAssociationClass(
                                urn=builder.make_term_urn("pii")
                            )
                        ],
                        auditStamp=models.AuditStampClass._construct_with_defaults(),
                    ),
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
            ],
        )

    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetSchemaTerms,
        pipeline_context=pipeline_context,
        config={
            "semantics": semantics,
            "term_pattern": {
                "rules": {
                    ".*first_name.*": [
                        builder.make_term_urn("Name"),
                        builder.make_term_urn("FirstName"),
                    ],
                    ".*last_name.*": [
                        builder.make_term_urn("Name"),
                        builder.make_term_urn("LastName"),
                    ],
                }
            },
        },
        aspect=models.SchemaMetadataClass(
            schemaName="customer",  # not used
            platform=builder.make_data_platform_urn(
                "hive"
            ),  # important <- platform must be an urn
            version=0,
            # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
            hash="",
            # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="address",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="first_name",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="last_name",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
            ],
        ),
    )

    return output


def test_pattern_dataset_schema_terms_transformation_patch(
    mock_time, mock_datahub_graph_instance
):
    output = run_pattern_dataset_schema_terms_transformation_semantics(
        TransformerSemantics.PATCH, mock_datahub_graph_instance
    )
    assert len(output) == 2
    # Check that glossary terms were added.
    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.SchemaMetadataClass)
    transform_aspect = cast(models.SchemaMetadataClass, output[0].record.aspect)
    field_path_vs_field: Dict[str, models.SchemaFieldClass] = {
        field.fieldPath: field for field in transform_aspect.fields
    }

    assert (
        field_path_vs_field.get("mobile_number") is not None
    )  # server field should be preserved during patch

    assert field_path_vs_field["first_name"].glossaryTerms is not None
    assert len(field_path_vs_field["first_name"].glossaryTerms.terms) == 3
    glossary_terms_urn = [
        term.urn for term in field_path_vs_field["first_name"].glossaryTerms.terms
    ]
    assert builder.make_term_urn("pii") in glossary_terms_urn
    assert builder.make_term_urn("FirstName") in glossary_terms_urn
    assert builder.make_term_urn("Name") in glossary_terms_urn


def test_pattern_dataset_schema_terms_transformation_overwrite(
    mock_time, mock_datahub_graph_instance
):
    output = run_pattern_dataset_schema_terms_transformation_semantics(
        TransformerSemantics.OVERWRITE, mock_datahub_graph_instance
    )

    assert len(output) == 2
    # Check that glossary terms were added.
    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.SchemaMetadataClass)
    transform_aspect = cast(models.SchemaMetadataClass, output[0].record.aspect)
    field_path_vs_field: Dict[str, models.SchemaFieldClass] = {
        field.fieldPath: field for field in transform_aspect.fields
    }

    assert (
        field_path_vs_field.get("mobile_number") is None
    )  # server field should not be preserved during overwrite

    assert field_path_vs_field["first_name"].glossaryTerms is not None
    assert len(field_path_vs_field["first_name"].glossaryTerms.terms) == 2
    glossary_terms_urn = [
        term.urn for term in field_path_vs_field["first_name"].glossaryTerms.terms
    ]
    assert builder.make_term_urn("pii") not in glossary_terms_urn
    assert builder.make_term_urn("FirstName") in glossary_terms_urn
    assert builder.make_term_urn("Name") in glossary_terms_urn


def run_pattern_dataset_schema_tags_transformation_semantics(
    semantics: TransformerSemantics,
    mock_datahub_graph_instance: DataHubGraph,
) -> List[RecordEnvelope]:
    pipeline_context = PipelineContext(run_id="test_pattern_dataset_schema_terms")
    pipeline_context.graph = mock_datahub_graph_instance

    # fake the server response
    def fake_schema_metadata(entity_urn: str) -> models.SchemaMetadataClass:
        return models.SchemaMetadataClass(
            schemaName="customer",  # not used
            platform=builder.make_data_platform_urn(
                "hive"
            ),  # important <- platform must be an urn
            version=0,
            # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
            hash="",
            # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="first_name",
                    globalTags=models.GlobalTagsClass(
                        tags=[
                            models.TagAssociationClass(tag=builder.make_tag_urn("pii"))
                        ],
                    ),
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="mobile_number",
                    globalTags=models.GlobalTagsClass(
                        tags=[
                            models.TagAssociationClass(tag=builder.make_tag_urn("pii"))
                        ],
                    ),
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
            ],
        )

    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternAddDatasetSchemaTags,
        pipeline_context=pipeline_context,
        config={
            "semantics": semantics,
            "tag_pattern": {
                "rules": {
                    ".*first_name.*": [
                        builder.make_tag_urn("Name"),
                        builder.make_tag_urn("FirstName"),
                    ],
                    ".*last_name.*": [
                        builder.make_tag_urn("Name"),
                        builder.make_tag_urn("LastName"),
                    ],
                }
            },
        },
        aspect=models.SchemaMetadataClass(
            schemaName="customer",  # not used
            platform=builder.make_data_platform_urn(
                "hive"
            ),  # important <- platform must be an urn
            version=0,
            # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
            hash="",
            # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="address",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="first_name",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="last_name",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
            ],
        ),
    )
    return output


def test_pattern_dataset_schema_tags_transformation_overwrite(
    mock_time, mock_datahub_graph_instance
):
    output = run_pattern_dataset_schema_tags_transformation_semantics(
        TransformerSemantics.OVERWRITE, mock_datahub_graph_instance
    )

    assert len(output) == 2
    # Check that glossary terms were added.
    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.SchemaMetadataClass)
    transform_aspect = cast(models.SchemaMetadataClass, output[0].record.aspect)
    field_path_vs_field: Dict[str, models.SchemaFieldClass] = {
        field.fieldPath: field for field in transform_aspect.fields
    }

    assert (
        field_path_vs_field.get("mobile_number") is None
    )  # server field should not be preserved during overwrite

    assert field_path_vs_field["first_name"].globalTags is not None
    assert len(field_path_vs_field["first_name"].globalTags.tags) == 2
    global_tags_urn = [
        tag.tag for tag in field_path_vs_field["first_name"].globalTags.tags
    ]
    assert builder.make_tag_urn("pii") not in global_tags_urn
    assert builder.make_tag_urn("FirstName") in global_tags_urn
    assert builder.make_tag_urn("Name") in global_tags_urn


def test_pattern_dataset_schema_tags_transformation_patch(
    mock_time, mock_datahub_graph_instance
):
    output = run_pattern_dataset_schema_tags_transformation_semantics(
        TransformerSemantics.PATCH, mock_datahub_graph_instance
    )

    assert len(output) == 2
    # Check that global tags were added.
    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.SchemaMetadataClass)
    transform_aspect = cast(models.SchemaMetadataClass, output[0].record.aspect)
    field_path_vs_field: Dict[str, models.SchemaFieldClass] = {
        field.fieldPath: field for field in transform_aspect.fields
    }

    assert (
        field_path_vs_field.get("mobile_number") is not None
    )  # server field should be preserved during patch

    assert field_path_vs_field["first_name"].globalTags is not None
    assert len(field_path_vs_field["first_name"].globalTags.tags) == 3
    global_tags_urn = [
        tag.tag for tag in field_path_vs_field["first_name"].globalTags.tags
    ]
    assert builder.make_tag_urn("pii") in global_tags_urn
    assert builder.make_tag_urn("FirstName") in global_tags_urn
    assert builder.make_tag_urn("Name") in global_tags_urn


def test_simple_dataset_data_product_transformation(mock_time):
    transformer = SimpleAddDatasetDataProduct.create(
        {
            "dataset_to_data_product_urns": {
                builder.make_dataset_urn(
                    "bigquery", "example1"
                ): "urn:li:dataProduct:first",
                builder.make_dataset_urn(
                    "bigquery", "example2"
                ): "urn:li:dataProduct:second",
                builder.make_dataset_urn(
                    "bigquery", "example3"
                ): "urn:li:dataProduct:first",
            }
        },
        PipelineContext(run_id="test-dataproduct"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [
                    make_generic_dataset(
                        entity_urn=builder.make_dataset_urn("bigquery", "example1")
                    ),
                    make_generic_dataset(
                        entity_urn=builder.make_dataset_urn("bigquery", "example2")
                    ),
                    make_generic_dataset(
                        entity_urn=builder.make_dataset_urn("bigquery", "example3")
                    ),
                    EndOfStream(),
                ]
            ]
        )
    )

    assert len(outputs) == 6

    # Check new dataproduct entity should be there
    assert outputs[3].record.entityUrn == "urn:li:dataProduct:first"
    assert outputs[3].record.aspectName == "dataProductProperties"

    first_data_product_aspect = json.loads(
        outputs[3].record.aspect.value.decode("utf-8")
    )
    assert [item["value"]["destinationUrn"] for item in first_data_product_aspect] == [
        builder.make_dataset_urn("bigquery", "example1"),
        builder.make_dataset_urn("bigquery", "example3"),
    ]

    second_data_product_aspect = json.loads(
        outputs[4].record.aspect.value.decode("utf-8")
    )
    assert [item["value"]["destinationUrn"] for item in second_data_product_aspect] == [
        builder.make_dataset_urn("bigquery", "example2")
    ]

    assert isinstance(outputs[5].record, EndOfStream)


def test_pattern_dataset_data_product_transformation(mock_time):
    transformer = PatternAddDatasetDataProduct.create(
        {
            "dataset_to_data_product_urns_pattern": {
                "rules": {
                    ".*example1.*": "urn:li:dataProduct:first",
                    ".*": "urn:li:dataProduct:second",
                }
            },
        },
        PipelineContext(run_id="test-dataproducts"),
    )

    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [
                    make_generic_dataset(
                        entity_urn=builder.make_dataset_urn("bigquery", "example1")
                    ),
                    make_generic_dataset(
                        entity_urn=builder.make_dataset_urn("bigquery", "example2")
                    ),
                    make_generic_dataset(
                        entity_urn=builder.make_dataset_urn("bigquery", "example3")
                    ),
                    EndOfStream(),
                ]
            ]
        )
    )

    assert len(outputs) == 6

    # Check new dataproduct entity should be there
    assert outputs[3].record.entityUrn == "urn:li:dataProduct:first"
    assert outputs[3].record.aspectName == "dataProductProperties"

    first_data_product_aspect = json.loads(
        outputs[3].record.aspect.value.decode("utf-8")
    )
    assert [item["value"]["destinationUrn"] for item in first_data_product_aspect] == [
        builder.make_dataset_urn("bigquery", "example1")
    ]

    second_data_product_aspect = json.loads(
        outputs[4].record.aspect.value.decode("utf-8")
    )
    assert [item["value"]["destinationUrn"] for item in second_data_product_aspect] == [
        builder.make_dataset_urn("bigquery", "example2"),
        builder.make_dataset_urn("bigquery", "example3"),
    ]

    assert isinstance(outputs[5].record, EndOfStream)


def dummy_data_product_resolver_method(dataset_urn):
    dataset_to_data_product_map = {
        builder.make_dataset_urn("bigquery", "example1"): "urn:li:dataProduct:first"
    }
    return dataset_to_data_product_map.get(dataset_urn)


def test_add_dataset_data_product_transformation():
    transformer = AddDatasetDataProduct.create(
        {
            "get_data_product_to_add": "tests.unit.test_transform_dataset.dummy_data_product_resolver_method"
        },
        PipelineContext(run_id="test-dataproduct"),
    )
    outputs = list(
        transformer.transform(
            [
                RecordEnvelope(input, metadata={})
                for input in [make_generic_dataset(), EndOfStream()]
            ]
        )
    )
    # Check new dataproduct entity should be there
    assert outputs[1].record.entityUrn == "urn:li:dataProduct:first"
    assert outputs[1].record.aspectName == "dataProductProperties"

    first_data_product_aspect = json.loads(
        outputs[1].record.aspect.value.decode("utf-8")
    )
    assert [item["value"]["destinationUrn"] for item in first_data_product_aspect] == [
        builder.make_dataset_urn("bigquery", "example1")
    ]


def _test_clean_owner_urns(
    in_pipeline_context: Any,
    in_owners: List[str],
    config: List[Union[re.Pattern, str]],
    cleaned_owner_urn: List[str],
) -> None:
    # Return fake aspect to simulate server behaviour
    def fake_ownership_class(entity_urn: str) -> models.OwnershipClass:
        return models.OwnershipClass(
            owners=[
                models.OwnerClass(owner=owner, type=models.OwnershipTypeClass.DATAOWNER)
                for owner in in_owners
            ]
        )

    in_pipeline_context.graph.get_ownership = fake_ownership_class  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternCleanUpOwnership,
        aspect=models.OwnershipClass(
            owners=[
                models.OwnerClass(owner=owner, type=models.OwnershipTypeClass.DATAOWNER)
                for owner in in_owners
            ]
        ),
        config={"pattern_for_cleanup": config},
        pipeline_context=in_pipeline_context,
    )

    assert len(output) == 2
    ownership_aspect = output[0].record.aspect
    assert isinstance(ownership_aspect, OwnershipClass)
    assert len(ownership_aspect.owners) == len(in_owners)

    out_owners = [owner.owner for owner in ownership_aspect.owners]
    assert set(out_owners) == set(cleaned_owner_urn)


def test_clean_owner_urn_transformation_remove_fixed_string(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # remove 'ABCDEF:'
    config: List[Union[re.Pattern, str]] = ["ABCDEF:"]
    expected_user_emails: List[str] = [
        "email_id@example.com",
        "123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_remove_multiple_values(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # remove multiple values
    config: List[Union[re.Pattern, str]] = ["ABCDEF:", "email"]
    expected_user_emails: List[str] = [
        "_id@example.com",
        "123_id@example.com",
        "_id@example.co.in",
        "_id@example.co.uk",
        "_test:XYZ@example.com",
        "_id:id1@example.com",
        "_id:id2@example.com",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_remove_values_using_regex(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # remove words after `_` using RegEx i.e. `id`, `test`
    config: List[Union[re.Pattern, str]] = [r"(?<=_)(\w+)"]
    expected_user_emails: List[str] = [
        "ABCDEF:email_@example.com",
        "ABCDEF:123email_@example.com",
        "email_@example.co.in",
        "email_@example.co.uk",
        "email_:XYZ@example.com",
        "email_:id1@example.com",
        "email_:id2@example.com",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_remove_digits(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # remove digits
    config: List[Union[re.Pattern, str]] = [r"\d+"]
    expected_user_emails: List[str] = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id@example.com",
        "email_id:id@example.com",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_remove_pattern(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # remove `example.*`
    config: List[Union[re.Pattern, str]] = [r"@example\.\S*"]
    expected_user_emails: List[str] = [
        "ABCDEF:email_id",
        "ABCDEF:123email_id",
        "email_id",
        "email_id",
        "email_test:XYZ",
        "email_id:id1",
        "email_id:id2",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_remove_word_in_capital_letters(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
        "email_test:XYabZ@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # if string between `:` and `@` is in CAPITAL then remove it
    config: List[Union[re.Pattern, str]] = ["(?<=:)[A-Z]+(?=@)"]
    expected_user_emails: List[str] = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
        "email_test:XYabZ@example.com",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_remove_pattern_with_alphanumeric_value(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # remove any pattern having `id` followed by any digits
    config: List[Union[re.Pattern, str]] = [r"id\d+"]
    expected_user_emails: List[str] = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:@example.com",
        "email_id:@example.com",
    ]
    expected_owner_urns: List[str] = []
    for user in expected_user_emails:
        expected_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )
    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, expected_owner_urns)


def test_clean_owner_urn_transformation_should_not_remove_system_identifier(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    user_emails = [
        "ABCDEF:email_id@example.com",
        "ABCDEF:123email_id@example.com",
        "email_id@example.co.in",
        "email_id@example.co.uk",
        "email_test:XYZ@example.com",
        "email_id:id1@example.com",
        "email_id:id2@example.com",
    ]

    in_owner_urns: List[str] = []
    for user in user_emails:
        in_owner_urns.append(
            builder.make_owner_urn(user, owner_type=builder.OwnerType.USER)
        )

    # should not remove system identifier
    config: List[Union[re.Pattern, str]] = ["urn:li:corpuser:"]

    _test_clean_owner_urns(pipeline_context, in_owner_urns, config, in_owner_urns)


def test_replace_external_url_word_replace(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_replace_external_url"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=ReplaceExternalUrlDataset,
        aspect=models.DatasetPropertiesClass(
            externalUrl="https://github.com/datahub/looker-demo/blob/master/foo.view.lkml",
            customProperties=EXISTING_PROPERTIES.copy(),
        ),
        config={"input_pattern": "datahub", "replacement": "starhub"},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert (
        output[0].record.aspect.externalUrl
        == "https://github.com/starhub/looker-demo/blob/master/foo.view.lkml"
    )


def test_replace_external_regex_replace_1(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_replace_external_url"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=ReplaceExternalUrlDataset,
        aspect=models.DatasetPropertiesClass(
            externalUrl="https://github.com/datahub/looker-demo/blob/master/foo.view.lkml",
            customProperties=EXISTING_PROPERTIES.copy(),
        ),
        config={"input_pattern": r"datahub/.*/", "replacement": "starhub/test/"},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert (
        output[0].record.aspect.externalUrl
        == "https://github.com/starhub/test/foo.view.lkml"
    )


def test_replace_external_regex_replace_2(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_replace_external_url"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_dataset_transformer_pipeline(
        transformer_type=ReplaceExternalUrlDataset,
        aspect=models.DatasetPropertiesClass(
            externalUrl="https://github.com/datahub/looker-demo/blob/master/foo.view.lkml",
            customProperties=EXISTING_PROPERTIES.copy(),
        ),
        config={"input_pattern": r"\b\w*hub\b", "replacement": "test"},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert (
        output[0].record.aspect.externalUrl
        == "https://test.com/test/looker-demo/blob/master/foo.view.lkml"
    )


def test_pattern_cleanup_usage_statistics_user_1(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_pattern_cleanup_usage_statistics_user"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    TS_1 = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternCleanupDatasetUsageUser,
        aspect=models.DatasetUsageStatisticsClass(
            timestampMillis=int(TS_1.timestamp() * 1000),
            userCounts=[
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn("IAM:user1"),
                    count=1,
                    userEmail="user1@exaple.com",
                ),
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn("user2"),
                    count=2,
                    userEmail="user2@exaple.com",
                ),
            ],
        ),
        config={"pattern_for_cleanup": ["IAM:"]},
        pipeline_context=pipeline_context,
    )

    expectedUsageStatistics = models.DatasetUsageStatisticsClass(
        timestampMillis=int(TS_1.timestamp() * 1000),
        userCounts=[
            DatasetUserUsageCountsClass(
                user=builder.make_user_urn("user1"),
                count=1,
                userEmail="user1@exaple.com",
            ),
            DatasetUserUsageCountsClass(
                user=builder.make_user_urn("user2"),
                count=2,
                userEmail="user2@exaple.com",
            ),
        ],
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert len(output[0].record.aspect.userCounts) == 2
    assert output[0].record.aspect.userCounts == expectedUsageStatistics.userCounts


def test_pattern_cleanup_usage_statistics_user_2(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_pattern_cleanup_usage_statistics_user"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    TS_1 = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternCleanupDatasetUsageUser,
        aspect=models.DatasetUsageStatisticsClass(
            timestampMillis=int(TS_1.timestamp() * 1000),
            userCounts=[
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn("test_user_1"),
                    count=1,
                    userEmail="user1@exaple.com",
                ),
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn("test_user_2"),
                    count=2,
                    userEmail="user2@exaple.com",
                ),
            ],
        ),
        config={"pattern_for_cleanup": ["_user"]},
        pipeline_context=pipeline_context,
    )

    expectedUsageStatistics = models.DatasetUsageStatisticsClass(
        timestampMillis=int(TS_1.timestamp() * 1000),
        userCounts=[
            DatasetUserUsageCountsClass(
                user=builder.make_user_urn("test_1"),
                count=1,
                userEmail="user1@exaple.com",
            ),
            DatasetUserUsageCountsClass(
                user=builder.make_user_urn("test_2"),
                count=2,
                userEmail="user2@exaple.com",
            ),
        ],
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert len(output[0].record.aspect.userCounts) == 2
    assert output[0].record.aspect.userCounts == expectedUsageStatistics.userCounts


def test_pattern_cleanup_usage_statistics_user_3(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_pattern_cleanup_usage_statistics_user"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    TS_1 = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

    output = run_dataset_transformer_pipeline(
        transformer_type=PatternCleanupDatasetUsageUser,
        aspect=models.DatasetUsageStatisticsClass(
            timestampMillis=int(TS_1.timestamp() * 1000),
            userCounts=[
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn("abc_user_1"),
                    count=1,
                    userEmail="user1@exaple.com",
                ),
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn("xyz_user_2"),
                    count=2,
                    userEmail="user2@exaple.com",
                ),
            ],
        ),
        config={"pattern_for_cleanup": [r"_user_\d+"]},
        pipeline_context=pipeline_context,
    )

    expectedUsageStatistics = models.DatasetUsageStatisticsClass(
        timestampMillis=int(TS_1.timestamp() * 1000),
        userCounts=[
            DatasetUserUsageCountsClass(
                user=builder.make_user_urn("abc"),
                count=1,
                userEmail="user1@exaple.com",
            ),
            DatasetUserUsageCountsClass(
                user=builder.make_user_urn("xyz"),
                count=2,
                userEmail="user2@exaple.com",
            ),
        ],
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert len(output[0].record.aspect.userCounts) == 2
    assert output[0].record.aspect.userCounts == expectedUsageStatistics.userCounts


def test_domain_mapping_based_on_tags_with_valid_tags(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    server_domain = builder.make_domain_urn("test.io")

    tag_one = builder.make_tag_urn("test:tag_1")

    # Return fake aspect to simulate server behaviour
    def fake_get_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(tags=[TagAssociationClass(tag=tag_one)])

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance

    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=DatasetTagDomainMapper,
        aspect=models.DomainsClass(domains=[server_domain]),
        config={"domain_mapping": {"test:tag_1": acryl_domain}},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0] is not None
    assert output[0].record is not None
    assert isinstance(output[0].record, MetadataChangeProposalWrapper)
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert acryl_domain in transformed_aspect.domains
    assert server_domain not in transformed_aspect.domains


def test_domain_mapping_based_on_tags_with_no_matching_tags(
    mock_datahub_graph_instance,
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    server_domain = builder.make_domain_urn("test.io")
    non_matching_tag = builder.make_tag_urn("nonMatching")

    pipeline_context = PipelineContext(run_id="no_match_pipeline")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(tags=[TagAssociationClass(tag=non_matching_tag)])

    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=DatasetTagDomainMapper,
        aspect=models.DomainsClass(domains=[server_domain]),
        config={
            "domain_mapping": {"test:tag_1": acryl_domain},
        },
        pipeline_context=pipeline_context,
    )
    assert len(output) == 2
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    assert len(output[0].record.aspect.domains) == 1
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert acryl_domain not in transformed_aspect.domains
    assert server_domain in transformed_aspect.domains


def test_domain_mapping_based_on_tags_with_empty_config(mock_datahub_graph_instance):
    some_tag = builder.make_tag_urn("someTag")

    pipeline_context = PipelineContext(run_id="empty_config_pipeline")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(tags=[TagAssociationClass(tag=some_tag)])

    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=DatasetTagDomainMapper,
        aspect=models.DomainsClass(domains=[]),
        config={"domain_mapping": {}},
        pipeline_context=pipeline_context,
    )
    assert len(output) == 2
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    assert len(output[0].record.aspect.domains) == 0


def test_domain_mapping_based__r_on_tags_with_multiple_tags(
    mock_datahub_graph_instance,
):
    # Two tags that match different rules in the domain mapping configuration
    tag_one = builder.make_tag_urn("test:tag_1")
    tag_two = builder.make_tag_urn("test:tag_2")
    existing_domain = builder.make_domain_urn("existing.io")
    finance = builder.make_domain_urn("finance")
    hr = builder.make_domain_urn("hr")

    pipeline_context = PipelineContext(run_id="multiple_matches_pipeline")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(
            tags=[TagAssociationClass(tag=tag_one), TagAssociationClass(tag=tag_two)]
        )

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[existing_domain])

    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore
    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=DatasetTagDomainMapper,
        aspect=models.DomainsClass(domains=[existing_domain]),
        config={
            "domain_mapping": {"test:tag_1": finance, "test:tag_2": hr},
            "semantics": "PATCH",
        },
        pipeline_context=pipeline_context,
    )

    # Assertions to verify the expected outcome
    assert len(output) == 2
    assert output[0].record is not None
    assert output[0].record.aspect is not None
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)

    # Expecting domains from both matched tags
    assert set(output[0].record.aspect.domains) == {existing_domain, finance, hr}
    assert len(transformed_aspect.domains) == 3


def test_domain_mapping_based_on_tags_with_empty_tags(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    server_domain = builder.make_domain_urn("test.io")
    pipeline_context = PipelineContext(run_id="empty_config_pipeline")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(tags=[])

    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=DatasetTagDomainMapper,
        aspect=models.DomainsClass(domains=[acryl_domain]),
        config={"domain_mapping": {"test:tag_1": server_domain}},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    assert len(output[0].record.aspect.domains) == 1
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert acryl_domain in transformed_aspect.domains
    assert server_domain not in transformed_aspect.domains


def test_domain_mapping_based_on_tags_with_no_tags(mock_datahub_graph_instance):
    acryl_domain = builder.make_domain_urn("acryl.io")
    server_domain = builder.make_domain_urn("test.io")
    pipeline_context = PipelineContext(run_id="empty_config_pipeline")
    pipeline_context.graph = mock_datahub_graph_instance

    # Return fake aspect to simulate server behaviour
    def fake_get_tags(entity_urn: str) -> Optional[models.GlobalTagsClass]:
        return None

    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=DatasetTagDomainMapper,
        aspect=models.DomainsClass(domains=[acryl_domain]),
        config={"domain_mapping": {"test:tag_1": server_domain}},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert isinstance(output[0].record.aspect, models.DomainsClass)
    assert len(output[0].record.aspect.domains) == 1
    transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
    assert len(transformed_aspect.domains) == 1
    assert acryl_domain in transformed_aspect.domains
    assert server_domain not in transformed_aspect.domains


def test_tags_to_terms_transformation(mock_datahub_graph_instance):
    # Create domain URNs for the test
    term_urn_example1 = builder.make_term_urn("example1")
    term_urn_example2 = builder.make_term_urn("example2")

    def fake_get_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=builder.make_tag_urn("example1")),
                TagAssociationClass(tag=builder.make_tag_urn("example2")),
            ]
        )

    # fake the server response
    def fake_schema_metadata(entity_urn: str) -> models.SchemaMetadataClass:
        return models.SchemaMetadataClass(
            schemaName="customer",  # not used
            platform=builder.make_data_platform_urn(
                "hive"
            ),  # important <- platform must be an urn
            version=0,
            # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
            hash="",
            # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="first_name",
                    globalTags=models.GlobalTagsClass(
                        tags=[
                            models.TagAssociationClass(
                                tag=builder.make_tag_urn("example2")
                            )
                        ],
                    ),
                    glossaryTerms=models.GlossaryTermsClass(
                        terms=[
                            models.GlossaryTermAssociationClass(
                                urn=builder.make_term_urn("pii")
                            )
                        ],
                        auditStamp=models.AuditStampClass._construct_with_defaults(),
                    ),
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="mobile_number",
                    glossaryTerms=models.GlossaryTermsClass(
                        terms=[
                            models.GlossaryTermAssociationClass(
                                urn=builder.make_term_urn("pii")
                            )
                        ],
                        auditStamp=models.AuditStampClass._construct_with_defaults(),
                    ),
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
            ],
        )

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_tags = fake_get_tags  # type: ignore
    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore

    # Configuring the transformer
    config = {"tags": ["example1", "example2"]}

    # Running the transformer within a test pipeline
    output = run_dataset_transformer_pipeline(
        transformer_type=TagsToTermMapper,
        aspect=models.GlossaryTermsClass(
            terms=[
                models.GlossaryTermAssociationClass(urn=builder.make_term_urn("pii"))
            ],
            auditStamp=models.AuditStampClass._construct_with_defaults(),
        ),
        config=config,
        pipeline_context=pipeline_context,
    )

    # Expected results
    expected_terms = [term_urn_example2, term_urn_example1]

    # Verify the output
    assert len(output) == 2  # One for result and one for end of stream
    terms_aspect = output[0].record.aspect
    assert isinstance(terms_aspect, models.GlossaryTermsClass)
    assert len(terms_aspect.terms) == len(expected_terms)
    assert set(term.urn for term in terms_aspect.terms) == {
        "urn:li:glossaryTerm:example1",
        "urn:li:glossaryTerm:example2",
    }


def test_tags_to_terms_with_no_matching_terms(mock_datahub_graph_instance):
    # Setup for test where no tags match the provided term mappings
    def fake_get_tags_no_match(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=builder.make_tag_urn("nonMatchingTag1")),
                TagAssociationClass(tag=builder.make_tag_urn("nonMatchingTag2")),
            ]
        )

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_tags = fake_get_tags_no_match  # type: ignore

    # No matching terms in config
    config = {"tags": ["example1", "example2"]}

    # Running the transformer within a test pipeline
    output = run_dataset_transformer_pipeline(
        transformer_type=TagsToTermMapper,
        aspect=models.GlossaryTermsClass(
            terms=[
                models.GlossaryTermAssociationClass(urn=builder.make_term_urn("pii"))
            ],
            auditStamp=models.AuditStampClass._construct_with_defaults(),
        ),
        config=config,
        pipeline_context=pipeline_context,
    )

    # Verify the output
    assert len(output) == 2  # One for result and one for end of stream
    terms_aspect = output[0].record.aspect
    assert isinstance(terms_aspect, models.GlossaryTermsClass)
    assert len(terms_aspect.terms) == 1


def test_tags_to_terms_with_missing_tags(mock_datahub_graph_instance):
    # Setup for test where no tags are present
    def fake_get_no_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(tags=[])

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_tags = fake_get_no_tags  # type: ignore

    config = {"tags": ["example1", "example2"]}

    # Running the transformer with no tags
    output = run_dataset_transformer_pipeline(
        transformer_type=TagsToTermMapper,
        aspect=models.GlossaryTermsClass(
            terms=[
                models.GlossaryTermAssociationClass(urn=builder.make_term_urn("pii"))
            ],
            auditStamp=models.AuditStampClass._construct_with_defaults(),
        ),
        config=config,
        pipeline_context=pipeline_context,
    )

    # Verify that no terms are added when there are no tags
    assert len(output) == 2
    terms_aspect = output[0].record.aspect
    assert isinstance(terms_aspect, models.GlossaryTermsClass)
    assert len(terms_aspect.terms) == 1


def test_tags_to_terms_with_partial_match(mock_datahub_graph_instance):
    # Setup for partial match scenario
    def fake_get_partial_match_tags(entity_urn: str) -> models.GlobalTagsClass:
        return models.GlobalTagsClass(
            tags=[
                TagAssociationClass(
                    tag=builder.make_tag_urn("example1")
                ),  # Should match
                TagAssociationClass(
                    tag=builder.make_tag_urn("nonMatchingTag")
                ),  # No match
            ]
        )

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph_instance
    pipeline_context.graph.get_tags = fake_get_partial_match_tags  # type: ignore

    config = {"tags": ["example1"]}  # Only 'example1' has a term mapped

    # Running the transformer with partial matching tags
    output = run_dataset_transformer_pipeline(
        transformer_type=TagsToTermMapper,
        aspect=models.GlossaryTermsClass(
            terms=[
                models.GlossaryTermAssociationClass(urn=builder.make_term_urn("pii"))
            ],
            auditStamp=models.AuditStampClass._construct_with_defaults(),
        ),
        config=config,
        pipeline_context=pipeline_context,
    )

    # Verify that only matched term is added
    assert len(output) == 2
    terms_aspect = output[0].record.aspect
    assert isinstance(terms_aspect, models.GlossaryTermsClass)
    assert len(terms_aspect.terms) == 1
    assert terms_aspect.terms[0].urn == "urn:li:glossaryTerm:example1"


def test_replace_external_url_container_word_replace(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_replace_external_url_container"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_container_transformer_pipeline(
        transformer_type=ReplaceExternalUrlContainer,
        aspect=models.ContainerPropertiesClass(
            externalUrl="https://github.com/datahub/looker-demo/blob/master/foo.view.lkml",
            customProperties=EXISTING_PROPERTIES.copy(),
            name="sample_test",
        ),
        config={"input_pattern": "datahub", "replacement": "starhub"},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert (
        output[0].record.aspect.externalUrl
        == "https://github.com/starhub/looker-demo/blob/master/foo.view.lkml"
    )


def test_replace_external_regex_container_replace_1(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_replace_external_url_container"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_container_transformer_pipeline(
        transformer_type=ReplaceExternalUrlContainer,
        aspect=models.ContainerPropertiesClass(
            externalUrl="https://github.com/datahub/looker-demo/blob/master/foo.view.lkml",
            customProperties=EXISTING_PROPERTIES.copy(),
            name="sample_test",
        ),
        config={"input_pattern": r"datahub/.*/", "replacement": "starhub/test/"},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert (
        output[0].record.aspect.externalUrl
        == "https://github.com/starhub/test/foo.view.lkml"
    )


def test_replace_external_regex_container_replace_2(
    mock_datahub_graph_instance,
):
    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_replace_external_url_container"
    )
    pipeline_context.graph = mock_datahub_graph_instance

    output = run_container_transformer_pipeline(
        transformer_type=ReplaceExternalUrlContainer,
        aspect=models.ContainerPropertiesClass(
            externalUrl="https://github.com/datahub/looker-demo/blob/master/foo.view.lkml",
            customProperties=EXISTING_PROPERTIES.copy(),
            name="sample_test",
        ),
        config={"input_pattern": r"\b\w*hub\b", "replacement": "test"},
        pipeline_context=pipeline_context,
    )

    assert len(output) == 2
    assert output[0].record
    assert output[0].record.aspect
    assert (
        output[0].record.aspect.externalUrl
        == "https://test.com/test/looker-demo/blob/master/foo.view.lkml"
    )
