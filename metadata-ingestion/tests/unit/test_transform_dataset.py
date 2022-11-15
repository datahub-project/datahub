import re
from typing import (
    Any,
    Callable,
    Dict,
    List,
    MutableSequence,
    Optional,
    Type,
    Union,
    cast,
)
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
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.transformer.add_dataset_browse_path import (
    AddDatasetBrowsePathTransformer,
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
from datahub.ingestion.transformer.dataset_domain import SimpleAddDatasetDomain
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.ingestion.transformer.mark_dataset_status import MarkDatasetStatus
from datahub.ingestion.transformer.remove_dataset_ownership import (
    SimpleRemoveDatasetOwnership,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnershipClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn


def make_generic_dataset(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
    aspects: List[Any] = [models.StatusClass(removed=False)],
) -> models.MetadataChangeEventClass:
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
        entityType=Urn.create_from_string(entity_urn).get_type(),
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
            workunit.MetadataWorkUnit(
                id=f"test-workunit-mce-{e.proposedSnapshot.urn}", mce=e
            )
            if isinstance(e, MetadataChangeEventClass)
            else workunit.MetadataWorkUnit(
                id=f"test-workunit-mcp-{e.entityUrn}-{e.aspectName}", mcp=e
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


def test_simple_dataset_ownership_transformation(mock_time):
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

    assert len(outputs) == len(inputs) + 1

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
            owner.type == models.OwnershipTypeClass.DATAOWNER
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
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in second_ownership_aspect.owners
        ]
    )

    # Verify that the third entry is unchanged.
    assert inputs[2] == outputs[2].record

    # Verify that the last entry is EndOfStream
    assert inputs[3] == outputs[4].record


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
    assert len(outputs) == 3

    # Check that tags were added.
    tags_aspect = outputs[1].record.aspect
    assert tags_aspect
    assert len(tags_aspect.tags) == 2
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("NeedsDocumentation")


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

    assert len(outputs) == 3
    tags_aspect = outputs[1].record.aspect
    assert tags_aspect
    assert len(tags_aspect.tags) == 2
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("Private")
    assert builder.make_tag_urn("Needs Documentation") not in tags_aspect.tags


def test_import_resolver():
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
                }
            },
        },
        PipelineContext(run_id="test"),
    )

    outputs = list(
        transformer.transform([RecordEnvelope(input, metadata={}) for input in inputs])
    )

    assert len(outputs) == len(inputs) + 1  # additional MCP due to the no-owner MCE

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

    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
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
    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
        mock_graph, "test_urn", mce_ownership
    )
    assert test_ownership and test_ownership.owners
    assert "foo" in [o.owner for o in test_ownership.owners]
    assert "baz" in [o.owner for o in test_ownership.owners]

    server_ownership = gen_owners(["baz", "foo"])
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
        mock_graph, "test_urn", None
    )
    assert not test_ownership


def test_ownership_patching_with_empty_mce_none_server(mock_time):
    mock_graph = mock.MagicMock()
    mce_ownership = gen_owners([])
    mock_graph.get_ownership.return_value = None
    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
        mock_graph, "test_urn", mce_ownership
    )
    # nothing to add, so we omit writing
    assert test_ownership is None


def test_ownership_patching_with_empty_mce_nonempty_server(mock_time):
    mock_graph = mock.MagicMock()
    server_ownership = gen_owners(["baz", "foo"])
    mce_ownership = gen_owners([])
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
        mock_graph, "test_urn", mce_ownership
    )
    # nothing to add, so we omit writing
    assert test_ownership is None


def test_ownership_patching_with_different_types_1(mock_time):
    mock_graph = mock.MagicMock()
    server_ownership = gen_owners(["baz", "foo"], models.OwnershipTypeClass.PRODUCER)
    mce_ownership = gen_owners(["foo"], models.OwnershipTypeClass.DATAOWNER)
    mock_graph.get_ownership.return_value = server_ownership
    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
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
    test_ownership = AddDatasetOwnership.get_patch_ownership_aspect(
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
    mock_datahub_graph: Callable[[DatahubClientConfig], DataHubGraph],
) -> List[RecordEnvelope]:
    pipeline_context = PipelineContext(run_id="test_pattern_dataset_schema_terms")
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

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


def test_simple_add_dataset_properties_overwrite(mock_datahub_graph):
    new_properties = {"new-simple-property": "new-value"}
    server_properties = {"p1": "value1"}

    output = run_simple_add_dataset_properties_transformer_semantics(
        semantics=TransformerSemantics.OVERWRITE,
        new_properties=new_properties,
        server_properties=server_properties,
        mock_datahub_graph=mock_datahub_graph,
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


def test_simple_add_dataset_properties_patch(mock_datahub_graph):
    new_properties = {"new-simple-property": "new-value"}
    server_properties = {"p1": "value1"}

    output = run_simple_add_dataset_properties_transformer_semantics(
        semantics=TransformerSemantics.PATCH,
        new_properties=new_properties,
        server_properties=server_properties,
        mock_datahub_graph=mock_datahub_graph,
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
    assert len(outputs) == 3
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
    assert len(outputs) == 2
    # Check that tags were added, this will be the second result
    tags_aspect = outputs[0].record.aspect
    assert tags_aspect
    assert len(tags_aspect.tags) == 3
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("Test")
    assert tags_aspect.tags[1].tag == builder.make_tag_urn("NeedsDocumentation")
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
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=str(
                DatasetUrn.create_from_ids(
                    platform_id="elasticsearch",
                    table_name=f"fooBarIndex{i}",
                    env="PROD",
                )
            ),
            aspectName="globalTags",
            aspect=GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Test")]),
        )
        for i in range(0, 10)
    ]
    mcps.extend(
        [
            MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=str(
                    DatasetUrn.create_from_ids(
                        platform_id="elasticsearch",
                        table_name=f"fooBarIndex{i}",
                        env="PROD",
                    )
                ),
                aspectName="datasetProperties",
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
        ) == 1


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
    transformer_type: Type[DatasetTransformer],
    aspect: Optional[builder.Aspect],
    config: dict,
    pipeline_context: PipelineContext = PipelineContext(run_id="transformer_pipe_line"),
    use_mce: bool = False,
) -> List[RecordEnvelope]:

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


def test_simple_add_dataset_domain(mock_datahub_graph):
    acryl_domain = builder.make_domain_urn("acryl.io")
    gslab_domain = builder.make_domain_urn("gslab.io")

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[gslab_domain]),
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
    assert gslab_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_mce_support(mock_datahub_graph):
    acryl_domain = builder.make_domain_urn("acryl.io")
    gslab_domain = builder.make_domain_urn("gslab.io")

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=None,
        config={"domains": [gslab_domain, acryl_domain]},
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
    assert gslab_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_replace_existing(mock_datahub_graph):
    acryl_domain = builder.make_domain_urn("acryl.io")
    gslab_domain = builder.make_domain_urn("gslab.io")

    pipeline_context: PipelineContext = PipelineContext(
        run_id="test_simple_add_dataset_domain"
    )
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[gslab_domain]),
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
    assert gslab_domain not in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains


def test_simple_add_dataset_domain_semantics_overwrite(mock_datahub_graph):
    acryl_domain = builder.make_domain_urn("acryl.io")
    gslab_domain = builder.make_domain_urn("gslab.io")
    server_domain = builder.make_domain_urn("test.io")

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[gslab_domain]),
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
    assert gslab_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert server_domain not in transformed_aspect.domains


def test_simple_add_dataset_domain_semantics_patch(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    acryl_domain = builder.make_domain_urn("acryl.io")
    gslab_domain = builder.make_domain_urn("gslab.io")
    server_domain = builder.make_domain_urn("test.io")

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

    # Return fake aspect to simulate server behaviour
    def fake_get_domain(entity_urn: str) -> models.DomainsClass:
        return models.DomainsClass(domains=[server_domain])

    pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

    output = run_dataset_transformer_pipeline(
        transformer_type=SimpleAddDatasetDomain,
        aspect=models.DomainsClass(domains=[gslab_domain]),
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
    assert gslab_domain in transformed_aspect.domains
    assert acryl_domain in transformed_aspect.domains
    assert server_domain in transformed_aspect.domains


def test_simple_dataset_ownership_transformer_semantics_patch(mock_datahub_graph):

    pipeline_context = PipelineContext(run_id="transformer_pipe_line")
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

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


def run_pattern_dataset_schema_terms_transformation_semantics(
    semantics: TransformerSemantics,
    mock_datahub_graph: Callable[[DatahubClientConfig], DataHubGraph],
) -> List[RecordEnvelope]:
    pipeline_context = PipelineContext(run_id="test_pattern_dataset_schema_terms")
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

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
                        auditStamp=models.AuditStampClass.construct_with_defaults(),
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
                        auditStamp=models.AuditStampClass.construct_with_defaults(),
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
    mock_time, mock_datahub_graph
):
    output = run_pattern_dataset_schema_terms_transformation_semantics(
        TransformerSemantics.PATCH, mock_datahub_graph
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
    mock_time, mock_datahub_graph
):
    output = run_pattern_dataset_schema_terms_transformation_semantics(
        TransformerSemantics.OVERWRITE, mock_datahub_graph
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
    mock_datahub_graph: Callable[[DatahubClientConfig], DataHubGraph],
) -> List[RecordEnvelope]:
    pipeline_context = PipelineContext(run_id="test_pattern_dataset_schema_terms")
    pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

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
    mock_time, mock_datahub_graph
):
    output = run_pattern_dataset_schema_tags_transformation_semantics(
        TransformerSemantics.OVERWRITE, mock_datahub_graph
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
    mock_time, mock_datahub_graph
):
    output = run_pattern_dataset_schema_tags_transformation_semantics(
        TransformerSemantics.PATCH, mock_datahub_graph
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
