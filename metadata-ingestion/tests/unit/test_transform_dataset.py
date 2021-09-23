import pytest

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.add_dataset_browse_path import (
    AddDatasetBrowsePathTransformer,
)
from datahub.ingestion.transformer.add_dataset_ownership import (
    PatternAddDatasetOwnership,
    SimpleAddDatasetOwnership,
)
from datahub.ingestion.transformer.add_dataset_tags import (
    AddDatasetTags,
    SimpleAddDatasetTags,
)
from datahub.ingestion.transformer.mark_dataset_status import MarkDatasetStatus
from datahub.ingestion.transformer.remove_dataset_ownership import (
    SimpleRemoveDatasetOwnership,
)


def make_generic_dataset():
    return models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
            aspects=[
                models.StatusClass(removed=False),
            ],
        ),
    )


def make_dataset_with_owner():
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

    inputs = [
        no_owner_aspect,
        with_owner_aspect,
        not_a_dataset,
    ]

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

    assert len(outputs) == len(inputs)

    # Check the first entry.
    first_ownership_aspect = builder.get_aspect_if_available(
        outputs[0].record, models.OwnershipClass
    )
    assert first_ownership_aspect
    assert len(first_ownership_aspect.owners) == 2
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
    assert len(second_ownership_aspect.owners) == 3
    assert all(
        [
            owner.type == models.OwnershipTypeClass.DATAOWNER
            for owner in first_ownership_aspect.owners
        ]
    )

    # Verify that the third entry is unchanged.
    assert inputs[2] == outputs[2].record


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

    output = list(transformer.transform([RecordEnvelope(input, metadata={})]))

    assert len(output) == 1

    ownership_aspect = builder.get_aspect_if_available(
        output[0].record, models.OwnershipClass
    )
    assert ownership_aspect
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


def test_mark_status_dataset():
    dataset = make_generic_dataset()

    transformer = MarkDatasetStatus.create(
        {"removed": True},
        PipelineContext(run_id="test"),
    )
    removed = list(transformer.transform([RecordEnvelope(dataset, metadata={})]))
    status_aspect = builder.get_aspect_if_available(
        removed[0].record, models.StatusClass
    )
    assert status_aspect
    assert status_aspect.removed is True

    transformer = MarkDatasetStatus.create(
        {"removed": False},
        PipelineContext(run_id="test"),
    )
    not_removed = list(transformer.transform([RecordEnvelope(dataset, metadata={})]))
    status_aspect = builder.get_aspect_if_available(
        not_removed[0].record, models.StatusClass
    )
    assert status_aspect
    assert status_aspect.removed is False


def test_add_dataset_browse_paths():
    dataset = make_generic_dataset()

    transformer = AddDatasetBrowsePathTransformer.create(
        {"path_templates": ["/abc"]},
        PipelineContext(run_id="test"),
    )
    transformed = list(transformer.transform([RecordEnvelope(dataset, metadata={})]))
    browse_path_aspect = builder.get_aspect_if_available(
        transformed[0].record, models.BrowsePathsClass
    )
    assert browse_path_aspect
    assert browse_path_aspect.paths == ["/abc"]

    transformer = AddDatasetBrowsePathTransformer.create(
        {
            "path_templates": [
                "/PLATFORM/foo/DATASET_PARTS/ENV",
                "/ENV/PLATFORM/bar/DATASET_PARTS/",
            ]
        },
        PipelineContext(run_id="test"),
    )
    transformed = list(transformer.transform([RecordEnvelope(dataset, metadata={})]))
    browse_path_aspect = builder.get_aspect_if_available(
        transformed[0].record, models.BrowsePathsClass
    )
    assert browse_path_aspect
    assert browse_path_aspect.paths == [
        "/abc",
        "/bigquery/foo/example1/prod",
        "/prod/bigquery/bar/example1/",
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
            [RecordEnvelope(input, metadata={}) for input in [dataset_mce]]
        )
    )
    assert len(outputs) == 1

    # Check that tags were added.
    tags_aspect = builder.get_aspect_if_available(
        outputs[0].record, models.GlobalTagsClass
    )
    assert tags_aspect
    assert len(tags_aspect.tags) == 2
    assert tags_aspect.tags[0].tag == builder.make_tag_urn("NeedsDocumentation")


def dummy_tag_resolver_method(dataset_snapshot):
    return []


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

    inputs = [
        no_owner_aspect,
        with_owner_aspect,
        not_a_dataset,
    ]

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

    assert len(outputs) == len(inputs)

    # Check the first entry.
    first_ownership_aspect = builder.get_aspect_if_available(
        outputs[0].record, models.OwnershipClass
    )
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
            for owner in first_ownership_aspect.owners
        ]
    )

    # Verify that the third entry is unchanged.
    assert inputs[2] == outputs[2].record


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

    output = list(transformer.transform([RecordEnvelope(input, metadata={})]))

    assert len(output) == 1

    ownership_aspect = builder.get_aspect_if_available(
        output[0].record, models.OwnershipClass
    )
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
