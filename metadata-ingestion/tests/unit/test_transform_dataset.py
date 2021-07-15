import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.add_dataset_ownership import (
    SimpleAddDatasetOwnership,
)
from datahub.ingestion.transformer.add_dataset_tags import (
    AddDatasetTags,
    SimpleAddDatasetTags,
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


def test_simple_dataset_ownership_tranformation(mock_time):
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

    # Check the second entry.
    second_ownership_aspect = builder.get_aspect_if_available(
        outputs[1].record, models.OwnershipClass
    )
    assert second_ownership_aspect
    assert len(second_ownership_aspect.owners) == 3

    # Verify that the third entry is unchanged.
    assert inputs[2] == outputs[2].record


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
