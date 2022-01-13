import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipClass,
)


def test_can_add_aspect():
    dataset_mce: MetadataChangeEventClass = builder.make_lineage_mce(
        [
            builder.make_dataset_urn("bigquery", "upstream1"),
            builder.make_dataset_urn("bigquery", "upstream2"),
        ],
        builder.make_dataset_urn("bigquery", "downstream"),
    )
    assert isinstance(dataset_mce.proposedSnapshot, DatasetSnapshotClass)

    assert builder.can_add_aspect(dataset_mce, DatasetPropertiesClass)
    assert builder.can_add_aspect(dataset_mce, OwnershipClass)
    assert not builder.can_add_aspect(dataset_mce, DataFlowInfoClass)


def test_guid_generator():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="PROD"
    )

    guid = key.guid()
    assert guid == "06a1f87f99e1d82efa3da13637913c87"


def test_guid_generator_with_empty_instance():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance=None
    )

    guid = key.guid()
    assert guid == "0ce13865e9e414406a895612787d9ae6"
