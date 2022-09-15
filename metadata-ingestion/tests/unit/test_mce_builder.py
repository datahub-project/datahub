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


def test_create_dataset_urn_with_reserved_chars() -> None:
    assert (
        builder.make_dataset_urn_with_platform_instance(
            "platform)",
            "table_(name)",
            "platform,instance",
            builder.DEFAULT_ENV,
        )
        == "urn:li:dataset:(urn:li:dataPlatform:platform%29,platform%2Cinstance.table_%28name%29,PROD)"
    )
