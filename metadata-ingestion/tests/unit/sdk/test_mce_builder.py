from datetime import datetime, timezone

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


def test_make_user_urn() -> None:
    assert builder.make_user_urn("someUser") == "urn:li:corpuser:someUser"
    assert (
        builder.make_user_urn("urn:li:corpuser:someUser") == "urn:li:corpuser:someUser"
    )
    assert (
        builder.make_user_urn("urn:li:corpGroup:someGroup")
        == "urn:li:corpGroup:someGroup"
    )


def test_make_group_urn() -> None:
    assert builder.make_group_urn("someGroup") == "urn:li:corpGroup:someGroup"
    assert (
        builder.make_group_urn("urn:li:corpGroup:someGroup")
        == "urn:li:corpGroup:someGroup"
    )
    assert (
        builder.make_group_urn("urn:li:corpuser:someUser") == "urn:li:corpuser:someUser"
    )


def test_ts_millis() -> None:
    assert builder.make_ts_millis(None) is None
    assert builder.parse_ts_millis(None) is None

    assert (
        builder.make_ts_millis(datetime(2024, 1, 1, 2, 3, 4, 5, timezone.utc))
        == 1704074584000
    )

    # We only have millisecond precision, don't support microseconds.
    ts = datetime.now(timezone.utc).replace(microsecond=0)
    ts_millis = builder.make_ts_millis(ts)
    assert builder.parse_ts_millis(ts_millis) == ts
