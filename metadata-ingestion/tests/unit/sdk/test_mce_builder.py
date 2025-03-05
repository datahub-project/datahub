from datetime import datetime, timezone

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipClass,
)
from datahub.utilities.urn_encoder import RESERVED_CHARS_EXTENDED

_RESERVED_CHARS_STRING = "".join(sorted(list(RESERVED_CHARS_EXTENDED)))


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


def test_create_urns_with_reserved_chars() -> None:
    assert (
        builder.make_dataset_urn(
            platform=f"platform){_RESERVED_CHARS_STRING}",
            name=f"table{_RESERVED_CHARS_STRING}",
            env=builder.DEFAULT_ENV,
        )
        == "urn:li:dataset:(urn:li:dataPlatform:platform%29%%28%29%2C%E2%90%9F,table%%28%29%2C%E2%90%9F,PROD)"
    )
    assert (
        builder.make_dataset_urn_with_platform_instance(
            platform=f"platform){_RESERVED_CHARS_STRING}",
            name=f"table{_RESERVED_CHARS_STRING}",
            platform_instance=f"platform-instance{_RESERVED_CHARS_STRING}",
            env=builder.DEFAULT_ENV,
        )
        == "urn:li:dataset:(urn:li:dataPlatform:platform%29%%28%29%2C%E2%90%9F,platform-instance%%28%29%2C%E2%90%9F.table%%28%29%2C%E2%90%9F,PROD)"
    )
    assert (
        builder.make_data_platform_urn(
            f"platform{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:dataPlatform:platform%%28%29%2C%E2%90%9F"
    )
    assert (
        builder.make_data_flow_urn(
            orchestrator=f"orchestrator{_RESERVED_CHARS_STRING}",
            flow_id=f"flowid{_RESERVED_CHARS_STRING}",
            cluster=f"cluster{_RESERVED_CHARS_STRING}",
            platform_instance=f"platform{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:dataFlow:(orchestrator%%28%29%2C%E2%90%9F,platform%%28%29%2C%E2%90%9F.flowid%%28%29%2C%E2%90%9F,cluster%%28%29%2C%E2%90%9F)"
    )
    assert (
        builder.make_data_job_urn(
            orchestrator=f"orchestrator{_RESERVED_CHARS_STRING}",
            flow_id=f"flowid{_RESERVED_CHARS_STRING}",
            cluster=f"cluster{_RESERVED_CHARS_STRING}",
            platform_instance=f"platform{_RESERVED_CHARS_STRING}",
            job_id=f"job_name{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:dataJob:(urn:li:dataFlow:(orchestrator%%28%29%2C%E2%90%9F,platform%%28%29%2C%E2%90%9F.flowid%%28%29%2C%E2%90%9F,cluster%%28%29%2C%E2%90%9F),job_name%%28%29%2C%E2%90%9F)"
    )
    assert (
        builder.make_user_urn(
            username=f"user{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:corpuser:user%%28%29%2C%E2%90%9F"
    )
    assert (
        builder.make_group_urn(
            groupname=f"group{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:corpGroup:group%%28%29%2C%E2%90%9F"
    )
    assert (
        builder.make_dashboard_urn(
            platform=f"platform{_RESERVED_CHARS_STRING}",
            name=f"dashboard{_RESERVED_CHARS_STRING}",
            platform_instance=f"platform-instance{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:dashboard:(platform%%28%29%2C%E2%90%9F,platform-instance%%28%29%2C%E2%90%9F.dashboard%%28%29%2C%E2%90%9F)"
    )
    assert (
        builder.make_dashboard_urn(
            platform=f"platform{_RESERVED_CHARS_STRING}",
            name=f"dashboard{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:dashboard:(platform%%28%29%2C%E2%90%9F,dashboard%%28%29%2C%E2%90%9F)"
    )
    assert (
        builder.make_chart_urn(
            platform=f"platform{_RESERVED_CHARS_STRING}",
            name=f"dashboard{_RESERVED_CHARS_STRING}",
            platform_instance=f"platform-instance{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:chart:(platform%%28%29%2C%E2%90%9F,platform-instance%%28%29%2C%E2%90%9F.dashboard%%28%29%2C%E2%90%9F)"
    )
    assert (
        builder.make_chart_urn(
            platform=f"platform{_RESERVED_CHARS_STRING}",
            name=f"dashboard{_RESERVED_CHARS_STRING}",
        )
        == "urn:li:chart:(platform%%28%29%2C%E2%90%9F,dashboard%%28%29%2C%E2%90%9F)"
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
