import json
import os
from typing import Iterable, List, Tuple
from unittest import mock

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.workunit_processors.ensure_data_platform_instance_first import (
    EnsureDataPlatformInstanceFirstProcessor,
    EnsureDataPlatformInstanceFirstProcessorReport,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    GenericAspectClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
    SubTypesClass,
    _Aspect,
)

C1 = make_container_urn("c1")
C2 = make_container_urn("c2")
D1 = make_dataset_urn_with_platform_instance("mysql", "db.t1", "inst")
_DPI = DataPlatformInstanceClass(
    platform=make_data_platform_urn("mysql"),
    instance=make_dataplatform_instance_urn("mysql", "inst"),
)


def _wu(urn: str, aspect: _Aspect) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect).as_workunit()


def _dpi(urn: str) -> MetadataWorkUnit:
    return _wu(urn, _DPI)


def _props(urn: str) -> MetadataWorkUnit:
    return _wu(urn, ContainerPropertiesClass(name=urn))


def _status(urn: str) -> MetadataWorkUnit:
    return _wu(urn, StatusClass(removed=False))


def _subtypes(urn: str) -> MetadataWorkUnit:
    return _wu(urn, SubTypesClass(typeNames=["Database"]))


def _process(
    wus: Iterable[MetadataWorkUnit],
) -> Tuple[List[MetadataWorkUnit], EnsureDataPlatformInstanceFirstProcessorReport]:
    processor = EnsureDataPlatformInstanceFirstProcessor.create(mock.MagicMock())
    return list(processor.process(wus)), processor.report


def _order(wus: List[MetadataWorkUnit]) -> List[Tuple[str, str]]:
    return [
        (
            wu.get_urn(),
            "mce"
            if isinstance(wu.metadata, MetadataChangeEventClass)
            else str(wu.metadata.aspectName),
        )
        for wu in wus
    ]


def test_moves_dpi_to_front_of_run_keeping_others_in_order() -> None:
    out, report = _process([_props(C1), _status(C1), _dpi(C1), _subtypes(C1)])

    assert _order(out) == [
        (C1, "dataPlatformInstance"),
        (C1, "containerProperties"),
        (C1, "status"),
        (C1, "subTypes"),
    ]
    assert report.num_runs_reordered == 1
    assert report.num_dpi_after_first_run == 0


def test_dpi_already_first_is_unchanged() -> None:
    wus = [_dpi(C1), _props(C1), _status(C1)]

    out, report = _process(wus)

    assert all(a is b for a, b in zip(out, wus, strict=True))
    assert report.num_runs_reordered == 0


def test_run_without_dpi_is_unchanged() -> None:
    wus = [_props(C1), _status(C1)]

    out, report = _process(wus)

    assert all(a is b for a, b in zip(out, wus, strict=True))
    assert report.num_runs_reordered == 0


def test_dpi_in_later_run_is_left_in_place_and_counted() -> None:
    out, report = _process([_props(C1), _status(C2), _status(C1), _dpi(C1)])

    assert _order(out) == [
        (C1, "containerProperties"),
        (C2, "status"),
        (C1, "status"),
        (C1, "dataPlatformInstance"),
    ]
    assert report.num_dpi_after_first_run == 1
    assert report.num_runs_reordered == 0


def test_dpi_re_emitted_after_first_run_that_had_one_is_not_counted() -> None:
    out, report = _process([_dpi(C1), _props(C1), _status(C2), _dpi(C1)])

    assert len(out) == 4
    assert report.num_dpi_after_first_run == 0


def test_dpi_after_first_run_without_one_is_counted() -> None:
    _, report = _process([_props(C1), _status(C2), _dpi(C1)])

    assert report.num_dpi_after_first_run == 1


def test_interleaved_urns_reorder_each_first_run() -> None:
    out, report = _process(
        [
            _props(C1),
            _dpi(C1),
            _props(C2),
            _wu(C2, ContainerClass(container=C1)),
            _dpi(C2),
            _wu(D1, DatasetPropertiesClass(name="t1")),
            _wu(D1, ContainerClass(container=C2)),
            _dpi(D1),
        ]
    )

    assert _order(out) == [
        (C1, "dataPlatformInstance"),
        (C1, "containerProperties"),
        (C2, "dataPlatformInstance"),
        (C2, "containerProperties"),
        (C2, "container"),
        (D1, "dataPlatformInstance"),
        (D1, "datasetProperties"),
        (D1, "container"),
    ]
    assert report.num_runs_reordered == 3


def test_end_of_stream_flushes_last_run() -> None:
    out, _ = _process([_status(C2), _props(C1), _dpi(C1)])

    assert _order(out) == [
        (C2, "status"),
        (C1, "dataPlatformInstance"),
        (C1, "containerProperties"),
    ]


def test_mce_passes_through_and_is_not_treated_as_dpi() -> None:
    mce = MetadataWorkUnit(
        id="d1-mce",
        mce=MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(
                urn=D1, aspects=[DatasetPropertiesClass(name="t1"), _DPI]
            )
        ),
    )
    snapshot_before = mce.metadata.to_obj()

    out, report = _process([mce, _status(D1), _dpi(D1)])

    assert _order(out) == [
        (D1, "dataPlatformInstance"),
        (D1, "mce"),
        (D1, "status"),
    ]
    assert out[1] is mce
    assert mce.metadata.to_obj() == snapshot_before
    assert report.num_runs_reordered == 1


def test_raw_mcp_dpi_is_moved() -> None:
    raw_dpi = MetadataWorkUnit(
        id="c1-raw-dpi",
        mcp_raw=MetadataChangeProposalClass(
            entityType="container",
            entityUrn=C1,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="dataPlatformInstance",
            aspect=GenericAspectClass(
                contentType="application/json",
                value=json.dumps(_DPI.to_obj()).encode(),
            ),
        ),
    )

    out, _ = _process([_props(C1), raw_dpi])

    assert out[0] is raw_dpi


def test_kill_switch_disables_processor() -> None:
    ctx = mock.MagicMock()
    with mock.patch.dict(os.environ, {"DATAHUB_INGEST_DISABLE_DPI_FIRST": "true"}):
        assert EnsureDataPlatformInstanceFirstProcessor.should_enable(ctx) is False
    with mock.patch.dict(os.environ, {}):
        os.environ.pop("DATAHUB_INGEST_DISABLE_DPI_FIRST", None)
        assert EnsureDataPlatformInstanceFirstProcessor.should_enable(ctx) is True
