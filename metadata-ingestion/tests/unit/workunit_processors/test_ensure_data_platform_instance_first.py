import json
import os
from itertools import zip_longest
from typing import Iterable, List, Tuple
from unittest import mock

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_utils import gen_schema_container
from datahub.ingestion.workunit_processors.ensure_data_platform_instance_first import (
    EnsureDataPlatformInstanceFirstProcessor,
    EnsureDataPlatformInstanceFirstProcessorReport,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
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


class _ListSource(Source):
    def __init__(self, ctx: PipelineContext, wus: List[MetadataWorkUnit]) -> None:
        super().__init__(ctx)
        self._wus = wus
        self.report = SourceReport()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._wus

    def get_report(self) -> SourceReport:
        return self.report


def test_source_pipeline_emits_dpi_first_and_reports_counter() -> None:
    source = _ListSource(
        PipelineContext(run_id="dpi-first-test"), [_props(C1), _status(C1), _dpi(C1)]
    )

    out = [wu for wu in source.get_workunits() if wu.get_urn() == C1]

    assert _order(out)[0] == (C1, "dataPlatformInstance")
    report = source.get_report().workunit_processor_reports[
        "EnsureDataPlatformInstanceFirstProcessor"
    ]
    assert report.as_obj() == {"num_runs_reordered": 1, "num_dpi_after_first_run": 0}


def test_kill_switch_removes_processor_from_source() -> None:
    source = _ListSource(PipelineContext(run_id="dpi-first-test"), [])
    with mock.patch.dict(os.environ, {"DATAHUB_INGEST_DISABLE_DPI_FIRST": "true"}):
        source.get_workunit_processors()

    assert (
        "EnsureDataPlatformInstanceFirstProcessor"
        not in source.get_report().workunit_processor_reports
    )


def test_interleaved_schema_containers_keep_their_browse_paths() -> None:
    # Threaded sources interleave containers one workunit at a time. The parent
    # Container aspect must reach AutoBrowsePathV2Processor before the rest of a
    # child container's aspects, or the child gets a root browse path.
    db_key = DatabaseKey(platform="snowflake", instance="inst", database="db")
    schema_keys = [
        SchemaKey(platform="snowflake", instance="inst", database="db", schema=f"s{i}")
        for i in range(3)
    ]
    per_schema = [
        list(
            gen_schema_container(
                schema=key.db_schema,
                database=key.database,
                sub_types=["Schema"],
                database_container_key=db_key,
                schema_container_key=key,
            )
        )
        for key in schema_keys
    ]
    round_robin = [
        wu for wus in zip_longest(*per_schema) for wu in wus if wu is not None
    ]
    source = _ListSource(PipelineContext(run_id="dpi-first-test"), round_robin)

    browse_paths = {
        wu.get_urn(): aspect
        for wu in source.get_workunits()
        if (aspect := wu.get_aspect_of_type(BrowsePathsV2Class))
    }

    db_urn = db_key.as_urn()
    for key in schema_keys:
        assert (
            BrowsePathEntryClass(id=db_urn, urn=db_urn)
            in browse_paths[key.as_urn()].path
        )
