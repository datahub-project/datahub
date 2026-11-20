import gc
from pathlib import Path
from types import GeneratorType
from typing import Iterator
from unittest import mock
from weakref import WeakSet

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.workunit_processors.auto_status_aspect import (
    AutoStatusAspectProcessor,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    MetadataChangeProposalClass,
    StatusClass,
)


def _processor() -> AutoStatusAspectProcessor:
    return AutoStatusAspectProcessor.create(
        WorkunitProcessorContext(
            source_report=SourceReport(),
            pipeline_context=PipelineContext(run_id="auto-status-test"),
            source_config=None,
            platform=None,
        )
    )


def _properties(urn: str, *, primary: bool = True) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=urn, aspect=DatasetPropertiesClass(name="test")
    ).as_workunit(is_primary_source=primary)


class _TrackableUrn(str):
    """A string whose lifetime can be observed without retaining it in the test."""


@mock.patch(
    "datahub.ingestion.workunit_processors.auto_status_aspect._gms_supports_status_patch",
    return_value=False,
)
def test_auto_status_bounds_retained_urns(_supports_patch: mock.Mock) -> None:
    live: WeakSet[_TrackableUrn] = WeakSet()
    count = 6000

    def source() -> Iterator[MetadataWorkUnit]:
        for i in range(count):
            urn = _TrackableUrn(make_dataset_urn("test", f"table_{i:05d}"))
            live.add(urn)
            yield _properties(urn)

    output = _processor().process(source())
    assert isinstance(output, GeneratorType)
    try:
        for _ in range(count):
            next(output)
        gc.collect()
        # Observe retention before the source has finished; allow a cache and current input.
        retained = len(live)
        assert retained < 2000
        assert sum(1 for _ in output) == count
    finally:
        output.close()


@pytest.mark.parametrize("use_patch", [False, True])
def test_auto_status_keeps_explicit_status_after_spill(use_patch: bool) -> None:
    protected = make_dataset_urn("test", "protected")
    secondary = make_dataset_urn("test", "secondary")
    processor = _processor()

    def source() -> Iterator[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=protected, aspect=StatusClass(removed=True)
        ).as_workunit()
        yield _properties(secondary, primary=False)
        for i in reversed(range(1200)):
            yield _properties(make_dataset_urn("test", f"table_{i:05d}"))
        # Ordinary aspects cannot undo an explicit status or non-primary decision,
        # even when those earlier entries have been evicted from the memory cache.
        yield _properties(protected)
        yield _properties(secondary)

    with mock.patch(
        "datahub.ingestion.workunit_processors.auto_status_aspect._gms_supports_status_patch",
        return_value=use_patch,
    ):
        output = iter(processor.process(source()))
        for _ in range(1204):
            next(output)
        statuses = list(output)
    assert [wu.get_urn() for wu in statuses] == [
        make_dataset_urn("test", f"table_{i:05d}") for i in range(1200)
    ]
    assert processor.report.num_status_aspects_emitted == 1200
    assert processor.report.status_patch_mode == use_patch
    for wu in statuses:
        assert isinstance(
            wu.metadata, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        )
        assert wu.metadata.changeType == ("PATCH" if use_patch else "UPSERT")


@pytest.mark.parametrize("phase", ["input", "finalization", "source_failure"])
def test_auto_status_cleans_temporary_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    processor = _processor()
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))

    def source() -> Iterator[MetadataWorkUnit]:
        yield _properties(make_dataset_urn("test", "table"))
        if phase == "source_failure":
            raise RuntimeError("source failure")

    output = processor.process(source())
    assert isinstance(output, GeneratorType)
    next(output)
    if phase == "source_failure":
        with pytest.raises(RuntimeError):
            next(output)
    else:
        if phase == "finalization":
            with mock.patch(
                "datahub.ingestion.workunit_processors.auto_status_aspect._gms_supports_status_patch",
                return_value=False,
            ):
                next(output)
        output.close()
    assert list(tmp_path.iterdir()) == []
