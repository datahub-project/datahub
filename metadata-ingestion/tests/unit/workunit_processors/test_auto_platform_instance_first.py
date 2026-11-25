from typing import Iterable, List
from unittest.mock import MagicMock

import pytest

from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.workunit_processors import auto_platform_instance_first
from datahub.ingestion.workunit_processors.auto_platform_instance_first import (
    AutoPlatformInstanceFirstProcessor,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    StatusClass,
)

_A = "urn:li:container:a"
_B = "urn:li:container:b"
_C = "urn:li:container:c"
_DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,inst.db.s.t,PROD)"


def _wu(urn: str, aspect: object) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=aspect,  # type: ignore[arg-type]
    ).as_workunit()


def _dpi(urn: str) -> MetadataWorkUnit:
    return _wu(
        urn,
        DataPlatformInstanceClass(
            platform="urn:li:dataPlatform:snowflake",
            instance="urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,inst)",
        ),
    )


def _run(stream: List[MetadataWorkUnit]) -> List[str]:
    processor = AutoPlatformInstanceFirstProcessor.create(MagicMock())
    return [
        f"{wu.get_urn()}:{wu.metadata.aspectName}"  # type: ignore[union-attr]
        for wu in processor.process(stream)
    ]


def test_moves_platform_instance_to_the_front_of_each_entity() -> None:
    stream = [
        _wu(_A, ContainerClass(container=_B)),
        _wu(_A, ContainerPropertiesClass(name="a")),
        _dpi(_A),
        _wu(_A, StatusClass(removed=False)),
        _wu(_B, ContainerPropertiesClass(name="b")),
        _dpi(_B),
    ]

    assert _run(stream) == [
        f"{_A}:dataPlatformInstance",
        f"{_A}:container",
        f"{_A}:containerProperties",
        f"{_A}:status",
        f"{_B}:dataPlatformInstance",
        f"{_B}:containerProperties",
    ]


def test_leaves_entities_without_platform_instance_unchanged() -> None:
    stream = [
        _wu(_A, ContainerPropertiesClass(name="a")),
        _wu(_A, StatusClass(removed=False)),
        _wu(_B, StatusClass(removed=False)),
    ]

    assert _run(stream) == [
        f"{_A}:containerProperties",
        f"{_A}:status",
        f"{_B}:status",
    ]


def test_interleaved_entities_each_get_platform_instance_first() -> None:
    # Threaded sources interleave the workunits of different containers.
    stream = [
        _wu(_A, ContainerClass(container=_C)),
        _wu(_B, ContainerClass(container=_C)),
        _wu(_A, ContainerPropertiesClass(name="a")),
        _dpi(_B),
        _wu(_B, ContainerPropertiesClass(name="b")),
        _dpi(_A),
        _wu(_A, StatusClass(removed=False)),
    ]

    assert _run(stream) == [
        f"{_B}:dataPlatformInstance",
        f"{_B}:container",
        f"{_B}:containerProperties",
        f"{_A}:dataPlatformInstance",
        f"{_A}:container",
        f"{_A}:containerProperties",
        f"{_A}:status",
    ]


def test_entity_types_with_the_instance_in_their_urn_pass_through() -> None:
    stream = [
        _wu(_DATASET, StatusClass(removed=False)),
        _wu(_A, ContainerPropertiesClass(name="a")),
        _dpi(_DATASET),
        _dpi(_A),
    ]

    assert _run(stream) == [
        f"{_DATASET}:status",
        f"{_DATASET}:dataPlatformInstance",
        f"{_A}:dataPlatformInstance",
        f"{_A}:containerProperties",
    ]


def test_held_workunits_are_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auto_platform_instance_first, "_MAX_HELD_WORKUNITS", 2)
    stream = [
        _wu(_A, ContainerPropertiesClass(name="a")),
        _wu(_B, ContainerPropertiesClass(name="b")),
        _wu(_C, ContainerPropertiesClass(name="c")),
        _dpi(_A),
    ]

    # Holding a third workunit releases the oldest entity without its instance.
    assert _run(stream) == [
        f"{_A}:containerProperties",
        f"{_A}:dataPlatformInstance",
        f"{_B}:containerProperties",
        f"{_C}:containerProperties",
    ]


class _ContainerSource(Source):
    def __init__(self, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.report = SourceReport()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        db = DatabaseKey(platform="snowflake", instance="inst", database="db")
        schema = SchemaKey(
            platform="snowflake", instance="inst", database="db", schema="s"
        )
        yield from gen_containers(container_key=db, name="db", sub_types=["Database"])
        yield from gen_containers(
            container_key=schema,
            name="s",
            sub_types=["Schema"],
            parent_container_key=db,
        )

    def get_report(self) -> SourceReport:
        return self.report

    def get_config(self) -> PlatformInstanceConfigMixin:
        return PlatformInstanceConfigMixin(platform_instance="inst")


def test_child_container_gets_platform_instance_first_and_keeps_browse_path() -> None:
    source = _ContainerSource(PipelineContext(run_id="dpi-first"))
    schema_urn = SchemaKey(
        platform="snowflake", instance="inst", database="db", schema="s"
    ).as_urn()

    schema_wus = [
        wu
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.get_urn() == schema_urn
    ]

    assert schema_wus[0].metadata.aspectName == "dataPlatformInstance"  # type: ignore[union-attr]
    browse_paths = [
        path
        for path in (wu.get_aspect_of_type(BrowsePathsV2Class) for wu in schema_wus)
        if path is not None
    ]
    # Built from the child's container aspect, which the browse-path processor
    # still sees first: the path ends at the parent database container.
    assert browse_paths
    assert (
        browse_paths[0].path[-1].urn
        == DatabaseKey(platform="snowflake", instance="inst", database="db").as_urn()
    )


def _ctx(**config: object) -> WorkunitProcessorContext:
    return WorkunitProcessorContext(
        source_report=SourceReport(),
        pipeline_context=PipelineContext(run_id="dpi-first"),
        source_config=MagicMock(spec=list(config), **config),
        platform=None,
    )


def test_enabled_only_when_the_source_writes_a_platform_instance() -> None:
    assert not AutoPlatformInstanceFirstProcessor.should_enable(_ctx())
    assert not AutoPlatformInstanceFirstProcessor.should_enable(
        _ctx(platform_instance=None)
    )
    assert AutoPlatformInstanceFirstProcessor.should_enable(
        _ctx(platform_instance="inst")
    )
    assert AutoPlatformInstanceFirstProcessor.should_enable(
        _ctx(include_data_platform_instance=True)
    )
