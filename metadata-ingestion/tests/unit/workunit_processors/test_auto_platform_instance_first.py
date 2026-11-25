from typing import Iterable, List
from unittest.mock import MagicMock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
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


def test_moves_platform_instance_to_the_front_of_each_entity_run() -> None:
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


def test_does_not_move_platform_instance_across_entities() -> None:
    # A later run of the same entity is a separate run: only the order within a
    # run of consecutive workunits changes, never the order between entities.
    stream = [
        _wu(_A, ContainerPropertiesClass(name="a")),
        _wu(_B, ContainerPropertiesClass(name="b")),
        _dpi(_A),
    ]

    assert _run(stream) == [
        f"{_A}:containerProperties",
        f"{_B}:containerProperties",
        f"{_A}:dataPlatformInstance",
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
