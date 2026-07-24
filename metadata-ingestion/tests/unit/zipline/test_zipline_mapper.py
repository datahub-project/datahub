from typing import Any, List, Type, TypeVar

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.zipline.config import ZiplineConfig
from datahub.ingestion.source.zipline.lineage import SourceResolver
from datahub.ingestion.source.zipline.mapper import ZiplineMapper
from datahub.ingestion.source.zipline.models import (
    Aggregation,
    Derivation,
    EventSource,
    GroupBy,
    MetaData,
    Query,
    Source,
)
from datahub.ingestion.source.zipline.report import ZiplineSourceReport
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    MLFeatureDataTypeClass,
    MLFeaturePropertiesClass,
    OwnershipClass,
)

_AspectT = TypeVar("_AspectT")


def _mapper(**config_overrides: Any) -> ZiplineMapper:
    config = ZiplineConfig(
        path="/tmp/x",
        source_platform_map={"data": "hive"},
        default_source_platform="hive",
        **config_overrides,
    )
    report = ZiplineSourceReport()
    return ZiplineMapper(config, report, SourceResolver(config, report))


def _aspects(
    workunits: List[MetadataWorkUnit], aspect_cls: Type[_AspectT]
) -> List[_AspectT]:
    result: List[_AspectT] = []
    for workunit in workunits:
        aspect = getattr(workunit.metadata, "aspect", None)
        if isinstance(aspect, aspect_cls):
            result.append(aspect)
    return result


def test_map_group_by_skips_join_source_but_keeps_real_sources():
    # A JoinSource can only be resolved via the parent Join's output, so the
    # mapper skips it (and counts the skip) while still resolving sibling sources.
    mapper = _mapper()
    group_by = GroupBy(
        metaData=MetaData(name="t.gb.v1", team="t"),
        keyColumns=["id"],
        sources=[
            Source(joinSource={"join": {"metaData": {"name": "other.join.v1"}}}),
            Source(events=EventSource(table="data.events")),
        ],
        aggregations=[Aggregation(inputColumn="amt", operation=7)],
    )

    workunits = list(mapper.map_group_by(group_by))

    assert mapper.report.join_sources_skipped == 1
    features = _aspects(workunits, MLFeaturePropertiesClass)
    assert features
    assert all(
        feature.sources
        == ["urn:li:dataset:(urn:li:dataPlatform:hive,data.events,PROD)"]
        for feature in features
    )


def test_map_group_by_derivations_type_features_unknown():
    # The compiled config never types derived columns, so a GroupBy with
    # derivations must fall back to UNKNOWN rather than an operation-inferred type.
    mapper = _mapper()
    group_by = GroupBy(
        metaData=MetaData(name="t.gb.v1", team="t"),
        keyColumns=["id"],
        sources=[
            Source(
                events=EventSource(
                    table="data.events", query=Query(selects={"amt": "amt"})
                )
            )
        ],
        aggregations=[Aggregation(inputColumn="amt", operation=7)],
        derivations=[Derivation(name="ratio", expression="amt / 2")],
    )

    workunits = list(mapper.map_group_by(group_by))

    features = _aspects(workunits, MLFeaturePropertiesClass)
    assert features
    assert all(
        feature.dataType == MLFeatureDataTypeClass.UNKNOWN for feature in features
    )


def test_map_group_by_team_without_owner_mapping_emits_no_ownership():
    mapper = _mapper(
        enable_owner_extraction=True,
        owner_mappings=[
            {
                "team_name": "other_team",
                "datahub_owner_urn": "urn:li:corpGroup:other_team",
            }
        ],
    )
    group_by = GroupBy(
        metaData=MetaData(name="t.gb.v1", team="my_team"),
        keyColumns=["id"],
        aggregations=[Aggregation(inputColumn="amt", operation=7)],
    )

    workunits = list(mapper.map_group_by(group_by))

    assert _aspects(workunits, OwnershipClass) == []


def test_map_group_by_valueless_tag_uses_bare_key():
    # A tag with an empty value becomes a bare `urn:li:tag:<key>`, not `<key>:`.
    mapper = _mapper(enable_tag_extraction=True)
    group_by = GroupBy(
        metaData=MetaData(
            name="t.gb.v1", team="t", customJson='{"groupby_tags": {"pii": ""}}'
        ),
        keyColumns=["id"],
        aggregations=[Aggregation(inputColumn="amt", operation=7)],
    )

    workunits = list(mapper.map_group_by(group_by))

    tag_urns = [
        association.tag
        for aspect in _aspects(workunits, GlobalTagsClass)
        for association in aspect.tags
    ]
    assert "urn:li:tag:pii" in tag_urns
