from typing import Dict, List, Mapping, Optional, Sequence

from datahub.emitter.mcp_builder import StructuredPropertyWriteMode
from datahub.ingestion.source.snowflake.snowflake_config import TagOption
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag
from datahub.ingestion.source.snowflake.snowflake_tag_entities import (
    SnowflakeTagPlatformResource,
    SnowflakeTagPlatformResourceRepository,
)
from datahub.ingestion.source.snowflake.snowflake_tag_resolver import (
    SnowflakeTagResolver,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OtherSchemaClass,
    PlatformResourceInfoClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
    TagKeyClass,
    _Aspect,
)
from datahub.metadata.urns import PlatformResourceUrn, StructuredPropertyUrn, TagUrn
from tests.unit.snowflake.conftest import (
    aspects,
    fake_tag_repository,
    make_v2_config,
    tag_record,
)

_TAG = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="HIGH")
_LOW = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="LOW")
_TIER = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="TIER", value="1")
_NATIVE_TAG_URN = TagUrn("test_db.public.pii:high")
_NATIVE_SP_URN = StructuredPropertyUrn("snowflake.test_db.public.pii")
_RETENTION = "urn:li:structuredProperty:retention"
_DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.t1,PROD)"
_UPSERT = StructuredPropertyWriteMode.UPSERT
_PATCH = StructuredPropertyWriteMode.PATCH


def resolver(
    records: Sequence[SnowflakeTagPlatformResource] = (),
    *,
    repository: Optional[SnowflakeTagPlatformResourceRepository] = None,
    no_repository: bool = False,
    in_datahub: Optional[Mapping[str, Sequence[_Aspect]]] = None,
    platform_instance: Optional[str] = None,
    extract_tags: Optional[TagOption] = None,
) -> SnowflakeTagResolver:
    overrides: Dict[str, object] = {}
    if extract_tags is not None:
        overrides["extract_tags"] = extract_tags
    config = make_v2_config(**overrides)
    report = SnowflakeV2Report()
    if repository is None and not no_repository:
        repository = fake_tag_repository(records, in_datahub=in_datahub)
    return SnowflakeTagResolver(
        repository=repository,
        config=config,
        report=report,
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        platform_instance=platform_instance,
    )


def _record_urn(record: SnowflakeTagPlatformResource) -> str:
    return str(PlatformResourceUrn(record.id.to_platform_resource_key().id))


def emitted_secondary_keys(r: SnowflakeTagResolver) -> List[List[str]]:
    infos = aspects(r.platform_resource_workunits(), PlatformResourceInfoClass)
    return [info.secondaryKeys or [] for info in infos.values()]


def _tags(global_tags: Optional[GlobalTagsClass]) -> Optional[List[str]]:
    return None if global_tags is None else [t.tag for t in global_tags.tags]


def test_no_repository_resolves_native_tag_and_property() -> None:
    r = resolver(no_repository=True)

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.is_native(_TAG)
    assert _tags(r.global_tags(_DATASET, [_TAG])) == [_NATIVE_TAG_URN.urn()]
    assert r.resolve_structured_properties(_DATASET, [_TAG], _UPSERT) == {
        _NATIVE_SP_URN: "HIGH"
    }
    assert emitted_secondary_keys(r) == []


def test_managed_tag_term_or_property_is_never_taken_from_snowflake() -> None:
    for record in [
        tag_record(_TAG, "urn:li:tag:pii"),
        tag_record(_TAG, "urn:li:glossaryTerm:abc"),
        tag_record(_TAG, _RETENTION, with_value=False),
    ]:
        r = resolver([record])

        assert r.resolve_tag_urn(_TAG) is None
        assert not r.is_native(_TAG)
        assert r.resolve_structured_properties(_DATASET, [_TAG], _PATCH) is None
        assert r.report.num_tags_skipped_datahub_managed == 1
        assert r.report.num_structured_properties_skipped_datahub_managed == 1


def test_global_tags_keep_the_managed_tags_datahub_holds() -> None:
    # Snowflake's tag_references lags: it may still list a tag the user just
    # removed in DataHub, or not yet list one just added. DataHub decides.
    r = resolver(
        [tag_record(_TAG, "urn:li:tag:pii"), tag_record(_TIER, "urn:li:tag:gold")],
        in_datahub={
            _DATASET: [
                GlobalTagsClass(
                    tags=[
                        TagAssociationClass("urn:li:tag:gold"),
                        TagAssociationClass("urn:li:tag:test_db.public.pii:old"),
                    ]
                )
            ]
        },
    )

    assert _tags(r.global_tags(_DATASET, [_TAG, _LOW])) == [
        "urn:li:tag:test_db.public.pii:low",
        "urn:li:tag:gold",
    ]
    assert r.report.num_managed_tags_preserved_from_datahub == 1


def test_only_managed_tags_still_drop_native_tags_removed_in_snowflake() -> None:
    # The asset's other Snowflake tag was removed in Snowflake: the whole write
    # must drop it, as it did before, while keeping DataHub's managed tag.
    r = resolver(
        [tag_record(_TAG, "urn:li:tag:pii")],
        in_datahub={
            _DATASET: [
                GlobalTagsClass(
                    tags=[
                        TagAssociationClass("urn:li:tag:pii"),
                        TagAssociationClass("urn:li:tag:test_db.public.tier:1"),
                    ]
                )
            ]
        },
    )

    assert _tags(r.global_tags(_DATASET, [_TAG])) == ["urn:li:tag:pii"]


def test_no_snowflake_tags_write_no_global_tags() -> None:
    r = resolver([tag_record(_TAG, "urn:li:tag:pii")])

    assert r.global_tags(_DATASET, []) is None
    assert r.repository is not None
    assert r.repository.graph.get_aspect.call_count == 0  # type: ignore[attr-defined]


def test_upsert_with_only_managed_properties_still_replaces_the_aspect() -> None:
    r = resolver(
        [tag_record(_TAG, _RETENTION, with_value=False)],
        in_datahub={
            _DATASET: [
                StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn="urn:li:structuredProperty:snowflake.old",
                            values=["x"],
                        ),
                    ]
                )
            ]
        },
    )

    assert r.resolve_structured_properties(_DATASET, [_TAG], _UPSERT) == {}
    assert r.resolve_structured_properties(_DATASET, [_TAG], _PATCH) is None


def test_datahub_is_not_read_without_managed_records() -> None:
    r = resolver([tag_record(_TAG, _NATIVE_TAG_URN.urn(), managed=False)])

    assert _tags(r.global_tags(_DATASET, [_TAG])) == [_NATIVE_TAG_URN.urn()]
    assert r.resolve_structured_properties(_DATASET, [_TAG], _UPSERT) == {
        _NATIVE_SP_URN: "HIGH"
    }
    assert r.repository is not None
    assert r.repository.graph.get_aspect.call_count == 0  # type: ignore[attr-defined]


def test_unreadable_datahub_aspect_writes_nothing() -> None:
    # Writing the aspect without DataHub's managed values would delete them.
    repository = fake_tag_repository(
        [
            tag_record(_TAG, "urn:li:tag:pii"),
            tag_record(_TAG, _RETENTION, with_value=False),
        ],
        read_error=RuntimeError("timed out"),
    )
    r = resolver(repository=repository)

    assert r.global_tags(_DATASET, [_TIER]) is None
    assert r.resolve_structured_properties(_DATASET, [_TIER], _UPSERT) is None
    assert r.report.num_datahub_aspect_read_failures == 2
    assert len(r.report.warnings) == 2


def test_upsert_keeps_the_values_datahub_holds_for_managed_properties() -> None:
    r = resolver(
        [tag_record(_TAG, _RETENTION, with_value=False)],
        in_datahub={
            _DATASET: [
                StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=_RETENTION, values=[30.0, 60.0]
                        ),
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn="urn:li:structuredProperty:snowflake.old",
                            values=["x"],
                        ),
                    ]
                )
            ]
        },
    )

    assert r.resolve_structured_properties(_DATASET, [_TAG, _TIER], _UPSERT) == {
        StructuredPropertyUrn("snowflake.test_db.public.tier"): "1",
        StructuredPropertyUrn("retention"): [30.0, 60.0],
    }
    assert r.report.num_managed_structured_properties_preserved_from_datahub == 1


def test_patch_does_not_read_datahub() -> None:
    r = resolver([tag_record(_TAG, _RETENTION, with_value=False)])

    assert r.resolve_structured_properties(_DATASET, [_TAG, _TIER], _PATCH) == {
        StructuredPropertyUrn("snowflake.test_db.public.tier"): "1"
    }
    assert r.repository is not None
    assert r.repository.graph.get_aspect.call_count == 0  # type: ignore[attr-defined]


def test_native_tag_queues_one_not_managed_record() -> None:
    r = resolver()

    r.resolve_tag_urn(_TAG)
    r.resolve_tag_urn(_TAG)

    assert emitted_secondary_keys(r) == [[_NATIVE_TAG_URN.urn()]]
    r.resolve_tag_urn(_TAG)
    assert emitted_secondary_keys(r) == []


def test_native_property_links_the_valueless_record() -> None:
    r = resolver()

    assert r.resolve_structured_properties(_DATASET, [_TAG], _UPSERT) == {
        _NATIVE_SP_URN: "HIGH"
    }
    assert emitted_secondary_keys(r) == [[_NATIVE_SP_URN.urn()]]


def test_existing_native_record_is_emitted_again_each_run() -> None:
    # Stale-entity removal soft-deletes records a run does not emit.
    r = resolver([tag_record(_TAG, _NATIVE_TAG_URN.urn(), managed=False)])

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert emitted_secondary_keys(r) == [[_NATIVE_TAG_URN.urn()]]


def test_managed_records_are_kept_out_of_stale_entity_removal() -> None:
    # Sync-back owns these records. A Status emitted as non-primary keeps them
    # out of this pipeline's state and on its skip list, so a run that doesn't
    # emit them (tags skipped, no database left) can't soft-delete them.
    other = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="LOW")
    managed = tag_record(_TAG, "urn:li:tag:pii", platform_instance="acct_a")
    native = tag_record(
        other,
        "urn:li:tag:test_db.public.pii:low",
        managed=False,
        platform_instance="acct_a",
    )
    elsewhere = tag_record(other, "urn:li:tag:pii", platform_instance="acct_b")
    r = resolver(
        [managed, native, elsewhere],
        platform_instance="acct_a",
        extract_tags=TagOption.without_lineage,
    )

    first_run_end = list(r.platform_resource_workunits())
    r.resolve_tag_urn(other)
    later = list(r.platform_resource_workunits())

    managed_urn = _record_urn(managed)
    assert [wu.get_urn() for wu in first_run_end] == [managed_urn]
    assert aspects(first_run_end, StatusClass) == {
        managed_urn: StatusClass(removed=False)
    }
    assert [wu.is_primary_source for wu in first_run_end] == [False]
    assert list(aspects(later, PlatformResourceInfoClass)) == [_record_urn(native)]
    assert managed_urn not in {wu.get_urn() for wu in later}


def test_new_record_written_meanwhile_by_another_writer_is_not_overwritten() -> None:
    repository = fake_tag_repository(
        [], written_meanwhile=[tag_record(_TAG, "urn:li:tag:pii")]
    )
    r = resolver(repository=repository)

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert emitted_secondary_keys(r) == []


def test_record_from_another_platform_instance_is_ignored() -> None:
    r = resolver(
        [tag_record(_TAG, "urn:li:tag:pii", platform_instance="acct_a")],
        platform_instance="acct_b",
    )

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.is_native(_TAG)


def test_records_are_loaded_in_pages_filtered_by_platform_instance() -> None:
    tags = [
        SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value=f"V{i}")
        for i in range(5)
    ]
    repository = fake_tag_repository(
        [tag_record(t, "urn:li:tag:pii", platform_instance="acct_a") for t in tags]
        + [tag_record(_TAG, "urn:li:tag:pii", platform_instance="acct_b")]
    )
    r = resolver(repository=repository, platform_instance="acct_a")

    for tag in [*tags, _TAG, *tags]:
        r.resolve_tag_urn(tag)

    graph = repository.graph
    assert graph.scroll_entities.call_count == 1  # type: ignore[attr-defined]
    assert graph.exists.call_count == 0  # type: ignore[attr-defined]
    assert graph.get_entity_semityped.call_count == 0  # type: ignore[attr-defined]
    rules = graph.scroll_entities.call_args.kwargs["filter"][0]["and"]  # type: ignore[attr-defined]
    assert {
        "field": "platformInstance",
        "values": [
            "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,acct_a)"
        ],
        "condition": "EQUAL",
    } in rules


def test_new_records_are_checked_for_other_writers_in_one_batch() -> None:
    repository = fake_tag_repository([])
    r = resolver(repository=repository)

    for i in range(5):
        r.resolve_tag_urn(
            SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value=f"V{i}")
        )

    assert len(emitted_secondary_keys(r)) == 5
    assert repository.graph.get_entities.call_count == 1  # type: ignore[attr-defined]


def test_malformed_record_is_skipped_and_the_rest_are_used() -> None:
    repository = fake_tag_repository(
        [tag_record(_TAG, "urn:li:glossaryTerm:abc")], malformed=1
    )
    r = resolver(repository=repository)

    assert not r.is_native(_TAG)
    assert r.report.num_tag_platform_resources_unparseable == 1
    assert r.report.num_tag_platform_resource_lookup_failures == 0
    assert r.repository is not None


def test_load_error_skips_tags_and_fails_the_run() -> None:
    # Without the records a sync-back tag would come back as a native duplicate
    # and replace DataHub's tag; a failed run also keeps stale-entity removal
    # from soft-deleting the records.
    repository = fake_tag_repository([], load_error=RuntimeError("search timed out"))
    r = resolver(repository=repository)

    assert r.resolve_tag_urn(_TAG) is None
    assert r.global_tags(_DATASET, [_TAG]) is None
    assert r.report.num_tag_platform_resource_lookup_failures == 1
    assert len(r.report.failures) == 1
    assert emitted_secondary_keys(r) == []


def test_managed_record_without_urn_is_still_datahubs() -> None:
    # Sync-back writes the record before it sets the tag, so a record it hasn't
    # linked yet still means the tag is DataHub's, never a native one.
    r = resolver([tag_record(_TAG)])

    assert r.resolve_tag_urn(_TAG) is None
    assert not r.is_native(_TAG)


def test_managed_records_are_protected_when_tags_are_skipped() -> None:
    managed = tag_record(_TAG, "urn:li:tag:pii")
    r = resolver([managed], extract_tags=TagOption.skip)

    workunits = list(r.platform_resource_workunits())

    assert [(wu.get_urn(), wu.is_primary_source) for wu in workunits] == [
        (_record_urn(managed), False)
    ]


def test_carried_over_datahub_tags_never_enter_stale_entity_state() -> None:
    # The DataHub tag is written into the asset's GlobalTags, so the referenced
    # tag would be materialized as this pipeline's entity and soft-deleted by a
    # later run that no longer references it. Its key aspect goes out as
    # non-primary instead: written as before, never in the state.
    r = resolver(
        [tag_record(_TAG, "urn:li:tag:pii")],
        in_datahub={
            _DATASET: [GlobalTagsClass(tags=[TagAssociationClass("urn:li:tag:pii")])]
        },
    )

    r.global_tags(_DATASET, [_TIER])
    r.global_tags(_DATASET, [_TIER])
    keys = [
        (wu.get_urn(), wu.is_primary_source)
        for wu in r.platform_resource_workunits()
        if wu.get_aspect_of_type(TagKeyClass) is not None
    ]

    assert keys == [("urn:li:tag:pii", False)]


def test_invalid_linked_urn_is_dropped_and_the_record_still_used() -> None:
    # Written by another tool: one bad URN must not stop the load.
    record = tag_record(_TAG, "urn:li:tag:pii")
    record.datahub_linked_resources().urns.append("not a urn")
    r = resolver([record, tag_record(_TIER, "urn:li:tag:gold")])

    assert not r.is_native(_TAG)
    assert not r.is_native(_TIER)
    assert r.report.num_tag_platform_resource_invalid_urns == 1
    assert r.report.num_tag_platform_resource_lookup_failures == 0


def test_load_error_does_not_count_tags_as_datahub_managed() -> None:
    repository = fake_tag_repository([], load_error=RuntimeError("search timed out"))
    r = resolver(repository=repository)

    r.resolve_tag_urn(_TAG)
    r.resolve_structured_properties(_DATASET, [_TAG], _PATCH)

    assert r.report.num_tags_skipped_datahub_managed == 0
    assert r.report.num_structured_properties_skipped_datahub_managed == 0


def _schema_with_column_tags(*tags: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="t1",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="col_1",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                globalTags=GlobalTagsClass(
                    tags=[TagAssociationClass(tag) for tag in tags]
                ),
            ),
            SchemaFieldClass(
                fieldPath="col_2",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
            ),
        ],
    )


def test_column_tags_are_kept_from_datahub_when_records_cannot_be_loaded() -> None:
    # Column tags are written inside schemaMetadata, whole: skipping them would
    # clear every column's tags for the run.
    repository = fake_tag_repository(
        [],
        load_error=RuntimeError("search timed out"),
        in_datahub={
            _DATASET: [_schema_with_column_tags("urn:li:tag:test_db.public.pii:high")]
        },
    )
    r = resolver(repository=repository)
    r.is_native(_TAG)

    kept = r.kept_column_tags(_DATASET)

    assert kept is not None
    assert {path: _tags(tags) for path, tags in kept.items()} == {
        "col_1": ["urn:li:tag:test_db.public.pii:high"]
    }


def test_column_tags_are_resolved_normally_when_records_load() -> None:
    r = resolver([tag_record(_TAG, "urn:li:tag:pii")])

    assert r.kept_column_tags(_DATASET) is None
