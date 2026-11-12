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
    PlatformResourceInfoClass,
    StatusClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
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


def test_only_managed_tags_write_no_global_tags() -> None:
    r = resolver([tag_record(_TAG, "urn:li:tag:pii")])

    assert r.global_tags(_DATASET, [_TAG]) is None
    assert r.repository is not None
    assert r.repository.graph.get_aspect.call_count == 0  # type: ignore[attr-defined]


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


def test_managed_records_stay_in_the_checkpoint_with_status_only() -> None:
    # Stale-entity removal would soft-delete a record that sync-back turned
    # managed, after which the search no longer finds it.
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


def test_load_error_falls_back_to_native() -> None:
    repository = fake_tag_repository([], load_error=RuntimeError("search timed out"))
    r = resolver(repository=repository)

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.report.num_tag_platform_resource_lookup_failures == 1
    assert len(r.report.warnings) == 1
    assert emitted_secondary_keys(r) == []


def test_managed_record_without_urn_is_native() -> None:
    r = resolver([tag_record(_TAG)])

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.is_native(_TAG)
