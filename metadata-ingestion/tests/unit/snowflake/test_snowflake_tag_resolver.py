from typing import Dict, List, Optional, Sequence

from datahub.emitter.mcp_builder import StructuredPropertyWriteMode
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
from datahub.metadata.schema_classes import PlatformResourceInfoClass
from datahub.metadata.urns import StructuredPropertyUrn, TagUrn
from tests.unit.snowflake.conftest import (
    aspects,
    fake_tag_repository,
    make_v2_config,
    tag_record,
)

_TAG = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="HIGH")
_TAG_30 = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="30")
_NATIVE_TAG_URN = TagUrn("test_db.public.pii:high")
_NATIVE_SP_URN = StructuredPropertyUrn("snowflake.test_db.public.pii")
_RETENTION = "urn:li:structuredProperty:retention"
_NUMBER = "urn:li:dataType:datahub.number"


def resolver(
    records: Sequence[SnowflakeTagPlatformResource] = (),
    *,
    repository: Optional[SnowflakeTagPlatformResourceRepository] = None,
    no_repository: bool = False,
    sp_mode: Optional[StructuredPropertyWriteMode] = None,
    value_type: Optional[str] = None,
    platform_instance: Optional[str] = None,
) -> SnowflakeTagResolver:
    overrides: Dict[str, object] = {}
    if sp_mode is not None:
        overrides["extract_tags_as_structured_properties"] = True
        overrides["structured_properties_write_mode"] = sp_mode
    config = make_v2_config(**overrides)
    report = SnowflakeV2Report()
    if repository is None and not no_repository:
        repository = fake_tag_repository(records, value_type)
    return SnowflakeTagResolver(
        repository=repository,
        config=config,
        report=report,
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        platform_instance=platform_instance,
    )


def emitted_secondary_keys(r: SnowflakeTagResolver) -> List[List[str]]:
    infos = aspects(r.platform_resource_workunits(), PlatformResourceInfoClass)
    return [info.secondaryKeys or [] for info in infos.values()]


def test_no_repository_resolves_native_tag_and_property() -> None:
    r = resolver(no_repository=True)

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.is_native(_TAG)
    assert r.resolve_structured_properties([_TAG]) == {_NATIVE_SP_URN: "HIGH"}
    assert emitted_secondary_keys(r) == []


def test_tag_mode_managed_tag_maps_back() -> None:
    r = resolver([tag_record(_TAG, "urn:li:tag:pii")])

    assert r.resolve_tag_urn(_TAG) == TagUrn("pii")
    assert not r.is_native(_TAG)
    assert r.report.num_tags_mapped_to_datahub_urn == 1


def test_managed_tag_is_skipped_when_mapping_back_is_off() -> None:
    r = resolver([tag_record(_TAG, "urn:li:tag:pii")])

    assert r.resolve_tag_urn(_TAG, map_back=False) is None
    assert r.report.num_tags_skipped_datahub_managed == 1


def test_tag_mode_managed_term_is_skipped() -> None:
    r = resolver([tag_record(_TAG, "urn:li:glossaryTerm:abc")])

    assert r.resolve_tag_urn(_TAG) is None
    assert r.report.num_tags_skipped_datahub_managed == 1


def test_tag_mode_managed_structured_property_is_skipped() -> None:
    r = resolver([tag_record(_TAG, _RETENTION, with_value=False)])

    assert r.resolve_tag_urn(_TAG) is None
    assert not r.is_native(_TAG)


def test_sp_upsert_managed_property_maps_back_with_cast() -> None:
    r = resolver(
        [tag_record(_TAG_30, _RETENTION, with_value=False)],
        sp_mode=StructuredPropertyWriteMode.UPSERT,
        value_type=_NUMBER,
    )

    assert r.resolve_structured_properties([_TAG_30]) == {
        StructuredPropertyUrn("retention"): 30.0
    }
    assert r.report.num_structured_properties_mapped_to_datahub_urn == 1


def test_sp_patch_managed_property_is_skipped() -> None:
    r = resolver(
        [tag_record(_TAG, _RETENTION, with_value=False)],
        sp_mode=StructuredPropertyWriteMode.PATCH,
    )

    assert r.resolve_structured_properties([_TAG]) == {}
    assert r.report.num_structured_properties_skipped_datahub_managed == 1


def test_sp_mode_managed_tag_or_term_is_skipped() -> None:
    for urn in ["urn:li:tag:pii", "urn:li:glossaryTerm:abc"]:
        r = resolver(
            [tag_record(_TAG, urn)], sp_mode=StructuredPropertyWriteMode.UPSERT
        )

        assert r.resolve_structured_properties([_TAG]) == {}


def test_native_tag_queues_one_not_managed_record() -> None:
    r = resolver()

    r.resolve_tag_urn(_TAG)
    r.resolve_tag_urn(_TAG)

    assert emitted_secondary_keys(r) == [[_NATIVE_TAG_URN.urn()]]
    r.resolve_tag_urn(_TAG)
    assert emitted_secondary_keys(r) == []


def test_native_property_links_the_valueless_record() -> None:
    r = resolver(sp_mode=StructuredPropertyWriteMode.UPSERT)

    assert r.resolve_structured_properties([_TAG]) == {_NATIVE_SP_URN: "HIGH"}
    assert emitted_secondary_keys(r) == [[_NATIVE_SP_URN.urn()]]


def test_existing_native_record_is_emitted_again_each_run() -> None:
    # Stale-entity removal soft-deletes records a run does not emit.
    r = resolver([tag_record(_TAG, _NATIVE_TAG_URN.urn(), managed=False)])

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert emitted_secondary_keys(r) == [[_NATIVE_TAG_URN.urn()]]


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


def test_records_are_loaded_with_one_search() -> None:
    repository = fake_tag_repository([tag_record(_TAG, "urn:li:tag:pii")])
    r = resolver(repository=repository)
    other = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="LOW")

    for tag in [_TAG, other, _TAG, other]:
        r.resolve_tag_urn(tag)
        r.is_native(tag)

    assert repository.search_by_filter.call_count == 1  # type: ignore[attr-defined]


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


def test_uncastable_managed_value_blocks_the_whole_upsert_aspect() -> None:
    # UPSERT replaces the aspect, so dropping one property would delete it.
    thirty = SnowflakeTag(
        database="TEST_DB", schema="PUBLIC", name="PII", value="thirty"
    )
    native = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="TIER", value="1")
    r = resolver(
        [tag_record(thirty, _RETENTION, with_value=False)],
        sp_mode=StructuredPropertyWriteMode.UPSERT,
        value_type=_NUMBER,
    )

    assert r.resolve_structured_properties([thirty, native]) is None
    assert r.report.num_structured_properties_unresolved == 1
    assert len(r.report.warnings) == 1
