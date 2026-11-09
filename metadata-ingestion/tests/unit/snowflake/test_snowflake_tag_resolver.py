from typing import Dict, List, Optional

from datahub.emitter.mcp_builder import StructuredPropertyWriteMode
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag
from datahub.ingestion.source.snowflake.snowflake_tag_entities import (
    SnowflakeTagPlatformResource,
)
from datahub.ingestion.source.snowflake.snowflake_tag_resolver import (
    ResolvedStructuredProperty,
    SnowflakeTagResolver,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.urns import StructuredPropertyUrn, TagUrn
from tests.unit.snowflake.conftest import (
    fake_tag_repository,
    make_v2_config,
    managed_record,
)

_TAG = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="HIGH")
_TAG_30 = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="30")
_NATIVE_TAG_URN = TagUrn("test_db.public.pii:high")
_NATIVE_SP_URN = StructuredPropertyUrn("snowflake.test_db.public.pii")
_NUMBER = "urn:li:dataType:datahub.number"


def resolver(
    records: Optional[Dict[str, SnowflakeTagPlatformResource]] = None,
    *,
    no_repository: bool = False,
    sp_mode: Optional[StructuredPropertyWriteMode] = None,
    value_type: Optional[str] = None,
    lookup_error: Optional[Exception] = None,
) -> SnowflakeTagResolver:
    overrides: Dict[str, object] = {}
    if sp_mode is not None:
        overrides["extract_tags_as_structured_properties"] = True
        overrides["structured_properties_write_mode"] = sp_mode
    config = make_v2_config(**overrides)
    report = SnowflakeV2Report()
    return SnowflakeTagResolver(
        repository=None
        if no_repository
        else fake_tag_repository(records or {}, value_type, lookup_error),
        config=config,
        report=report,
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        platform_instance=None,
    )


def _resource_urns(r: SnowflakeTagResolver) -> List[str]:
    urns: List[str] = []
    for wu in r.platform_resource_workunits():
        urns.append(wu.get_urn())
    return urns


def test_no_repository_resolves_native_tag_and_property() -> None:
    r = resolver(no_repository=True)

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.is_native(_TAG)
    assert r.resolve_structured_property(_TAG) == ResolvedStructuredProperty(
        urn=_NATIVE_SP_URN, value="HIGH"
    )
    assert _resource_urns(r) == []


def test_tag_mode_managed_tag_maps_back() -> None:
    r = resolver({"test_db.public.pii:high": managed_record("urn:li:tag:pii")})

    assert r.resolve_tag_urn(_TAG) == TagUrn("pii")
    assert not r.is_native(_TAG)
    assert r.report.num_tags_mapped_to_datahub_urn == 1


def test_tag_mode_managed_term_is_skipped() -> None:
    r = resolver({"test_db.public.pii:high": managed_record("urn:li:glossaryTerm:abc")})

    assert r.resolve_tag_urn(_TAG) is None
    assert r.report.num_tags_skipped_datahub_managed == 1


def test_tag_mode_managed_structured_property_is_skipped() -> None:
    r = resolver(
        {"test_db.public.pii": managed_record("urn:li:structuredProperty:io.acryl.pii")}
    )

    assert r.resolve_tag_urn(_TAG) is None
    assert not r.is_native(_TAG)


def test_sp_upsert_managed_property_maps_back_with_cast() -> None:
    r = resolver(
        {"test_db.public.pii": managed_record("urn:li:structuredProperty:retention")},
        sp_mode=StructuredPropertyWriteMode.UPSERT,
        value_type=_NUMBER,
    )

    assert r.resolve_structured_property(_TAG_30) == ResolvedStructuredProperty(
        urn=StructuredPropertyUrn("retention"), value=30.0
    )
    assert r.report.num_structured_properties_mapped_to_datahub_urn == 1


def test_sp_patch_managed_property_is_skipped() -> None:
    r = resolver(
        {"test_db.public.pii": managed_record("urn:li:structuredProperty:retention")},
        sp_mode=StructuredPropertyWriteMode.PATCH,
    )

    assert r.resolve_structured_property(_TAG) is None
    assert r.report.num_structured_properties_skipped_datahub_managed == 1


def test_sp_mode_managed_tag_or_term_is_skipped() -> None:
    for urn in ["urn:li:tag:pii", "urn:li:glossaryTerm:abc"]:
        r = resolver(
            {"test_db.public.pii:high": managed_record(urn)},
            sp_mode=StructuredPropertyWriteMode.UPSERT,
        )

        assert r.resolve_structured_property(_TAG) is None


def test_native_tag_queues_one_not_managed_record() -> None:
    r = resolver({})

    r.resolve_tag_urn(_TAG)
    r.resolve_tag_urn(_TAG)
    urns = _resource_urns(r)

    assert len(set(urns)) == 1
    assert _resource_urns(r) == []
    r.resolve_tag_urn(_TAG)
    assert _resource_urns(r) == []


def test_native_property_links_the_valueless_record() -> None:
    r = resolver({}, sp_mode=StructuredPropertyWriteMode.UPSERT)

    assert r.resolve_structured_property(_TAG) == ResolvedStructuredProperty(
        urn=_NATIVE_SP_URN, value="HIGH"
    )
    assert len(set(_resource_urns(r))) == 1


def test_lookup_error_falls_back_to_native() -> None:
    r = resolver(lookup_error=RuntimeError("search timed out"))

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.report.num_tag_platform_resource_lookup_failures == 1
    assert len(r.report.warnings) == 1


def test_managed_record_without_urn_is_native() -> None:
    r = resolver({"test_db.public.pii:high": managed_record()})

    assert r.resolve_tag_urn(_TAG) == _NATIVE_TAG_URN
    assert r.is_native(_TAG)


def test_uncastable_value_is_skipped_and_reported() -> None:
    thirty = SnowflakeTag(
        database="TEST_DB", schema="PUBLIC", name="PII", value="thirty"
    )
    r = resolver(
        {"test_db.public.pii": managed_record("urn:li:structuredProperty:retention")},
        sp_mode=StructuredPropertyWriteMode.UPSERT,
        value_type=_NUMBER,
    )

    assert r.resolve_structured_property(thirty) is None
    assert r.report.num_structured_properties_skipped_datahub_managed == 1
    assert len(r.report.warnings) == 1
