from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag
from datahub.ingestion.source.snowflake.snowflake_tag_entities import (
    SnowflakeTagPlatformResource,
    SnowflakeTagPlatformResourceId,
)

_TAG = SnowflakeTag(database="TEST_DB", schema="PUBLIC", name="PII", value="HIGH")


def test_primary_key_with_and_without_value() -> None:
    with_value = SnowflakeTagPlatformResourceId.from_snowflake_tag(
        _TAG, None, with_value=True
    )
    without = SnowflakeTagPlatformResourceId.from_snowflake_tag(
        _TAG, None, with_value=False
    )

    assert (
        with_value.to_platform_resource_key().primary_key == "test_db.public.pii:high"
    )
    assert without.to_platform_resource_key().primary_key == "test_db.public.pii"
    assert with_value.to_platform_resource_key().platform == "snowflake"
    assert (
        with_value.to_platform_resource_key().resource_type
        == "SnowflakeTagPlatformResource"
    )


def test_key_is_case_insensitive() -> None:
    upper = SnowflakeTagPlatformResourceId(
        database="TEST_DB", schema_name="PUBLIC", tag_name="PII", tag_value="HIGH"
    )
    lower = SnowflakeTagPlatformResourceId(
        database="test_db", schema_name="public", tag_name="pii", tag_value="high"
    )

    assert upper.to_platform_resource_key().id == lower.to_platform_resource_key().id


def test_platform_instance_is_part_of_the_key() -> None:
    a = SnowflakeTagPlatformResourceId.from_snowflake_tag(
        _TAG, "acct_a", with_value=True
    )
    b = SnowflakeTagPlatformResourceId.from_snowflake_tag(
        _TAG, "acct_b", with_value=True
    )

    assert a.to_platform_resource_key().id != b.to_platform_resource_key().id


def test_platform_resource_round_trip_keeps_flag_and_urns() -> None:
    entity = SnowflakeTagPlatformResource.create_default(
        SnowflakeTagPlatformResourceId.from_snowflake_tag(_TAG, None, with_value=True),
        managed_by_datahub=True,
    )
    entity.datahub_linked_resources().add("urn:li:tag:pii")

    resource = entity.as_platform_resource()

    assert resource.resource_info is not None
    assert resource.resource_info.secondary_keys == ["urn:li:tag:pii"]
    assert resource.resource_info.value is not None
    restored = resource.resource_info.value.as_pydantic_object(
        SnowflakeTagPlatformResource
    )
    assert restored.is_managed_by_datahub()
    assert restored.datahub_linked_resources().urns == ["urn:li:tag:pii"]
    assert restored.id.database == "TEST_DB"
