from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_schema import (
    ShareOrigin,
    SnowflakeDatabase,
)


def test_share_origin_org_qualified() -> None:
    origin = ShareOrigin.parse("MYORG.ACCOUNT2.SHARE_X")
    assert origin == ShareOrigin(
        org_name="MYORG", account_name="ACCOUNT2", share_name="SHARE_X"
    )
    assert origin.account_identifier == "MYORG.ACCOUNT2"


def test_share_origin_locator_only() -> None:
    origin = ShareOrigin.parse("XY12345.SHARE_X")
    assert origin == ShareOrigin(
        org_name=None, account_name="XY12345", share_name="SHARE_X"
    )
    assert origin.account_identifier == "XY12345"


def test_share_origin_none_for_empty_or_malformed() -> None:
    assert ShareOrigin.parse(None) is None
    assert ShareOrigin.parse("") is None
    assert ShareOrigin.parse("JUST_ONE_PART") is None
    assert ShareOrigin.parse("a.b.c.d") is None


def test_is_shared_database_aligns_with_share_origin() -> None:
    # The two predicates must never disagree.
    cases = [
        (None, False),
        ("", False),
        ("MYORG.ACCT.SHARE", True),
        ("ACCT.SHARE", True),
        ("JUST_ONE_PART", False),  # malformed → both False
        ("a.b.c.d", False),
    ]
    for origin, expected in cases:
        db = SnowflakeDatabase(name="DB", created=None, comment=None, origin=origin)
        assert db.is_shared_database() is expected
        assert (db.share_origin is not None) is expected


# --- Phase C: account_mapping resolution ---


def test_resolve_account_exact_match() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"myorg.account2": "instance2", "myorg.account3": "instance3"},
    )
    assert config.resolve_account_to_platform_instance("myorg.account2") == "instance2"


def test_resolve_account_case_insensitive() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"MYORG.ACCOUNT2": "instance2"},
    )
    assert config.resolve_account_to_platform_instance("myorg.account2") == "instance2"
    assert config.resolve_account_to_platform_instance("MYORG.ACCOUNT2") == "instance2"


def test_resolve_account_returns_none_for_unknown() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"myorg.account2": "instance2"},
    )
    assert config.resolve_account_to_platform_instance("myorg.unknown") is None


def test_resolve_account_returns_none_for_empty_mapping() -> None:
    config = SnowflakeV2Config(account_id="abc12345")
    assert config.resolve_account_to_platform_instance("myorg.account2") is None


def test_resolve_account_matches_bare_locator_in_org_qualified_input() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"account2": "instance2"},
    )
    assert config.resolve_account_to_platform_instance("myorg.account2") == "instance2"


def test_resolve_account_matches_via_account_locator_arg() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"account2": "instance2"},
    )
    assert (
        config.resolve_account_to_platform_instance(
            "myorg.other", account_locator="account2"
        )
        == "instance2"
    )


def test_resolve_account_locator_fallback_disabled_by_default() -> None:
    config = SnowflakeV2Config(account_id="abc12345")
    assert (
        config.resolve_account_to_platform_instance(
            "myorg.unknown", account_locator="xy12345"
        )
        is None
    )


def test_resolve_account_locator_fallback_when_enabled() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_locator_fallback=True,
    )
    assert (
        config.resolve_account_to_platform_instance(
            "myorg.unknown", account_locator="XY12345"
        )
        == "xy12345"
    )


def test_resolve_account_mapping_takes_precedence_over_fallback() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"myorg.account2": "instance2"},
        account_locator_fallback=True,
    )
    assert (
        config.resolve_account_to_platform_instance(
            "myorg.account2", account_locator="account2"
        )
        == "instance2"
    )
