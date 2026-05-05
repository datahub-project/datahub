from typing import List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    ShareOrigin,
    SnowflakeDatabase,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
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
    # Empty segments are malformed even when the part count would otherwise match.
    assert ShareOrigin.parse(".a.b") is None
    assert ShareOrigin.parse("a..b") is None
    assert ShareOrigin.parse("a.b.") is None
    assert ShareOrigin.parse(".") is None


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


# --- account_mapping resolution ---


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


# --- SHOW DATABASES → information_schema merge ---
#
# The merge logic in SnowflakeSchemaGenerator.get_databases populates
# origin / kind / is_transient / retention_time / owner on the ischema-derived
# results. These fields drive auto-share discovery (via `origin`) and database
# customProperties, so a regression here silently breaks the cross-account
# lineage feature for every user.


def _make_generator(
    show_databases_rows: List[SnowflakeDatabase],
    ischema_rows: List[SnowflakeDatabase],
    config: Optional[SnowflakeV2Config] = None,
) -> SnowflakeSchemaGenerator:
    """Build a minimal SnowflakeSchemaGenerator for testing get_databases().

    Bypasses the heavy real constructor and attaches only the fields the
    method actually reads.
    """
    gen = object.__new__(SnowflakeSchemaGenerator)
    gen.config = config or SnowflakeV2Config(account_id="abc12345")  # type: ignore[attr-defined]
    gen.report = SnowflakeV2Report()  # type: ignore[attr-defined]
    gen.filters = MagicMock()  # type: ignore[attr-defined]
    data_dict = MagicMock()
    data_dict.show_databases.return_value = show_databases_rows
    data_dict.get_databases.return_value = ischema_rows
    gen.data_dictionary = data_dict  # type: ignore[attr-defined]
    return gen


def test_show_databases_fields_merged_into_ischema_results() -> None:
    show_db = SnowflakeDatabase(
        name="DB1",
        created=None,
        comment=None,
        origin="MYORG.ACCT_A.ANALYTICS_SHARE",
        kind="IMPORTED DATABASE",
        is_transient=True,
        retention_time=7,
        owner="ACCOUNTADMIN",
    )
    ischema_db = SnowflakeDatabase(name="DB1", created=None, comment=None)
    gen = _make_generator([show_db], [ischema_db])

    result = gen.get_databases()

    assert result is not None
    assert len(result) == 1
    merged = result[0]
    assert merged.origin == "MYORG.ACCT_A.ANALYTICS_SHARE"
    assert merged.kind == "IMPORTED DATABASE"
    assert merged.is_transient is True
    assert merged.retention_time == 7
    assert merged.owner == "ACCOUNTADMIN"


def test_show_databases_merge_uses_uppercase_keying() -> None:
    # SHOW DATABASES returns names in their stored case; ischema may differ
    # only in case if someone created a quoted identifier. The lookup is
    # uppercased on both sides so the merge still succeeds.
    show_db = SnowflakeDatabase(
        name="db1", created=None, comment=None, origin="A.B.C", kind="STANDARD"
    )
    ischema_db = SnowflakeDatabase(name="DB1", created=None, comment=None)
    gen = _make_generator([show_db], [ischema_db])

    result = gen.get_databases()

    assert result is not None
    assert result[0].origin == "A.B.C"
    assert result[0].kind == "STANDARD"


def test_show_databases_merge_skipped_when_no_matching_show_row() -> None:
    # ischema returns a DB that wasn't in SHOW DATABASES. The fields stay at
    # their defaults rather than raising.
    show_db = SnowflakeDatabase(
        name="OTHER_DB", created=None, comment=None, origin="X.Y.Z"
    )
    ischema_db = SnowflakeDatabase(name="DB1", created=None, comment=None)
    gen = _make_generator([show_db], [ischema_db])

    result = gen.get_databases()

    assert result is not None
    assert result[0].origin is None
    assert result[0].kind is None
    assert result[0].is_transient is None
    assert result[0].retention_time is None
    assert result[0].owner is None


def test_show_databases_merge_disabled_via_flag() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        include_show_databases_metadata=False,
        auto_discover_inbound_shares=False,
    )
    show_db = SnowflakeDatabase(
        name="DB1", created=None, comment=None, origin="X.Y.Z", kind="STANDARD"
    )
    ischema_db = SnowflakeDatabase(name="DB1", created=None, comment=None)
    gen = _make_generator([show_db], [ischema_db], config=config)

    result = gen.get_databases()

    assert result is not None
    assert result[0].origin is None
    assert result[0].kind is None


def test_show_databases_merge_works_when_config_lacks_flag() -> None:
    # SnowflakeSummaryConfig doesn't define `include_show_databases_metadata`.
    # The getattr default keeps the merge enabled for that code path.
    config = MagicMock(spec=[])  # no attributes
    config.push_down_metadata_patterns = False
    show_db = SnowflakeDatabase(
        name="DB1", created=None, comment=None, origin="X.Y.Z", kind="STANDARD"
    )
    ischema_db = SnowflakeDatabase(name="DB1", created=None, comment=None)
    gen = _make_generator([show_db], [ischema_db], config=config)  # type: ignore[arg-type]

    result = gen.get_databases()

    assert result is not None
    assert result[0].origin == "X.Y.Z"
    assert result[0].kind == "STANDARD"


def test_disabling_show_databases_metadata_auto_disables_share_discovery() -> None:
    # Auto-discovery reads the SHOW DATABASES `origin` field. Without the merge
    # it would silently no-op, so the validator forces auto_discover off and
    # warns the user instead of letting them shoot themselves in the foot.
    config = SnowflakeV2Config(
        account_id="abc12345",
        include_show_databases_metadata=False,
        # auto_discover_inbound_shares defaults to True
    )
    assert config.auto_discover_inbound_shares is False


def test_show_databases_metadata_disabled_alone_is_allowed() -> None:
    # The flag exists for gradual rollout. Disabling it on its own (with
    # auto-discovery already off) is a valid combo.
    config = SnowflakeV2Config(
        account_id="abc12345",
        include_show_databases_metadata=False,
        auto_discover_inbound_shares=False,
    )
    assert config.include_show_databases_metadata is False
    assert config.auto_discover_inbound_shares is False
