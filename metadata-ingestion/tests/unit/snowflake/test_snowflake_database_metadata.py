from datetime import datetime

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeDatabase


def test_snowflake_database_new_fields_defaults() -> None:
    db = SnowflakeDatabase(name="DB1", created=None, comment=None)
    assert db.origin is None
    assert db.kind is None
    assert db.is_transient is False
    assert db.retention_time is None
    assert db.owner is None


def test_snowflake_database_new_fields_populated() -> None:
    db = SnowflakeDatabase(
        name="SHARED_DB",
        created=datetime(2024, 1, 1),
        comment="A shared database",
        origin="MYORG.ACCOUNT2.SHARE_X",
        kind="IMPORTED DATABASE",
        is_transient=False,
        retention_time=1,
        owner="SYSADMIN",
    )
    assert db.origin == "MYORG.ACCOUNT2.SHARE_X"
    assert db.kind == "IMPORTED DATABASE"
    assert db.retention_time == 1
    assert db.owner == "SYSADMIN"


def test_is_shared_database() -> None:
    local_db = SnowflakeDatabase(name="LOCAL_DB", created=None, comment=None)
    assert not local_db.is_shared_database()

    shared_db = SnowflakeDatabase(
        name="SHARED_DB",
        created=None,
        comment=None,
        origin="MYORG.ACCOUNT2.SHARE_X",
    )
    assert shared_db.is_shared_database()


def test_get_share_origin_org_qualified() -> None:
    db = SnowflakeDatabase(
        name="DB",
        created=None,
        comment=None,
        origin="MYORG.ACCOUNT2.SHARE_X",
    )
    result = db.get_share_origin()
    assert result == ("MYORG", "ACCOUNT2", "SHARE_X")


def test_get_share_origin_locator_only() -> None:
    db = SnowflakeDatabase(
        name="DB",
        created=None,
        comment=None,
        origin="XY12345.SHARE_X",
    )
    result = db.get_share_origin()
    assert result == (None, "XY12345", "SHARE_X")


def test_get_share_origin_none_for_local() -> None:
    db = SnowflakeDatabase(name="DB", created=None, comment=None)
    assert db.get_share_origin() is None


def test_get_share_origin_unexpected_format() -> None:
    db = SnowflakeDatabase(
        name="DB", created=None, comment=None, origin="JUST_ONE_PART"
    )
    assert db.get_share_origin() is None


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


def test_resolve_account_fallback_to_identifier() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        account_mapping={"myorg.account2": "instance2"},
    )
    assert (
        config.resolve_account_to_platform_instance("myorg.unknown") == "myorg.unknown"
    )


def test_resolve_account_empty_mapping() -> None:
    config = SnowflakeV2Config(account_id="abc12345")
    assert (
        config.resolve_account_to_platform_instance("myorg.account2")
        == "myorg.account2"
    )
