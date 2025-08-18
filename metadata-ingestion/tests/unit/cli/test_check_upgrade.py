from datetime import datetime, timedelta, timezone

from packaging.version import Version

from datahub.upgrade.upgrade import (
    ClientVersionStats,
    DataHubVersionStats,
    ServerVersionStats,
    VersionStats,
    _safe_version_stats,
    get_days,
    is_client_server_compatible,
    is_server_default_cli_ahead,
)


def test_is_client_server_compatible():
    # Client and Server are Compatible
    assert compare_client_server_version_string("0.9.6", "0.9.6") == 0
    assert compare_client_server_version_string("0.9.6.1", "0.9.6") == 0

    # Server is behind Client
    assert compare_client_server_version_string("0.9.6", "0.9.5") < 0
    assert compare_client_server_version_string("0.9.6.1", "0.9.5") < 0
    assert compare_client_server_version_string("0.9.5.1", "0.8.5") < 0

    # Server is ahead of Client
    assert compare_client_server_version_string("0.9.6", "0.9.7") > 0
    assert compare_client_server_version_string("0.9.6.1", "0.9.7") > 0
    assert compare_client_server_version_string("0.8.45.1", "0.9.7") > 0

    # Pre-relase versions are ignored and compatibility is assumed
    assert compare_client_server_version_string("0.9.6.1rc1", "0.9.6") == 0
    assert compare_client_server_version_string("0.9.6.1rc1", "0.9.5") == 0
    assert compare_client_server_version_string("0.9.6.1rc1", "0.9.7") == 0


def compare_client_server_version_string(client_ver_str, server_ver_str):
    client_ver = VersionStats(version=Version(client_ver_str))
    server_ver = VersionStats(version=Version(server_ver_str))

    return is_client_server_compatible(client_ver, server_ver)


def test_safe_version_stats():
    # Valid version strings
    result = _safe_version_stats("1.2.3")
    assert result is not None
    assert result.version == Version("1.2.3")
    assert result.release_date is None

    # Invalid version strings
    assert _safe_version_stats("invalid.version") is None
    assert _safe_version_stats("") is None
    assert _safe_version_stats("not-a-version") is None

    # Edge case - this is actually valid
    assert _safe_version_stats("1.2.3.4.5.6.7") is not None


def test_get_days():
    assert get_days(None) == ""

    # Test with different time periods
    one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
    result = get_days(one_day_ago)
    assert result.startswith("(released ") and result.endswith(" ago)")
    assert "day" in result


def test_is_server_default_cli_ahead():
    def make_version_stats(client_ver, server_default_cli_ver):
        return DataHubVersionStats(
            server=ServerVersionStats(
                current=VersionStats(version=Version("0.9.6")),
                current_server_default_cli_version=(
                    VersionStats(version=Version(server_default_cli_ver))
                    if server_default_cli_ver
                    else None
                ),
            ),
            client=ClientVersionStats(
                current=VersionStats(version=Version(client_ver))
            ),
        )

    # No server default CLI version
    assert not is_server_default_cli_ahead(make_version_stats("0.9.5", None))

    # Server default CLI ahead
    assert is_server_default_cli_ahead(make_version_stats("0.9.5", "0.9.7"))

    # Server default CLI same or behind
    assert not is_server_default_cli_ahead(make_version_stats("0.9.5", "0.9.5"))
    assert not is_server_default_cli_ahead(make_version_stats("0.9.5", "0.9.4"))

    # Invalid versions (prerelease)
    assert not is_server_default_cli_ahead(make_version_stats("0.9.5rc1", "0.9.7"))
    assert not is_server_default_cli_ahead(make_version_stats("0.9.5", "0.9.7rc1"))
