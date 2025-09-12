from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from packaging.version import Version

from datahub.upgrade.upgrade import (
    ClientVersionStats,
    DataHubVersionStats,
    ServerVersionStats,
    VersionStats,
    _maybe_print_upgrade_message,
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


@patch("datahub.upgrade.upgrade.click.echo")
def test_cloud_server_skips_version_warnings(mock_echo):
    """Test that cloud servers (serverEnv=cloud) skip version mismatch warnings."""
    version_stats = DataHubVersionStats(
        server=ServerVersionStats(
            current=VersionStats(version=Version("0.3.13")),
            current_server_type="prod",  # serverType from config
            is_cloud_server=True,  # serverEnv="cloud" -> is_datahub_cloud=True
        ),
        client=ClientVersionStats(current=VersionStats(version=Version("1.2.0.4"))),
    )

    # Should not show any version warnings for cloud servers
    _maybe_print_upgrade_message(version_stats)
    mock_echo.assert_not_called()


@patch("datahub.upgrade.upgrade.click.echo")
def test_non_cloud_server_shows_version_warnings(mock_echo):
    """Test that non-cloud servers show version mismatch warnings."""
    version_stats = DataHubVersionStats(
        server=ServerVersionStats(
            current=VersionStats(version=Version("0.9.5")),
            current_server_type="oss",
            is_cloud_server=False,
        ),
        client=ClientVersionStats(current=VersionStats(version=Version("0.9.7"))),
    )

    # Should show version warning for non-cloud servers with version mismatch
    _maybe_print_upgrade_message(version_stats)
    mock_echo.assert_called_once()

    # Verify the warning message contains the expected text
    call_args = mock_echo.call_args[0][0]
    assert "❗Client-Server Incompatible❗" in call_args
    assert "0.9.7" in call_args  # Client version
    assert "0.9.5" in call_args  # Server version
