from packaging.version import Version

from datahub.upgrade.upgrade import VersionStats, is_client_server_compatible


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
