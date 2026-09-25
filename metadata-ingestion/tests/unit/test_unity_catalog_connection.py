"""Tests for Unity Catalog connection helpers — focused on Databricks
User-Agent attribution so Databricks's partner-usage telemetry can identify
DataHub traffic."""

import pytest

from datahub.ingestion.source.unity.connection import (
    DATABRICKS_USER_AGENT_ENTRY,
    UnityCatalogConnectionConfig,
    _sanitize_semver,
    create_workspace_client,
    get_sql_connection_params,
)


class TestSanitizeSemver:
    def test_three_segment_passes_through(self) -> None:
        assert _sanitize_semver("1.6.0") == "1.6.0"

    def test_four_segment_truncates(self) -> None:
        # acryl-datahub releases use a 4-segment PEP-440 version like 1.5.0.19.
        # The Databricks SDK SemVer regex only accepts 3 segments.
        assert _sanitize_semver("1.5.0.19") == "1.5.0"

    def test_dev_sentinel_falls_back(self) -> None:
        # nice_version_name() returns this literal phrase in develop installs.
        assert (
            _sanitize_semver("unavailable (installed in develop mode)") == "0.0.0-dev"
        )

    def test_pep440_epoch_falls_back(self) -> None:
        # The in-repo __version__ literal is "1!0.0.0.dev0" — PEP 440 epoch
        # syntax does not match the SemVer regex.
        assert _sanitize_semver("1!0.0.0.dev0") == "0.0.0-dev"

    def test_empty_string_falls_back(self) -> None:
        assert _sanitize_semver("") == "0.0.0-dev"

    def test_pre_release_suffix_stripped(self) -> None:
        # Only the leading 3 numeric segments matter; suffixes are dropped to
        # keep the value unambiguous for downstream parsers.
        assert _sanitize_semver("1.6.0rc1") == "1.6.0"


class TestUserAgentAssembly:
    def test_workspace_client_registers_partner_tag(self) -> None:
        # Exercises create_workspace_client and verifies the assembled UA
        # contains both `partner/datahub` (the partner-telemetry token) and a
        # well-formed `datahub/<semver>` product token. The product/version
        # slot lives on the per-client Config; the partner tag lives in
        # SDK-global extras and is appended by to_string().
        config = UnityCatalogConnectionConfig(
            workspace_url="https://example.cloud.databricks.com",
            token="dapi-fake-token-for-test",
        )

        client = create_workspace_client(config)

        ua = client.config.user_agent
        assert "partner/datahub" in ua, f"Expected partner tag in UA, got: {ua}"
        assert ua.startswith(f"{DATABRICKS_USER_AGENT_ENTRY}/"), (
            f"Expected UA to lead with {DATABRICKS_USER_AGENT_ENTRY}/<ver>, got: {ua}"
        )

    @pytest.mark.parametrize(
        "warehouse_id",
        ["abc-123-warehouse", "0123456789abcdef"],
    )
    def test_sql_connection_params_carry_versioned_user_agent(
        self, warehouse_id: str
    ) -> None:
        # The databricks-sql-connector uses its own user_agent_entry field,
        # separate from the SDK's UA assembly. Confirm we send the versioned
        # form there too.
        config = UnityCatalogConnectionConfig(
            workspace_url="https://example.cloud.databricks.com",
            token="dapi-fake-token-for-test",
            warehouse_id=warehouse_id,
        )
        client = create_workspace_client(config)

        params = get_sql_connection_params(client)

        entry = params["user_agent_entry"]
        assert entry.startswith(f"{DATABRICKS_USER_AGENT_ENTRY}/"), entry
        # Whatever sanitization produced, the version slot must be non-empty
        # and must not contain whitespace (would corrupt the UA header).
        _, _, version = entry.partition("/")
        assert version, "version slot must be populated"
        assert " " not in version, "version must not contain whitespace"
