import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.common.gcp_wif_config import (
    GCPWIFConfig,
    load_wif_credentials,
)


class TestGCPWIFConfigValidation:
    def test_json_field_rejects_invalid_json_string(self) -> None:
        # Strings are auto-coerced to dicts, so invalid JSON must raise clearly.
        with pytest.raises(ValueError, match="must be valid JSON"):
            GCPWIFConfig(gcp_wif_configuration_json="not-json")

    def test_json_field_accepts_valid_json_string(self) -> None:
        # Backward compat: a valid JSON string is auto-coerced to a dict.
        payload = {"type": "external_account"}
        config = GCPWIFConfig(gcp_wif_configuration_json=json.dumps(payload))
        assert config.gcp_wif_configuration_json == payload

    def test_json_field_rejects_non_dict(self) -> None:
        with pytest.raises(ValueError, match="must be a dict"):
            GCPWIFConfig(gcp_wif_configuration_json=12345)

    def test_json_string_field_invalid_raises(self):
        with pytest.raises(ValueError, match="must be valid JSON"):
            GCPWIFConfig(gcp_wif_configuration_json_string="not-json")

    def test_two_wif_options_raises(self) -> None:
        with pytest.raises(
            ValueError, match="Cannot specify multiple WIF configuration options"
        ):
            GCPWIFConfig(
                gcp_wif_configuration="/path/to/wif.json",
                gcp_wif_configuration_json={"type": "external_account"},
            )

    def test_all_three_wif_options_raises(self) -> None:
        with pytest.raises(
            ValueError, match="Cannot specify multiple WIF configuration options"
        ):
            GCPWIFConfig(
                gcp_wif_configuration="/path/to/wif.json",
                gcp_wif_configuration_json={"type": "external_account"},
                gcp_wif_configuration_json_string='{"type": "external_account"}',
            )


class TestLoadWIFCredentials:
    def _make_mock_credentials(self) -> MagicMock:
        creds = MagicMock()
        creds.token = "fake-token"
        creds.expiry = None
        creds.requires_scopes = True  # simulate a Scoped credential
        scoped = MagicMock()
        scoped.token = "fake-token"
        scoped.expiry = None
        creds.with_scopes.return_value = scoped
        return creds

    @patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
    def test_load_from_json_dict(self, mock_load: MagicMock) -> None:
        mock_creds = self._make_mock_credentials()
        mock_load.return_value = (mock_creds, "my-project")

        config = GCPWIFConfig(gcp_wif_configuration_json={"type": "external_account"})
        creds, project_id = load_wif_credentials(config)

        mock_load.assert_called_once_with({"type": "external_account"})
        assert project_id == "my-project"

    @patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
    def test_load_from_json_string(self, mock_load: MagicMock) -> None:
        mock_creds = self._make_mock_credentials()
        mock_load.return_value = (mock_creds, None)

        payload = json.dumps({"type": "external_account"})
        config = GCPWIFConfig(gcp_wif_configuration_json_string=payload)
        creds, project_id = load_wif_credentials(config)

        mock_load.assert_called_once_with({"type": "external_account"})
        assert project_id is None

    @patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
    def test_load_from_json_string_field(self, mock_load: MagicMock) -> None:
        mock_creds = self._make_mock_credentials()
        mock_load.return_value = (mock_creds, "proj")

        payload = json.dumps({"type": "external_account"})
        config = GCPWIFConfig(gcp_wif_configuration_json_string=payload)
        load_wif_credentials(config)

        mock_load.assert_called_once_with({"type": "external_account"})

    def test_load_from_file_not_found(self) -> None:
        config = GCPWIFConfig(gcp_wif_configuration="/nonexistent/path.json")
        with pytest.raises(ValueError, match="WIF configuration file not found"):
            load_wif_credentials(config)

    @patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
    def test_load_credentials_from_dict_raises(self, mock_load: MagicMock) -> None:
        mock_load.side_effect = Exception("invalid credential type")
        config = GCPWIFConfig(gcp_wif_configuration_json={"type": "invalid"})
        with pytest.raises(
            ValueError, match="Failed to load Workload Identity Federation"
        ):
            load_wif_credentials(config)

    def test_no_config_raises(self) -> None:
        config = GCPWIFConfig()
        with pytest.raises(ValueError, match="No valid WIF configuration provided"):
            load_wif_credentials(config)

    @patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
    def test_cloud_platform_scope_applied(self, mock_load: MagicMock) -> None:
        mock_creds = self._make_mock_credentials()
        scoped = mock_creds.with_scopes.return_value
        mock_load.return_value = (mock_creds, "proj")

        config = GCPWIFConfig(gcp_wif_configuration_json={"type": "external_account"})
        creds, project_id = load_wif_credentials(config)

        mock_creds.with_scopes.assert_called_once_with(
            ["https://www.googleapis.com/auth/cloud-platform"]
        )
        assert creds is scoped
        assert project_id == "proj"


class TestToWifDict:
    def test_from_file(self, tmp_path: Path) -> None:
        payload = {"type": "external_account", "audience": "//iam"}
        f = tmp_path / "wif.json"
        f.write_text(json.dumps(payload))
        config = GCPWIFConfig(gcp_wif_configuration=str(f))
        assert config.to_wif_dict() == payload

    def test_from_dict(self) -> None:
        payload = {"type": "external_account"}
        config = GCPWIFConfig(gcp_wif_configuration_json=payload)
        assert config.to_wif_dict() == payload

    def test_from_dict_returns_deep_copy_not_alias(self) -> None:
        # to_wif_dict() must return a deep copy so callers cannot mutate the
        # model's internal state — including nested dicts like credential_source.
        payload = {
            "type": "external_account",
            "credential_source": {"url": "https://example.com/token"},
        }
        config = GCPWIFConfig(gcp_wif_configuration_json=payload)
        result = config.to_wif_dict()
        result["injected"] = "value"
        result["credential_source"]["injected"] = True
        fresh = config.to_wif_dict()
        assert "injected" not in fresh
        assert "injected" not in fresh["credential_source"]

    def test_from_json_string_field(self) -> None:
        payload = {"type": "external_account"}
        config = GCPWIFConfig(gcp_wif_configuration_json_string=json.dumps(payload))
        assert config.to_wif_dict() == payload

    def test_no_config_raises(self) -> None:
        config = GCPWIFConfig()
        with pytest.raises(ValueError, match="No valid WIF configuration provided"):
            config.to_wif_dict()

    def test_non_dict_json_rejected(self) -> None:
        # JSON arrays / scalars pass the mode="before" validator (which only
        # checks parseability) but must be rejected at resolution time.
        config = GCPWIFConfig(gcp_wif_configuration_json_string="[1, 2, 3]")
        with pytest.raises(ValueError, match="must be a JSON object"):
            config.to_wif_dict()

    def test_file_not_found_raises_clear_message(self) -> None:
        config = GCPWIFConfig(gcp_wif_configuration="/nonexistent/wif.json")
        with pytest.raises(ValueError, match="WIF configuration file not found"):
            config.to_wif_dict()

    def test_file_invalid_json_raises_clear_message(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "wif.json"
        bad_file.write_text("not json")
        config = GCPWIFConfig(gcp_wif_configuration=str(bad_file))
        with pytest.raises(ValueError, match="is not valid JSON"):
            config.to_wif_dict()


class TestWifConfigSource:
    def test_file_label(self) -> None:
        config = GCPWIFConfig(gcp_wif_configuration="/path/to/wif.json")
        assert config.wif_config_source() == "file:/path/to/wif.json"

    def test_inline_json_label(self) -> None:
        config = GCPWIFConfig(gcp_wif_configuration_json={"type": "external_account"})
        assert config.wif_config_source() == "inline_json"

    def test_json_string_label(self) -> None:
        config = GCPWIFConfig(
            gcp_wif_configuration_json_string=json.dumps({"type": "external_account"})
        )
        assert config.wif_config_source() == "json_string"

    def test_no_source(self) -> None:
        config = GCPWIFConfig()
        assert config.wif_config_source() is None
