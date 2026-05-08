import json
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.common.gcp_wif_config import (
    GCPWIFConfig,
    load_wif_credentials,
)


class TestGCPWIFConfigValidation:
    def test_file_path_option(self):
        config = GCPWIFConfig(gcp_wif_configuration="/path/to/wif.json")
        assert config.gcp_wif_configuration == "/path/to/wif.json"

    def test_dict_option(self):
        config = GCPWIFConfig(
            gcp_wif_configuration_json={"type": "external_account", "audience": "//iam"}
        )
        assert config.gcp_wif_configuration_json == {
            "type": "external_account",
            "audience": "//iam",
        }

    def test_json_string_option(self):
        payload = json.dumps({"type": "external_account"})
        config = GCPWIFConfig(gcp_wif_configuration_json_string=payload)
        assert config.gcp_wif_configuration_json_string == payload

    def test_json_string_invalid_raises(self):
        with pytest.raises(Exception, match="must be valid JSON"):
            GCPWIFConfig(gcp_wif_configuration_json="not-json")

    def test_json_string_invalid_type_raises(self):
        # gcp_wif_configuration_json must be str or dict, not int
        with pytest.raises(
            Exception, match="must be either a JSON string or a dictionary"
        ):
            GCPWIFConfig(gcp_wif_configuration_json=12345)

    def test_json_string_field_invalid_raises(self):
        with pytest.raises(Exception, match="must be valid JSON"):
            GCPWIFConfig(gcp_wif_configuration_json_string="not-json")

    def test_all_none_is_valid(self):
        # GCPWIFConfig itself does not enforce that one option must be set
        config = GCPWIFConfig()
        assert config.gcp_wif_configuration is None
        assert config.gcp_wif_configuration_json is None
        assert config.gcp_wif_configuration_json_string is None


class TestLoadWIFCredentials:
    def _make_mock_credentials(self) -> MagicMock:
        creds = MagicMock()
        creds.token = "fake-token"
        creds.expiry = None
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
        config = GCPWIFConfig(gcp_wif_configuration_json=payload)
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
        with pytest.raises(
            ValueError, match="Failed to load Workload Identity Federation"
        ):
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
    def test_refresh_failure_still_returns_credentials(
        self, mock_load: MagicMock
    ) -> None:
        mock_creds = self._make_mock_credentials()
        scoped = mock_creds.with_scopes.return_value
        scoped.refresh.side_effect = Exception("token endpoint unavailable")
        mock_load.return_value = (mock_creds, "proj")

        config = GCPWIFConfig(gcp_wif_configuration_json={"type": "external_account"})
        creds, project_id = load_wif_credentials(config)

        # Credentials are still returned despite refresh failure
        assert creds is scoped
        assert project_id == "proj"
