"""Unit tests for Google Sheets config module."""

from unittest.mock import patch

import pytest

from datahub.ingestion.source.google_sheets.config import GoogleSheetsSourceConfig


class TestGoogleSheetsSourceConfig:
    """Tests for GoogleSheetsSourceConfig."""

    @patch("os.path.exists")
    def test_invalid_credentials_path(self, mock_exists):
        """Test that invalid credentials path raises error."""
        mock_exists.return_value = False
        with pytest.raises(ValueError, match="Credentials file .* does not exist"):
            GoogleSheetsSourceConfig.model_validate({"credentials": "nonexistent.json"})
