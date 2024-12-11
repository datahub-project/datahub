from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioContainerFiltering:
    @pytest.fixture
    def dremio_api(self, monkeypatch):
        # Mock the requests.Session
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))

        # Mock the authentication response
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        config = DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
            schema_pattern=dict(allow=[".*"], deny=[]),
        )
        report = DremioSourceReport()
        return DremioAPIOperations(config, report)

    def test_basic_allow_pattern(self, dremio_api):
        """Test basic allow pattern matching"""
        dremio_api.allow_schema_pattern = ["test"]
        dremio_api.deny_schema_pattern = []

        assert dremio_api.should_include_container([], "test")
        assert dremio_api.should_include_container(["test"], "subfolder")
        assert not dremio_api.should_include_container([], "prod_space")

    def test_basic_deny_pattern(self, dremio_api):
        """Test basic deny pattern matching"""
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = ["test_space.*"]

        assert not dremio_api.should_include_container([], "test_space")
        assert not dremio_api.should_include_container(["test_space"], "subfolder")
        assert dremio_api.should_include_container([], "prod_space")

    def test_hierarchical_matching(self, dremio_api):
        """Test matching with hierarchical paths"""
        dremio_api.allow_schema_pattern = ["prod.data.*"]
        dremio_api.deny_schema_pattern = []

        assert dremio_api.should_include_container([], "prod")
        assert dremio_api.should_include_container(["prod"], "data")
        assert dremio_api.should_include_container(["prod", "data"], "sales")
        assert not dremio_api.should_include_container([], "dev")
        assert not dremio_api.should_include_container(["dev"], "data")

    def test_allow_and_deny_patterns(self, dremio_api):
        """Test combination of allow and deny patterns"""
        dremio_api.allow_schema_pattern = ["prod.*"]
        dremio_api.deny_schema_pattern = ["prod.internal.*"]

        assert dremio_api.should_include_container([], "prod")
        assert dremio_api.should_include_container(["prod"], "public")
        assert dremio_api.should_include_container(["prod", "public"], "next")
        assert not dremio_api.should_include_container(["prod"], "internal")
        assert not dremio_api.should_include_container(["prod", "internal"], "secrets")

    def test_wildcard_patterns(self, dremio_api):
        """Test wildcard pattern handling"""
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        assert dremio_api.should_include_container([], "any_space")
        assert dremio_api.should_include_container(["any_space"], "any_folder")

        # Test with specific wildcard in middle
        dremio_api.allow_schema_pattern = ["prod.*.public"]
        assert dremio_api.should_include_container(["prod", "customer"], "public")
        assert not dremio_api.should_include_container(["prod", "customer"], "private")

    def test_case_insensitive_matching(self, dremio_api):
        """Test case-insensitive pattern matching"""
        dremio_api.allow_schema_pattern = ["PROD.*"]
        dremio_api.deny_schema_pattern = []

        assert dremio_api.should_include_container([], "prod")
        assert dremio_api.should_include_container([], "PROD")
        assert dremio_api.should_include_container(["prod"], "DATA")
        assert dremio_api.should_include_container(["PROD"], "data")

    def test_empty_patterns(self, dremio_api):
        """Test behavior with empty patterns"""
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        # Should allow everything when allow pattern is empty
        assert dremio_api.should_include_container([], "any_space")
        assert dremio_api.should_include_container(["any_space"], "any_folder")

    def test_partial_path_matching(self, dremio_api):
        """Test matching behavior with partial paths"""
        dremio_api.allow_schema_pattern = ["^pr.*.data.*"]
        dremio_api.deny_schema_pattern = []

        assert dremio_api.should_include_container(["prod"], "data")
        # Should match the partial path even though pattern doesn't have wildcards
        assert dremio_api.should_include_container(["prod", "data"], "sales")
        assert not dremio_api.should_include_container([], "dev")
        assert not dremio_api.should_include_container(["dev", "data"], "sales")

    def test_partial_start_end_chars(self, dremio_api):
        """Test matching behavior with partial paths"""
        dremio_api.allow_schema_pattern = ["pr.*.data$"]
        dremio_api.deny_schema_pattern = []

        assert dremio_api.should_include_container(["prod"], "data")
        # Should match the partial path even though pattern doesn't have wildcards
        assert not dremio_api.should_include_container(["prod", "data"], "sales")
