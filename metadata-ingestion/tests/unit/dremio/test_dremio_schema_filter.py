from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_models import DremioEntityContainerType
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
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["test"], deny=[]
        )

        assert dremio_api.filter.should_include_container([], "test")
        assert dremio_api.filter.should_include_container(["test"], "subfolder")
        assert not dremio_api.filter.should_include_container([], "prod_space")
        assert not dremio_api.filter.should_include_container([], "development")
        assert not dremio_api.filter.should_include_container(["other"], "subfolder")
        # Test prefix matching behavior - "testing" matches "test" pattern due to prefix logic
        assert dremio_api.filter.should_include_container([], "testing")

    def test_basic_deny_pattern(self, dremio_api):
        """Test basic deny pattern matching"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=[".*"], deny=dremio_api.filter.config.schema_pattern.deny
        )
        dremio_api.filter.config.schema_pattern.deny = ["test_space.*"]

        assert not dremio_api.filter.should_include_container([], "test_space")
        assert not dremio_api.filter.should_include_container(
            ["test_space"], "subfolder"
        )
        assert dremio_api.filter.should_include_container([], "prod_space")
        assert dremio_api.filter.should_include_container([], "dev_space")
        assert dremio_api.filter.should_include_container(["prod_space"], "anything")

    def test_hierarchical_matching(self, dremio_api):
        """Test matching with hierarchical paths"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["prod.data.*"], deny=[]
        )

        assert dremio_api.filter.should_include_container([], "prod")
        assert dremio_api.filter.should_include_container(["prod"], "data")
        assert dremio_api.filter.should_include_container(["prod", "data"], "sales")
        assert not dremio_api.filter.should_include_container([], "dev")
        assert not dremio_api.filter.should_include_container(["dev"], "data")
        # Test container with dots in name vs hierarchical pattern
        assert dremio_api.filter.should_include_container([], "prod.data")

    def test_allow_and_deny_patterns(self, dremio_api):
        """Test combination of allow and deny patterns"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["prod.*"], deny=dremio_api.filter.config.schema_pattern.deny
        )
        dremio_api.filter.config.schema_pattern.deny = ["prod.internal.*"]

        assert dremio_api.filter.should_include_container([], "prod")
        assert dremio_api.filter.should_include_container(["prod"], "public")
        assert dremio_api.filter.should_include_container(["prod", "public"], "next")
        assert not dremio_api.filter.should_include_container(["prod"], "internal")
        assert not dremio_api.filter.should_include_container(
            ["prod", "internal"], "secrets"
        )
        assert not dremio_api.filter.should_include_container([], "dev")
        assert not dremio_api.filter.should_include_container([], "test")

    def test_wildcard_patterns(self, dremio_api):
        """Test wildcard pattern handling"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=[".*"], deny=[]
        )

        assert dremio_api.filter.should_include_container([], "any_space")
        assert dremio_api.filter.should_include_container(["any_space"], "any_folder")

        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["prod.*.public"], deny=[]
        )
        assert dremio_api.filter.should_include_container(
            ["prod", "customer"], "public"
        )
        assert not dremio_api.filter.should_include_container(
            ["prod", "customer"], "private"
        )
        assert not dremio_api.filter.should_include_container([], "dev")
        assert not dremio_api.filter.should_include_container(
            ["dev", "customer"], "public"
        )

    def test_case_insensitive_matching(self, dremio_api):
        """Test case-insensitive pattern matching"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["PROD.*"], deny=[]
        )

        assert dremio_api.filter.should_include_container([], "prod")
        assert dremio_api.filter.should_include_container([], "PROD")
        assert dremio_api.filter.should_include_container(["prod"], "DATA")
        assert dremio_api.filter.should_include_container(["PROD"], "data")
        assert not dremio_api.filter.should_include_container([], "dev")
        assert not dremio_api.filter.should_include_container([], "TEST")
        assert not dremio_api.filter.should_include_container(["other"], "data")

    def test_empty_patterns(self, dremio_api):
        """Test behavior with empty patterns"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=[".*"], deny=[]
        )

        assert dremio_api.filter.should_include_container([], "any_space")
        assert dremio_api.filter.should_include_container(["any_space"], "any_folder")
        assert dremio_api.filter.should_include_container(["completely"], "different")
        assert dremio_api.filter.should_include_container(["anything", "goes"], "here")

    def test_partial_path_matching(self, dremio_api):
        """Test matching behavior with partial paths"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["^pr.*.data.*"], deny=[]
        )

        assert dremio_api.filter.should_include_container(["prod"], "data")
        assert dremio_api.filter.should_include_container(["prod", "data"], "sales")
        assert not dremio_api.filter.should_include_container([], "dev")
        assert not dremio_api.filter.should_include_container(["dev", "data"], "sales")

    def test_partial_start_end_chars(self, dremio_api):
        """Test matching behavior with partial paths"""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["pr.*.data$"], deny=[]
        )

        assert dremio_api.filter.should_include_container(["prod"], "data")
        assert not dremio_api.filter.should_include_container(["prod", "data"], "sales")
        assert not dremio_api.filter.should_include_container([], "dev")
        assert not dremio_api.filter.should_include_container(["test"], "data")

    def test_get_all_containers_populates_filter_report_counters(self, dremio_api):
        """Regression lock for SOURCE/SPACE wiring of report_container_scanned/filtered."""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["^allowed_src$", "^allowed_space$"], deny=[]
        )

        catalog_root_response = {
            "data": [
                {
                    "id": "src-1",
                    "containerType": DremioEntityContainerType.SOURCE.value,
                    "name": "allowed_src",
                    "path": ["allowed_src"],
                },
                {
                    "id": "src-2",
                    "containerType": DremioEntityContainerType.SOURCE.value,
                    "name": "denied_src",
                    "path": ["denied_src"],
                },
                {
                    "id": "space-1",
                    "containerType": DremioEntityContainerType.SPACE.value,
                    "name": "allowed_space",
                    "path": ["allowed_space"],
                },
                {
                    "id": "space-2",
                    "containerType": DremioEntityContainerType.SPACE.value,
                    "name": "denied_space",
                    "path": ["denied_space"],
                },
            ]
        }
        per_source_response = {"config": {}, "id": "stub", "containerType": "SOURCE"}

        def fake_get(url: str) -> dict:
            if url == "/catalog":
                return catalog_root_response
            return per_source_response

        dremio_api.get = Mock(side_effect=fake_get)
        dremio_api.get_containers_for_location = Mock(return_value=[])

        dremio_api.get_all_containers()

        assert dremio_api.report.containers_scanned == 2
        assert dremio_api.report.containers_filtered == 2
        assert set(dremio_api.report.filtered) == {"denied_src", "denied_space"}

    def test_folder_traversal_populates_filter_report_counters(self, dremio_api):
        """Regression lock for FOLDER wiring; full dotted path recorded for filtered folders."""
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=[r"^src$", r"^src\.allowed_folder"], deny=[]
        )

        source_entity_response = {
            "entityType": "source",
            "children": [
                {
                    "id": "folder-allowed",
                    "type": DremioEntityContainerType.CONTAINER,
                    "path": ["src", "allowed_folder"],
                },
                {
                    "id": "folder-denied",
                    "type": DremioEntityContainerType.CONTAINER,
                    "path": ["src", "denied_folder"],
                },
            ],
        }
        folder_allowed_response = {
            "entityType": DremioEntityContainerType.FOLDER.lower(),
            "children": [],
        }
        folder_denied_response = {
            "entityType": DremioEntityContainerType.FOLDER.lower(),
            "children": [],
        }

        def fake_get(url: str) -> dict:
            if url.endswith("/folder-allowed"):
                return folder_allowed_response
            if url.endswith("/folder-denied"):
                return folder_denied_response
            return source_entity_response

        dremio_api.get = Mock(side_effect=fake_get)

        dremio_api.get_containers_for_location(
            resource_id="src-id",
            path=["src"],
            root_container_type=DremioEntityContainerType.SOURCE,
        )

        assert dremio_api.report.containers_scanned == 1
        assert dremio_api.report.containers_filtered == 1
        # report.filtered is a LossyList; iterate (via set) to unwrap the stored tuples.
        assert "src.denied_folder" in set(dremio_api.report.filtered)

    def test_escaped_dot_anchored_patterns(self, dremio_api):
        """Anchored patterns with explicitly-escaped dots (e.g. ``^prod\\.analytics``).

        The standard ``AllowDenyPattern`` treats inputs as regex, so users frequently
        escape dots to disambiguate the separator from the regex wildcard. The filter
        must therefore:

          * allow the root container that is a prefix of an escaped-dot pattern,
          * allow the matched sub-tree (and any descendants beneath it),
          * reject sibling sub-trees under the same root that do not match.

        This locks in the behaviour relied on by recipes shaped like::

            schema_pattern:
              allow:
                - ^prod$
                - ^prod\\.analytics
                - ^prod\\.reporting
        """
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=[r"^prod$", r"^prod\.analytics", r"^prod\.reporting"],
            deny=[],
        )

        assert dremio_api.filter.should_include_container([], "prod")

        assert dremio_api.filter.should_include_container(["prod"], "analytics")
        assert dremio_api.filter.should_include_container(["prod"], "reporting")

        assert dremio_api.filter.should_include_container(
            ["prod", "analytics"], "daily"
        )
        assert dremio_api.filter.should_include_container(
            ["prod", "analytics", "daily"], "rollup"
        )
        assert dremio_api.filter.should_include_container(
            ["prod", "reporting"], "exec_summary"
        )

        assert not dremio_api.filter.should_include_container(["prod"], "legacy")
        assert not dremio_api.filter.should_include_container(
            ["prod", "legacy"], "anything"
        )

        assert not dremio_api.filter.should_include_container([], "staging")
        assert not dremio_api.filter.should_include_container(["staging"], "analytics")

    def test_specific_dotstar_pattern_issue(self, dremio_api):
        """Test issue with specific .* patterns - should reject non-matching containers"""
        # This reproduces a common configuration issue
        dremio_api.filter.config.schema_pattern.allow = [
            "sdlc.mart.analytics.*",
            "sdlc.staging.analytics.*",
        ]
        dremio_api.filter.config.schema_pattern.deny = []

        # These should be allowed (matching the patterns)
        assert dremio_api.filter.should_include_container([], "sdlc")
        assert dremio_api.filter.should_include_container(["sdlc"], "mart")
        assert dremio_api.filter.should_include_container(["sdlc", "mart"], "analytics")
        assert dremio_api.filter.should_include_container(
            ["sdlc", "mart", "analytics"], "anything"
        )
        assert dremio_api.filter.should_include_container(["sdlc"], "staging")
        assert dremio_api.filter.should_include_container(
            ["sdlc", "staging"], "analytics"
        )
        assert dremio_api.filter.should_include_container(
            ["sdlc", "staging", "analytics"], "anything"
        )

        # These should be rejected (not matching the patterns)
        assert not dremio_api.filter.should_include_container([], "sdlc_master")
        assert not dremio_api.filter.should_include_container([], "finance")
        assert not dremio_api.filter.should_include_container([], "legacy")
        assert not dremio_api.filter.should_include_container([], "external")
        assert not dremio_api.filter.should_include_container(
            ["sdlc", "mart"], "reporting"
        )
        assert not dremio_api.filter.should_include_container(
            ["sdlc", "staging"], "models"
        )
