"""
Tests for Dremio Software 26.1.0+ compatibility where
sys.jobs_recent.queried_datasets changed from VARCHAR to ARRAY<VARCHAR>.
"""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries


class TestQueriedDatasetsArrayQuery:
    def test_array_query_uses_array_size_predicate(self):
        # 26.1.0+ array column — LENGTH() no longer applies.
        query = DremioSQLQueries.get_query_all_jobs_array()
        assert "ARRAY_SIZE(queried_datasets)>0" in query
        assert "LENGTH(queried_datasets)" not in query

    def test_array_query_wraps_array_as_bracket_string(self):
        # Keeps DremioQuery._get_queried_datasets's "[ds1,ds2]" parser stable.
        query = DremioSQLQueries.get_query_all_jobs_array()
        assert "CONCAT('[', ARRAY_TO_STRING(queried_datasets, ','), ']')" in query

    def test_array_query_targets_jobs_recent_not_history(self):
        # EE/CE 26.1.0+ stays on sys.jobs_recent (cloud uses history.jobs).
        query = DremioSQLQueries.get_query_all_jobs_array()
        assert "SYS.JOBS_RECENT" in query
        assert "sys.project.history.jobs" not in query

    def test_legacy_query_unchanged(self):
        query = DremioSQLQueries.get_query_all_jobs()
        assert "LENGTH(queried_datasets)>0" in query
        assert "ARRAY_SIZE(queried_datasets)" not in query


class TestSupportsArrayQueriedDatasetsProbe:
    @pytest.fixture
    def dremio_api(self, monkeypatch):
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        config = DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )
        report = DremioSourceReport()
        api = DremioAPIOperations(config, report)
        api.session = mock_session
        return api

    def test_cloud_skips_probe_and_returns_true(self, dremio_api):
        dremio_api.edition = DremioEdition.CLOUD
        # Probe would target the wrong table on Cloud — must not run.
        dremio_api.execute_query_iter = Mock(
            side_effect=AssertionError("Cloud must not run the EE/CE probe")
        )
        assert dremio_api._supports_array_queried_datasets() is True

    def test_array_column_probe_succeeds(self, dremio_api):
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.execute_query_iter = Mock(return_value=iter([{"sz": 0}]))
        assert dremio_api._supports_array_queried_datasets() is True

    def test_varchar_column_probe_raises_and_falls_back(self, dremio_api):
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.execute_query_iter = Mock(
            side_effect=DremioAPIException(
                "Function ARRAY_SIZE not found with arguments [VARCHAR]"
            )
        )
        assert dremio_api._supports_array_queried_datasets() is False

    def test_probe_falls_back_on_non_dremio_exception(self, dremio_api):
        # RuntimeError (FAILED/CANCELED) and transport errors must also fall back.
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.execute_query_iter = Mock(
            side_effect=RuntimeError("Query failed: planner exception")
        )
        assert dremio_api._supports_array_queried_datasets() is False

    def test_probe_is_cached_after_success(self, dremio_api):
        dremio_api.edition = DremioEdition.ENTERPRISE
        mock = Mock(return_value=iter([{"sz": 0}]))
        dremio_api.execute_query_iter = mock

        assert dremio_api._supports_array_queried_datasets() is True
        assert dremio_api._supports_array_queried_datasets() is True
        assert mock.call_count == 1

    def test_probe_is_cached_after_failure(self, dremio_api):
        dremio_api.edition = DremioEdition.ENTERPRISE
        mock = Mock(side_effect=DremioAPIException("type mismatch"))
        dremio_api.execute_query_iter = mock

        assert dremio_api._supports_array_queried_datasets() is False
        assert dremio_api._supports_array_queried_datasets() is False
        assert mock.call_count == 1


class TestExtractAllQueriesDispatch:
    """Verify the right SQL form is sent to Dremio per edition + probe result."""

    @pytest.fixture
    def dremio_api(self, monkeypatch):
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        config = DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )
        report = DremioSourceReport()
        api = DremioAPIOperations(config, report)
        api.session = mock_session
        api.start_time = None  # type: ignore[assignment]
        api.end_time = None  # type: ignore[assignment]
        return api

    def _captured_query(self, dremio_api):
        sent: list = []

        def _capture(query):
            sent.append(query)
            return iter([])

        dremio_api.execute_query_iter = Mock(side_effect=_capture)
        list(dremio_api.extract_all_queries())
        return sent[-1]

    def test_cloud_uses_history_jobs_query(self, dremio_api):
        dremio_api.edition = DremioEdition.CLOUD
        query = self._captured_query(dremio_api)
        assert "sys.project.history.jobs" in query

    def test_enterprise_array_uses_new_query(self, dremio_api):
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api._queried_datasets_is_array = True
        query = self._captured_query(dremio_api)
        assert "SYS.JOBS_RECENT" in query
        assert "ARRAY_SIZE(queried_datasets)>0" in query
        assert "CONCAT('[', ARRAY_TO_STRING(queried_datasets, ','), ']')" in query

    def test_enterprise_legacy_uses_legacy_query(self, dremio_api):
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api._queried_datasets_is_array = False
        query = self._captured_query(dremio_api)
        assert "SYS.JOBS_RECENT" in query
        assert "LENGTH(queried_datasets)>0" in query
        assert "ARRAY_SIZE(queried_datasets)" not in query

    def test_community_array_uses_new_query(self, dremio_api):
        dremio_api.edition = DremioEdition.COMMUNITY
        dremio_api._queried_datasets_is_array = True
        query = self._captured_query(dremio_api)
        assert "ARRAY_SIZE(queried_datasets)>0" in query
