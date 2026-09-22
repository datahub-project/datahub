from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.source.langfuse.langfuse_client import (
    LangfuseAuthenticationError,
    LangfuseClient,
    LangfuseObservation,
    LangfusePromptVersion,
    LangfuseScore,
)
from datahub.ingestion.source.langfuse.langfuse_config import LangfuseConnectionConfig


@pytest.fixture
def connection() -> LangfuseConnectionConfig:
    return LangfuseConnectionConfig(
        host="http://langfuse.test",
        public_key="pk-test",
        secret_key=SecretStr("sk-test"),
    )


@pytest.fixture
def mock_session():
    with patch("requests.Session") as session_cls:
        yield session_cls.return_value


@pytest.fixture
def client(
    connection: LangfuseConnectionConfig, mock_session: MagicMock
) -> LangfuseClient:
    return LangfuseClient(connection=connection, page_size=2)


def _response(status_code: int, payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    if status_code >= 400 and status_code != 401:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestCursorPagination:
    def test_follows_cursor_across_multiple_pages_and_stops_when_absent(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.side_effect = [
            _response(
                200, {"data": [{"id": "1"}, {"id": "2"}], "meta": {"cursor": "abc"}}
            ),
            _response(200, {"data": [{"id": "3"}], "meta": {"cursor": None}}),
        ]

        items = list(client._iter_cursor_paginated("/api/public/v2/observations", {}))

        assert [i["id"] for i in items] == ["1", "2", "3"]
        assert mock_session.get.call_count == 2
        # Second call must carry the cursor returned by the first.
        second_call_params = mock_session.get.call_args_list[1].kwargs["params"]
        assert second_call_params["cursor"] == "abc"

    def test_stops_immediately_when_first_page_has_no_cursor(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.return_value = _response(
            200, {"data": [{"id": "1"}], "meta": {}}
        )

        items = list(client._iter_cursor_paginated("/api/public/v3/scores", {}))

        assert len(items) == 1
        assert mock_session.get.call_count == 1


class TestPagePagination:
    def test_follows_pages_until_total_pages_reached(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.side_effect = [
            _response(
                200,
                {"data": [{"id": "1"}], "meta": {"page": 1, "totalPages": 2}},
            ),
            _response(
                200,
                {"data": [{"id": "2"}], "meta": {"page": 2, "totalPages": 2}},
            ),
        ]

        items = list(client._iter_page_paginated("/api/public/v2/prompts", {}))

        assert [i["id"] for i in items] == ["1", "2"]
        assert mock_session.get.call_count == 2

    def test_stops_when_page_returns_no_items(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.return_value = _response(200, {"data": [], "meta": {}})

        items = list(client._iter_page_paginated("/api/public/v2/prompts", {}))

        assert items == []
        assert mock_session.get.call_count == 1


class TestCleanParams:
    def test_booleans_are_lowercased_strings(self):
        cleaned = LangfuseClient._clean_params({"isRootObservation": True, "x": False})
        assert cleaned == {"isRootObservation": "true", "x": "false"}

    def test_none_values_are_dropped(self):
        cleaned = LangfuseClient._clean_params({"a": None, "b": "keep"})
        assert cleaned == {"b": "keep"}

    def test_datetimes_are_isoformatted(self):
        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        cleaned = LangfuseClient._clean_params({"fromStartTime": dt})
        assert cleaned["fromStartTime"] == dt.isoformat()


class TestAuthentication:
    def test_401_raises_langfuse_authentication_error(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.return_value = _response(
            401, {"message": "No authorization header"}
        )

        with pytest.raises(LangfuseAuthenticationError):
            client._get("/api/public/v2/prompts")

    def test_get_project_raises_when_no_projects_returned(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.return_value = _response(200, {"data": []})

        with pytest.raises(LangfuseAuthenticationError):
            client.get_project()


class TestMalformedRecordIsolation:
    """A single malformed record must not abort the whole paginated fetch."""

    def test_observation_missing_required_field_is_skipped_not_fatal(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.return_value = _response(
            200,
            {
                "data": [
                    {"id": "good-1", "traceId": "t1", "type": "GENERATION"},
                    {"id": "bad-1"},  # missing required "traceId"
                    {"id": "good-2", "traceId": "t2", "type": "GENERATION"},
                ],
                "meta": {},
            },
        )

        now = datetime.now(tz=timezone.utc)
        observations = list(client.iter_observations(now, now))

        assert [o.id for o in observations] == ["good-1", "good-2"]

    def test_score_missing_required_field_is_skipped_not_fatal(
        self, client: LangfuseClient, mock_session: MagicMock
    ):
        mock_session.get.return_value = _response(
            200,
            {
                "data": [
                    {"id": "good-1", "name": "helpfulness", "dataType": "NUMERIC"},
                    {"id": "bad-1"},  # missing required "name"
                ],
                "meta": {},
            },
        )

        now = datetime.now(tz=timezone.utc)
        scores = list(client.iter_scores(now, now))

        assert [s.id for s in scores] == ["good-1"]


class TestModelParsing:
    def test_observation_from_json_defaults_missing_optional_fields(self):
        obs = LangfuseObservation.from_json(
            {"id": "obs-1", "traceId": "trace-1", "type": "GENERATION"}
        )
        assert obs.is_root_observation is False
        assert obs.usage_details == {}
        assert obs.model is None

    def test_score_from_json_extracts_observation_subject(self):
        score = LangfuseScore.from_json(
            {
                "id": "score-1",
                "name": "helpfulness",
                "value": 0.9,
                "dataType": "NUMERIC",
                "timestamp": "2026-01-01T00:00:00Z",
                "subject": {"kind": "observation", "id": "obs-1", "traceId": "trace-1"},
            }
        )
        assert score.subject_kind == "observation"
        assert score.subject_id == "obs-1"
        assert score.subject_trace_id == "trace-1"

    def test_prompt_version_from_json_parses_chat_prompt(self):
        prompt = LangfusePromptVersion.from_json(
            {
                "name": "greeting",
                "version": 3,
                "type": "chat",
                "prompt": [{"role": "system", "content": "Hi"}],
                "config": {"temperature": 0.5},
                "labels": ["production"],
                "tags": ["v1"],
            }
        )
        assert prompt.prompt_type == "chat"
        assert prompt.labels == ["production"]
        assert prompt.prompt == [{"role": "system", "content": "Hi"}]
