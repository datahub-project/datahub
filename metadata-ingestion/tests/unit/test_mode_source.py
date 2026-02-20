"""Unit tests for Mode connector helper functions and error handling.

Tests cover:
- _is_http_404 with various error types
- _replace_definitions circular reference and max depth protection
- _get_creator cache-on-success-only semantics
- HTTPError429/HTTPError504 response preservation
- _get_data_sources_by_id / _get_definitions_map retry on failure
- _process_report error isolation (including error handler failures)
- Dataset error isolation in _emit_workunits_for_space
- exclude_personal_collections config
"""

from typing import Dict, Iterator, List
from unittest.mock import MagicMock, patch

from requests.models import HTTPError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.mode import (
    HTTPError429,
    HTTPError504,
    ModeConfig,
    ModeSource,
    _is_http_404,
)

# ──────────────────────────────────────────────────────────────────────
# _is_http_404
# ──────────────────────────────────────────────────────────────────────


class TestIsHttp404:
    def test_with_404_response(self):
        resp = MagicMock()
        resp.status_code = 404
        error = HTTPError("not found", response=resp)
        assert _is_http_404(error) is True

    def test_with_non_404_response(self):
        resp = MagicMock()
        resp.status_code = 500
        error = HTTPError("server error", response=resp)
        assert _is_http_404(error) is False

    def test_with_none_response(self):
        error = HTTPError("no response")
        error.response = None  # type: ignore[assignment]
        assert _is_http_404(error) is False

    def test_with_no_response_attribute(self):
        """HTTPError without response attribute set at all."""
        error = HTTPError("bare error")
        assert _is_http_404(error) is False

    def test_with_non_http_error(self):
        assert _is_http_404(ValueError("something")) is False

    def test_with_429_response(self):
        resp = MagicMock()
        resp.status_code = 429
        error = HTTPError429("rate limited", response=resp)
        assert _is_http_404(error) is False


# ──────────────────────────────────────────────────────────────────────
# HTTPError429 / HTTPError504 response preservation
# ──────────────────────────────────────────────────────────────────────


class TestHTTPErrorSubclasses:
    def test_http_error_429_preserves_response(self):
        resp = MagicMock()
        resp.status_code = 429
        error = HTTPError429("rate limited", response=resp)
        assert error.response is resp
        assert error.response.status_code == 429

    def test_http_error_504_preserves_response(self):
        resp = MagicMock()
        resp.status_code = 504
        error = HTTPError504("gateway timeout", response=resp)
        assert error.response is resp
        assert error.response.status_code == 504

    def test_http_error_429_is_http_error(self):
        error = HTTPError429("test")
        assert isinstance(error, HTTPError)

    def test_http_error_504_is_http_error(self):
        error = HTTPError504("test")
        assert isinstance(error, HTTPError)


# ──────────────────────────────────────────────────────────────────────
# Thread-safe counter increments via report._lock
# ──────────────────────────────────────────────────────────────────────


class TestThreadSafeCounters:
    def test_locked_counter_increments_are_thread_safe(self):
        """Concurrent increments protected by _lock must not lose updates."""
        import concurrent.futures

        from datahub.ingestion.source.mode import ModeSourceReport

        report = ModeSourceReport()
        increments_per_thread = 1000
        num_threads = 8

        def worker():
            for _ in range(increments_per_thread):
                with report._lock:
                    report.num_sql_parsed += 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as pool:
            futures = [pool.submit(worker) for _ in range(num_threads)]
            for f in futures:
                f.result()

        assert report.num_sql_parsed == increments_per_thread * num_threads


# ──────────────────────────────────────────────────────────────────────
# _replace_definitions
# ──────────────────────────────────────────────────────────────────────


def _make_source_with_definitions(
    definitions_map: Dict[str, str],
) -> ModeSource:
    """Create a ModeSource with mocked API and a pre-populated definitions cache."""
    config = ModeConfig(
        token="test",
        password="test",
        workspace="test_workspace",
    )

    with (
        patch("datahub.ingestion.source.mode.requests.Session"),
        patch.object(ModeSource, "_get_request_json", return_value={}),
    ):
        ctx = MagicMock()
        ctx.graph = None
        ctx.pipeline_name = "test"
        ctx.run_id = "test-run"
        ctx.pipeline_config = None
        source = ModeSource(ctx, config)

    source._definitions_map_cache = definitions_map
    return source


class TestReplaceDefinitions:
    def test_no_definitions_returns_query_unchanged(self):
        source = _make_source_with_definitions({})
        query = "SELECT * FROM users"
        assert source._replace_definitions(query) == query

    def test_simple_definition_replacement(self):
        source = _make_source_with_definitions({"my_def": "SELECT id FROM orders"})
        query = "SELECT * FROM {{ @my_def as order_tbl }}"
        result = source._replace_definitions(query)
        assert "(SELECT id FROM orders) as order_tbl" in result

    def test_circular_reference_detected(self):
        source = _make_source_with_definitions(
            {
                "def_a": "SELECT * FROM {{ @def_b as alias_b }}",
                "def_b": "SELECT * FROM {{ @def_a as alias_a }}",
            }
        )
        query = "SELECT * FROM {{ @def_a as alias_a }}"
        result = source._replace_definitions(query)
        # Should complete without infinite recursion
        assert result is not None
        # The circular def_a reference should be broken (replaced with name)
        assert "def_a as alias_a" in result

    def test_max_depth_exceeded(self):
        """Deeply nested definitions beyond depth 10 return query as-is."""
        # Build a chain: def_0 -> def_1 -> def_2 -> ... -> def_11
        definitions = {}
        for i in range(12):
            if i < 11:
                definitions[f"def_{i}"] = f"SELECT * FROM {{{{ @def_{i + 1} as tbl }}}}"
            else:
                definitions[f"def_{i}"] = "SELECT 1"

        source = _make_source_with_definitions(definitions)
        query = "SELECT * FROM {{ @def_0 as tbl }}"
        result = source._replace_definitions(query)
        # Should not recurse infinitely — depth limit kicks in
        assert result is not None

    def test_missing_definition_uses_name(self):
        source = _make_source_with_definitions({})
        query = "SELECT * FROM {{ @unknown_def as alias_tbl }}"
        result = source._replace_definitions(query)
        assert "unknown_def as alias_tbl" in result


# ──────────────────────────────────────────────────────────────────────
# _get_creator cache semantics
# ──────────────────────────────────────────────────────────────────────


class TestGetCreatorCache:
    def _make_source(self) -> ModeSource:
        config = ModeConfig(
            token="test",
            password="test",
            workspace="test_workspace",
        )
        with (
            patch("datahub.ingestion.source.mode.requests.Session"),
            patch.object(ModeSource, "_get_request_json", return_value={}),
        ):
            ctx = MagicMock()
            ctx.graph = None
            ctx.pipeline_name = "test"
            ctx.run_id = "test-run"
            ctx.pipeline_config = None
            source = ModeSource(ctx, config)
        return source

    def test_successful_lookup_is_cached(self):
        source = self._make_source()
        with patch.object(
            source,
            "_get_request_json",
            return_value={"username": "alice", "email": "alice@example.com"},
        ) as mock_get:
            # First call — hits API
            result1 = source._get_creator("/api/user1")
            assert result1 == "alice"
            assert mock_get.call_count == 1

            # Second call — should use cache, not API
            result2 = source._get_creator("/api/user1")
            assert result2 == "alice"
            assert mock_get.call_count == 1

    def test_failed_lookup_is_not_cached(self):
        source = self._make_source()
        with patch.object(
            source,
            "_get_request_json",
            side_effect=[
                HTTPError("transient error"),
                {"username": "bob", "email": "bob@example.com"},
            ],
        ) as mock_get:
            # First call — fails
            result1 = source._get_creator("/api/user2")
            assert result1 is None
            assert mock_get.call_count == 1

            # Second call — should retry (not cached)
            result2 = source._get_creator("/api/user2")
            assert result2 == "bob"
            assert mock_get.call_count == 2

    def test_empty_href_returns_none_without_api_call(self):
        source = self._make_source()
        with patch.object(source, "_get_request_json") as mock_get:
            result = source._get_creator("")
            assert result is None
            mock_get.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# _get_data_sources_by_id and _get_definitions_map retry on failure
# ──────────────────────────────────────────────────────────────────────


class TestDataSourcesRetryOnFailure:
    def _make_source(self) -> ModeSource:
        config = ModeConfig(
            token="test",
            password="test",
            workspace="test_workspace",
        )
        with (
            patch("datahub.ingestion.source.mode.requests.Session"),
            patch.object(ModeSource, "_get_request_json", return_value={}),
        ):
            ctx = MagicMock()
            ctx.graph = None
            ctx.pipeline_name = "test"
            ctx.run_id = "test-run"
            ctx.pipeline_config = None
            source = ModeSource(ctx, config)
        return source

    def test_data_sources_retries_after_failure(self):
        source = self._make_source()
        ds_response = {
            "_embedded": {
                "data_sources": [
                    {
                        "id": 1,
                        "adapter": "jdbc:postgresql",
                        "name": "db",
                        "database": "mydb",
                    }
                ]
            }
        }
        with patch.object(
            source,
            "_get_request_json",
            side_effect=[HTTPError("fail"), ds_response],
        ) as mock_get:
            # First call fails — returns empty
            result1 = source._get_data_sources_by_id()
            assert result1 == {}
            assert mock_get.call_count == 1

            # Second call retries — should succeed
            result2 = source._get_data_sources_by_id()
            assert 1 in result2
            assert mock_get.call_count == 2

    def test_data_sources_caches_on_success(self):
        source = self._make_source()
        ds_response = {
            "_embedded": {
                "data_sources": [
                    {
                        "id": 1,
                        "adapter": "jdbc:postgresql",
                        "name": "db",
                        "database": "mydb",
                    }
                ]
            }
        }
        with patch.object(
            source, "_get_request_json", return_value=ds_response
        ) as mock_get:
            result1 = source._get_data_sources_by_id()
            result2 = source._get_data_sources_by_id()
            assert result1 == result2
            assert 1 in result1
            # Should only call API once — cached
            assert mock_get.call_count == 1

    def test_definitions_map_retries_after_failure(self):
        source = self._make_source()
        def_response = {
            "_embedded": {"definitions": [{"name": "my_def", "source": "SELECT 1"}]}
        }
        with patch.object(
            source,
            "_get_request_json",
            side_effect=[HTTPError("fail"), def_response],
        ) as mock_get:
            # First call fails
            result1 = source._get_definitions_map()
            assert result1 == {}

            # Second call retries
            result2 = source._get_definitions_map()
            assert "my_def" in result2
            assert mock_get.call_count == 2


# ──────────────────────────────────────────────────────────────────────
# Helpers for error isolation tests
# ──────────────────────────────────────────────────────────────────────


def _make_source() -> ModeSource:
    """Create a ModeSource with mocked API calls."""
    config = ModeConfig(
        token="test",
        password="test",
        workspace="test_workspace",
    )
    with (
        patch("datahub.ingestion.source.mode.requests.Session"),
        patch.object(ModeSource, "_get_request_json", return_value={}),
    ):
        ctx = MagicMock()
        ctx.graph = None
        ctx.pipeline_name = "test"
        ctx.run_id = "test-run"
        ctx.pipeline_config = None
        source = ModeSource(ctx, config)
    return source


def _make_workunit(id: str) -> MagicMock:
    wu = MagicMock()
    wu.id = id
    return wu


# ──────────────────────────────────────────────────────────────────────
# _process_report error isolation
# ──────────────────────────────────────────────────────────────────────


class TestProcessReportErrorIsolation:
    def test_failing_report_does_not_stop_other_reports(self):
        """If _process_report_inner raises, the error is caught and
        remaining reports can still be processed."""
        source = _make_source()

        report_ok = {"token": "ok_tok", "name": "OK Report"}
        report_bad = {"token": "bad_tok", "name": "Bad Report"}

        def fake_inner(space_token: str, report: dict) -> Iterator:
            if report["token"] == "bad_tok":
                raise RuntimeError("boom")
            yield _make_workunit(f"wu-{report['token']}")

        with patch.object(source, "_process_report_inner", side_effect=fake_inner):
            # Process the bad report first — should not raise
            bad_results = list(source._process_report("space1", report_bad))
            assert bad_results == []

            # Good report should still work
            good_results = list(source._process_report("space1", report_ok))
            assert len(good_results) == 1

        # Failure should have been recorded
        assert source.report.failures

    def test_error_handler_failure_does_not_propagate(self):
        """If report_failure itself raises (e.g. serialization error),
        the exception must not escape _process_report — otherwise
        ThreadedIteratorExecutor would kill all workers."""
        source = _make_source()
        report = {"token": "tok", "name": "Report"}

        def exploding_inner(space_token: str, report: dict) -> Iterator:
            raise RuntimeError("inner error")
            yield  # make it a generator

        with (
            patch.object(source, "_process_report_inner", side_effect=exploding_inner),
            patch.object(
                source.report,
                "report_failure",
                side_effect=TypeError("serialization bug"),
            ),
        ):
            # Must not raise — the nested except catches the handler failure
            results = list(source._process_report("space1", report))
            assert results == []


# ──────────────────────────────────────────────────────────────────────
# Dataset error isolation in _emit_workunits_for_space
# ──────────────────────────────────────────────────────────────────────


class TestDatasetErrorIsolation:
    def test_failing_dataset_does_not_abort_remaining(self):
        """An exception processing one dataset should not prevent
        subsequent datasets from being processed."""
        source = _make_source()
        source.space_tokens = {"space1": "TestSpace"}

        dataset_good = {"token": "ds_good"}
        dataset_bad = {"token": "ds_bad"}
        query = {
            "id": 1,
            "token": "q1",
            "raw_query": "SELECT 1",
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
            "data_source_id": 1,
            "last_run_id": 1,
            "_links": {"creator": {"href": ""}},
        }

        call_order: List[str] = []

        original_construct = source.construct_query_or_dataset

        def mock_construct(report_token, query_data, **kwargs):
            call_order.append(report_token)
            if report_token == "ds_bad":
                raise RuntimeError("dataset processing failed")
            return original_construct(report_token, query_data, **kwargs)

        with (
            patch.object(
                source,
                "construct_space_container",
                return_value=iter([]),
            ),
            patch.object(source, "_get_reports", return_value=iter([])),
            patch.object(
                source,
                "_get_datasets",
                return_value=iter([[dataset_bad, dataset_good]]),
            ),
            patch.object(source, "_get_queries", return_value=[query]),
            patch.object(
                source, "construct_query_or_dataset", side_effect=mock_construct
            ),
        ):
            # Should not raise
            list(source._emit_workunits_for_space("space1", "TestSpace"))

        # Both datasets should have been attempted
        assert "ds_bad" in call_order
        assert "ds_good" in call_order
        # Failure should have been recorded
        assert source.report.failures


# ──────────────────────────────────────────────────────────────────────
# exclude_personal_collections config
# ──────────────────────────────────────────────────────────────────────


class TestExcludePersonalCollections:
    def _make_source_with_config(self, **kwargs: object) -> ModeSource:
        config = ModeConfig(
            token="test",
            password="test",
            workspace="test_workspace",
            **kwargs,
        )
        with (
            patch("datahub.ingestion.source.mode.requests.Session"),
            patch.object(ModeSource, "_get_request_json", return_value={}),
        ):
            ctx = MagicMock()
            ctx.graph = None
            ctx.pipeline_name = "test"
            ctx.run_id = "test-run"
            ctx.pipeline_config = None
            source = ModeSource(ctx, config)
        return source

    def test_default_uses_custom_filter(self):
        """Default exclude_personal_collections=True should use ?filter=custom."""
        source = self._make_source_with_config()
        assert source.config.exclude_personal_collections is True

        with patch.object(
            source, "_get_paged_request_json", return_value=iter([])
        ) as mock_paged:
            source._get_space_name_and_tokens()
            mock_paged.assert_called_once()
            url_arg = mock_paged.call_args[0][0]
            assert "?filter=custom" in url_arg

    def test_false_uses_all_filter(self):
        """exclude_personal_collections=False should use ?filter=all."""
        source = self._make_source_with_config(exclude_personal_collections=False)

        with patch.object(
            source, "_get_paged_request_json", return_value=iter([])
        ) as mock_paged:
            source._get_space_name_and_tokens()
            mock_paged.assert_called_once()
            url_arg = mock_paged.call_args[0][0]
            assert "?filter=all" in url_arg

    def test_space_pattern_still_applies_with_custom_filter(self):
        """Even with server-side filtering, client-side space_pattern
        should still filter spaces."""
        source = self._make_source_with_config(
            space_pattern=AllowDenyPattern(allow=["^Allowed"]),
        )
        spaces_page = [
            {"token": "tok1", "name": "Allowed Space"},
            {"token": "tok2", "name": "Blocked Space"},
        ]

        with patch.object(
            source, "_get_paged_request_json", return_value=iter([spaces_page])
        ):
            result = source._get_space_name_and_tokens()

        assert "tok1" in result
        assert "tok2" not in result
