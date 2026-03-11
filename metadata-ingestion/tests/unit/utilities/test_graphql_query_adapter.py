"""Unit tests for GraphQL query adaptation."""

import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pytest
from graphql import GraphQLSchema, build_schema
from graphql.utilities import introspection_from_schema

from datahub.utilities.graphql_query_adapter import (
    SCHEMA_FAILURE_TTL_SECONDS,
    SCHEMA_TTL_SECONDS,
    QueryProjector,
)


@pytest.fixture(autouse=True)
def _isolate_disk_cache(tmp_path):
    """Prevent tests from reading/writing the real ~/.datahub/schema_cache."""
    with patch(
        "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
        str(tmp_path / "schema_cache"),
    ):
        yield


@pytest.fixture
def old_server_schema():
    """Schema without Document entity (simulates v0.12.5 server)."""
    schema_str = """
        type Query {
            searchAcrossEntities(input: SearchInput!): SearchResults
        }

        input SearchInput {
            query: String!
            types: [String!]
        }

        type SearchResults {
            searchResults: [SearchResult!]!
        }

        type SearchResult {
            entity: Entity
        }

        union Entity = Dataset | Chart | Dashboard

        type Dataset {
            urn: String!
            name: String!
        }

        type Chart {
            urn: String!
            title: String!
        }

        type Dashboard {
            urn: String!
            title: String!
        }
    """
    return build_schema(schema_str)


@pytest.fixture
def new_server_schema():
    """Schema with Document entity (simulates v0.13.0+ server)."""
    schema_str = """
        type Query {
            searchAcrossEntities(input: SearchInput!): SearchResults
        }

        input SearchInput {
            query: String!
            types: [String!]
        }

        type SearchResults {
            searchResults: [SearchResult!]!
        }

        type SearchResult {
            entity: Entity
        }

        union Entity = Dataset | Chart | Dashboard | Document

        type Dataset {
            urn: String!
            name: String!
        }

        type Chart {
            urn: String!
            title: String!
        }

        type Dashboard {
            urn: String!
            title: String!
        }

        type Document {
            urn: String!
            info: DocumentInfo!
        }

        type DocumentInfo {
            title: String!
            description: String
        }
    """
    return build_schema(schema_str)


def _make_mock_graph(
    schema: GraphQLSchema,
    commit_hash: str = "abc123",
    server_url: str = "http://localhost:8080",
) -> Mock:
    """Create a mock DataHubGraph that returns introspection for the given schema.

    The mock's execute_graphql matches the real signature including
    strip_unsupported_fields, and returns the introspection result directly
    (matching real execute_graphql which returns result["data"]).
    """
    graph = Mock()
    call_log: List[Dict[str, Any]] = []

    def execute_graphql(query: str, strip_unsupported_fields: bool = False) -> dict:
        call_log.append(
            {"query": query, "strip_unsupported_fields": strip_unsupported_fields}
        )
        if "__schema" in query:
            return dict(introspection_from_schema(schema))
        return {}

    graph.execute_graphql = execute_graphql
    graph._execute_graphql_call_log = call_log
    graph._gms_server = server_url
    graph.server_config = Mock()
    graph.server_config.commit_hash = commit_hash
    return graph


@pytest.fixture
def mock_graph_old_schema(old_server_schema):
    """Mock DataHubGraph that returns old schema introspection."""
    return _make_mock_graph(old_server_schema)


@pytest.fixture
def mock_graph_new_schema(new_server_schema):
    """Mock DataHubGraph that returns new schema introspection."""
    return _make_mock_graph(new_server_schema)


SIMPLE_QUERY = """
    query {
        searchAcrossEntities(input: {query: "*"}) {
            searchResults {
                entity {
                    ... on Dataset { urn }
                }
            }
        }
    }
"""


class TestQueryProjector:
    """Tests for the QueryProjector class."""

    def test_adapt_query_removes_unsupported_inline_fragment(
        self, mock_graph_old_schema
    ):
        """Test that inline fragments for unsupported types are removed."""
        query = """
            query SearchQuery {
                searchAcrossEntities(input: {query: "*", types: []}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                                name
                            }
                            ... on Document {
                                urn
                                info {
                                    title
                                }
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert len(removed_fields) == 1
        assert "Document" in removed_fields[0]

        assert "... on Dataset" in adapted_query
        assert "urn" in adapted_query
        assert "name" in adapted_query

    def test_adapt_query_keeps_all_fragments_when_supported(
        self, mock_graph_new_schema
    ):
        """Test that all fragments are kept when server supports them."""
        query = """
            query SearchQuery {
                searchAcrossEntities(input: {query: "*", types: []}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                                name
                            }
                            ... on Document {
                                urn
                                info {
                                    title
                                }
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_new_schema
        )

        assert len(removed_fields) == 0
        assert "... on Dataset" in adapted_query
        assert "... on Document" in adapted_query

    def test_adapt_query_removes_unsupported_field(self):
        """Test that unsupported fields on supported types are removed."""
        schema_without_description = build_schema(
            """
            type Query {
                searchAcrossEntities(input: SearchInput!): SearchResults
            }

            input SearchInput {
                query: String!
            }

            type SearchResults {
                searchResults: [SearchResult!]!
            }

            type SearchResult {
                entity: Entity
            }

            union Entity = Dataset

            type Dataset {
                urn: String!
                name: String!
            }
        """
        )

        graph = _make_mock_graph(schema_without_description)

        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                                name
                                description
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(query, graph)

        assert "description" not in adapted_query
        assert len(removed_fields) == 1
        assert "description" in removed_fields[0].lower()

        assert "urn" in adapted_query
        assert "name" in adapted_query

    def test_schema_caching(self, mock_graph_old_schema):
        """Test that schema is cached and reused across calls."""
        projector = QueryProjector()

        projector.adapt_query(SIMPLE_QUERY, mock_graph_old_schema)
        assert projector._cached_schema is not None
        assert projector._schema_generation == 1

        projector.adapt_query(SIMPLE_QUERY, mock_graph_old_schema)
        # Still generation 1 — schema was reused, not re-fetched
        assert projector._schema_generation == 1

        # Only one introspection call total
        introspection_calls = [
            c
            for c in mock_graph_old_schema._execute_graphql_call_log
            if "__schema" in c["query"]
        ]
        assert len(introspection_calls) == 1

    def test_introspection_fields_preserved(self, mock_graph_old_schema):
        """Test that introspection fields like __typename are preserved."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            __typename
                            ... on Dataset {
                                urn
                                name
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "__typename" in adapted_query
        assert len(removed_fields) == 0

    def test_multiple_unsupported_fragments_removed(self, mock_graph_old_schema):
        """Test removal of multiple unsupported inline fragments."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset { urn }
                            ... on Document { urn }
                            ... on UnsupportedType { urn }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert "... on UnsupportedType" not in adapted_query
        assert len(removed_fields) == 2

        assert "... on Dataset" in adapted_query

    def test_parse_error_propagated(self, mock_graph_old_schema):
        """Test that parse errors are properly propagated."""
        from graphql import GraphQLSyntaxError

        invalid_query = "query { searchAcrossEntities(input: {query: '*'}"

        projector = QueryProjector()
        with pytest.raises(GraphQLSyntaxError):
            projector.adapt_query(invalid_query, mock_graph_old_schema)

    def test_empty_query(self, mock_graph_old_schema):
        """Test handling of empty queries."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "searchAcrossEntities" in adapted_query
        assert len(removed_fields) == 0

    def test_introspection_uses_strip_false(self, old_server_schema):
        """Test that introspection call passes strip_unsupported_fields=False."""
        graph = _make_mock_graph(old_server_schema)

        projector = QueryProjector()
        projector.adapt_query(SIMPLE_QUERY, graph)

        # The introspection call should have strip_unsupported_fields=False
        introspection_calls = [
            c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
        ]
        assert len(introspection_calls) == 1
        assert introspection_calls[0]["strip_unsupported_fields"] is False

    def test_strip_unsupported_fields_false_bypasses_projection(
        self, old_server_schema
    ):
        """Test that strip_unsupported_fields=False skips projection entirely."""
        query_with_unsupported = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Document { urn }
                        }
                    }
                }
            }
        """

        # When strip_unsupported_fields=True, Document gets stripped
        graph = _make_mock_graph(old_server_schema)
        projector = QueryProjector()
        adapted_query, removed = projector.adapt_query(query_with_unsupported, graph)
        assert "... on Document" not in adapted_query
        assert len(removed) == 1

    def test_default_strip_unsupported_fields_applies_projection(
        self, mock_graph_old_schema
    ):
        """Test that default (strip_unsupported_fields=True) applies projection."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset { urn }
                            ... on Document { urn }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert "... on Dataset" in adapted_query
        assert len(removed_fields) == 1


class TestSchemaInvalidation:
    """Tests for schema caching, TTL, commit-hash invalidation, and negative caching."""

    def test_schema_ttl_expiry(self, old_server_schema):
        """Schema is re-fetched after TTL expires, even if commit hash hasn't changed."""
        graph = _make_mock_graph(old_server_schema)
        projector = QueryProjector()

        projector.adapt_query(SIMPLE_QUERY, graph)
        assert projector._schema_generation == 1

        # Backdate fetched_at to simulate TTL expiry
        projector._schema_fetched_at = time.monotonic() - SCHEMA_TTL_SECONDS - 1

        projector.adapt_query(SIMPLE_QUERY, graph)
        # Generation bumps even if schema was loaded from disk cache
        assert projector._schema_generation == 2

    def test_schema_invalidated_on_commit_hash_change(self, old_server_schema):
        """Schema is re-fetched when server commit hash changes, even before TTL."""
        graph = _make_mock_graph(old_server_schema, commit_hash="hash_v1")
        projector = QueryProjector()

        projector.adapt_query(SIMPLE_QUERY, graph)
        assert projector._schema_generation == 1
        assert projector._schema_commit_hash == "hash_v1"

        # Simulate server upgrade — change commit hash
        graph.server_config.commit_hash = "hash_v2"

        projector.adapt_query(SIMPLE_QUERY, graph)
        assert projector._schema_generation == 2
        assert projector._schema_commit_hash == "hash_v2"

    def test_negative_caching_on_introspection_failure(self, old_server_schema):
        """Failed introspection is negatively cached; retries after failure TTL."""
        graph = _make_mock_graph(old_server_schema)

        # Make introspection fail
        original_execute = graph.execute_graphql

        def failing_execute(
            query: str, strip_unsupported_fields: bool = False
        ) -> Dict[str, Any]:
            if "__schema" in query:
                raise RuntimeError("server down")
            return original_execute(query, strip_unsupported_fields)

        graph.execute_graphql = failing_execute

        projector = QueryProjector()

        # First call fails
        with pytest.raises(RuntimeError, match="server down"):
            projector.adapt_query(SIMPLE_QUERY, graph)

        # Second call hits negative cache (fast failure)
        with pytest.raises(RuntimeError, match="retry after backoff"):
            projector.adapt_query(SIMPLE_QUERY, graph)

        # After failure TTL, retry actually hits server again
        projector._schema_fetched_at = time.monotonic() - SCHEMA_FAILURE_TTL_SECONDS - 1

        # Restore working introspection
        graph.execute_graphql = original_execute
        projector.adapt_query(SIMPLE_QUERY, graph)
        assert projector._cached_schema is not None
        assert projector._schema_fetch_failed is False

    def test_concurrent_introspection_serialized(self, old_server_schema):
        """Multiple threads only trigger one introspection call."""
        graph = _make_mock_graph(old_server_schema)
        projector = QueryProjector()

        barrier = threading.Barrier(5)
        errors: List[Exception] = []

        def worker():
            try:
                barrier.wait(timeout=5)
                projector.adapt_query(SIMPLE_QUERY, graph)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        # Only 1 introspection call despite 5 concurrent threads
        introspection_calls = [
            c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
        ]
        assert len(introspection_calls) == 1


class TestQueryResultCache:
    """Tests for the per-query result cache."""

    def test_query_result_caching(self, mock_graph_old_schema):
        """Same query twice produces one cache entry and skips re-parsing."""
        projector = QueryProjector()

        result1 = projector.adapt_query(SIMPLE_QUERY, mock_graph_old_schema)
        result2 = projector.adapt_query(SIMPLE_QUERY, mock_graph_old_schema)

        assert result1 == result2
        assert len(projector._query_cache) == 1

    def test_query_cache_invalidated_on_schema_refresh(self, old_server_schema):
        """Query cache is cleared when schema generation bumps."""
        graph = _make_mock_graph(old_server_schema)
        projector = QueryProjector()

        projector.adapt_query(SIMPLE_QUERY, graph)
        assert len(projector._query_cache) == 1

        # Force schema refresh via TTL expiry
        projector._schema_fetched_at = time.monotonic() - SCHEMA_TTL_SECONDS - 1

        projector.adapt_query(SIMPLE_QUERY, graph)
        # Cache was cleared and repopulated — still 1 entry but generation bumped
        assert projector._schema_generation == 2
        cache_keys = list(projector._query_cache.keys())
        assert all(gen == 2 for _, gen in cache_keys)

    def test_query_cache_eviction(self, mock_graph_old_schema):
        """Oldest entries are evicted when cache exceeds max size."""
        projector = QueryProjector()

        with patch("datahub.utilities.graphql_query_adapter.QUERY_CACHE_MAX_SIZE", 3):
            for i in range(5):
                query = f"""
                    query {{
                        searchAcrossEntities(input: {{query: "test{i}"}}) {{
                            searchResults {{
                                entity {{
                                    ... on Dataset {{ urn }}
                                }}
                            }}
                        }}
                    }}
                """
                projector.adapt_query(query, mock_graph_old_schema)

            assert len(projector._query_cache) == 3


class TestDiskCache:
    """Tests for on-disk schema caching."""

    def test_disk_cache_save_and_load(self, old_server_schema, tmp_path):
        """Introspection is saved to disk; a new projector loads it without introspecting."""
        graph = _make_mock_graph(old_server_schema, commit_hash="disk_test_hash")

        with patch(
            "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
            str(tmp_path / "schema_cache"),
        ):
            # First projector: introspects and saves to disk
            projector1 = QueryProjector()
            projector1.adapt_query(SIMPLE_QUERY, graph)

            introspection_calls_1 = [
                c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
            ]
            assert len(introspection_calls_1) == 1

            # Verify file was written
            cache_files = list((tmp_path / "schema_cache").glob("*.json"))
            assert len(cache_files) == 1

            # Second projector: should load from disk, no introspection
            graph2 = _make_mock_graph(old_server_schema, commit_hash="disk_test_hash")
            projector2 = QueryProjector()
            projector2.adapt_query(SIMPLE_QUERY, graph2)

            introspection_calls_2 = [
                c for c in graph2._execute_graphql_call_log if "__schema" in c["query"]
            ]
            assert len(introspection_calls_2) == 0

    def test_disk_cache_miss_on_different_commit(self, old_server_schema, tmp_path):
        """Changing commit hash causes disk cache miss and fresh introspection."""
        with patch(
            "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
            str(tmp_path / "schema_cache"),
        ):
            graph1 = _make_mock_graph(old_server_schema, commit_hash="hash_v1")
            projector = QueryProjector()
            projector.adapt_query(SIMPLE_QUERY, graph1)

            # New graph with different commit hash
            graph2 = _make_mock_graph(old_server_schema, commit_hash="hash_v2")
            projector2 = QueryProjector()
            projector2.adapt_query(SIMPLE_QUERY, graph2)

            # Both should have introspected (different hashes)
            calls1 = [
                c for c in graph1._execute_graphql_call_log if "__schema" in c["query"]
            ]
            calls2 = [
                c for c in graph2._execute_graphql_call_log if "__schema" in c["query"]
            ]
            assert len(calls1) == 1
            assert len(calls2) == 1

            # Two cache files on disk
            cache_files = list((tmp_path / "schema_cache").glob("*.json"))
            assert len(cache_files) == 2

    def test_disk_cache_error_falls_back_to_introspection(
        self, old_server_schema, tmp_path
    ):
        """Corrupt cache file causes fallback to live introspection."""
        with patch(
            "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
            str(tmp_path / "schema_cache"),
        ):
            # Write a corrupt cache file
            graph = _make_mock_graph(old_server_schema, commit_hash="corrupt_test")
            projector = QueryProjector()

            # Pre-create corrupt file at the expected path
            cache_path = projector._disk_cache_path(
                "http://localhost:8080", "corrupt_test"
            )
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("not valid json {{{")

            # Should fall back to introspection
            projector.adapt_query(SIMPLE_QUERY, graph)

            introspection_calls = [
                c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
            ]
            assert len(introspection_calls) == 1
            assert projector._cached_schema is not None

    def test_disk_cache_skipped_when_no_commit_hash(self, old_server_schema, tmp_path):
        """Disk cache is not used when commit hash is unavailable."""
        graph = _make_mock_graph(old_server_schema)
        graph.server_config.commit_hash = None

        with patch(
            "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
            str(tmp_path / "schema_cache"),
        ):
            projector = QueryProjector()
            projector.adapt_query(SIMPLE_QUERY, graph)

            # No cache files written
            cache_dir = tmp_path / "schema_cache"
            if cache_dir.exists():
                assert len(list(cache_dir.glob("*.json"))) == 0

    def test_old_cache_files_cleaned_up(self, old_server_schema, tmp_path):
        """Cache files older than 7 days are cleaned up."""
        import os

        with patch(
            "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
            str(tmp_path / "schema_cache"),
        ):
            cache_dir = tmp_path / "schema_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)

            # Create an old cache file (8 days old)
            old_file = cache_dir / "oldfile.json"
            old_file.write_text("{}")
            old_mtime = time.time() - (8 * 86400)
            os.utime(old_file, (old_mtime, old_mtime))

            # Create a recent cache file
            recent_file = cache_dir / "recentfile.json"
            recent_file.write_text("{}")

            graph = _make_mock_graph(old_server_schema, commit_hash="cleanup_test")
            projector = QueryProjector()
            projector.adapt_query(SIMPLE_QUERY, graph)

            # Old file should be cleaned up, recent file preserved
            assert not old_file.exists()
            assert recent_file.exists()


class TestUnsupportedFieldRemover:
    """Tests for the UnsupportedFieldRemover visitor."""

    def test_nested_field_removal(self, mock_graph_old_schema):
        """Test removal of nested fields within unsupported fragments."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Document {
                                urn
                                info {
                                    title
                                    description
                                }
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert "info" not in adapted_query
        assert len(removed_fields) >= 1


class TestExecuteGraphqlFailSafe:
    """Tests that execute_graphql fails safe when projection errors occur."""

    def test_projection_error_falls_back_to_original_query(self):
        """When adapt_query raises, execute_graphql should fall back to the original query."""
        from datahub.ingestion.graph.client import DataHubGraph

        original_query = "query { me { corpUser { urn } } }"

        with patch.object(DataHubGraph, "__init__", lambda self, *a, **kw: None):
            graph = DataHubGraph.__new__(DataHubGraph)
            graph._query_projector = Mock()
            graph._query_projector.adapt_query.side_effect = RuntimeError(
                "introspection failed"
            )

            # Mock _post_generic to capture the query that was sent
            sent_body: Dict[str, Any] = {}

            def fake_post_generic(url: str, body: Dict) -> Dict:
                sent_body.update(body)
                return {"data": {"me": {"corpUser": {"urn": "urn:li:corpuser:test"}}}}

            graph._gms_server = "http://localhost:8080"

            with patch.object(graph, "_post_generic", fake_post_generic):
                result = graph.execute_graphql(
                    original_query, strip_unsupported_fields=True
                )

                # The original query should have been sent (not a projected one)
                assert sent_body["query"] == original_query
                assert result == {"me": {"corpUser": {"urn": "urn:li:corpuser:test"}}}

    def test_import_error_falls_back_gracefully(self):
        """When graphql-core is not installed, execute_graphql should fall back."""
        from datahub.ingestion.graph.client import DataHubGraph

        original_query = "query { me { corpUser { urn } } }"

        with patch.object(DataHubGraph, "__init__", lambda self, *a, **kw: None):
            graph = DataHubGraph.__new__(DataHubGraph)
            graph._query_projector = None  # Not yet initialized

            sent_body: Dict[str, Any] = {}

            def fake_post_generic(url: str, body: Dict) -> Dict:
                sent_body.update(body)
                return {"data": {"me": {"corpUser": {"urn": "urn:li:corpuser:test"}}}}

            graph._gms_server = "http://localhost:8080"

            # Simulate graphql-core not being installed
            with (
                patch.object(graph, "_post_generic", fake_post_generic),
                patch.dict(
                    "sys.modules",
                    {"datahub.utilities.graphql_query_adapter": None},
                ),
            ):
                result = graph.execute_graphql(
                    original_query, strip_unsupported_fields=True
                )

            assert sent_body["query"] == original_query
            assert result == {"me": {"corpUser": {"urn": "urn:li:corpuser:test"}}}


class TestDiskCacheResilience:
    """Edge-case tests for disk cache robustness under adverse conditions."""

    def test_read_only_cache_dir(self, old_server_schema, tmp_path):
        """Cache write failure on read-only dir doesn't crash; falls back to introspection."""
        cache_dir = tmp_path / "schema_cache"
        cache_dir.mkdir()
        cache_dir.chmod(0o444)  # read-only

        try:
            graph = _make_mock_graph(old_server_schema, commit_hash="readonly_test")
            projector = QueryProjector()
            adapted, removed = projector.adapt_query(SIMPLE_QUERY, graph)
            assert "searchAcrossEntities" in adapted
            assert projector._cached_schema is not None
        finally:
            cache_dir.chmod(0o755)

    def test_cache_dir_permission_denied_on_mkdir(self, old_server_schema, tmp_path):
        """Permission denied creating cache dir doesn't crash."""
        parent = tmp_path / "locked"
        parent.mkdir()
        parent.chmod(0o444)

        try:
            with patch(
                "datahub.utilities.graphql_query_adapter.DISK_CACHE_DIR",
                str(parent / "schema_cache"),
            ):
                graph = _make_mock_graph(
                    old_server_schema, commit_hash="perm_denied_test"
                )
                projector = QueryProjector()
                adapted, _ = projector.adapt_query(SIMPLE_QUERY, graph)
                assert "searchAcrossEntities" in adapted
        finally:
            parent.chmod(0o755)

    def test_symlink_cache_file_rejected_on_load(self, old_server_schema, tmp_path):
        """Symlinked cache file is rejected (not followed) during load."""
        graph = _make_mock_graph(old_server_schema, commit_hash="symlink_test")
        projector = QueryProjector()

        cache_path = projector._disk_cache_path("http://localhost:8080", "symlink_test")
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Create a symlink pointing to a decoy file
        target = tmp_path / "decoy.json"
        target.write_text('{"fake": true}')
        cache_path.symlink_to(target)

        # Load should reject the symlink and fall back to introspection
        projector.adapt_query(SIMPLE_QUERY, graph)
        introspection_calls = [
            c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
        ]
        assert len(introspection_calls) == 1

    def test_symlink_cache_file_rejected_on_save(self, old_server_schema, tmp_path):
        """Symlinked cache file is not overwritten during save."""
        graph = _make_mock_graph(old_server_schema, commit_hash="symlink_save_test")
        projector = QueryProjector()

        cache_path = projector._disk_cache_path(
            "http://localhost:8080", "symlink_save_test"
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Create a symlink pointing to a sensitive file
        target = tmp_path / "sensitive.txt"
        target.write_text("DO NOT OVERWRITE")
        cache_path.symlink_to(target)

        # Should refuse to write through symlink
        projector.adapt_query(SIMPLE_QUERY, graph)
        assert target.read_text() == "DO NOT OVERWRITE"

    def test_valid_json_but_invalid_introspection_schema(
        self, old_server_schema, tmp_path
    ):
        """Valid JSON that isn't an introspection result falls back to introspection."""
        graph = _make_mock_graph(old_server_schema, commit_hash="bad_schema_test")
        projector = QueryProjector()

        cache_path = projector._disk_cache_path(
            "http://localhost:8080", "bad_schema_test"
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text('{"data": null}')

        projector.adapt_query(SIMPLE_QUERY, graph)
        introspection_calls = [
            c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
        ]
        assert len(introspection_calls) == 1

    def test_empty_cache_file_falls_back(self, old_server_schema, tmp_path):
        """Empty cache file is handled gracefully."""
        graph = _make_mock_graph(old_server_schema, commit_hash="empty_file_test")
        projector = QueryProjector()

        cache_path = projector._disk_cache_path(
            "http://localhost:8080", "empty_file_test"
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("")

        projector.adapt_query(SIMPLE_QUERY, graph)
        assert projector._cached_schema is not None

    def test_binary_garbage_cache_file_falls_back(self, old_server_schema, tmp_path):
        """Binary garbage in cache file doesn't crash; falls back to introspection."""
        graph = _make_mock_graph(old_server_schema, commit_hash="binary_test")
        projector = QueryProjector()

        cache_path = projector._disk_cache_path("http://localhost:8080", "binary_test")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(b"\x00\x01\x02\xff\xfe\xfd")

        projector.adapt_query(SIMPLE_QUERY, graph)
        assert projector._cached_schema is not None

    def test_concurrent_cleanup_doesnt_crash(self, old_server_schema, tmp_path):
        """Cleanup tolerates files deleted by another process mid-iteration."""
        cache_dir = tmp_path / "schema_cache"
        cache_dir.mkdir(parents=True)

        # Create multiple old files
        old_mtime = time.time() - (8 * 86400)
        for i in range(5):
            f = cache_dir / f"old_{i}.json"
            f.write_text("{}")
            os.utime(f, (old_mtime, old_mtime))

        projector = QueryProjector()

        # Delete some files mid-cleanup to simulate race condition
        original_unlink = Path.unlink

        unlink_count = 0

        def racing_unlink(self_path, *args, **kwargs):
            nonlocal unlink_count
            unlink_count += 1
            if unlink_count == 2:
                # Simulate another process deleting remaining files
                for f in cache_dir.glob("old_*.json"):
                    try:
                        original_unlink(f)
                    except FileNotFoundError:
                        pass
            original_unlink(self_path, *args, **kwargs)

        with patch.object(Path, "unlink", racing_unlink):
            # Should not raise despite files disappearing
            projector._cleanup_old_cache_files(cache_dir, max_age_days=7)

    def test_gms_server_none_skips_disk_cache(self, old_server_schema, tmp_path):
        """graph._gms_server=None doesn't crash; disk cache is skipped."""
        graph = _make_mock_graph(old_server_schema)
        graph._gms_server = None

        projector = QueryProjector()
        adapted, _ = projector.adapt_query(SIMPLE_QUERY, graph)
        assert "searchAcrossEntities" in adapted

        # No cache files written
        cache_dir = tmp_path / "schema_cache"
        if cache_dir.exists():
            assert len(list(cache_dir.glob("*.json"))) == 0

    def test_server_config_missing_attribute(self, old_server_schema, tmp_path):
        """graph with no server_config attribute doesn't crash."""
        graph = _make_mock_graph(old_server_schema)
        del graph.server_config

        projector = QueryProjector()
        adapted, _ = projector.adapt_query(SIMPLE_QUERY, graph)
        assert "searchAcrossEntities" in adapted

    def test_server_config_commit_hash_raises(self, old_server_schema, tmp_path):
        """commit_hash property that raises doesn't crash."""
        graph = _make_mock_graph(old_server_schema)
        type(graph.server_config).commit_hash = property(
            lambda self: (_ for _ in ()).throw(ConnectionError("no server"))
        )

        projector = QueryProjector()
        adapted, _ = projector.adapt_query(SIMPLE_QUERY, graph)
        assert "searchAcrossEntities" in adapted

    def test_partial_introspection_response_negatively_cached(self):
        """Partial/malformed introspection response is caught and negatively cached."""
        graph = Mock()
        graph._gms_server = "http://localhost:8080"
        graph.server_config = Mock()
        graph.server_config.commit_hash = "partial_test"

        # Return a response that build_client_schema will reject
        graph.execute_graphql = Mock(return_value={"__schema": {}})

        projector = QueryProjector()

        # First call: build_client_schema fails, negatively cached
        with pytest.raises(KeyError):
            projector.adapt_query(SIMPLE_QUERY, graph)

        assert projector._schema_fetch_failed is True

        # Second call hits negative cache instead of retrying
        with pytest.raises(RuntimeError, match="retry after backoff"):
            projector.adapt_query(SIMPLE_QUERY, graph)

    def test_atomic_write_no_leftover_temp_files(self, old_server_schema, tmp_path):
        """Atomic write cleans up temp files even on json.dump failure."""
        graph = _make_mock_graph(old_server_schema, commit_hash="atomic_test")
        projector = QueryProjector()

        # Make json.dump raise an error to test temp file cleanup
        def failing_dump(obj, fp):
            raise IOError("disk full")

        with patch("datahub.utilities.graphql_query_adapter.json.dump", failing_dump):
            projector.adapt_query(SIMPLE_QUERY, graph)

        # No leftover .tmp files
        cache_dir = tmp_path / "schema_cache"
        if cache_dir.exists():
            tmp_files = list(cache_dir.glob("*.tmp"))
            assert len(tmp_files) == 0
