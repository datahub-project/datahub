"""
Smoke tests for `datahub search` CLI command.

These tests run the CLI end-to-end against a live DataHub instance and prove
that filters, output formats, pagination, and error handling work correctly —
things unit tests can't verify because they mock the graph client.

Test data (search_test_data.json) contains:
  - 2 datasets on snowflake (orders, customers)
  - 1 dataset on bigquery (revenue)
  - 1 chart on looker (revenue_chart)
  - 1 dashboard on looker (sales_dashboard)

All URNs use a "search_smoke_test" namespace for easy identification/cleanup.
CorpUser filter tests rely on built-in DataHub system users (always present).
"""

import json
import logging

import pytest

from tests.utils import (
    delete_urns_from_file,
    ingest_file_via_rest,
    run_datahub_cmd,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

_TEST_DATA = "tests/cli/search_cmd/search_test_data.json"

_SNOWFLAKE_DATASET_URNS = [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,search_smoke_test.orders,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,search_smoke_test.customers,PROD)",
]
_BIGQUERY_DATASET_URNS = [
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,search_smoke_test.revenue,PROD)",
]
_CHART_URNS = [
    "urn:li:chart:(looker,search_smoke_test.revenue_chart)",
]
_DASHBOARD_URNS = [
    "urn:li:dashboard:(looker,search_smoke_test.sales_dashboard)",
]
_ALL_TEST_URNS = (
    _SNOWFLAKE_DATASET_URNS + _BIGQUERY_DATASET_URNS + _CHART_URNS + _DASHBOARD_URNS
)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    logger.info("Deleting search smoke test data for idempotency")
    delete_urns_from_file(graph_client, _TEST_DATA)

    logger.info("Ingesting search smoke test data")
    ingest_file_via_rest(auth_session, _TEST_DATA)
    wait_for_writes_to_sync()

    yield

    logger.info("Cleaning up search smoke test data")
    delete_urns_from_file(graph_client, _TEST_DATA)
    wait_for_writes_to_sync()


def _run_search(auth_session, args: list) -> tuple:
    """Run `datahub search ...` with auth credentials. Returns (exit_code, stdout, stderr)."""
    result = run_datahub_cmd(
        ["search"] + args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )
    return result.exit_code, result.stdout, result.stderr


class TestSearchBasic:
    """Core output format and structure tests."""

    def test_wildcard_returns_json(self, auth_session):
        """Default search returns valid JSON with results."""
        exit_code, stdout, _ = _run_search(auth_session, ["*", "--limit", "5"])

        assert exit_code == 0
        data = json.loads(stdout)
        assert "searchResults" in data
        assert "total" in data
        assert data["total"] > 0

    def test_json_result_structure(self, auth_session):
        """Each result has entity.urn."""
        exit_code, stdout, _ = _run_search(auth_session, ["*", "--limit", "5"])

        assert exit_code == 0
        for result in json.loads(stdout)["searchResults"]:
            assert "entity" in result
            assert result["entity"]["urn"].startswith("urn:li:")

    def test_urns_only_format(self, auth_session):
        """--urns-only returns one valid URN per line."""
        exit_code, stdout, _ = _run_search(
            auth_session, ["*", "--limit", "5", "--urns-only"]
        )

        assert exit_code == 0
        lines = [line for line in stdout.strip().splitlines() if line]
        assert len(lines) > 0
        for urn in lines:
            assert urn.startswith("urn:li:")

    def test_table_format(self, auth_session):
        """--table returns human-readable output."""
        exit_code, stdout, _ = _run_search(
            auth_session, ["*", "--limit", "5", "--table"]
        )

        assert exit_code == 0
        assert "Showing" in stdout

    def test_facets_only(self, auth_session):
        """--facets-only returns facets without searchResults."""
        exit_code, stdout, _ = _run_search(auth_session, ["*", "--facets-only"])

        assert exit_code == 0
        data = json.loads(stdout)
        assert "facets" in data
        assert "searchResults" not in data
        assert len(data["facets"]) > 0

    def test_limit_respected(self, auth_session):
        """--limit controls results returned."""
        _, stdout3, _ = _run_search(auth_session, ["*", "--limit", "3"])
        _, stdout7, _ = _run_search(auth_session, ["*", "--limit", "7"])

        data3 = json.loads(stdout3)
        data7 = json.loads(stdout7)
        assert len(data3["searchResults"]) <= 3
        assert len(data7["searchResults"]) <= 7
        if data7["total"] >= 7:
            assert len(data7["searchResults"]) > len(data3["searchResults"])


class TestSearchFiltersByEntityType:
    """Prove --filter entity_type=X returns only that type from the backend."""

    def test_filter_dataset_returns_only_datasets(self, auth_session):
        exit_code, stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=dataset", "--limit", "20"]
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        for result in data["searchResults"]:
            assert result["entity"]["type"] == "DATASET"

    def test_filter_chart_returns_only_charts(self, auth_session):
        exit_code, stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=chart", "--limit", "20"]
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        for result in data["searchResults"]:
            assert result["entity"]["type"] == "CHART"

    def test_filter_dashboard_returns_only_dashboards(self, auth_session):
        exit_code, stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=dashboard", "--limit", "20"]
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        for result in data["searchResults"]:
            assert result["entity"]["type"] == "DASHBOARD"

    def test_filter_corpuser_returns_only_corpusers(self, auth_session):
        """CorpUser entities are always present (system users), no fixture needed."""
        exit_code, stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=corpuser", "--limit", "10"]
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        for result in data["searchResults"]:
            assert result["entity"]["type"] == "CORP_USER"

    def test_entity_type_filter_narrows_results(self, auth_session):
        """Filtering by entity_type returns fewer results than unfiltered."""
        _, all_stdout, _ = _run_search(auth_session, ["*", "--limit", "1"])
        _, filtered_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=dataset", "--limit", "1"]
        )

        all_total = json.loads(all_stdout)["total"]
        filtered_total = json.loads(filtered_stdout)["total"]
        assert filtered_total < all_total


class TestSearchFiltersByPlatform:
    """Prove --filter platform=X returns only entities from that platform."""

    def test_filter_snowflake_returns_snowflake_datasets(self, auth_session):
        exit_code, stdout, _ = _run_search(
            auth_session,
            ["*", "--filter", "platform=snowflake", "--limit", "20"],
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        # All our snowflake test URNs should be findable
        result_urns = {r["entity"]["urn"] for r in data["searchResults"]}
        for urn in _SNOWFLAKE_DATASET_URNS:
            assert urn in result_urns, f"Expected snowflake URN not found: {urn}"

    def test_filter_bigquery_returns_bigquery_datasets(self, auth_session):
        exit_code, stdout, _ = _run_search(
            auth_session,
            ["*", "--filter", "platform=bigquery", "--limit", "20"],
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        result_urns = {r["entity"]["urn"] for r in data["searchResults"]}
        for urn in _BIGQUERY_DATASET_URNS:
            assert urn in result_urns, f"Expected bigquery URN not found: {urn}"

    def test_filter_snowflake_excludes_bigquery(self, auth_session):
        exit_code, stdout, _ = _run_search(
            auth_session,
            ["*", "--filter", "platform=snowflake", "--limit", "50"],
        )

        assert exit_code == 0
        result_urns = {r["entity"]["urn"] for r in json.loads(stdout)["searchResults"]}
        for urn in _BIGQUERY_DATASET_URNS:
            assert urn not in result_urns, (
                f"BigQuery URN leaked into snowflake results: {urn}"
            )


class TestSearchFilterCombinations:
    """Prove AND logic and OR logic work correctly."""

    def test_multiple_filters_are_anded(self, auth_session):
        """entity_type=dataset AND platform=snowflake is more restrictive than either alone."""
        _, type_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=dataset", "--limit", "1"]
        )
        _, combined_stdout, _ = _run_search(
            auth_session,
            [
                "*",
                "--filter",
                "entity_type=dataset",
                "--filter",
                "platform=snowflake",
                "--limit",
                "1",
            ],
        )

        type_total = json.loads(type_stdout)["total"]
        combined_total = json.loads(combined_stdout)["total"]
        assert combined_total <= type_total

    def test_comma_or_logic_returns_union(self, auth_session):
        """platform=snowflake,bigquery returns entities from both platforms."""
        _, snow_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "platform=snowflake", "--limit", "1"]
        )
        _, bq_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "platform=bigquery", "--limit", "1"]
        )
        _, or_stdout, _ = _run_search(
            auth_session,
            ["*", "--filter", "platform=snowflake,bigquery", "--limit", "50"],
        )

        snow_total = json.loads(snow_stdout)["total"]
        bq_total = json.loads(bq_stdout)["total"]
        or_total = json.loads(or_stdout)["total"]

        assert or_total >= snow_total
        assert or_total >= bq_total

        or_urns = {r["entity"]["urn"] for r in json.loads(or_stdout)["searchResults"]}
        for urn in _SNOWFLAKE_DATASET_URNS + _BIGQUERY_DATASET_URNS:
            assert urn in or_urns, f"Expected URN missing from OR results: {urn}"

    def test_complex_filters_json_and(self, auth_session):
        """--filters JSON AND logic returns correct entity type."""
        exit_code, stdout, _ = _run_search(
            auth_session,
            [
                "*",
                "--filters",
                '{"and": [{"entity_type": ["dataset"]}, {"platform": ["snowflake"]}]}',
                "--limit",
                "20",
            ],
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        result_urns = {r["entity"]["urn"] for r in data["searchResults"]}
        for urn in _SNOWFLAKE_DATASET_URNS:
            assert urn in result_urns


class TestSearchPagination:
    """Prove --limit + --offset return non-overlapping pages."""

    def test_offset_returns_different_results(self, auth_session):
        _, page1_stdout, _ = _run_search(
            auth_session, ["*", "--limit", "3", "--offset", "0"]
        )
        _, page2_stdout, _ = _run_search(
            auth_session, ["*", "--limit", "3", "--offset", "3"]
        )

        page1 = json.loads(page1_stdout)
        page2 = json.loads(page2_stdout)

        if page1["total"] <= 3:
            pytest.skip("Not enough entities for pagination test")

        urns1 = {r["entity"]["urn"] for r in page1["searchResults"]}
        urns2 = {r["entity"]["urn"] for r in page2["searchResults"]}
        assert not urns1 & urns2, f"Pages should not overlap, shared: {urns1 & urns2}"

    def test_total_is_consistent_across_pages(self, auth_session):
        _, p1, _ = _run_search(auth_session, ["*", "--limit", "3", "--offset", "0"])
        _, p2, _ = _run_search(auth_session, ["*", "--limit", "3", "--offset", "3"])

        assert json.loads(p1)["total"] == json.loads(p2)["total"]


class TestSearchDryRun:
    """--dry-run works offline (no live connection needed)."""

    def test_dry_run_offline(self):
        result = run_datahub_cmd(
            ["search", "*", "--dry-run"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["operation_name"] == "search"
        assert data["graphql_field"] == "searchAcrossEntities"
        assert "variables" in data

    def test_dry_run_shows_compiled_filter(self):
        result = run_datahub_cmd(
            ["search", "*", "--filter", "entity_type=dataset", "--dry-run"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["variables"]["orFilters"]

    def test_dry_run_with_projection_shows_query(self):
        result = run_datahub_cmd(
            ["search", "*", "--projection", "urn type", "--dry-run"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "query" in data
        assert "urn type" in data["query"]


class TestSearchErrorHandling:
    """Exit codes for bad inputs — no live connection needed."""

    def test_invalid_limit_exit_code_2(self):
        result = run_datahub_cmd(
            ["search", "*", "--limit", "0"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )
        assert result.exit_code == 2

    def test_invalid_offset_exit_code_2(self):
        result = run_datahub_cmd(
            ["search", "*", "--offset", "-1"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )
        assert result.exit_code == 2

    def test_conflicting_filters_exit_code_2(self):
        result = run_datahub_cmd(
            [
                "search",
                "*",
                "--filter",
                "entity_type=dataset",
                "--filters",
                '{"entity_type": ["dataset"]}',
            ],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )
        assert result.exit_code == 2

    def test_invalid_projection_exit_code_2(self):
        result = run_datahub_cmd(
            ["search", "*", "--projection", "mutation { bad }"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )
        assert result.exit_code == 2

    def test_list_filters_subcommand(self):
        result = run_datahub_cmd(
            ["search", "list-filters"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )
        assert result.exit_code == 0
        assert "Available Filters" in result.stdout

    def test_describe_filter_subcommand(self):
        result = run_datahub_cmd(
            ["search", "describe-filter", "platform"],
            env={"DATAHUB_GMS_URL": "http://localhost:1", "DATAHUB_GMS_TOKEN": "x"},
        )
        assert result.exit_code == 0
        assert "Filter: platform" in result.stdout


class TestSearchProjectionLive:
    """--projection against live backend."""

    def test_minimal_projection_urn_only(self, auth_session):
        """Projection with only urn returns no extra fields."""
        exit_code, stdout, _ = _run_search(
            auth_session,
            [
                "*",
                "--filter",
                "entity_type=dataset",
                "--projection",
                "urn",
                "--limit",
                "3",
            ],
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        for result in data["searchResults"]:
            extra = set(result["entity"].keys()) - {"urn", "__typename"}
            assert not extra, f"Unexpected fields in minimal projection: {extra}"

    def test_projection_reduces_payload(self, auth_session):
        """Custom projection produces smaller entity payload than default."""
        _, full_stdout, _ = _run_search(
            auth_session,
            ["*", "--filter", "entity_type=dataset", "--limit", "5"],
        )
        _, minimal_stdout, _ = _run_search(
            auth_session,
            [
                "*",
                "--filter",
                "entity_type=dataset",
                "--projection",
                "urn",
                "--limit",
                "5",
            ],
        )

        full_entities = json.loads(full_stdout)["searchResults"]
        minimal_entities = json.loads(minimal_stdout)["searchResults"]

        if not full_entities:
            pytest.skip("No results")

        assert len(json.dumps(minimal_entities)) < len(json.dumps(full_entities))


class TestSearchWhereFilter:
    """Prove --where SQL-like expressions filter correctly against a live instance."""

    def test_where_platform_eq(self, auth_session):
        """--where 'platform = snowflake' returns only snowflake entities."""
        exit_code, stdout, _ = _run_search(
            auth_session,
            ["*", "--where", "platform = snowflake", "--limit", "20"],
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["total"] > 0
        result_urns = {r["entity"]["urn"] for r in data["searchResults"]}
        for urn in _SNOWFLAKE_DATASET_URNS:
            assert urn in result_urns, f"Expected snowflake URN not found: {urn}"
        for urn in _BIGQUERY_DATASET_URNS:
            assert urn not in result_urns, (
                f"BigQuery URN leaked into snowflake --where results: {urn}"
            )

    def test_where_and_narrows_results(self, auth_session):
        """--where 'entity_type = dataset AND platform = snowflake' is more selective than either alone."""
        _, type_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "entity_type=dataset", "--limit", "1"]
        )
        _, where_stdout, _ = _run_search(
            auth_session,
            [
                "*",
                "--where",
                "entity_type = dataset AND platform = snowflake",
                "--limit",
                "1",
            ],
        )

        type_total = json.loads(type_stdout)["total"]
        where_total = json.loads(where_stdout)["total"]
        assert where_total <= type_total

    def test_where_in_list(self, auth_session):
        """--where 'platform IN (snowflake, bigquery)' returns entities from both platforms."""
        _, snow_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "platform=snowflake", "--limit", "1"]
        )
        _, bq_stdout, _ = _run_search(
            auth_session, ["*", "--filter", "platform=bigquery", "--limit", "1"]
        )
        _, where_stdout, _ = _run_search(
            auth_session,
            ["*", "--where", "platform IN (snowflake, bigquery)", "--limit", "50"],
        )

        snow_total = json.loads(snow_stdout)["total"]
        bq_total = json.loads(bq_stdout)["total"]
        where_total = json.loads(where_stdout)["total"]
        where_urns = {
            r["entity"]["urn"] for r in json.loads(where_stdout)["searchResults"]
        }

        assert where_total >= snow_total
        assert where_total >= bq_total
        for urn in _SNOWFLAKE_DATASET_URNS + _BIGQUERY_DATASET_URNS:
            assert urn in where_urns, (
                f"Expected URN missing from --where IN results: {urn}"
            )


class TestSearchDiagnose:
    """Prove `datahub search diagnose` works against a live instance.

    NOTE: None of these tests assert that semantic search is enabled.
    CI environments typically do not have semantic search configured,
    so any assertion on semantic_enabled/semantic_available would be
    environment-specific and must never be added here.
    """

    def test_diagnose_text_exits_cleanly(self, auth_session):
        """Default (text) format exits 0 and reports connectivity."""
        exit_code, stdout, _ = _run_search(auth_session, ["diagnose"])

        assert exit_code == 0
        assert "Search Diagnostics" in stdout
        assert "Connected to DataHub" in stdout

    def test_diagnose_json_structure(self, auth_session):
        """--format json returns valid JSON with keyword and semantic top-level keys."""
        exit_code, stdout, _ = _run_search(
            auth_session, ["diagnose", "--format", "json"]
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert "keyword" in data
        assert "semantic" in data

    def test_diagnose_keyword_connected(self, auth_session):
        """keyword section reports connected=True and search works.

        Does NOT check anything about semantic search — keyword search
        must always work regardless of backend configuration.
        """
        _, stdout, _ = _run_search(auth_session, ["diagnose", "--format", "json"])

        kw = json.loads(stdout)["keyword"]
        assert kw["connected"] is True
        assert kw["search_works"] is True
        assert kw["total_entities"] > 0

    def test_diagnose_semantic_section_has_expected_keys(self, auth_session):
        """semantic section always has the right keys, whatever their values.

        Does NOT assert semantic_available or semantic_enabled are True —
        semantic search is optional and may not be configured in CI.
        """
        _, stdout, _ = _run_search(auth_session, ["diagnose", "--format", "json"])

        sem = json.loads(stdout)["semantic"]
        assert "semantic_available" in sem
        assert "semantic_enabled" in sem
        # semantic_error is None when working, a string when not — both are valid
        assert "semantic_error" in sem
