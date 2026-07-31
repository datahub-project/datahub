import warnings
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.hex.api import HexApiConnection
from datahub.ingestion.source.hex.config import HexConnectionDetail, HexSourceConfig
from datahub.ingestion.source.hex.hex import HexSource
from tests.unit.hex.conftest import load_json_data


class TestHexSourceConfig:
    minimum_input_config = {
        "workspace_name": "test-workspace",
        "token": "test-token",
    }

    def test_required_fields(self):
        with pytest.raises(ValueError):
            input_config = {**self.minimum_input_config}
            del input_config["workspace_name"]
            HexSourceConfig.model_validate(input_config)

        with pytest.raises(ValueError):
            input_config = {**self.minimum_input_config}
            del input_config["token"]
            HexSourceConfig.model_validate(input_config)

    def test_minimum_config(self):
        config = HexSourceConfig.model_validate(self.minimum_input_config)

        assert config
        assert config.workspace_name == "test-workspace"
        assert config.token.get_secret_value() == "test-token"

    def test_lineage_config(self):
        config = HexSourceConfig.model_validate(self.minimum_input_config)
        assert config and config.include_lineage

        input_config = {**self.minimum_input_config, "include_lineage": False}
        config = HexSourceConfig.model_validate(input_config)
        assert config and not config.include_lineage

    def test_connection_platform_map_rich_object_round_trips(self):
        config = HexSourceConfig.model_validate(
            {
                **self.minimum_input_config,
                "connection_platform_map": {
                    "conn-sf": {
                        "platform": "snowflake",
                        "platform_instance": "prod_snowflake",
                    },
                },
            }
        )
        detail = config.connection_platform_map["conn-sf"]
        assert detail.platform == "snowflake"
        assert detail.platform_instance == "prod_snowflake"

    def test_deprecated_lineage_fields_emit_warnings(self):
        """Removed fields emit ConfigurationWarning instead of silently being ignored."""
        deprecated_fields = [
            "lineage_start_time",
            "lineage_end_time",
            "datahub_page_size",
        ]
        for field in deprecated_fields:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                HexSourceConfig.model_validate(
                    {**self.minimum_input_config, field: "some-value"}
                )
                assert any(
                    issubclass(warning.category, ConfigurationWarning) for warning in w
                ), f"Expected ConfigurationWarning for deprecated field '{field}'"

    def test_category_pattern_filtering(self):
        """Test that category_pattern filters projects/components correctly using page3_data"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure to deny exact "Scratchpad" match but not "Keep_Scratchpad"
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "deny": ["^Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-category-filtering")
        source = HexSource.create(config, ctx)

        # Mock the API to return page3_data
        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with "Scratchpad" category should be filtered out
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both "Scratchpad" and "Keep_Scratchpad" should be filtered out
        # (deny takes precedence)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" not in source.project_registry

        # Verify: component with "Keep_Scratchpad" category should be kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_category_pattern_allow(self):
        """Test that category_pattern allow list works correctly using page3_data"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure to only allow "Keep_Scratchpad" category
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Keep_Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-category-allow")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with "Scratchpad" should be filtered out (not in allow list)
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both "Scratchpad" and "Keep_Scratchpad" should be kept
        # (has at least one allowed category)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" in source.project_registry

        # Verify: only component with "Keep_Scratchpad" should be kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_category_pattern_mixed_categories_deny_precedence(self):
        """Test that deny patterns take precedence over having allowed categories"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure with both allow and deny patterns
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Keep_Scratchpad$"],
                "deny": ["^Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-mixed-categories-deny")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with only "Scratchpad" is filtered out
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both categories is filtered out (deny takes precedence)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" not in source.project_registry

        # Verify: component with only "Keep_Scratchpad" is kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_category_pattern_mixed_categories_no_deny(self):
        """Test that items with multiple categories are kept if any category matches allow"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure with only allow pattern (no deny)
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Keep_Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-mixed-categories-allow-only")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with only "Scratchpad" is filtered out (not in allow list)
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both categories is kept (has allowed category)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" in source.project_registry

        # Verify: component with "Keep_Scratchpad" is kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_no_category_pattern_filtering(self):
        """Test that all items are kept when no category_pattern is configured"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure without any category_pattern
        config = {
            **self.minimum_input_config,
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-no-category-filter")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: all projects are kept regardless of categories
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" in source.project_registry
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" in source.project_registry

        # Verify: all components are kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry


class TestResolveConnections:
    """Tests for HexSource._resolve_connections — the boundary that resolves
    raw API output + user overrides into a {conn_id → HexConnection} map.
    """

    def _make_source(self, **overrides: object) -> HexSource:
        config = HexSourceConfig.model_validate(
            {
                "workspace_name": "ws",
                "token": "t",
                **overrides,
            }
        )
        return HexSource(config, PipelineContext(run_id="resolve-conn-test"))

    def test_api_only_known_types_resolve(self):
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-sf": HexApiConnection(name="Analytics Hub", type="snowflake"),
                "conn-bq": HexApiConnection(name="BQ Hub", type="bigquery"),
            },
            connection_overrides={},
        )
        assert connections["conn-sf"].name == "Analytics Hub"
        assert connections["conn-sf"].platform == "snowflake"
        # No user override → no platform_instance pinned.
        assert connections["conn-sf"].platform_instance is None
        assert connections["conn-bq"].name == "BQ Hub"
        assert connections["conn-bq"].platform == "bigquery"

    def test_api_unknown_type_is_unmapped_and_warned(self):
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-vt": HexApiConnection(name="Vertica Prod", type="vertica"),
            },
            connection_overrides={},
        )
        # Name is retained for the document builder…
        assert connections["conn-vt"].name == "Vertica Prod"
        # …but platform is not resolvable, so the lineage builder will skip it.
        assert connections["conn-vt"].platform is None
        # A warning is surfaced naming the offending type in its context.
        assert any(
            any("vertica" in c.lower() for c in w.context)
            for w in source.report.warnings
        )

    def test_user_override_bypasses_canonical_map(self):
        """An override naming a platform outside CONNECTION_TYPE_TO_DATAHUB_PLATFORM
        should still resolve — this is the M2 fix."""
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-vt": HexApiConnection(name="Vertica Prod", type="vertica"),
            },
            connection_overrides={"conn-vt": HexConnectionDetail(platform="vertica")},
        )
        # Display name from the API is preserved, override supplies platform.
        assert connections["conn-vt"].name == "Vertica Prod"
        assert connections["conn-vt"].platform == "vertica"
        # Override fully resolves the connection — no spurious "unmapped"
        # warning should fire.
        assert not source.report.warnings

    def test_partial_override_still_warns_for_remaining_unmapped(self):
        """When only some connections of an unmapped type are overridden,
        warn about the rest — but not the overridden ones."""
        source = self._make_source()
        source._resolve_connections(
            api_connections={
                "conn-vt-1": HexApiConnection(name="Vertica A", type="vertica"),
                "conn-vt-2": HexApiConnection(name="Vertica B", type="vertica"),
                "conn-mz": HexApiConnection(name="Materialize", type="materialize"),
            },
            connection_overrides={
                "conn-vt-1": HexConnectionDetail(platform="vertica"),
            },
        )
        # Both unmapped types appear in the warning context; only the
        # still-unmapped vertica connection is counted.
        assert source.report.warnings
        contexts = [c for w in source.report.warnings for c in w.context]
        joined = " ".join(contexts).lower()
        assert "vertica (x1)" in joined
        assert "materialize (x1)" in joined

    def test_user_override_for_unknown_connection_id(self):
        """Override applies even when the connection isn't in the API result
        (deleted connection, permission gap)."""
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={},
            connection_overrides={
                "conn-deleted": HexConnectionDetail(platform="snowflake"),
            },
        )
        # Falls back to the conn_id itself when no display name is known.
        assert connections["conn-deleted"].name == "conn-deleted"
        assert connections["conn-deleted"].platform == "snowflake"

    def test_user_override_wins_over_api(self):
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-1": HexApiConnection(name="Prod SF", type="snowflake"),
            },
            connection_overrides={"conn-1": HexConnectionDetail(platform="redshift")},
        )
        assert connections["conn-1"].platform == "redshift"
        # Display name from the API is preserved on override.
        assert connections["conn-1"].name == "Prod SF"

    def test_empty_inputs_produce_empty_outputs(self):
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={}, connection_overrides={}
        )
        assert connections == {}

    def test_rich_override_with_platform_instance_and_env(self):
        """When the user supplies a HexConnectionDetail, the resolved
        HexConnection carries platform_instance for the lineage builder."""
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-sf": HexApiConnection(name="Prod SF", type="snowflake"),
            },
            connection_overrides={
                "conn-sf": HexConnectionDetail(platform_instance="prod_snowflake"),
            },
        )
        c = connections["conn-sf"]
        # platform still auto-resolves from the API since override.platform is None
        assert c.platform == "snowflake"
        assert c.platform_instance == "prod_snowflake"

    def test_api_default_database_and_schema_flow_through(self):
        """API-extracted defaults populate the HexConnection when no override
        touches them — caller has no override fields set."""
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-sf": HexApiConnection(
                    name="A",
                    type="snowflake",
                    default_database="ANALYTICS",
                    default_schema="PUBLIC",
                ),
            },
            connection_overrides={},
        )
        c = connections["conn-sf"]
        assert c.default_database == "ANALYTICS"
        assert c.default_schema == "PUBLIC"

    def test_override_default_schema_only_preserves_api_default_database(self):
        """Per-field merge: override.default_schema is set, default_database
        is None on the override → only schema is replaced, db comes from API.
        """
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-sf": HexApiConnection(
                    name="A",
                    type="snowflake",
                    default_database="ANALYTICS",
                    default_schema="PUBLIC",
                ),
            },
            connection_overrides={
                "conn-sf": HexConnectionDetail(default_schema="REPORTING"),
            },
        )
        c = connections["conn-sf"]
        assert c.default_database == "ANALYTICS"  # unchanged from API
        assert c.default_schema == "REPORTING"  # override wins

    def test_override_default_database_only_preserves_api_default_schema(self):
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-sf": HexApiConnection(
                    name="A",
                    type="snowflake",
                    default_database="ANALYTICS",
                    default_schema="PUBLIC",
                ),
            },
            connection_overrides={
                "conn-sf": HexConnectionDetail(default_database="WAREHOUSE"),
            },
        )
        c = connections["conn-sf"]
        assert c.default_database == "WAREHOUSE"
        assert c.default_schema == "PUBLIC"  # unchanged from API

    def test_override_supplies_defaults_when_api_has_none(self):
        """API didn't extract any defaults (e.g. type missing from
        CONNECTION_TYPE_DEFAULTS, or `connectionDetails` absent). The override
        fills in both slots."""
        source = self._make_source()
        connections = source._resolve_connections(
            api_connections={
                "conn-vt": HexApiConnection(name="Vertica", type="vertica"),
            },
            connection_overrides={
                "conn-vt": HexConnectionDetail(
                    platform="vertica",
                    default_database="WAREHOUSE",
                    default_schema="public",
                ),
            },
        )
        c = connections["conn-vt"]
        assert c.default_database == "WAREHOUSE"
        assert c.default_schema == "public"


class TestHexTestConnection:
    """Tests for test_connection() — especially the cells access probe."""

    def _make_response(self, status_code: int, json_data: dict) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.ok = status_code < 400
        resp.json.return_value = json_data
        resp.raise_for_status = MagicMock(
            side_effect=None if status_code < 400 else Exception(f"HTTP {status_code}")
        )
        return resp

    def _run_test_connection(self, cells_status: int) -> "TestConnectionReport":
        """Run test_connection with a controlled cells endpoint status code."""
        config = {
            "workspace_name": "test-ws",
            "token": "test-token",
            "base_url": "https://app.hex.tech/api/v1",
        }

        def mock_get(url, **kwargs):
            if "users/me" in url:
                return self._make_response(200, {"email": "test@test.com"})
            if "projects" in url and "queriedTables" in url:
                return self._make_response(403, {})
            if "projects" in url:
                return self._make_response(
                    200, {"values": [{"id": "proj-1"}], "pagination": {}}
                )
            if "data-connections" in url:
                return self._make_response(200, {"values": []})
            if "cells" in url:
                return self._make_response(cells_status, {"values": []})
            return self._make_response(404, {})

        def mock_post(url, **kwargs):
            # Export API — return a valid but empty YAML so sampling completes quickly
            if "export" in url:
                return self._make_response(
                    200,
                    {
                        "filename": "test.yaml",
                        "content": "meta:\n  title: Test\ncells: []\n",
                    },
                )
            return self._make_response(404, {})

        with patch(
            "datahub.ingestion.source.hex.hex.HexApi._create_retry_session"
        ) as mock_session_factory:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session.post.side_effect = mock_post
            mock_session.request.side_effect = lambda method, url, **kw: (
                mock_post(url, **kw)
                if method.upper() == "POST"
                else mock_get(url, **kw)
            )
            mock_session_factory.return_value = mock_session

            return HexSource.test_connection(config)

    def test_cells_accessible_reports_lineage_capable(self):
        report = self._run_test_connection(cells_status=200)
        assert report.capability_report is not None
        lineage_cap = report.capability_report.get(
            "Lineage via SQL parsing (all tiers)"
        )
        assert lineage_cap is not None
        assert lineage_cap.capable is True

    def test_cells_403_reports_lineage_not_capable(self):
        """Metadata-only token (can list projects, cannot read cells) → lineage not capable."""
        report = self._run_test_connection(cells_status=403)
        assert report.capability_report is not None
        lineage_cap = report.capability_report.get(
            "Lineage via SQL parsing (all tiers)"
        )
        assert lineage_cap is not None
        assert lineage_cap.capable is False
        assert lineage_cap.failure_reason is not None
        assert "403" in lineage_cap.failure_reason
        assert "Read projects" in lineage_cap.failure_reason


class TestQueriedTablesFallback:
    """Ingestion-level: when use_queried_tables_lineage=True and Hex's
    /projects/{id}/queriedTables returns 403, SQL-cell parsing takes over,
    the tier is marked unavailable, and a warning is surfaced in the report."""

    def test_queried_tables_403_falls_back_to_sql_parsing(self):
        project_id = "proj-1"
        # lastPublishedAt is required — _build_lineage skips queriedTables for drafts.
        project = {
            "id": project_id,
            "title": "Published",
            "type": "PROJECT",
            "lastPublishedAt": "2024-08-22T10:00:00Z",
        }
        sql_cell = {
            "staticId": "cell-1",
            "cellType": "SQL",
            "dataConnectionId": "conn-sf",
            "contents": {"sqlCell": {"source": "SELECT * FROM db.public.customers"}},
        }

        def make_response(status: int, payload: dict) -> MagicMock:
            response = MagicMock(status_code=status, ok=status < 400)
            response.json.return_value = payload
            response.raise_for_status = MagicMock(
                side_effect=None if status < 400 else Exception(f"HTTP {status}")
            )
            return response

        def mock_get(url: str, **_: object) -> MagicMock:
            if "queriedTables" in url:
                return make_response(403, {})
            if url.endswith("/data-connections"):
                return make_response(
                    200,
                    {"values": [{"id": "conn-sf", "name": "SF", "type": "snowflake"}]},
                )
            if url.endswith("/cells"):
                return make_response(200, {"values": [sql_cell], "pagination": {}})
            if url.endswith("/projects"):
                return make_response(200, {"values": [project], "pagination": {}})
            return make_response(404, {})

        # Empty export forces the connector down the /cells path.
        def mock_post(_url: str, **_kwargs: object) -> MagicMock:
            return make_response(200, {"content": "cells: []\n"})

        config = {
            "workspace_name": "ws",
            "workspace_id": "ws-uuid",  # skip /users/me discovery
            "token": "t",
            "use_queried_tables_lineage": True,
            "include_run_history": False,
            "include_context_documents": False,
        }

        with patch(
            "datahub.ingestion.source.hex.hex.HexApi._create_retry_session"
        ) as factory:
            session = MagicMock()
            session.get.side_effect = mock_get
            session.post.side_effect = mock_post
            session.request.side_effect = lambda method, url, **kw: (
                mock_post(url, **kw)
                if method.upper() == "POST"
                else mock_get(url, **kw)
            )
            factory.return_value = session

            source = HexSource.create(config, PipelineContext(run_id="t"))
            list(source.get_workunits_internal())

        assert source.hex_api._queried_tables_tier_available is False
        upstream = source.project_registry[project_id].upstream_datasets
        assert upstream and any("db.public.customers" in u for u in upstream)
        assert any("queriedTables" in (w.title or "") for w in source.report.warnings)
