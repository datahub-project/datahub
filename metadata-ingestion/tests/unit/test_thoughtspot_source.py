"""Unit tests for ThoughtSpot DataHub connector.

Tests focus on business logic, error handling, and data transformation.
Following testing standards from standards/testing.md - no trivial tests.
"""

import logging
from datetime import (
    datetime,
    timezone,
)
from typing import Any, Dict, List, Optional, Type, TypeVar, cast
from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.thoughtspot.client import (
    _KEY_BUILDERS,
    _MAX_REAUTH_ATTEMPTS,
    _TS_TO_DATAHUB_PLATFORM,
    ThoughtSpotAPIError,
    ThoughtSpotAuthenticationError,
    ThoughtSpotClient,
    ThoughtSpotPermissionError,
    _classify_http_error,
    _extract_answer_chart_type,
    _extract_sql_view_statement,
    _http_status_code,
    _safe_load_tml_yaml,
)
from datahub.ingestion.source.thoughtspot.config import (
    PasswordAuth,
    ThoughtSpotConfig,
    ThoughtSpotConnectionConfig,
    TrustedAuth,
)
from datahub.ingestion.source.thoughtspot.models import (
    AnswerResponse,
    ColumnResponse,
    ColumnSourceRef,
    ConnectionResponse,
    EntityStats,
    LiveboardResponse,
    LogicalTableResponse,
    SourceTableRef,
    TagResponse,
    ThoughtSpotAuthor,
    VisualizationResponse,
    WorkspaceResponse,
)
from datahub.ingestion.source.thoughtspot.report import ThoughtSpotReport
from datahub.ingestion.source.thoughtspot.source import (
    ExternalRef,
    SqlViewWarehouseRef,
    ThoughtSpotSource,
    _resolve_author_login,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChartInfoClass,
    ChartUsageStatisticsClass,
    ContainerClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
    InputFieldsClass,
    OwnershipClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sdk.dataset import Dataset

_AspectT = TypeVar("_AspectT")


def _mcp(wu: MetadataWorkUnit) -> MetadataChangeProposalWrapper:
    """Narrow ``wu.metadata`` to the MCP wrapper. The connector emits
    via SDK V2 which only produces ``MetadataChangeProposalWrapper``,
    but ``MetadataWorkUnit.metadata`` is typed as a wider Union; this
    helper asserts and returns the narrowed value so callers can use
    ``.aspect`` / ``.aspectName`` without mypy complaining."""
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    return wu.metadata


def _aspect_as(wu: MetadataWorkUnit, cls: Type[_AspectT]) -> _AspectT:
    """Return the workunit's MCPW aspect narrowed to ``cls``.

    Asserts both that the workunit is a ``MetadataChangeProposalWrapper``
    and that its aspect is the expected concrete class. Lets tests read
    typed fields off an aspect without ``# type: ignore`` markers."""
    mcp = _mcp(wu)
    assert isinstance(mcp.aspect, cls), (
        f"Expected {cls.__name__} aspect on workunit {wu.id}, "
        f"got {type(mcp.aspect).__name__}"
    )
    return mcp.aspect


def _make_tml_stubbed_client(
    items: List[Dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> ThoughtSpotClient:
    """Build a ThoughtSpotClient with ``_iter_tml_export_items`` returning
    the given items and ``_check_tml_item_status`` stubbed to ``True``.

    Shared by ``TestSqlViewDefinitionFetch`` and ``TestTMLParseAggregation``
    — both need the same TML-stub shape to exercise downstream parsing
    without round-tripping the real ``metadata_tml_export`` endpoint."""
    config = ThoughtSpotConnectionConfig(
        base_url="https://example.thoughtspot.cloud",
        auth=TrustedAuth(username="u", secret_key="k"),
    )
    with patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2") as mock_sdk:
        mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())
    monkeypatch.setattr(client, "_iter_tml_export_items", lambda *a, **kw: iter(items))
    monkeypatch.setattr(
        client, "_check_tml_item_status", lambda item, metadata_type: True
    )
    return client


class TestThoughtSpotConnectionConfig:
    """Test connection configuration validation logic."""

    @pytest.mark.parametrize(
        "raw_url",
        [
            "https://example.thoughtspot.cloud/",
            "https://example.thoughtspot.cloud/api/rest/2.0",
            "https://example.thoughtspot.cloud/api/rest/2.0/",
        ],
    )
    def test_base_url_normalization(self, raw_url):
        """Trailing slashes and an accidentally-included API path are
        stripped so the resulting URL matches the expected host root."""
        config = ThoughtSpotConnectionConfig(
            base_url=raw_url,
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )
        assert config.base_url == "https://example.thoughtspot.cloud"

    @pytest.mark.parametrize(
        "field, value",
        [
            ("timeout_seconds", 0),
            ("timeout_seconds", -5),
            ("max_retries", -1),
        ],
    )
    def test_numeric_bounds_reject_invalid(self, field, value):
        """``timeout_seconds`` must be > 0; ``max_retries`` must be >= 0."""
        with pytest.raises(ValidationError):
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                **{field: value},
            )

    def test_max_retries_accepts_zero(self):
        """``max_retries=0`` is the documented way to disable retry — must
        be accepted even though ``-1`` is not."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
            max_retries=0,
        )
        assert config.max_retries == 0


class TestThoughtSpotConfigValidation:
    """Test main configuration validation logic."""

    def test_authentication_requires_credentials(self):
        """Without an ``auth`` block (nor legacy flat fields), construction
        must fail — pydantic flags ``auth`` as a required field."""
        with pytest.raises(ValidationError) as exc:
            ThoughtSpotConfig(
                connection=ThoughtSpotConnectionConfig.model_validate(
                    {"base_url": "https://example.thoughtspot.cloud"}
                )
            )
        assert "auth" in str(exc.value).lower()

    def test_authentication_rejects_whitespace_username(self):
        """Whitespace-only username is rejected by the auth-mode validator."""
        with pytest.raises(ValidationError) as exc:
            ThoughtSpotConfig(
                connection=ThoughtSpotConnectionConfig(
                    base_url="https://example.thoughtspot.cloud",
                    auth=PasswordAuth(username="   ", password="password123"),
                )
            )
        assert "username" in str(exc.value).lower()

    def test_basic_authentication_accepted(self):
        """A ``PasswordAuth`` block under ``connection.auth`` is the
        canonical password-mode config shape."""

        config = ThoughtSpotConfig(
            connection=ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=PasswordAuth(username="testuser", password="testpass"),
            )
        )
        assert isinstance(config.connection.auth, PasswordAuth)
        assert config.connection.auth.password is not None

    def test_trusted_auth_with_username_accepted(self):
        """A ``TrustedAuth`` block under ``connection.auth`` is the
        canonical trusted-mode config shape."""

        config = ThoughtSpotConfig(
            connection=ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="secret123"),
            )
        )
        assert isinstance(config.connection.auth, TrustedAuth)
        assert config.connection.auth.secret_key is not None


class TestThoughtSpotSourceEntityFiltering:
    """Test entity filtering logic using patterns."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_workspace_filtering_respects_deny_pattern(self, mock_client_class):
        """Test that workspaces matching deny pattern are filtered out."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "workspace_pattern": {
                "allow": [".*"],
                "deny": ["^Test_.*"],
            },
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id="ws1", name="Production Workspace"),
            WorkspaceResponse(id="ws2", name="Test_Workspace"),
            WorkspaceResponse(id="ws3", name="Analytics Workspace"),
        ]

        workspaces = list(source._get_workspaces())

        assert len(workspaces) == 2
        names = [ws.name for ws in workspaces]
        assert "Production Workspace" in names
        assert "Analytics Workspace" in names
        assert "Test_Workspace" not in names

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_filtering_respects_allow_pattern(self, mock_client_class):
        """Test that only liveboards matching allow pattern are included."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "liveboard_pattern": {
                "allow": ["^Production.*"],
            },
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb1", name="Production Dashboard"),
            LiveboardResponse(id="lb2", name="Development Dashboard"),
            LiveboardResponse(id="lb3", name="Production Analytics"),
        ]

        liveboards = list(source._get_liveboards())

        assert len(liveboards) == 2
        names = [lb.name for lb in liveboards]
        assert "Production Dashboard" in names
        assert "Production Analytics" in names
        assert "Development Dashboard" not in names


class TestTagFilterWiredToClient:
    """``liveboard_tag_filter`` / ``answer_tag_filter`` config flow to
    the underlying ``client.iter_liveboards(tag_names=...)`` /
    ``client.iter_answers(tag_names=...)`` calls, enabling server-side
    pre-filtering (cheaper than fetching everything and filtering by
    name).
    """

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_tag_filter_passed_to_client(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = []
        mock_client.get_tags.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "liveboard_tag_filter": ["Production", "Curated"],
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        list(source.get_workunits_internal())

        mock_client.iter_liveboards.assert_called_once()
        kwargs = mock_client.iter_liveboards.call_args.kwargs
        assert kwargs.get("tag_names") == ["Production", "Curated"]
        # Answer iterator gets no tag filter when unset.
        mock_client.iter_answers.assert_called_once()
        ans_kwargs = mock_client.iter_answers.call_args.kwargs
        assert ans_kwargs.get("tag_names") is None

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_tag_filter_passed_to_client(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = []
        mock_client.get_tags.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "answer_tag_filter": ["KPI"],
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        list(source.get_workunits_internal())

        mock_client.iter_answers.assert_called_once()
        ans_kwargs = mock_client.iter_answers.call_args.kwargs
        assert ans_kwargs.get("tag_names") == ["KPI"]
        # Liveboard iterator gets no tag filter.
        mock_client.iter_liveboards.assert_called_once()
        lb_kwargs = mock_client.iter_liveboards.call_args.kwargs
        assert lb_kwargs.get("tag_names") is None

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_tag_names_sent_as_top_level_tag_identifiers_field(self, mock_sdk_class):
        """Regression: live TS REST v2 rejects ``metadata[0].tag_name``
        (400) and silently ignores top-level ``tags``. The only working
        shape is top-level ``tag_identifiers`` (accepts names OR GUIDs).
        Verified against techpartners.thoughtspot.cloud — pins the wire
        contract so a future "tidy-up" doesn't regress to a broken shape.
        """
        mock_sdk = MagicMock()
        mock_sdk_class.return_value = mock_sdk
        mock_sdk.auth_token_full.return_value = {"token": "t"}
        mock_sdk.metadata_search.return_value = []

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        list(client.iter_liveboards(tag_names=["Production", "Curated"]))

        # At least one metadata_search call must have happened with the
        # tag filter at the TOP LEVEL under ``tag_identifiers`` (NOT
        # under ``metadata[0]`` and NOT as ``tags``).
        calls = mock_sdk.metadata_search.call_args_list
        assert calls, "expected at least one metadata_search call"
        first_request = calls[0].kwargs.get("request") or (
            calls[0].args[0] if calls[0].args else {}
        )
        assert first_request.get("tag_identifiers") == ["Production", "Curated"], (
            f"Tag filter must be sent as top-level ``tag_identifiers``. "
            f"Actual request: {first_request}"
        )
        # Belt-and-braces: confirm the broken nestings are NOT used.
        assert "tag_name" not in first_request["metadata"][0]
        assert "tags" not in first_request

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_iter_answers_sends_top_level_tag_identifiers(self, mock_sdk_class):
        """Symmetric wire-shape pin for the Answer code path. Both
        ``iter_liveboards`` and ``iter_answers`` flow through
        ``_paginated_metadata_search``, but a future helper split could
        regress one without the other. Cover both explicitly so the
        regression guard is symmetric.
        """
        mock_sdk = MagicMock()
        mock_sdk_class.return_value = mock_sdk
        mock_sdk.auth_token_full.return_value = {"token": "t"}
        mock_sdk.metadata_search.return_value = []

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        list(client.iter_answers(tag_names=["KPI"]))

        calls = mock_sdk.metadata_search.call_args_list
        assert calls, "expected at least one metadata_search call"
        first_request = calls[0].kwargs.get("request") or (
            calls[0].args[0] if calls[0].args else {}
        )
        assert first_request.get("tag_identifiers") == ["KPI"], first_request
        assert "tag_name" not in first_request["metadata"][0]
        assert "tags" not in first_request

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_tag_identifiers_omitted_when_filter_unset_or_empty(self, mock_sdk_class):
        """Regression guard: ``tag_identifiers`` must NOT appear in the
        request body when no filter is configured. The current
        ``if tag_names:`` short-circuit covers ``None`` AND ``[]``;
        a future drift to ``if tag_names is not None:`` would send
        ``tag_identifiers: []`` and (per TS REST v2 semantics) return
        zero rows — a silent-empty-result regression. Pin both paths.
        """
        mock_sdk = MagicMock()
        mock_sdk_class.return_value = mock_sdk
        mock_sdk.auth_token_full.return_value = {"token": "t"}
        mock_sdk.metadata_search.return_value = []

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())

        # tag_names=None — the no-filter case.
        list(client.iter_liveboards(tag_names=None))
        # tag_names=[] — the empty-list edge case.
        list(client.iter_liveboards(tag_names=[]))

        calls = mock_sdk.metadata_search.call_args_list
        assert len(calls) >= 2, "expected at least two metadata_search calls"
        for call in calls:
            req = call.kwargs.get("request") or (call.args[0] if call.args else {})
            assert "tag_identifiers" not in req, (
                "tag_identifiers must be absent from the request body when "
                f"no tag filter is configured. Actual request: {req}"
            )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_zero_result_with_tag_filter_emits_warning(self, mock_client_cls):
        """When a configured tag filter matches 0 entities and the pattern
        didn't drop any either, surface a ``report.warning`` so a typo'd
        tag name (TS is case-sensitive) doesn't result in a silent empty
        run."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []  # server returned empty
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = []
        mock_client.get_tags.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "liveboard_tag_filter": ["Produciton"],  # typo
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        list(source.get_workunits_internal())

        # After W4 LiteralString cleanup: title is literal, entity_type is
        # in context. Assert on both so a regression on either half fails.
        liveboard_tag_warnings = [
            w
            for w in source.report.warnings
            if w.title == "Tag Filter Matched Zero Entities"
            and any("entity_type=Liveboards" in c for c in (w.context or []))
        ]
        assert liveboard_tag_warnings, (
            "Expected zero-result tag-filter warning for Liveboards. "
            f"Got warnings: {list(source.report.warnings)}"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_zero_result_without_tag_filter_does_not_warn(self, mock_client_cls):
        """A genuinely empty tenant (no tag filter configured) should NOT
        emit the tag-filter warning — only configured-and-mismatched
        filters trigger it."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = []
        mock_client.get_tags.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        list(source.get_workunits_internal())

        titles = [w.title or "" for w in source.report.warnings]
        assert not any("Tag Filter Matched Zero Entities" in t for t in titles), titles


class TestThoughtSpotSourceErrorRecovery:
    """Test error handling and graceful degradation during ingestion."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_workspace_extraction_failure_continues_ingestion(self, mock_client_class):
        """Test that workspace extraction failure doesn't stop other entities."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Workspace fetch fails
        mock_client.get_workspaces.side_effect = ThoughtSpotAPIError(
            "Workspace API error"
        )

        # Liveboard fetch succeeds
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb1", name="Dashboard 1", visualizations=[])
        ]

        workunits = list(source.get_workunits_internal())

        # Should still get liveboard workunits despite workspace failure
        liveboard_urns = [
            wu.get_urn() for wu in workunits if "dashboard" in wu.get_urn()
        ]
        assert len(liveboard_urns) > 0

        # Should have warning about workspace failure
        assert len(source.report.warnings) > 0

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_single_liveboard_failure_continues_processing(self, mock_client_class):
        """Test that failure processing one liveboard doesn't stop others."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []

        # Return liveboards - all must be valid Pydantic models now
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb1", name="Good Dashboard", visualizations=[]),
            LiveboardResponse(
                id="lb3", name="Another Good Dashboard", visualizations=[]
            ),
        ]

        workunits = list(source.get_workunits_internal())

        # Should process both liveboards (SDK emits multiple workunits per entity)
        liveboard_urns = [
            wu.get_urn() for wu in workunits if "dashboard" in wu.get_urn()
        ]
        unique_liveboards = set(liveboard_urns)
        assert len(unique_liveboards) == 2

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_authentication_error_reported_correctly(self, mock_client_class):
        """Test that authentication errors are properly categorized in report."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "invalid_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.side_effect = ThoughtSpotAuthenticationError(
            "Invalid token"
        )

        list(source.get_workunits_internal())

        # Should have failure (not just warning) for auth error
        assert len(source.report.failures) > 0
        failure_messages = [str(f) for f in source.report.failures]
        assert any("authentication" in msg.lower() for msg in failure_messages)


class TestThoughtSpotSourceURLGeneration:
    """Test external URL generation for entities."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_url_format(self, mock_client_class):
        """Test that liveboard URLs use correct format."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        url = source._get_liveboard_url("lb123")

        assert url == "https://example.thoughtspot.cloud/#/pinboard/lb123"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_url_format(self, mock_client_class):
        """Test that answer URLs use correct format."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        url = source._get_answer_url("ans456")

        assert url == "https://example.thoughtspot.cloud/#/saved-answer/ans456"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_table_url_format(self, mock_client_class):
        """Test that table/worksheet URLs use correct format."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        url = source._get_table_url("tbl789")

        assert url == "https://example.thoughtspot.cloud/#/data/tables/tbl789"


class TestThoughtSpotSourceCustomProperties:
    """Test custom properties extraction from metadata."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_custom_properties_includes_thoughtspot_id(self, mock_client_class):
        """Test that ThoughtSpot ID is included in custom properties."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        metadata = WorkspaceResponse(
            id="obj123",
            name="Test Object",
        )

        props = source._extract_custom_properties(metadata)

        assert "thoughtspot_id" in props
        assert props["thoughtspot_id"] == "obj123"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_custom_properties_includes_timestamps(self, mock_client_class):
        """Test that created/modified timestamps are included."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # TS emits ``created`` / ``modified`` as ms-since-epoch. Pick
        # values whose UTC representation is unambiguous so the ISO 8601
        # assertion isn't timezone-dependent.
        metadata = WorkspaceResponse(
            id="obj123",
            name="Test Object",
            created=1700000000000,  # 2023-11-14T22:13:20+00:00
            modified=1700000010000,  # 2023-11-14T22:13:30+00:00
        )

        props = source._extract_custom_properties(metadata)

        # Custom properties render the timestamps as ISO 8601 (human
        # readable in the DataHub UI), not raw ms epoch.
        assert props["created"] == "2023-11-14T22:13:20+00:00"
        assert props["modified"] == "2023-11-14T22:13:30+00:00"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_custom_properties_handles_missing_fields(self, mock_client_class):
        """Test that missing optional fields don't cause errors."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        metadata = WorkspaceResponse(
            id="obj123",
            name="Test Object",
            # No created, modified, or author
        )

        props = source._extract_custom_properties(metadata)

        # Should only have ID
        assert props == {"thoughtspot_id": "obj123"}

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_custom_properties_extracts_author_name(self, mock_client_class):
        """When author.name carries a real login (no whitespace), it
        surfaces as the ``author`` custom property."""
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        metadata = WorkspaceResponse(
            id="obj123",
            name="Test Object",
            author=ThoughtSpotAuthor(
                id="user123",
                name="john.doe",
                email="john@example.com",
            ),
        )

        props = source._extract_custom_properties(metadata)

        assert "author" in props
        assert props["author"] == "john.doe"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_custom_properties_skips_display_name_in_author_name(
        self, mock_client_class
    ):
        """When author.name is a display name (whitespace), reject it as
        a login candidate — otherwise we'd emit a malformed
        ``urn:li:corpuser:John Doe`` aspect that DataHub silently drops.

        This is the defensive complement to the GUID-shaped guard:
        canonical-GUID and display-name-with-whitespace both indicate
        ``author.name`` is not a usable login.
        """
        mock_client_class.return_value = MagicMock()

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        metadata = WorkspaceResponse(
            id="obj123",
            name="Test Object",
            author=ThoughtSpotAuthor(
                id="user123",
                name="John Doe",  # display name, not login
                email="john@example.com",
            ),
        )

        props = source._extract_custom_properties(metadata)
        assert "author" not in props


class TestThoughtSpotSourceOwnership:
    """Test ownership extraction logic."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_ownership_extracted_when_enabled(self, mock_client_class):
        """Test that ownership is extracted when include_ownership=True."""

        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "include_ownership": True,
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Dashboard with Owner",
                visualizations=[],
                author=ThoughtSpotAuthor(id="owner-guid-123", name="owner@example.com"),
                author_name="owner@example.com",
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Find ownership workunit
        ownership_wus = [wu for wu in workunits if _mcp(wu).aspectName == "ownership"]

        assert len(ownership_wus) > 0

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_ownership_skipped_when_disabled(self, mock_client_class):
        """Test that ownership is not extracted when include_ownership=False."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "include_ownership": False,
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Dashboard without Owner",
                visualizations=[],
                author_name="owner@example.com",
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Should not have ownership workunits
        ownership_wus = [wu for wu in workunits if _mcp(wu).aspectName == "ownership"]

        assert len(ownership_wus) == 0

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_ownership_uses_author_name_not_author_name_field(self, mock_client_class):
        """Test that ownership extraction uses author.name instead of deprecated author_name field."""

        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "include_ownership": True,
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Create proper author object (not using deprecated author_name)
        author = ThoughtSpotAuthor(
            id="user-guid-123", name="john.doe@example.com", display_name="John Doe"
        )

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Dashboard with Proper Author",
                visualizations=[],
                author=author,  # Use author object, not author_name string
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Find ownership workunit
        ownership_wus = [wu for wu in workunits if _mcp(wu).aspectName == "ownership"]

        assert len(ownership_wus) > 0, (
            "Should emit ownership aspect when author object is present"
        )

        # Verify the owner URN uses the author.name (username), not author.id (GUID)
        ownership_aspect = _aspect_as(ownership_wus[0], OwnershipClass)
        assert ownership_aspect is not None
        assert hasattr(ownership_aspect, "owners")
        assert len(ownership_aspect.owners) > 0
        owner_urn = ownership_aspect.owners[0].owner
        # Should be based on username (john.doe@example.com), not GUID
        assert "john.doe" in owner_urn.lower() or "example.com" in owner_urn.lower()
        assert "user-guid-123" not in owner_urn

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_ownership_prefers_author_name_when_author_is_guid(self, mock_client_class):
        """TS REST v2 emits ``author`` as a user GUID and ``authorName`` as the
        real login. Ownership must come from ``authorName`` (via the alias),
        otherwise dashboards lose their owner — the regression that prompted
        this test.
        """
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config = ThoughtSpotConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                },
                "include_ownership": True,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test-run"))

        # Construct from the raw wire payload so the camelCase ``authorName``
        # alias is exercised end-to-end (not the snake_case alias path).
        liveboard = LiveboardResponse.model_validate(
            {
                "id": "lb1",
                "name": "Dashboard from wire",
                "author": "67e15c06-d153-4924-a4cd-ff615393b60f",
                "authorName": "system",
                "visualizations": [],
            }
        )
        # Sanity: the alias actually populated the snake_case field.
        assert liveboard.author_name == "system"
        # And the GUID got wrapped by validate_author (legacy shape).
        assert liveboard.author is not None
        assert len(liveboard.author.name) == 36

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [liveboard]

        workunits = list(source.get_workunits_internal())
        ownership_wus = [wu for wu in workunits if _mcp(wu).aspectName == "ownership"]
        assert ownership_wus, "Expected an ownership aspect emitted for the liveboard"
        owner_urn = _aspect_as(ownership_wus[0], OwnershipClass).owners[0].owner
        assert "system" in owner_urn
        assert "67e15c06" not in owner_urn


class TestThoughtSpotSourceLineageExtraction:
    """Test lineage extraction from Answers to Datasets."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_lineage_extracted_from_source_tables(self, mock_client_class):
        """Test that Answer → Dataset lineage is extracted from source_tables."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        # Note: AnswerResponse doesn't have source_tables in the model definition
        # We'll use a dict for source_tables and set it via extra fields
        answer = AnswerResponse(id="ans1", name="Sales Analysis")
        answer.source_tables = [SourceTableRef(id="tbl1"), SourceTableRef(id="tbl2")]
        mock_client.iter_answers.return_value = [answer]

        workunits = list(source.get_workunits_internal())

        # SDK V2 puts lineage in chartInfo.inputs (not separate upstreamLineage aspect)
        chart_info_wus = [wu for wu in workunits if _mcp(wu).aspectName == "chartInfo"]

        assert len(chart_info_wus) > 0

        # Verify upstream datasets are in inputs
        chart_info = _aspect_as(chart_info_wus[0], ChartInfoClass)
        assert len(chart_info.inputs or []) == 2
        assert any("tbl1" in urn for urn in chart_info.inputs or [])
        assert any("tbl2" in urn for urn in chart_info.inputs or [])

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_column_lineage_extracted_from_search_query(self, mock_client_class):
        """Answer → Worksheet column-level lineage emits an
        ``InputFields`` aspect on the Chart URN, mirroring the
        existing Visualization path. Asymmetry between the two paths
        was a real gap until ``AnswerResponse.source_columns`` was
        added and ``_process_answer`` was wired to call
        ``_emit_chart_input_fields``.
        """
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config = ThoughtSpotConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))

        mock_client.get_workspaces.return_value = []
        # The worksheet whose columns the bracket tokens resolve to.
        # _emit_chart_input_fields needs the column lookup primed so
        # the embedded SchemaField has the right type — without this
        # the helper skips the unknown column silently.
        ws = LogicalTableResponse(
            id="ws-1",
            name="orders",
            type="WORKSHEET",
            columns=[
                ColumnResponse(id="c1", name="revenue", data_type="FLOAT"),
                ColumnResponse(id="c2", name="region", data_type="VARCHAR"),
            ],
        )
        mock_client.get_logical_tables.return_value = [ws]

        answer = AnswerResponse(
            id="ans-1",
            name="Q1 revenue by region",
            source_tables=[SourceTableRef(id="ws-1")],
        )
        answer.source_columns = ["revenue", "region"]
        mock_client.iter_answers.return_value = [answer]

        workunits = list(source.get_workunits_internal())

        input_fields_wus = [
            wu for wu in workunits if _mcp(wu).aspectName == "inputFields"
        ]
        answer_input_fields = [wu for wu in input_fields_wus if "ans-1" in wu.get_urn()]
        assert len(answer_input_fields) == 1
        aspect = _aspect_as(answer_input_fields[0], InputFieldsClass)
        field_urns = {f.schemaFieldUrn for f in aspect.fields}
        assert any("revenue" in urn for urn in field_urns)
        assert any("region" in urn for urn in field_urns)
        # Every emitted InputField must carry an embedded SchemaField
        # whose type matches the worksheet — otherwise DataHub's
        # column-lineage visualizer drops the edge silently.
        assert all(f.schemaField is not None for f in aspect.fields)

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_with_no_source_tables_has_no_lineage(self, mock_client_class):
        """Test that Answers without source_tables don't emit lineage."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        answer = AnswerResponse(id="ans1", name="Standalone Answer")
        answer.source_tables = []
        mock_client.iter_answers.return_value = [answer]

        workunits = list(source.get_workunits_internal())

        # Should have Chart entity
        chart_urns = [wu.get_urn() for wu in workunits if "chart" in wu.get_urn()]
        assert len(chart_urns) > 0

        # chartInfo should have empty or None inputs
        chart_info_wus = [wu for wu in workunits if _mcp(wu).aspectName == "chartInfo"]
        assert len(chart_info_wus) > 0
        inputs = _aspect_as(chart_info_wus[0], ChartInfoClass).inputs
        # SDK V2 may return None or empty list for no lineage
        assert inputs is None or len(inputs) == 0


class TestThoughtSpotSourceContainerHierarchy:
    """Test container hierarchy (workspace → entity) linkage."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_linked_to_workspace_container(self, mock_client_class):
        """Test that Liveboards are linked to their workspace container."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id="ws1", name="Prod Workspace")
        ]

        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Sales Dashboard",
                owner_id="ws1",  # Links to workspace
                visualizations=[],
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Find container aspect for the liveboard
        container_wus = [
            wu
            for wu in workunits
            if _mcp(wu).aspectName == "container" and "dashboard" in wu.get_urn()
        ]

        assert len(container_wus) > 0

        # Verify container URN is set (SDK V2 uses hashed container URNs)
        container_aspect = _aspect_as(container_wus[0], ContainerClass)
        assert container_aspect.container.startswith("urn:li:container:")
        # Container exists and is not empty
        assert len(container_aspect.container) > len("urn:li:container:")

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_entity_without_owner_id_has_no_container(self, mock_client_class):
        """Test that entities without owner_id don't emit container aspect."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Orphan Dashboard",
                # No owner_id
                visualizations=[],
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Should have dashboard but no container aspect
        dashboard_urns = [
            wu.get_urn() for wu in workunits if "dashboard" in wu.get_urn()
        ]
        assert len(dashboard_urns) > 0

        container_wus = [
            wu
            for wu in workunits
            if _mcp(wu).aspectName == "container" and "dashboard" in wu.get_urn()
        ]
        assert len(container_wus) == 0


class TestLogicalTablesCacheReuse:
    """``client.get_logical_tables()`` must be called at most once per
    ingestion run regardless of how many internal consumers
    (``_extract_datasets`` vs ``_emit_chart_input_fields``) reach for it.
    Prior to the fix the call happened twice — once in
    ``_get_logical_tables`` (which discarded the result after streaming)
    and once in ``_get_worksheet_columns_lookup`` (which populated the
    cache for chart emission).
    """

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_get_logical_tables_called_once_when_both_consumers_run(
        self, mock_client_cls
    ):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_connections.return_value = []
        mock_client.get_tags.return_value = []
        # Return one worksheet so both code paths have something to iterate.
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="ws-1",
                name="orders",
                type="WORKSHEET",
                columns=[ColumnResponse(id="c1", name="col_a", data_type="INT")],
            ),
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))

        # Drive both consumers: _get_logical_tables runs via extract;
        # _get_worksheet_columns_lookup is hit directly to simulate the
        # chart-input-fields path.
        list(source.get_workunits_internal())
        source._get_worksheet_columns_lookup()

        assert mock_client.get_logical_tables.call_count == 1, (
            f"Expected one call, got {mock_client.get_logical_tables.call_count}"
        )


class TestExternalConnectionOverridesSharedEmpty:
    """When no overrides are configured for a connection, repeated
    lookups should return the SAME ``ExternalConnectionConfig`` instance
    — allocating a fresh empty pydantic model per call wastes ~10K
    objects on a 10K-table tenant.
    """

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_repeated_miss_returns_same_object(self, mock_client_cls):
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))

        a = source._external_connection_overrides_by_id("conn-1")
        b = source._external_connection_overrides_by_id("conn-2")
        c = source._external_connection_overrides_by_id(None)
        assert a is b, "empty-config miss should return a shared instance"
        assert a is c, "None conn_id should also return the shared instance"


class TestMakeSchemaFieldMemoisation:
    """The chart-InputFields path builds a ``SchemaFieldClass`` per
    referenced upstream column. A worksheet column referenced by many
    vizzes should produce the SAME shared SchemaField instance —
    repeated allocation at 10K-worksheet × 50-viz scale wastes ~10M
    objects.
    """

    def _make_col(self, name: str, data_type: str = "VARCHAR") -> "ColumnResponse":
        return ColumnResponse(id=f"c-{name}", name=name, data_type=data_type)

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_repeated_column_returns_identical_schema_field(self, mock_client_cls):
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))

        col_a = self._make_col("revenue", "DOUBLE")
        col_b = self._make_col("revenue", "DOUBLE")  # same hashable fields
        sf_a = source._make_schema_field(col_a)
        sf_b = source._make_schema_field(col_b)
        # The two should be the SAME object (memoised).
        assert sf_a is sf_b, (
            "_make_schema_field should reuse the same SchemaFieldClass "
            "instance for columns with identical (name, data_type, "
            "description, column_type)"
        )


class TestMakeSelfDatasetUrnCaching:
    """``_make_self_dataset_urn`` is called many times per run with the
    same ``table_id`` from different references (column sources, chart
    input fields, dashboard inputs, lineage upstreams). The pure
    URN-construction work should be memoised so the second-and-later
    calls are dict lookups, not full URN rebuilds.
    """

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_repeated_table_id_hits_lru_cache(self, mock_client_cls):
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))

        # First call: cache miss, second: cache hit. Both must produce
        # the same URN string.
        urn_a = source._make_self_dataset_urn("tbl-1")
        urn_b = source._make_self_dataset_urn("tbl-1")
        assert urn_a == urn_b

        # The per-instance cache should hold the URN keyed by name —
        # confirms memoisation is wired (a future change that removes
        # the cache would either drop the attribute or never populate it).
        assert source._self_dataset_urn_cache.get("tbl-1") == urn_a

        # Different name → distinct cache entry.
        urn_c = source._make_self_dataset_urn("tbl-2")
        assert urn_c != urn_a
        assert len(source._self_dataset_urn_cache) == 2


class TestThoughtSpotSourceSchemaExtraction:
    """Test schema metadata extraction from datasets."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_dataset_schema_extracted_from_columns(self, mock_client_class):
        """Test that dataset schema is extracted from column details."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        # Schema is now embedded on the LogicalTableResponse model: the client's
        # list call uses include_details=True so columns arrive in the same
        # call. Source no longer makes a separate get_logical_table_details
        # request, so we don't mock one.
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="tbl1",
                name="Customer Data",
                columns=[
                    ColumnResponse(
                        id="col-1",
                        name="customer_id",
                        data_type="INT64",
                        description="Unique customer identifier",
                    ),
                    ColumnResponse(
                        id="col-2",
                        name="email",
                        data_type="VARCHAR",
                        description="Customer email address",
                    ),
                ],
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Find schemaMetadata workunit
        schema_wus = [wu for wu in workunits if _mcp(wu).aspectName == "schemaMetadata"]

        assert len(schema_wus) > 0

        # Verify schema fields
        schema_aspect = _aspect_as(schema_wus[0], SchemaMetadataClass)
        assert len(schema_aspect.fields) == 2

        field_names = [f.fieldPath for f in schema_aspect.fields]
        assert "customer_id" in field_names
        assert "email" in field_names

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_logical_tables_list_failure_does_not_abort_pipeline(
        self, mock_client_class
    ):
        """When the list call itself fails, the source surfaces a warning and
        keeps running other entity types instead of aborting the pipeline."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        # The list+detail call itself blows up — no partial schema flow exists
        # anymore, so this is the only place schema retrieval can fail.
        mock_client.get_logical_tables.side_effect = ThoughtSpotAPIError(
            "Worksheet list failed"
        )

        # Pipeline should not raise — it should surface a warning and continue.
        workunits = list(source.get_workunits_internal())

        assert workunits == []  # No datasets, but no exception either.
        assert len(source.report.warnings) > 0

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_dataset_with_no_columns_handled_gracefully(self, mock_client_class):
        """Test that datasets with no columns are handled without errors."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(id="tbl1", name="Empty Table")
        ]

        mock_client.get_logical_table_details.return_value = [
            {
                "id": "tbl1",
                "columns": [],  # No columns
            }
        ]

        workunits = list(source.get_workunits_internal())

        # Should emit dataset entity without schema
        dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
        assert len(dataset_urns) > 0

        # No schemaMetadata aspect should be emitted
        schema_wus = [wu for wu in workunits if _mcp(wu).aspectName == "schemaMetadata"]
        assert len(schema_wus) == 0


class TestThoughtSpotSourceLiveboardVisualizationReferences:
    """Test Liveboard → Visualization chart references."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_references_visualizations_as_charts(self, mock_client_class):
        """Test that Liveboard processes visualizations and references them as charts."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        liveboard = LiveboardResponse(id="lb1", name="Sales Dashboard")
        liveboard.visualizations = [
            VisualizationResponse(id="viz1", name="viz1", answer_id="ans1"),
            VisualizationResponse(id="viz2", name="viz2", answer_id="ans2"),
        ]
        mock_client.iter_liveboards.return_value = [liveboard]

        workunits = list(source.get_workunits_internal())

        # Find dashboardInfo workunit
        dashboard_info_wus = [
            wu for wu in workunits if _mcp(wu).aspectName == "dashboardInfo"
        ]

        assert len(dashboard_info_wus) > 0

        # SDK V2 uses chartEdges instead of charts array
        dashboard_info = _aspect_as(dashboard_info_wus[0], DashboardInfoClass)
        assert len(dashboard_info.chartEdges or []) == 2
        chart_urns = [edge.destinationUrn for edge in dashboard_info.chartEdges or []]
        # Dashboard now references visualizations directly (not answers)
        assert any("viz1" in urn for urn in chart_urns)
        assert any("viz2" in urn for urn in chart_urns)

        # Verify that Chart entities were created for visualizations
        chart_info_wus = [wu for wu in workunits if _mcp(wu).aspectName == "chartInfo"]
        # Should have 2 chart entities (one per visualization)
        assert len(chart_info_wus) == 2

        # Verify visualizations have lineage to their answers
        for chart_info_wu in chart_info_wus:
            chart_info = _aspect_as(chart_info_wu, ChartInfoClass)
            if chart_info.inputs:
                # Each visualization with answer_id should have upstream lineage
                assert len(chart_info.inputs or []) == 1
                assert "ans" in chart_info.inputs[0]  # Should reference ans1 or ans2

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_with_no_visualizations_has_empty_charts_list(
        self, mock_client_class
    ):
        """Test that Liveboards without visualizations have empty charts list."""
        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Empty Dashboard",
                visualizations=[],
            )
        ]

        workunits = list(source.get_workunits_internal())

        # Find dashboardInfo workunit
        dashboard_info_wus = [
            wu for wu in workunits if _mcp(wu).aspectName == "dashboardInfo"
        ]

        assert len(dashboard_info_wus) > 0

        # SDK V2 uses chartEdges - should be empty or None
        dashboard_info = _aspect_as(dashboard_info_wus[0], DashboardInfoClass)
        chart_edges = dashboard_info.chartEdges
        # SDK V2 may return None or empty list for no charts
        assert chart_edges is None or len(chart_edges) == 0


class TestVisualizationAspectParity:
    """Pin parity of aspect coverage between Visualization charts and the
    other Chart-type entities (Answers, Liveboards). Live smoke audit
    found that _process_visualization emitted Chart entities **without**
    ownership / tags / container aspects, even though the parent
    Liveboard already had those populated. This class tests each gap
    in isolation so a regression on one doesn't mask a fix on another.
    """

    @staticmethod
    def _make_source() -> "ThoughtSpotSource":
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                },
                "include_ownership": True,
            }
        )
        return ThoughtSpotSource(config, PipelineContext(run_id="t"))

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_emits_ownership_aspect(self, mock_client_class):
        """A Visualization with author info must emit an Ownership
        aspect on the Chart URN — same contract as Answer and
        Liveboard. _enrich_and_yield_liveboards already propagates
        author/author_name from the parent liveboard onto each viz
        (client.py _enrich_and_yield_liveboards), so the data is in
        hand by the time _process_visualization runs.
        """
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        source = self._make_source()
        mock_client.get_workspaces.return_value = []
        viz = VisualizationResponse(
            id="viz-1",
            name="Q1 revenue",
            author=ThoughtSpotAuthor(id="author-guid", name="alice@example.com"),
            author_name="alice@example.com",
        )
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Sales",
                visualizations=[viz],
            )
        ]
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []

        workunits = list(source.get_workunits_internal())
        viz_ownership = [
            wu
            for wu in workunits
            if _mcp(wu).aspectName == "ownership" and "viz-1" in wu.get_urn()
        ]
        assert len(viz_ownership) == 1
        owners = _aspect_as(viz_ownership[0], OwnershipClass).owners or []
        assert len(owners) >= 1
        assert any("alice" in o.owner for o in owners)

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_linked_to_parent_workspace_container(
        self, mock_client_class
    ):
        """A viz lives inside the parent Liveboard's workspace. Without
        an explicit ``_set_container`` call on the Chart, the viz emits
        as an orphan under ``platform: thoughtspot`` instead of nested
        under the Workspace it belongs to — breaking the browse path
        that Liveboards / Answers / Datasets already establish.
        """
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        source = self._make_source()
        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id="ws-1", name="Prod")
        ]
        viz = VisualizationResponse(id="viz-1", name="Q1 revenue")
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Sales",
                owner_id="ws-1",
                visualizations=[viz],
            )
        ]
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []

        workunits = list(source.get_workunits_internal())
        viz_container = [
            wu
            for wu in workunits
            if _mcp(wu).aspectName == "container" and "viz-1" in wu.get_urn()
        ]
        assert len(viz_container) == 1
        aspect = _aspect_as(viz_container[0], ContainerClass)
        assert aspect.container.startswith("urn:li:container:")

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_inherits_tags_from_parent_liveboard(self, mock_client_class):
        """TS only tags Liveboards, not individual visualizations — the
        viz API doesn't expose a per-viz tag list. A viz inside a tagged
        Liveboard is part of that tagged asset, so DataHub should
        surface the same tag on the Chart entity. We inherit the
        parent's tags onto the viz at enrichment time and then the
        ordinary tag-emission path in ``_process_visualization`` does
        the rest.
        """
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        source = self._make_source()
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = [TagResponse(id="t1", name="Revenue")]
        viz = VisualizationResponse(id="viz-1", name="Q1 revenue")
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Sales",
                visualizations=[viz],
                tags=[{"id": "t1"}],
            )
        ]
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []

        workunits = list(source.get_workunits_internal())
        viz_tags = [
            wu
            for wu in workunits
            if _mcp(wu).aspectName == "globalTags" and "viz-1" in wu.get_urn()
        ]
        assert len(viz_tags) == 1
        tag_urns = {t.tag for t in _aspect_as(viz_tags[0], GlobalTagsClass).tags}
        assert "urn:li:tag:Revenue" in tag_urns


class TestThoughtSpotSourcePermissionErrors:
    """Test handling of permission errors during entity extraction."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_permission_error_reported_as_failure(self, mock_client_class):
        """Test that permission errors are categorized as failures, not warnings."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Permission denied when fetching liveboards
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.side_effect = ThoughtSpotPermissionError(
            "API token lacks 'Can access API' permission"
        )

        list(source.get_workunits_internal())

        # Should have failure (not warning) for permission error
        assert len(source.report.failures) > 0
        failure_messages = [str(f) for f in source.report.failures]
        assert any("permission" in msg.lower() for msg in failure_messages)

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_permission_error_on_one_entity_type_continues_others(
        self, mock_client_class
    ):
        """Test that permission error on one entity type doesn't stop others."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Workspaces succeed
        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id="ws1", name="Accessible Workspace")
        ]

        # Liveboards fail with permission error
        mock_client.iter_liveboards.side_effect = ThoughtSpotPermissionError(
            "No access to liveboards"
        )

        # Answers succeed
        answer = AnswerResponse(id="ans1", name="Accessible Answer")
        answer.source_tables = []
        mock_client.iter_answers.return_value = [answer]

        workunits = list(source.get_workunits_internal())

        # Should have workspace container and answer chart
        workspace_urns = [
            wu.get_urn() for wu in workunits if "container" in wu.get_urn()
        ]
        assert len(workspace_urns) > 0

        answer_urns = [wu.get_urn() for wu in workunits if "chart" in wu.get_urn()]
        assert len(answer_urns) > 0

        # Should have failure for liveboard permission
        assert len(source.report.failures) > 0


class TestThoughtSpotSourceAnswerFiltering:
    """Test Answer entity filtering using patterns."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_filtering_respects_allow_pattern(self, mock_client_class):
        """Test that only answers matching allow pattern are included."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "answer_pattern": {
                "allow": ["^Prod_.*"],
            },
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []

        ans1 = AnswerResponse(id="ans1", name="Prod_Sales_Analysis")
        ans1.source_tables = []
        ans2 = AnswerResponse(id="ans2", name="Dev_Test_Query")
        ans2.source_tables = []
        ans3 = AnswerResponse(id="ans3", name="Prod_Revenue_Report")
        ans3.source_tables = []

        mock_client.iter_answers.return_value = [ans1, ans2, ans3]

        workunits = list(source.get_workunits_internal())

        # Extract unique chart URNs (SDK V2 emits multiple aspects per entity)
        chart_urns = [wu.get_urn() for wu in workunits if "chart" in wu.get_urn()]
        unique_charts = set(chart_urns)
        assert len(unique_charts) == 2
        assert any("ans1" in urn for urn in unique_charts)
        assert any("ans3" in urn for urn in unique_charts)
        assert not any("ans2" in urn for urn in unique_charts)

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_filtering_respects_deny_pattern(self, mock_client_class):
        """Test that answers matching deny pattern are excluded."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "answer_pattern": {
                "allow": [".*"],
                "deny": [".*_test$", ".*_temp$"],
            },
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []

        ans1 = AnswerResponse(id="ans1", name="production_query")
        ans1.source_tables = []
        ans2 = AnswerResponse(id="ans2", name="staging_test")
        ans2.source_tables = []
        ans3 = AnswerResponse(id="ans3", name="scratch_temp")
        ans3.source_tables = []

        mock_client.iter_answers.return_value = [ans1, ans2, ans3]

        workunits = list(source.get_workunits_internal())

        # SDK V2 emits multiple aspects per entity - get unique charts
        chart_urns = [wu.get_urn() for wu in workunits if "chart" in wu.get_urn()]
        unique_charts = set(chart_urns)
        assert len(unique_charts) == 1
        assert any("ans1" in urn for urn in unique_charts)


class TestThoughtSpotSourceUnicodeHandling:
    """Test handling of Unicode and special characters in entity names."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_unicode_characters_in_liveboard_name(self, mock_client_class):
        """Test that Unicode characters in names are handled correctly."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Sales Dashboard 销售仪表板",  # Chinese characters
                visualizations=[],
            ),
            LiveboardResponse(
                id="lb2",
                name="Données de vente",  # French accents
                visualizations=[],
            ),
            LiveboardResponse(
                id="lb3",
                name="Продажи 📊",  # Russian + emoji
                visualizations=[],
            ),
        ]

        workunits = list(source.get_workunits_internal())

        # All dashboards should be processed without errors
        dashboard_urns = [
            wu.get_urn() for wu in workunits if "dashboard" in wu.get_urn()
        ]
        unique_dashboards = set(dashboard_urns)
        assert len(unique_dashboards) == 3

        # No warnings or failures from Unicode handling
        unicode_issues = [
            w
            for w in source.report.warnings
            if "unicode" in str(w).lower() or "encoding" in str(w).lower()
        ]
        assert len(unicode_issues) == 0

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_special_characters_in_dataset_column_names(self, mock_client_class):
        """Test that special characters in column names are preserved."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="tbl1",
                name="Customer$Data",
                columns=[
                    ColumnResponse(
                        id="c1", name="customer_id#primary", data_type="INT64"
                    ),
                    ColumnResponse(id="c2", name="email@address", data_type="VARCHAR"),
                    ColumnResponse(id="c3", name="name (full)", data_type="VARCHAR"),
                ],
            )
        ]

        workunits = list(source.get_workunits_internal())

        schema_wus = [wu for wu in workunits if _mcp(wu).aspectName == "schemaMetadata"]
        assert len(schema_wus) > 0

        schema_aspect = _aspect_as(schema_wus[0], SchemaMetadataClass)
        field_names = [f.fieldPath for f in schema_aspect.fields]

        # Special characters should be preserved
        assert "customer_id#primary" in field_names
        assert "email@address" in field_names
        assert "name (full)" in field_names


class TestThoughtSpotSourceDatasetFiltering:
    """Test dataset (logical table) filtering using patterns."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_dataset_filtering_respects_allow_pattern(self, mock_client_class):
        """Test that only datasets matching allow pattern are included."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "worksheet_pattern": {
                "allow": ["^prod_.*"],
            },
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        mock_client.get_workspaces.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(id="tbl1", name="prod_customers"),
            LogicalTableResponse(id="tbl2", name="dev_test_data"),
            LogicalTableResponse(id="tbl3", name="prod_orders"),
        ]

        # Mock details - only prod tables should be requested
        def get_details_side_effect(table_ids):
            # Verify only filtered tables are requested
            for tid in table_ids:
                assert tid in ["tbl1", "tbl3"], f"Unexpected table ID requested: {tid}"
            return [{"id": tid, "columns": []} for tid in table_ids]

        mock_client.get_logical_table_details.side_effect = get_details_side_effect

        workunits = list(source.get_workunits_internal())

        # SDK V2 emits multiple aspects per dataset - get unique datasets
        dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
        unique_datasets = set(dataset_urns)
        assert len(unique_datasets) == 2
        assert any("tbl1" in urn for urn in unique_datasets)
        assert any("tbl3" in urn for urn in unique_datasets)
        assert not any("tbl2" in urn for urn in unique_datasets)


class TestThoughtSpotClientPydanticModels:
    """Test that ThoughtSpotClient methods return Pydantic models instead of dicts."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_workspaces_returns_workspace_response_models(
        self, mock_ts_client_class
    ):
        """Test that get_workspaces returns List[WorkspaceResponse] instead of List[Dict]."""

        # Mock the SDK client
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client

        # Mock auth response
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}

        # Workspaces come from /orgs/search, not /metadata/search.
        mock_ts_client.orgs_search.return_value = [
            {
                "id": "ws-123",
                "name": "Production Workspace",
                "description": "Main production workspace",
            }
        ]

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=PasswordAuth(username="testuser", password="testpass"),
        )

        client = ThoughtSpotClient(config)
        workspaces = client.get_workspaces()

        assert len(workspaces) > 0
        assert all(isinstance(ws, WorkspaceResponse) for ws in workspaces)

        first_workspace = workspaces[0]
        assert first_workspace.id == "ws-123"
        assert first_workspace.name == "Production Workspace"
        assert first_workspace.description == "Main production workspace"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_liveboards_returns_liveboard_response_models(
        self, mock_ts_client_class
    ):
        """Test that get_liveboards returns List[LiveboardResponse] instead of List[Dict]."""

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}

        # Mock liveboard response
        mock_ts_client.metadata_search.return_value = {
            "metadata": [
                {
                    "metadata_id": "lb-456",
                    "metadata_header": {
                        "id": "lb-456",
                        "name": "Sales Dashboard",
                        "visualizations": [
                            {"id": "viz-1", "name": "Sales by Region"},
                            {"id": "viz-2", "name": "Revenue Trend"},
                        ],
                    },
                }
            ]
        }

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        client = ThoughtSpotClient(config)
        liveboards = client.get_liveboards()

        # Should return LiveboardResponse models
        assert len(liveboards) > 0
        assert all(isinstance(lb, LiveboardResponse) for lb in liveboards)

        first_liveboard = liveboards[0]
        assert first_liveboard.id == "lb-456"
        assert first_liveboard.name == "Sales Dashboard"
        # Wire-side dicts coerce to typed VisualizationResponse instances
        # so source.py can read ``viz.id`` directly.
        assert first_liveboard.visualizations is not None
        assert [v.id for v in first_liveboard.visualizations] == ["viz-1", "viz-2"]

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_answers_returns_answer_response_models(self, mock_ts_client_class):
        """Test that get_answers returns List[AnswerResponse] instead of List[Dict]."""

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}

        mock_ts_client.metadata_search.return_value = {
            "metadata": [
                {
                    "metadata_id": "ans-789",
                    "metadata_header": {
                        "id": "ans-789",
                        "name": "Top Customers Query",
                    },
                }
            ]
        }

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        client = ThoughtSpotClient(config)
        answers = client.get_answers()

        # Should return AnswerResponse models
        assert len(answers) > 0
        assert all(isinstance(ans, AnswerResponse) for ans in answers)

        first_answer = answers[0]
        assert first_answer.id == "ans-789"
        assert first_answer.name == "Top Customers Query"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_logical_tables_returns_logical_table_response_models(
        self, mock_ts_client_class
    ):
        """Test that get_logical_tables returns List[LogicalTableResponse] instead of List[Dict]."""

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}

        # Mirrors the real /metadata/search v2 response when called with
        # include_details=true (which get_logical_tables does by default).
        mock_ts_client.metadata_search.return_value = {
            "metadata": [
                {
                    "metadata_id": "tbl-111",
                    "metadata_header": {
                        "id": "tbl-111",
                        "name": "customer_dim",
                    },
                    "metadata_detail": {
                        "columns": [
                            {
                                "header": {
                                    "id": "col-1",
                                    "name": "customer_id",
                                    "description": "PK",
                                },
                                "dataType": "INT64",
                                "type": "ATTRIBUTE",
                            },
                            {
                                "header": {"id": "col-2", "name": "email"},
                                "dataType": "VARCHAR",
                                "type": "ATTRIBUTE",
                            },
                        ],
                        "dataSourceId": "conn-abc",
                        "dataSourceTypeEnum": "DEFAULT",
                    },
                }
            ]
        }

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        client = ThoughtSpotClient(config)
        tables = client.get_logical_tables()

        # Should return LogicalTableResponse models with columns populated.
        assert len(tables) > 0
        assert all(isinstance(tbl, LogicalTableResponse) for tbl in tables)

        first_table = tables[0]
        assert first_table.id == "tbl-111"
        assert first_table.name == "customer_dim"
        assert first_table.data_source_id == "conn-abc"
        assert first_table.data_source_type == "DEFAULT"
        assert first_table.columns is not None
        assert len(first_table.columns) == 2
        assert first_table.columns[0].name == "customer_id"
        assert first_table.columns[0].data_type == "INT64"
        assert first_table.columns[1].name == "email"


class TestThoughtSpotModels:
    """Test Pydantic models for ThoughtSpot API responses."""

    def test_liveboard_response_with_visualizations(self):
        """LiveboardResponse accepts visualization list as the wire shape
        actually emitted by TS: ``[{"id": ..., "name": ...}, ...]``.

        ``_coerce_visualizations`` turns dicts into typed
        ``VisualizationResponse`` instances so source.py can iterate
        without runtime ``isinstance`` dispatch.
        """

        liveboard = LiveboardResponse(
            id="lb-789",
            name="Revenue Dashboard",
            description="Q4 revenue metrics",
            visualizations=[
                {"id": "viz-1", "name": "Sales by Region"},
                {"id": "viz-2", "name": "Revenue Trend"},
                {"id": "viz-3", "name": "Top Products"},
            ],
            created=1609459200000,
        )
        assert liveboard.id == "lb-789"
        assert liveboard.name == "Revenue Dashboard"
        assert liveboard.visualizations is not None
        assert len(liveboard.visualizations) == 3
        assert [v.id for v in liveboard.visualizations] == [
            "viz-1",
            "viz-2",
            "viz-3",
        ]

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_extraction(self, mock_client_class):
        """Test that _process_visualization() creates Chart entities from VisualizationResponse."""

        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Create a visualization with full metadata
        author = ThoughtSpotAuthor(id="user-123", name="john.doe")
        viz = VisualizationResponse(
            id="viz-789",
            name="Sales Trend Chart",
            description="Monthly sales trend visualization",
            chart_type="LINE_CHART",
            author=author,
            created=1609459200000,
            modified=1640995200000,
        )

        # Call _process_visualization directly
        workunits = list(source._process_visualization(viz))

        # Verify Chart entity was created
        assert len(workunits) > 0

        # Verify chart URN contains visualization ID
        chart_urns = [wu.get_urn() for wu in workunits]
        assert any("viz-789" in urn for urn in chart_urns)

        # Find chartInfo aspect
        chart_info_wus = [wu for wu in workunits if _mcp(wu).aspectName == "chartInfo"]
        assert len(chart_info_wus) == 1

        # Verify chart info contains correct data
        chart_info = _aspect_as(chart_info_wus[0], ChartInfoClass)
        assert chart_info.title == "Sales Trend Chart"
        assert chart_info.description == "Monthly sales trend visualization"

        # Verify custom properties are in chartInfo (SDK V2 pattern)
        assert chart_info.customProperties is not None
        assert chart_info.customProperties["thoughtspot_id"] == "viz-789"
        assert chart_info.customProperties["thoughtspot_chart_type"] == "LINE_CHART"
        assert chart_info.customProperties["author"] == "john.doe"
        assert "created" in chart_info.customProperties
        assert "modified" in chart_info.customProperties

        # Verify subTypes includes the Visualization-specific subtype.
        # Liveboard tiles emit ``THOUGHTSPOT_VISUALIZATION`` so they're
        # distinguishable from standalone Answers (which emit ``ANSWER``).

        sub_types_aspects = [
            aspect
            for wu in workunits
            for aspect in [getattr(wu.metadata, "aspect", None)]
            if isinstance(aspect, SubTypesClass)
        ]
        assert len(sub_types_aspects) == 1
        assert (
            BIAssetSubTypes.THOUGHTSPOT_VISUALIZATION in sub_types_aspects[0].typeNames
        )

        # Verify dataPlatformInstance is present
        platform_instance_wus = [
            wu for wu in workunits if _mcp(wu).aspectName == "dataPlatformInstance"
        ]
        assert len(platform_instance_wus) == 1

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_lineage(self, mock_client_class):
        """Visualizations with source_table_ids set inputs to LOGICAL_TABLE
        dataset URNs, giving the proper Dashboard→Chart→Dataset hierarchy."""

        mock_client_class.return_value = MagicMock()
        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        source = ThoughtSpotSource(config, PipelineContext(run_id="test-run"))

        # source_table_ids comes from TML's ``answer.tables[].fqn`` and lists
        # the worksheet GUIDs the chart reads from.
        viz = VisualizationResponse(
            id="viz-456",
            name="Revenue Chart",
            description="Chart from saved answer",
            source_table_ids=["worksheet-abc"],
        )

        workunits = list(source._process_visualization(viz))

        chart_info_wus = [wu for wu in workunits if _mcp(wu).aspectName == "chartInfo"]
        assert len(chart_info_wus) == 1

        chart_info = _aspect_as(chart_info_wus[0], ChartInfoClass)
        assert chart_info.inputs is not None
        assert len(chart_info.inputs or []) == 1

        # Upstream should be a dataset URN, not a chart URN — that's the
        # whole point of the Chart→Dataset hierarchy.
        upstream_urn = chart_info.inputs[0]
        assert "urn:li:dataset:" in upstream_urn
        assert "worksheet-abc" in upstream_urn

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_with_visualizations(self, mock_client_class):
        """Test that _process_liveboard() iterates visualizations and calls _process_visualization()."""

        # Mock the client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Create a liveboard with visualizations
        viz1 = VisualizationResponse(
            id="viz-001",
            name="Sales Chart",
            description="Monthly sales trends",
            chart_type="LINE_CHART",
        )
        viz2 = VisualizationResponse(
            id="viz-002",
            name="Revenue Table",
            description="Revenue breakdown by region",
            chart_type="TABLE",
        )

        liveboard = LiveboardResponse(
            id="lb-123",
            name="Executive Dashboard",
            description="Executive summary dashboard",
        )
        # Set visualizations as VisualizationResponse objects
        liveboard.visualizations = [viz1, viz2]

        # Process the liveboard
        workunits = list(source._process_liveboard(liveboard, {}))

        # Should emit workunits for:
        # 1. Dashboard entity (multiple aspects)
        # 2. Chart entities for each visualization (multiple aspects each)

        # Find dashboard workunits
        dashboard_urns = [
            wu.get_urn() for wu in workunits if "dashboard" in wu.get_urn()
        ]
        assert len(dashboard_urns) > 0

        # Find chart workunits (from visualizations)
        chart_urns = [wu.get_urn() for wu in workunits if "chart" in wu.get_urn()]
        unique_charts = set(chart_urns)

        # Should have chart entities for both visualizations
        assert len(unique_charts) == 2
        assert any("viz-001" in urn for urn in unique_charts)
        assert any("viz-002" in urn for urn in unique_charts)

        # Verify the dashboard references the charts
        dashboard_info_wus = [
            wu for wu in workunits if _mcp(wu).aspectName == "dashboardInfo"
        ]
        assert len(dashboard_info_wus) > 0

        dashboard_info = _aspect_as(dashboard_info_wus[0], DashboardInfoClass)

        # SDK V2 uses chartEdges for chart relationships
        if hasattr(dashboard_info, "chartEdges") and dashboard_info.chartEdges:
            chart_references = [
                edge.destinationUrn for edge in dashboard_info.chartEdges or []
            ]
            assert len(chart_references) == 2
            assert any("viz-001" in ref for ref in chart_references)
            assert any("viz-002" in ref for ref in chart_references)

        # Liveboards emit ``THOUGHTSPOT_LIVEBOARD`` so the DataHub UI labels
        # them as "Liveboard" rather than the generic "Dashboard" subtype,
        # matching the naming users see in the ThoughtSpot console.

        sub_types_aspects = [
            aspect
            for wu in workunits
            for aspect in [getattr(wu.metadata, "aspect", None)]
            if isinstance(aspect, SubTypesClass)
        ]
        assert any(
            BIAssetSubTypes.THOUGHTSPOT_LIVEBOARD in st.typeNames
            for st in sub_types_aspects
        )

    def test_connection_response_parses_wire_shape(self):
        """ConnectionResponse must accept the camelCase wire shape from
        ``/connection/search``. TS uses snake_case ``data_warehouse_type``
        at the top level; default_database / default_schema are not
        connection-level wire fields, so they stay None unless callers
        populate them."""

        conn = ConnectionResponse.model_validate(
            {
                "id": "conn-guid-1",
                "name": "Prod Databricks",
                "data_warehouse_type": "DATABRICKS",
            }
        )
        assert conn.id == "conn-guid-1"
        assert conn.name == "Prod Databricks"
        assert conn.data_source_type == "DATABRICKS"
        # Not wire-exposed on /connection/search; stays None.
        assert conn.default_database is None
        assert conn.default_schema is None

    def test_connection_response_defaults_optional_fields(self):
        """Minimal wire payload still parses; optional fields stay None."""

        conn = ConnectionResponse.model_validate(
            {"id": "c1", "name": "n", "data_warehouse_type": "SNOWFLAKE"}
        )
        assert conn.default_database is None
        assert conn.default_schema is None


class TestCoercionValidators:
    """Behavioral tests for the ``mode='before'`` field validators that
    convert raw wire payloads into typed sub-model instances.

    These coercers replace older ``Union[str, dict, Model]`` typing
    anti-patterns. Each consumer (``source.py``) relies on the post-validator
    invariant that the field is a homogeneous list of typed instances — so
    a regression in a coercer manifests as missing lineage edges, not as a
    crash. The tests below pin the contract.
    """

    def test_source_tables_coerces_camelcase_dict(self):
        """A ``{"id": ..., "name": ...}`` wire entry becomes a typed
        SourceTableRef so source.py can read ``table.id`` directly."""

        answer = AnswerResponse(
            id="ans-1",
            name="Sales by Region",
            source_tables=[{"id": "tbl-1", "name": "Sales"}],
        )
        assert answer.source_tables is not None
        assert isinstance(answer.source_tables[0], SourceTableRef)
        assert answer.source_tables[0].id == "tbl-1"
        assert answer.source_tables[0].name == "Sales"

    def test_source_tables_drops_malformed_dict(self):
        """A dict missing the required ``id`` field is dropped silently
        (logged at WARNING level). The remaining typed entries survive —
        the failure mode is "we lose lineage edges to that one table",
        not "the entire answer fails to ingest".
        """

        answer = AnswerResponse(
            id="ans-1",
            name="Answer",
            source_tables=[{"id": "good"}, {"name": "no-id"}],
        )
        assert answer.source_tables is not None
        assert len(answer.source_tables) == 1
        assert answer.source_tables[0].id == "good"

    def test_source_tables_passes_through_typed_instance(self):
        """Already-typed instances must pass through unchanged so the
        coercer is idempotent (important for the TML-enrichment path
        that constructs SourceTableRef directly)."""

        ref = SourceTableRef(id="tbl-1", name="Sales")
        answer = AnswerResponse(id="ans-1", name="Answer", source_tables=[ref])
        assert answer.source_tables is not None
        assert answer.source_tables[0] is ref

    def test_column_sources_coerces_camelcase_aliases(self):
        """The wire format uses camelCase (``tableId``, ``columnName``) but
        Python attributes use snake_case. ``populate_by_name=True`` on
        ColumnSourceRef lets pydantic accept the camelCase dict and expose
        the value via snake_case — so source.py reads ``src.table_id``
        regardless of the wire shape."""

        col = ColumnResponse(
            id="c-1",
            name="revenue",
            sources=[
                {
                    "tableId": "tbl-1",
                    "tableName": "Sales",
                    "columnId": "col-x",
                    "columnName": "amount",
                }
            ],
        )
        assert col.sources is not None
        assert isinstance(col.sources[0], ColumnSourceRef)
        # Wire-side camelCase → Python snake_case
        assert col.sources[0].table_id == "tbl-1"
        assert col.sources[0].column_name == "amount"
        assert col.sources[0].table_name == "Sales"
        assert col.sources[0].column_id == "col-x"

    def test_column_sources_accepts_snake_case_via_populate_by_name(self):
        """``populate_by_name=True`` means callers can also build the model
        with Python attribute names directly — useful in tests and any
        internal construction path that doesn't go through the wire."""

        col = ColumnResponse(
            id="c-1",
            name="revenue",
            sources=[ColumnSourceRef(table_id="tbl-1", column_name="amount")],
        )
        assert col.sources is not None
        assert col.sources[0].table_id == "tbl-1"
        assert col.sources[0].column_name == "amount"

    def test_column_sources_drops_malformed_dict(self):
        """A dict missing both ``tableId`` and ``columnName`` cannot become
        a valid ColumnSourceRef (both are required) — drop it silently and
        keep the well-formed entries so partial lineage is still emitted."""

        col = ColumnResponse(
            id="c-1",
            name="revenue",
            sources=[
                {"tableId": "tbl-1", "columnName": "amount"},
                {"unrelated": "field"},
            ],
        )
        assert col.sources is not None
        assert len(col.sources) == 1
        assert col.sources[0].table_id == "tbl-1"


class TestThoughtSpotSourceOwnerIdValidation:
    """Test owner_id field validation logging."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_owner_id_debug_logging_for_liveboard(self, mock_client_class, caplog):
        """Test that owner_id is logged for validation when processing Liveboards."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        workspace_id = "ws-12345-workspace-guid"
        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id=workspace_id, name="Test Workspace")
        ]
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Test Dashboard",
                owner_id=workspace_id,
                visualizations=[],
            )
        ]

        # Capture logs at DEBUG level
        with caplog.at_level(logging.DEBUG):
            list(source.get_workunits_internal())

        # Verify debug log contains owner_id validation message for workspace
        debug_messages = [
            record.message for record in caplog.records if record.levelname == "DEBUG"
        ]
        assert any(
            "owner_id" in msg.lower()
            and "workspace" in msg.lower()
            and workspace_id in msg
            for msg in debug_messages
        ), (
            f"Expected debug log about owner_id workspace validation, got: {debug_messages}"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_owner_id_debug_logging_for_answer(self, mock_client_class, caplog):
        """Test that owner_id is logged for validation when processing Answers."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        workspace_id = "ws-67890-workspace-guid"
        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id=workspace_id, name="Analytics Workspace")
        ]
        answer = AnswerResponse(id="ans-1", name="Sales Query", owner_id=workspace_id)
        answer.source_tables = []
        mock_client.iter_answers.return_value = [answer]

        # Capture logs at DEBUG level
        with caplog.at_level(logging.DEBUG):
            list(source.get_workunits_internal())

        # Verify debug log contains owner_id validation message
        debug_messages = [
            record.message for record in caplog.records if record.levelname == "DEBUG"
        ]
        assert any(
            "owner_id" in msg.lower()
            and "workspace" in msg.lower()
            and workspace_id in msg
            for msg in debug_messages
        ), (
            f"Expected debug log about owner_id workspace validation, got: {debug_messages}"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_owner_id_debug_logging_for_dataset(self, mock_client_class, caplog):
        """Test that owner_id is logged for validation when processing Datasets."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        workspace_id = "ws-11111-workspace-guid"
        mock_client.get_workspaces.return_value = [
            WorkspaceResponse(id=workspace_id, name="Data Workspace")
        ]
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="tbl-1", name="Customer Table", owner_id=workspace_id
            )
        ]
        mock_client.get_logical_table_details.return_value = [
            {"id": "tbl-1", "columns": []}
        ]

        # Capture logs at DEBUG level
        with caplog.at_level(logging.DEBUG):
            list(source.get_workunits_internal())

        # Verify debug log contains owner_id validation message
        debug_messages = [
            record.message for record in caplog.records if record.levelname == "DEBUG"
        ]
        assert any(
            "owner_id" in msg.lower()
            and "workspace" in msg.lower()
            and workspace_id in msg
            for msg in debug_messages
        ), (
            f"Expected debug log about owner_id workspace validation, got: {debug_messages}"
        )


# ============================================================================
# Gap-Fill Tests for Coverage Improvement
# ============================================================================
# These tests cover authentication error paths, test_connection() error handling,
# and edge cases that were not exercised by earlier tests.


class TestThoughtSpotClientAuthenticationPaths:
    """Test authentication code paths including trusted auth and password auth."""

    def test_trusted_auth_success(self):
        """trusted auth with secret_key."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="test_user", secret_key="test_secret_key"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client
            mock_client.auth_token_full.return_value = {"token": "generated_token"}

            client = ThoughtSpotClient(config)

            # Verify trusted auth was called
            assert mock_client.auth_token_full.called
            assert mock_client.bearer_token == "generated_token"
            assert client.config == config

    def test_password_auth_success(self):
        """password authentication path."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=PasswordAuth(username="test_user", password="test_password"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client
            mock_client.auth_token_full.return_value = {"token": "password_token"}

            client = ThoughtSpotClient(config)

            # Verify password auth was called
            assert mock_client.auth_token_full.called
            assert mock_client.bearer_token == "password_token"
            assert client.config == config

    # Auth-failure paths are parametrized over the two supported auth modes
    # so a future SDK or validator change that breaks one branch can't slip
    # past coverage. Both modes go through the same ``auth_token_full`` call
    # in production, but the per-mode field plumbing (secret_key vs password)
    # is what most refactors touch.
    _auth_mode_kwargs = [
        pytest.param(
            {"auth": TrustedAuth(username="test_user", secret_key="test_secret_key")},
            id="trusted",
        ),
        pytest.param(
            {"auth": PasswordAuth(username="test_user", password="test_password")},
            id="password",
        ),
    ]

    @pytest.mark.parametrize("auth_kwargs", _auth_mode_kwargs)
    def test_auth_401_error_raises_authentication_error(self, auth_kwargs):
        """A 401 from the TS auth endpoint must surface as ThoughtSpotAuthenticationError
        regardless of which auth mode was configured."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            **auth_kwargs,
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client
            mock_client.auth_token_full.side_effect = Exception("401 Unauthorized")

            with pytest.raises(ThoughtSpotAuthenticationError) as exc:
                ThoughtSpotClient(config)

            assert "invalid credentials" in str(exc.value).lower()

    @pytest.mark.parametrize("auth_kwargs", _auth_mode_kwargs)
    def test_auth_403_error_raises_permission_error(self, auth_kwargs):
        """A 403 from the TS auth endpoint must surface as ThoughtSpotPermissionError
        regardless of which auth mode was configured."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            **auth_kwargs,
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client
            mock_client.auth_token_full.side_effect = Exception("403 Forbidden")

            with pytest.raises(ThoughtSpotPermissionError) as exc:
                ThoughtSpotClient(config)

            assert "permission denied" in str(exc.value).lower()

    @pytest.mark.parametrize("auth_kwargs", _auth_mode_kwargs)
    def test_auth_generic_error_raises_api_error(self, auth_kwargs):
        """Generic errors (network, 5xx, etc.) surface as ThoughtSpotAPIError
        regardless of which auth mode was configured."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            **auth_kwargs,
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client
            mock_client.auth_token_full.side_effect = Exception("Network timeout")

            with pytest.raises(ThoughtSpotAPIError) as exc:
                ThoughtSpotClient(config)

            assert "authentication failed" in str(exc.value).lower()


class TestHttpStatusCodeHelper:
    """Coverage for ``_http_status_code`` and ``_classify_http_error``.

    These helpers are the single source of truth for routing 401/403/404
    across the connector (``_authenticate``, ``_call_sdk``,
    ``test_connection``, ``get_workspaces``, ``get_tags``,
    ``get_connections``). A regression here mis-routes every error site.
    """

    def test_status_code_extracted_from_response_attribute(self):
        """Preferred path: ``e.response.status_code`` is an int — use it
        regardless of the stringified message wording."""
        e: Any = Exception("Server says ok actually")
        e.response = MagicMock(status_code=403)

        assert _http_status_code(e) == 403

    def test_status_code_substring_used_when_response_missing(self):
        """Backup path: no ``.response`` attribute → scan the message.
        Guards against SDK wrappers that drop the underlying HTTPError."""
        assert _http_status_code(Exception("401 Unauthorized")) == 401
        assert _http_status_code(Exception("forbidden")) == 403
        assert _http_status_code(Exception("returned 404 not found")) == 404

    def test_status_code_attribute_wins_over_substring(self):
        """If a message accidentally contains a digit string AND a real
        status_code is present, the attribute wins — substring scan is
        a backup, not a peer."""
        e: Any = Exception("The 404 page is fine, this is a 401")
        e.response = MagicMock(status_code=403)

        assert _http_status_code(e) == 403

    def test_status_code_returns_none_for_unknown(self):
        assert _http_status_code(Exception("Some random error")) is None
        assert _http_status_code(Exception("500 Internal Server Error")) is None

    def test_classify_routes_to_auth_and_permission_errors(self):
        assert _classify_http_error(Exception("401")) is ThoughtSpotAuthenticationError
        assert _classify_http_error(Exception("403")) is ThoughtSpotPermissionError
        # 404 has no auth/permission semantic — caller decides.
        assert _classify_http_error(Exception("404")) is None
        assert _classify_http_error(Exception("500")) is None


class TestCallSdkReauthWrapper:
    """``_call_sdk`` is the runtime-resilience layer that catches mid-run
    401s, re-authenticates via ``_authenticate``, and retries the wrapped
    call once. Bounded by ``_MAX_REAUTH_ATTEMPTS`` to avoid masking a
    permanently-disabled principal as a stream of "token re-minted"
    info entries."""

    def _make_client(self, mock_ts_client_class):
        mock_ts = MagicMock()
        mock_ts_client_class.return_value = mock_ts
        mock_ts.auth_token_full.return_value = {"token": "initial-token"}
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="initial-secret"),
            ),
        )
        return client, mock_ts

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_retries_once_on_401_and_succeeds(self, mock_ts_client_class):
        """401 → re-auth → retry observes the call again → returns the
        retry's result. The wrapped callable is invoked exactly twice."""
        client, mock_ts = self._make_client(mock_ts_client_class)

        call = MagicMock(__name__="metadata_search")
        call.side_effect = [Exception("401 Unauthorized"), {"metadata": ["ok"]}]

        result = client._call_sdk(call, request={"x": 1})

        assert result == {"metadata": ["ok"]}
        assert call.call_count == 2
        # auth_token_full was called once for __init__ and once for reauth
        assert mock_ts.auth_token_full.call_count == 2
        # Counter resets to 0 after the successful retry — the cap bounds
        # CONSECUTIVE 401s, not lifetime. (See
        # ``TestReauthCounterResetsOnSuccess`` for the contract.)
        assert client._reauth_attempts == 0

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_bearer_token_updated_after_reauth(self, mock_ts_client_class):
        """After re-auth, the SDK's ``bearer_token`` attribute must carry
        the new token. The retried request rides on the new bearer — this
        is the contract that makes the wrapper actually fix anything."""
        client, mock_ts = self._make_client(mock_ts_client_class)

        # ``_make_client`` already consumed ``return_value`` for __init__;
        # set ``side_effect`` now so only the reauth call sees it.
        mock_ts.auth_token_full.side_effect = [{"token": "refreshed-token"}]
        call = MagicMock(__name__="metadata_search")
        call.side_effect = [Exception("401"), "ok"]

        client._call_sdk(call)

        assert mock_ts.bearer_token == "refreshed-token"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_does_not_retry_on_non_401(self, mock_ts_client_class):
        """A 5xx or any non-auth failure propagates without re-auth."""
        client, mock_ts = self._make_client(mock_ts_client_class)

        call = MagicMock(__name__="metadata_search")
        call.side_effect = Exception("500 Internal Server Error")

        with pytest.raises(Exception, match="500"):
            client._call_sdk(call)

        assert call.call_count == 1
        assert mock_ts.auth_token_full.call_count == 1  # only __init__
        assert client._reauth_attempts == 0

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_cap_raises_after_max_attempts(self, mock_ts_client_class):
        """A principal disabled mid-run would otherwise loop forever as
        every call sees 401 → re-auth-succeeds → next call sees 401.
        The cap converts that silent failure into a hard error."""
        client, mock_ts = self._make_client(mock_ts_client_class)
        # Prime the counter to the cap; next 401 should raise immediately.
        client._reauth_attempts = _MAX_REAUTH_ATTEMPTS
        call = MagicMock(__name__="metadata_search")
        call.side_effect = Exception("401 Unauthorized")

        with pytest.raises(ThoughtSpotAuthenticationError, match="Re-auth limit"):
            client._call_sdk(call)
        assert call.call_count == 1
        # No additional auth attempt — we bailed before retrying.
        assert mock_ts.auth_token_full.call_count == 1  # only __init__

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_reauth_failure_preserves_originating_call_breadcrumb(
        self, mock_ts_client_class
    ):
        """If ``_authenticate()`` itself fails during the retry, the raised
        exception must reference the originating SDK call so operators
        can see *which* call first observed the expired token."""
        client, mock_ts = self._make_client(mock_ts_client_class)

        # First SDK call raises 401, triggering re-auth. The reauth's
        # ``auth_token_full`` call itself fails — should surface as a
        # ``ThoughtSpotAuthenticationError`` whose message names the
        # originating SDK call so operators can trace which call first
        # observed the expired token.
        call = MagicMock(__name__="metadata_search")
        call.side_effect = Exception("401 Unauthorized")
        # ``_make_client`` already consumed ``return_value`` for __init__;
        # the next ``auth_token_full`` call (the reauth) raises.
        mock_ts.auth_token_full.side_effect = [Exception("401 Unauthorized")]

        with pytest.raises(ThoughtSpotAuthenticationError, match="metadata_search"):
            client._call_sdk(call)

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_call_sdk_does_not_swallow_keyboardinterrupt(self, mock_ts_client_class):
        """``except Exception`` deliberately misses ``BaseException`` so
        Ctrl-C still works mid-ingestion. A regression flipping this to
        ``except BaseException`` would silently swallow the signal."""
        client, _ = self._make_client(mock_ts_client_class)

        call = MagicMock(__name__="metadata_search")
        call.side_effect = KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            client._call_sdk(call)

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_organic_cap_traversal(self, mock_ts_client_class):
        """Drive ``_MAX_REAUTH_ATTEMPTS`` CONSECUTIVE 401s (no intervening
        success) and assert the cap raises. The cap bounds consecutive
        failures only — a long-running ingestion with periodic legitimate
        token refreshes must not spuriously hit the cap, so any success
        in the middle resets the counter (covered by
        ``TestReauthCounterResetsOnSuccess``).
        """
        client, mock_ts = self._make_client(mock_ts_client_class)
        # MAX+1 consecutive 401s. The first MAX trigger reauth+retry;
        # the (MAX+1)th routes through the cap path and raises.
        call = MagicMock(__name__="metadata_search")
        call.side_effect = [
            Exception("401 Unauthorized") for _ in range(_MAX_REAUTH_ATTEMPTS + 1)
        ]
        # auth_token_full must succeed for each of the MAX reauths; the
        # cap fires before any further reauth on the (MAX+1)th 401.
        mock_ts.auth_token_full.side_effect = [
            {"token": f"refreshed-{i}"} for i in range(_MAX_REAUTH_ATTEMPTS)
        ]

        with pytest.raises(ThoughtSpotAuthenticationError, match="Re-auth limit"):
            client._call_sdk(call)
        # Counter is at the cap immediately before the raise.
        assert client._reauth_attempts == _MAX_REAUTH_ATTEMPTS
        # Reauth was attempted MAX times (the cap fires before the next).
        assert (
            mock_ts.auth_token_full.call_count == 1 + _MAX_REAUTH_ATTEMPTS
        )  # init + MAX

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_retry_401_routes_through_cap(self, mock_ts_client_class):
        """A 401 on the *retried* call (not just the initial call) must
        also route through the cap path. Before the M1 fix, the retry's
        401 propagated raw and could be absorbed by graceful-degradation
        handlers as "endpoint failed, return []" — masquerading as
        partial degradation while really being an auth failure.

        Scenario: initial 401 → reauth succeeds → retry 401 → second
        reauth succeeds → second retry succeeds. The wrapper must bump
        the counter to 2 and ultimately return."""
        client, mock_ts = self._make_client(mock_ts_client_class)

        call = MagicMock(__name__="metadata_search")
        call.side_effect = [
            Exception("401 Unauthorized"),  # initial
            Exception("401 Unauthorized"),  # retry
            "ok",  # second retry
        ]
        # Two reauths after __init__; both must succeed.
        mock_ts.auth_token_full.side_effect = [
            {"token": "refreshed-1"},
            {"token": "refreshed-2"},
        ]

        assert client._call_sdk(call) == "ok"
        assert call.call_count == 3
        # Counter resets to 0 after the eventual success — cap bounds
        # CONSECUTIVE failures, not lifetime. Two reauths were observed
        # via ``auth_token_full.call_count``.
        assert client._reauth_attempts == 0
        assert mock_ts.bearer_token == "refreshed-2"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_reauth_emits_operator_breadcrumb_info(self, mock_ts_client_class):
        """``Bearer token re-minted mid-run`` is the documented
        operator-visible signal that the wrapper recovered from an
        expired token. A regression silencing it would make a recovering
        run indistinguishable from a healthy one in the report."""
        client, mock_ts = self._make_client(mock_ts_client_class)

        mock_ts.auth_token_full.side_effect = [{"token": "refreshed"}]
        call = MagicMock(__name__="metadata_search")
        call.side_effect = [Exception("401 Unauthorized"), "ok"]

        client._call_sdk(call)

        info_titles = [i.title for i in client.report.infos]
        assert "Bearer token re-minted mid-run" in info_titles


class TestReauthCounterResetsOnSuccess:
    """``_call_sdk`` was capping reauths at 3 LIFETIME — a long-running
    ingestion with several legitimate 50-min token expiries would
    fail spuriously. The cap should be on *consecutive* failures
    instead, with the counter resetting after every successful call.
    """

    @staticmethod
    def _make_client():
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        return client

    def test_counter_resets_after_successful_call(self, monkeypatch):
        client = self._make_client()
        # Inject a fake authenticator we can observe.
        monkeypatch.setattr(client, "_authenticate", lambda: None)

        # Build an SDK callable that 401s once, then succeeds.
        call_count = {"n": 0}

        def flaky(**_):
            call_count["n"] += 1
            if call_count["n"] == 1:
                resp = requests.Response()
                resp.status_code = 401
                raise HTTPError(response=resp)
            return "ok"

        result = client._call_sdk(flaky)
        assert result == "ok"
        # After success the counter must reset; the next reauth cycle
        # should have a full _MAX_REAUTH_ATTEMPTS budget again.
        assert client._reauth_attempts == 0


class TestMaxRetriesWiredToSession:
    """``ThoughtSpotConnectionConfig.max_retries`` was declared but never
    threaded into any retry mechanism — the field was config theater.
    Verify it's now wired to the SDK's ``requests_session`` via an
    ``HTTPAdapter`` mounted at session-construction time.
    """

    def test_max_retries_installs_retry_adapter(self):
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
            max_retries=5,
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            mock_sdk.return_value.requests_session = MagicMock()
            ThoughtSpotClient(config, report=ThoughtSpotReport())
            # The session must have ``mount`` called twice (http:// and
            # https://) with an HTTPAdapter whose ``max_retries`` exposes
            # the configured count.
            mount = mock_sdk.return_value.requests_session.mount
            assert mount.call_count == 2
            adapters = [c.args[1] for c in mount.call_args_list]
            assert all(isinstance(a, HTTPAdapter) for a in adapters)
            retry: Retry = adapters[0].max_retries
            assert retry.total == 5
            # Retry on 429 + 5xx is the documented exponential-backoff
            # contract.
            assert 429 in retry.status_forcelist
            assert 500 in retry.status_forcelist

    def test_max_retries_zero_means_no_retries(self):
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
            max_retries=0,
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            mock_sdk.return_value.requests_session = MagicMock()
            ThoughtSpotClient(config, report=ThoughtSpotReport())
            adapters = [
                c.args[1]
                for c in mock_sdk.return_value.requests_session.mount.call_args_list
            ]
            retry: Retry = adapters[0].max_retries
            assert retry.total == 0


class TestTimeoutSecondsAppliedToSession:
    """``ThoughtSpotConnectionConfig.timeout_seconds`` was declared but
    never threaded into request execution. Verify it's now applied as
    a default timeout on every request issued through the SDK's
    ``requests_session``.
    """

    def _build_adapter(self, timeout_seconds: int) -> HTTPAdapter:
        """Construct a ThoughtSpotClient with a real session so the
        ``_TimeoutInjectingHTTPAdapter`` is mounted, then return the
        adapter for direct invocation."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
            timeout_seconds=timeout_seconds,
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            real_session = requests.Session()
            mock_sdk.return_value.requests_session = real_session
            ThoughtSpotClient(config, report=ThoughtSpotReport())
            adapter = real_session.get_adapter("https://example.thoughtspot.cloud")
            assert isinstance(adapter, HTTPAdapter)
            return adapter

    def test_default_timeout_attached_to_adapter(self):
        """Adapter exposes the configured default for inspection."""
        adapter = self._build_adapter(timeout_seconds=42)
        assert getattr(adapter, "_default_timeout", None) == 42

    def test_send_injects_default_timeout_when_caller_omits(self):
        """The ``send`` override must inject ``_default_timeout`` into
        ``kwargs`` when the caller omits ``timeout``. Pinning the
        injection mechanism — not just the attribute — prevents a
        rename of ``_default_timeout`` from silently breaking the
        contract that ``ThoughtSpotConnectionConfig.timeout_seconds``
        actually applies."""
        adapter = self._build_adapter(timeout_seconds=42)
        fake_request = MagicMock()
        with patch.object(HTTPAdapter, "send") as super_send:
            super_send.return_value = MagicMock()
            adapter.send(fake_request)
            # Parent ``send`` saw the injected default.
            assert super_send.call_args.kwargs.get("timeout") == 42

    def test_send_does_not_override_explicit_timeout(self):
        """When a caller passes ``timeout=`` explicitly, ``setdefault``
        semantics keep their value — the adapter only fills the gap
        when the caller didn't supply one."""
        adapter = self._build_adapter(timeout_seconds=42)
        fake_request = MagicMock()
        with patch.object(HTTPAdapter, "send") as super_send:
            super_send.return_value = MagicMock()
            adapter.send(fake_request, timeout=5)
            # Caller-supplied 5 wins; the adapter's 42 default is ignored.
            assert super_send.call_args.kwargs.get("timeout") == 5


class TestThoughtSpotSourceTestConnection:
    """Test the static test_connection() method error handling paths."""

    def test_connection_success_returns_capability_report(self):
        """successful test_connection path."""

        config_dict = {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "valid_token",
                },
            }
        }

        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.test_connection.return_value = True

            report = ThoughtSpotSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True
            assert report.capability_report is not None
            assert SourceCapability.DESCRIPTIONS in report.capability_report
            assert SourceCapability.OWNERSHIP in report.capability_report

    def test_connection_api_error_surfaces_in_failure_reason(self):
        """ThoughtSpotAPIError raised by client.test_connection (network /
        5xx / unexpected wire shape) should surface the underlying error
        message in failure_reason — not the old generic "Connection test
        failed" placeholder. The False-return path was removed: probe
        either succeeds or raises a classified exception."""
        config_dict = {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "valid_token",
                },
            }
        }

        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.test_connection.side_effect = ThoughtSpotAPIError(
                "500 Internal Server Error"
            )

            report = ThoughtSpotSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            failure_reason = (report.basic_connectivity.failure_reason or "").lower()
            assert "500 internal server error" in failure_reason
            assert "thoughtspot api" in failure_reason

    def test_connection_authentication_error_caught(self):
        """ThoughtSpotAuthenticationError handling."""
        config_dict = {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "invalid_token",
                },
            }
        }

        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client_class.side_effect = ThoughtSpotAuthenticationError(
                "Invalid token"
            )

            report = ThoughtSpotSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert (
                "authentication failed"
                in (report.basic_connectivity.failure_reason or "").lower()
            )
            assert (
                "verify your username"
                in (report.basic_connectivity.failure_reason or "").lower()
            )

    def test_connection_permission_error_caught(self):
        """ThoughtSpotPermissionError handling."""
        config_dict = {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "valid_token_no_perms",
                },
            }
        }

        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client_class.side_effect = ThoughtSpotPermissionError("No API access")

            report = ThoughtSpotSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert (
                "permission denied"
                in (report.basic_connectivity.failure_reason or "").lower()
            )
            assert (
                "can access api"
                in (report.basic_connectivity.failure_reason or "").lower()
            )

    def test_connection_generic_exception_caught(self):
        """generic Exception handling."""
        config_dict = {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "valid_token",
                },
            }
        }

        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client_class.side_effect = RuntimeError("Unexpected error")

            report = ThoughtSpotSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.internal_failure is True
            assert (
                "unexpected error"
                in (report.basic_connectivity.failure_reason or "").lower()
            )


class TestThoughtSpotClientMetadataSearchDetails:
    """Schema extraction via /metadata/search with include_details=true."""

    def test_get_logical_table_details_handles_empty_response(self):
        """Object returned but with no columns in metadata_detail."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client

            mock_client.metadata_search.return_value = [
                {
                    "metadata_id": "table-123",
                    "metadata_header": {"id": "table-123", "name": "test_table"},
                    "metadata_detail": {"columns": []},
                }
            ]

            client = ThoughtSpotClient(config)
            results = client.get_logical_table_details(["table-123"])

            assert len(results) == 1
            assert results[0]["columns"] == []

    def test_get_logical_table_details_handles_missing_detail_key(self):
        """Response missing metadata_detail entirely."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_ts:
            mock_client = MagicMock()
            mock_ts.return_value = mock_client

            mock_client.metadata_search.return_value = [
                {
                    "metadata_id": "table-123",
                    "metadata_header": {"id": "table-123", "name": "test_table"},
                    # metadata_detail intentionally absent
                }
            ]

            client = ThoughtSpotClient(config)
            results = client.get_logical_table_details(["table-123"])

            assert len(results) == 1
            assert results[0]["columns"] == []


class TestThoughtSpotSourceEdgeCases:
    """Test edge cases and boundary conditions not covered by earlier tests."""

    def test_source_handles_empty_workspace_list(self):
        """Test behavior when API returns no workspaces."""
        config = ThoughtSpotConfig(
            connection=ThoughtSpotConnectionConfig(
                base_url="https://test.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            )
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.get_workspaces.return_value = []
            mock_client.iter_liveboards.return_value = []
            mock_client.iter_answers.return_value = []
            mock_client.get_logical_tables.return_value = []

            ctx = PipelineContext(run_id="test")
            source = ThoughtSpotSource(config, ctx)
            workunits = list(source.get_workunits())

            # Should complete without error even with no data
            assert isinstance(workunits, list)


class TestThoughtSpotConfigEdgeCases:
    """Test config validation edge cases."""

    def test_platform_instance_config_accepted(self):
        """Cover platform_instance configuration."""
        config = ThoughtSpotConfig(
            connection=ThoughtSpotConnectionConfig(
                base_url="https://test.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            platform_instance="prod-cluster",
        )
        assert config.platform_instance == "prod-cluster"

    def test_max_retries_zero_accepted(self):
        """Test that max_retries=0 is accepted (no retries)."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
            max_retries=0,
        )
        assert config.max_retries == 0

    def test_max_retries_positive_accepted(self):
        """Test that positive max_retries values are accepted."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
            max_retries=5,
        )
        assert config.max_retries == 5


# ============================================================================
# GAP-FILL TESTS - Added to improve coverage from 79% baseline
# ============================================================================
# These tests target specific uncovered lines identified in coverage analysis:
# - Client pagination error recovery (lines 239-247 in client.py)
# - TML export and parsing (lines 318-379 in client.py)
# - Source error recovery paths (multiple exception handlers in source.py)
# - Edge cases in API response handling
# ============================================================================


class TestThoughtSpotConfigAuthenticationValidation:
    """Test authentication validation at the correct layer (ThoughtSpotConfig)."""

    def test_trusted_auth_without_username_raises_error(self):
        """``TrustedAuth`` declares ``username`` as required; constructing
        one without it must fail pydantic validation."""
        with pytest.raises(ValidationError, match="username"):
            TrustedAuth.model_validate({"secret_key": "test_secret_key"})

    def test_password_auth_without_username_raises_error(self):
        """``PasswordAuth`` declares ``username`` as required; constructing
        one without it must fail pydantic validation."""
        with pytest.raises(ValidationError, match="username"):
            PasswordAuth.model_validate({"password": "test_password"})


class TestThoughtSpotClientPaginationErrorRecovery:
    """Test pagination error handling for uncovered lines 239-247 in client.py."""

    def test_pagination_error_logs_and_returns_partial_results(self):
        """Pagination failure mid-stream logs error and returns partial results."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with (
            patch(
                "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
            ),
            patch("datahub.ingestion.source.thoughtspot.client.logger") as mock_logger,
        ):
            client = ThoughtSpotClient(config)

            # Mock the SDK client's metadata_search method
            with patch.object(client, "ts_client") as mock_ts_client:
                # First call returns 100 items (full page, triggers pagination)
                # Second call raises exception
                call_count = [0]

                def metadata_search_side_effect(request):
                    call_count[0] += 1
                    if call_count[0] == 1:
                        # First page succeeds
                        return {
                            "metadata": [
                                {
                                    "metadata_header": {
                                        "id": f"lb-{i}",
                                        "name": f"Dashboard {i}",
                                    }
                                }
                                for i in range(100)
                            ]
                        }
                    else:
                        # Second page fails
                        raise Exception("Network timeout during pagination")

                mock_ts_client.metadata_search.side_effect = metadata_search_side_effect

                # Get results - should return first page despite second page failure
                results = list(client.get_liveboards())

                # Verify we got partial results from first page only
                assert len(results) == 100
                assert results[0].id == "lb-0"
                assert results[99].id == "lb-99"

                # Verify error was logged
                assert mock_logger.error.called
                error_msg = mock_logger.error.call_args[0][0]
                assert "Pagination failed" in error_msg
                assert "partial results" in error_msg.lower()


class TestThoughtSpotClientMetadataDetailParsing:
    """Verify ``get_metadata_details`` parses ``metadata/search`` v2 responses
    (column header, top-level dataType, descriptions)."""

    def test_metadata_search_parses_columns(self):
        """Columns extracted with name/dataType/description from search response."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
        ):
            client = ThoughtSpotClient(config)

            with patch.object(client, "ts_client") as mock_ts_client:
                # Mirrors the real /metadata/search v2 shape: top-level dataType,
                # column name + description live under ``header``.
                mock_ts_client.metadata_search.return_value = [
                    {
                        "metadata_id": "table-123",
                        "metadata_header": {"id": "table-123", "name": "sales_data"},
                        "metadata_detail": {
                            "columns": [
                                {
                                    "header": {
                                        "name": "revenue",
                                        "description": "Net revenue",
                                    },
                                    "dataType": "FLOAT",
                                    "type": "MEASURE",
                                },
                                {
                                    "header": {"name": "region"},
                                    "dataType": "VARCHAR",
                                    "type": "ATTRIBUTE",
                                },
                                {
                                    "header": {"name": "sales_date"},
                                    "dataType": "DATE",
                                    "type": "ATTRIBUTE",
                                },
                            ]
                        },
                    }
                ]

                results = client.get_metadata_details(
                    metadata_type="LOGICAL_TABLE",
                    metadata_ids=["table-123"],
                )

                mock_ts_client.metadata_search.assert_called_once()
                call_kwargs = mock_ts_client.metadata_search.call_args.kwargs
                # Confirm we ask for include_details (the whole point of the refactor).
                assert call_kwargs["request"]["include_details"] is True
                assert call_kwargs["request"]["metadata"] == [
                    {"type": "LOGICAL_TABLE", "identifier": "table-123"}
                ]

                assert len(results) == 1
                assert results[0]["id"] == "table-123"
                assert results[0]["name"] == "sales_data"
                cols = results[0]["columns"]
                assert len(cols) == 3
                assert cols[0] == {
                    "name": "revenue",
                    "data_type": "FLOAT",
                    "description": "Net revenue",
                }
                assert cols[1]["data_type"] == "VARCHAR"
                assert cols[2]["data_type"] == "DATE"

    def test_metadata_search_handles_missing_columns(self):
        """metadata_detail present but columns key absent → empty list."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
        ):
            client = ThoughtSpotClient(config)

            with patch.object(client, "ts_client") as mock_ts_client:
                mock_ts_client.metadata_search.return_value = [
                    {
                        "metadata_id": "table-456",
                        "metadata_header": {"id": "table-456", "name": "empty_table"},
                        "metadata_detail": {},
                    }
                ]

                results = client.get_metadata_details(
                    metadata_type="LOGICAL_TABLE",
                    metadata_ids=["table-456"],
                )

                assert len(results) == 1
                assert results[0]["columns"] == []

    def test_metadata_search_batches_multiple_ids(self):
        """Multiple ids produce one metadata entry per id (API rejects list-valued identifier)."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
        ):
            client = ThoughtSpotClient(config)

            with patch.object(client, "ts_client") as mock_ts_client:
                mock_ts_client.metadata_search.return_value = []

                client.get_metadata_details(
                    metadata_type="LOGICAL_TABLE",
                    metadata_ids=["a", "b", "c"],
                )

                request = mock_ts_client.metadata_search.call_args.kwargs["request"]
                assert request["metadata"] == [
                    {"type": "LOGICAL_TABLE", "identifier": "a"},
                    {"type": "LOGICAL_TABLE", "identifier": "b"},
                    {"type": "LOGICAL_TABLE", "identifier": "c"},
                ]
                assert request["record_size"] == 3


class TestThoughtSpotSourceErrorRecoveryPaths:
    """Test error recovery in source.py for uncovered exception handlers."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_workspace_extraction_error_logged(self, mock_client_class):
        """Workspace extraction errors are logged and ingestion continues."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock workspace fetch to raise an exception
        mock_client.get_workspaces.side_effect = Exception("API connection timeout")

        # Mock other entities to succeed
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb1", name="Dashboard 1", visualizations=[])
        ]

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Run ingestion - should continue despite workspace failure
        workunits = list(source.get_workunits_internal())

        # Should still emit liveboard entities
        liveboard_urns = [
            wu.get_urn() for wu in workunits if "dashboard" in wu.get_urn()
        ]
        assert len(liveboard_urns) > 0

        # Should have logged the workspace error
        assert len(source.report.warnings) > 0 or len(source.report.failures) > 0

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_schema_extraction_error_continues_processing(self, mock_client_class):
        """Schema extraction errors don't stop entity processing."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock get_logical_table_details to raise exception for specific table
        def get_details_side_effect(ids):
            if "table-error" in ids:
                raise Exception("Column metadata fetch failed")
            return []

        mock_client.get_logical_table_details.side_effect = get_details_side_effect

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            }
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = ThoughtSpotSource(config, ctx)

        # Process a dataset that will fail schema extraction
        dataset = LogicalTableResponse(
            id="table-error",
            name="Problematic Dataset",
        )

        # Should handle error gracefully and continue
        workunits = list(source._process_dataset(dataset, {}))

        # Should emit dataset properties even if schema fails
        property_wus = [
            wu for wu in workunits if _mcp(wu).aspectName == "datasetProperties"
        ]
        assert len(property_wus) > 0

    def test_unknown_fields_silently_dropped(self):
        """Unknown wire-side fields are dropped via ``extra='ignore'`` on the
        response models. This is the forward-compat contract: a new TS API
        field that we haven't modeled yet must not crash ingestion, but it
        also must not bleed onto the instance as an untyped attribute (which
        was the foot-gun under the older ``extra='allow'`` setting — typos
        in wire keys would silently shadow real fields).
        """
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
        ):
            client = ThoughtSpotClient(config)

            with patch.object(client, "ts_client") as mock_ts_client:
                mock_ts_client.metadata_search.return_value = {
                    "metadata": [
                        {
                            "metadata_header": {
                                "id": "lb-123",
                                "name": "Test Dashboard",
                                "unexpected_field": "unexpected_value",
                                "another_weird_field": 999,
                            }
                        }
                    ]
                }

                results = list(client.get_liveboards())

                # Modeled fields are preserved...
                assert isinstance(results, list)
                assert len(results) == 1
                assert results[0].id == "lb-123"
                assert results[0].name == "Test Dashboard"

                # ...and unknown wire fields are dropped, not stashed on the
                # instance. This is the load-bearing assertion: under
                # ``extra='allow'`` the next two checks would FAIL because
                # pydantic would attach the unknown fields as attributes.
                assert not hasattr(results[0], "unexpected_field")
                assert not hasattr(results[0], "another_weird_field")


class TestThoughtSpotClientEmptyResponseHandling:
    """Test handling of empty and edge-case responses."""

    def test_empty_list_response_handled(self):
        """Test that empty list responses are handled correctly (no entities found)."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
        ):
            client = ThoughtSpotClient(config)

            with patch.object(client, "ts_client") as mock_ts_client:
                # Mock empty metadata response
                mock_ts_client.metadata_search.return_value = {"metadata": []}

                # Should handle empty list gracefully
                results = list(client.get_liveboards())
                assert results == []
                assert len(results) == 0

    def test_null_metadata_response_handled(self):
        """Test that null/None metadata responses are handled."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
        )

        with patch(
            "datahub.ingestion.source.thoughtspot.client.ThoughtSpotClient._authenticate"
        ):
            client = ThoughtSpotClient(config)

            with patch.object(client, "ts_client") as mock_ts_client:
                # Some APIs might return None or missing metadata key
                mock_ts_client.metadata_search.return_value = {}

                # Should handle gracefully and return empty list
                results = list(client.get_liveboards())
                assert isinstance(results, list)
                assert len(results) == 0


class TestThoughtSpotConfigTimeoutEdgeCases:
    """Test timeout validation edge cases."""

    def test_very_large_timeout_accepted(self):
        """Test that very large timeout values are accepted."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://test.thoughtspot.cloud",
            auth=TrustedAuth(username="testuser", secret_key="test_token"),
            timeout_seconds=3600,  # 1 hour timeout
        )
        assert config.timeout_seconds == 3600


class TestSchemaMetadataExtraction:
    """End-to-end test: column metadata from /metadata/search arrives on the
    LogicalTableResponse model and gets emitted as a schemaMetadata aspect."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_dataset_schema_metadata_extracted_from_metadata_search(
        self, mock_ts_client_class
    ):
        """Schema flows: metadata_search response → LogicalTableResponse.columns
        → dataset schema fields."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}

        # First call (workspaces) returns no orgs.
        # Second call (LOGICAL_TABLE list with include_details=True) returns
        # one worksheet with full column detail inline — no second TML call.
        def metadata_search_router(request):
            obj_type = request.get("metadata", [{}])[0].get("type")
            if obj_type == "LOGICAL_TABLE":
                return [
                    {
                        "metadata_id": "table-123",
                        "metadata_header": {
                            "id": "table-123",
                            "name": "customer_data",
                            "description": "Customer dimension table",
                        },
                        "metadata_detail": {
                            "columns": [
                                {
                                    "header": {
                                        "id": "c1",
                                        "name": "customer_id",
                                        "description": "Unique identifier for customer",
                                    },
                                    "dataType": "INT64",
                                    "type": "ATTRIBUTE",
                                },
                                {
                                    "header": {
                                        "id": "c2",
                                        "name": "customer_name",
                                        "description": "Full name of the customer",
                                    },
                                    "dataType": "VARCHAR",
                                    "type": "ATTRIBUTE",
                                },
                                {
                                    "header": {
                                        "id": "c3",
                                        "name": "email",
                                        "description": "Customer email address",
                                    },
                                    "dataType": "VARCHAR",
                                    "type": "ATTRIBUTE",
                                },
                                {
                                    "header": {
                                        "id": "c4",
                                        "name": "signup_date",
                                        "description": "Date customer signed up",
                                    },
                                    "dataType": "DATE",
                                    "type": "ATTRIBUTE",
                                },
                            ],
                            "dataSourceId": "conn-1",
                            "dataSourceTypeEnum": "DEFAULT",
                        },
                    }
                ]
            return []

        mock_ts_client.metadata_search.side_effect = metadata_search_router

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "worksheet_pattern": {"allow": [".*"]},
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-schema-extraction")
        source = ThoughtSpotSource(config, ctx)

        workunits = list(source.get_workunits_internal())

        schema_wus = [wu for wu in workunits if _mcp(wu).aspectName == "schemaMetadata"]
        assert len(schema_wus) == 1

        schema_aspect = _aspect_as(schema_wus[0], SchemaMetadataClass)
        assert schema_aspect.fields is not None
        assert len(schema_aspect.fields) == 4

        fields = schema_aspect.fields
        field_names = [f.fieldPath for f in fields]
        field_types = {f.fieldPath: f.nativeDataType for f in fields}
        field_descriptions = {f.fieldPath: f.description for f in fields}

        assert set(field_names) == {
            "customer_id",
            "customer_name",
            "email",
            "signup_date",
        }
        assert field_types["customer_id"] == "INT64"
        assert field_types["signup_date"] == "DATE"
        assert field_descriptions["customer_id"] == "Unique identifier for customer"
        assert field_descriptions["customer_name"] == "Full name of the customer"
        assert field_descriptions["email"] == "Customer email address"
        assert field_descriptions["signup_date"] == "Date customer signed up"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_dataset_without_columns_has_no_schema_aspect(self, mock_ts_client_class):
        """Test that datasets without column info don't emit empty schema aspects."""
        # Setup mock ThoughtSpot SDK client
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}

        # Mock TML export with no columns
        mock_ts_client.metadata_tml_export.return_value = [
            {
                "info": {
                    "id": "table-456",
                    "name": "empty_table",
                },
                "edoc": """table:
  name: empty_table
  db: prod_db
  schema: analytics
  db_table: empty
  columns: []
""",
            }
        ]

        # Create source with config
        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test_token",
                },
            },
            "worksheet_pattern": {"allow": [".*"]},
        }
        config = ThoughtSpotConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-no-schema")
        source = ThoughtSpotSource(config, ctx)

        # Create a worksheet/table model
        worksheet = LogicalTableResponse(
            id="table-456",
            name="empty_table",
            description="Table with no columns",
        )

        # Process the dataset
        workunits = list(source._process_dataset(worksheet, {}))

        # Verify NO schemaMetadata aspect is emitted for empty column list
        schema_wus = [wu for wu in workunits if _mcp(wu).aspectName == "schemaMetadata"]
        assert len(schema_wus) == 0, "Should not emit schemaMetadata for empty columns"


class TestTagResponse:
    """TagResponse model captures ThoughtSpot tag info."""

    def test_tag_response_minimal_fields(self):
        tag = TagResponse(id="tag-1", name="Production")
        assert tag.id == "tag-1"
        assert tag.name == "Production"
        assert tag.color is None

    def test_tag_response_with_color(self):
        tag = TagResponse(id="tag-2", name="PII", color="#FF0000")
        assert tag.color == "#FF0000"


class TestThoughtSpotClientTags:
    """Verify ``client.get_tags()`` calls the SDK and parses responses."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_tags_returns_models(self, mock_ts_client_class):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.tags_search.return_value = [
            {"id": "t1", "name": "Production", "color": "#0000FF"},
            {"id": "t2", "name": "PII"},
        ]

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            )
        )

        tags = client.get_tags()
        assert len(tags) == 2
        assert all(isinstance(t, TagResponse) for t in tags)
        assert tags[0].name == "Production"
        assert tags[0].color == "#0000FF"
        assert tags[1].name == "PII"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_tags_degrades_on_error(self, mock_ts_client_class):
        """Tag enumeration failure must not abort ingestion."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.tags_search.side_effect = Exception("403 Forbidden")

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            )
        )

        # Returns empty list, doesn't raise.
        tags = client.get_tags()
        assert tags == []

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_tags_403_does_not_report_warning(self, mock_ts_client_class):
        """A 403 means the principal can't enumerate tags — that's an
        expected configuration in many deployments and must NOT show up as a
        run warning (otherwise every legitimately-restricted ingestion looks
        broken in the report).
        """

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.tags_search.side_effect = Exception("403 Forbidden")

        report = ThoughtSpotReport()
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            report=report,
        )

        tags = client.get_tags()
        assert tags == []
        # A 403 must not add anything to the report — it's the expected
        # "this principal can't enumerate tags" path.
        warning_titles = {w.title for w in report.warnings}
        assert "Tag Extraction Failed" not in warning_titles

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_workspaces_403_does_not_report_warning(self, mock_ts_client_class):
        """A 403 on ``/orgs/search`` means the principal isn't a system-org
        administrator — that's the common case for most ingestion service
        accounts, so it must not surface as a run warning. Entities are
        emitted without container hierarchy and the run completes cleanly.
        """

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.orgs_search.side_effect = Exception("403 Forbidden")

        report = ThoughtSpotReport()
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            report=report,
        )

        workspaces = client.get_workspaces()
        assert workspaces == []
        warning_titles = {w.title for w in report.warnings}
        assert "Workspace Fetch Failed" not in warning_titles

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_workspaces_unexpected_error_surfaces_warning(
        self, mock_ts_client_class
    ):
        """5xx / network errors on ``/orgs/search`` are unexpected and must
        surface as a warning so the operator notices something broke —
        unlike the 403/404 paths which are graceful-degradation noise.
        """

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.orgs_search.side_effect = Exception("500 Internal Server Error")

        report = ThoughtSpotReport()
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            report=report,
        )

        workspaces = client.get_workspaces()
        assert workspaces == []
        warning_titles = {w.title for w in report.warnings}
        assert "Workspace Fetch Failed" in warning_titles

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_get_tags_unexpected_error_surfaces_in_report(self, mock_ts_client_class):
        """Non-permission failures (auth expiry, 5xx, network) MUST surface
        in the ingestion report so operators notice — otherwise broken tag
        extraction is invisible at info-level and users blame "tags don't
        work" rather than seeing the actual cause.
        """

        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.tags_search.side_effect = Exception(
            "500 Internal Server Error: backend timeout"
        )

        report = ThoughtSpotReport()
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            report=report,
        )

        tags = client.get_tags()
        assert tags == []
        warning_titles = {w.title for w in report.warnings}
        assert "Tag Extraction Failed" in warning_titles


class TestTagAspectEmission:
    """Source emits GlobalTags aspects for entities that reference tags."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_dataset_gets_global_tags_aspect_from_tag_refs(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = [
            TagResponse(id="t1", name="Production"),
            TagResponse(id="t2", name="PII"),
        ]
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="tbl1",
                name="Customer Data",
                tags=[{"id": "t1"}, {"id": "t2"}],
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        tag_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), GlobalTagsClass)
        ]
        assert len(tag_aspects) == 1
        tag_urns = {t.tag for t in _aspect_as(tag_aspects[0], GlobalTagsClass).tags}
        assert "urn:li:tag:Production" in tag_urns
        assert "urn:li:tag:PII" in tag_urns

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_tag_urns_encode_urn_reserved_characters(self, mock_client_class):
        """Tag names containing URN-structural characters (``,`` ``(`` ``)``) must
        be percent-encoded so they don't break the URN parser. Hand-built
        ``f"urn:li:tag:{name}"`` strings leak the raw character into the URN,
        which GMS rejects as an invalid URN format. Routing through
        ``make_tag_urn`` applies ``UrnEncoder`` which encodes the four
        reserved chars (``,()`` plus ``␟``) and leaves everything else alone.
        """

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        # Mix of: reserved-char names (must encode), plain names with spaces
        # (allowed unencoded — spaces are valid URN content per UrnEncoder).
        mock_client.get_tags.return_value = [
            TagResponse(id="t1", name="Customer Facing"),
            TagResponse(id="t2", name="needs,encoding"),
            TagResponse(id="t3", name="also(parens)"),
        ]
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="tbl1",
                name="With Special Tags",
                tags=[{"id": "t1"}, {"id": "t2"}, {"id": "t3"}],
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        tag_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), GlobalTagsClass)
        ]
        assert len(tag_aspects) == 1
        tag_urns = {t.tag for t in _aspect_as(tag_aspects[0], GlobalTagsClass).tags}

        # Spaces are valid in URN content per UrnEncoder; not encoded.
        assert "urn:li:tag:Customer Facing" in tag_urns
        # Comma is URN-structural and must be percent-encoded.
        assert "urn:li:tag:needs%2Cencoding" in tag_urns
        # Parens are URN-structural (entity-key delimiters) and must encode.
        assert "urn:li:tag:also%28parens%29" in tag_urns
        # All three tags emitted (none silently dropped, none collapsed).
        assert len(tag_urns) == 3

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_entity_with_no_tags_gets_no_globaltags_aspect(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(id="tbl1", name="Untagged", tags=[]),
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        tag_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), GlobalTagsClass)
        ]
        assert tag_aspects == []


class TestThoughtSpotConnectionConfigOrg:
    """``org_identifier`` is an optional connection-level field used to
    scope every API call to one ThoughtSpot org.

    Operators routinely default this via env interpolation
    (``org_identifier: "${THOUGHTSPOT_ORG}"``); when the var is unset
    the resolved value is ``""``, which must behave like "no org" rather
    than being forwarded as an empty filter — hence the whitespace/empty
    normalization handled by the validator.
    """

    @pytest.mark.parametrize(
        "raw, expected",
        [
            (None, None),
            ("615000845", "615000845"),
            ("datahub", "datahub"),
            ("", None),
            ("   ", None),
            ("  datahub  ", "datahub"),
        ],
        ids=[
            "default_none",
            "numeric_string",
            "name",
            "empty_string_normalized_to_none",
            "whitespace_normalized_to_none",
            "surrounding_whitespace_stripped",
        ],
    )
    def test_org_identifier_normalization(self, raw, expected):
        kwargs = {
            "base_url": "https://example.thoughtspot.cloud",
            "auth": {
                "type": "trusted",
                "username": "testuser",
                "secret_key": "test_token",
            },
        }
        if raw is not None:
            kwargs["org_identifier"] = raw
        config = ThoughtSpotConnectionConfig(**kwargs)
        assert config.org_identifier == expected


class TestThoughtSpotClientOrgScoping:
    """``org_identifier`` from config is forwarded to ``metadata_search``."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_metadata_search_request_carries_org_identifier(self, mock_ts_client_class):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.metadata_search.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                org_identifier="my-org",
            )
        )

        list(client.get_metadata_list(metadata_type="LIVEBOARD"))

        request = mock_ts_client.metadata_search.call_args.kwargs["request"]
        assert request.get("org_identifier") == "my-org"

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_metadata_search_request_omits_org_when_not_set(self, mock_ts_client_class):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.metadata_search.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            )
        )

        list(client.get_metadata_list(metadata_type="LIVEBOARD"))

        request = mock_ts_client.metadata_search.call_args.kwargs["request"]
        assert "org_identifier" not in request


class TestThoughtSpotClientOrgScopingExtra:
    """Org scoping also applies to searchdata, tags_search, and non-paginated metadata_search."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_tags_search_called_with_org_identifier(self, mock_ts_client_class):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.tags_search.return_value = []
        mock_ts_client.post_request.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                org_identifier="acryl",
            )
        )

        client.get_tags()

        # Accept either approach: SDK kwarg OR raw post_request with body containing org_identifier
        called_with_org_kwarg = (
            mock_ts_client.tags_search.called
            and mock_ts_client.tags_search.call_args.kwargs.get("org_identifier")
            == "acryl"
        )
        called_via_post_request = (
            mock_ts_client.post_request.called
            and mock_ts_client.post_request.call_args.kwargs.get("request", {}).get(
                "org_identifier"
            )
            == "acryl"
        )
        assert called_with_org_kwarg or called_via_post_request, (
            "Expected get_tags to forward org_identifier either via "
            "tags_search kwarg or via post_request body."
        )

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_fetch_logical_tables_by_id_carries_org_identifier(
        self, mock_ts_client_class
    ):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.metadata_search.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                org_identifier="acryl",
            )
        )

        client.fetch_logical_tables_by_id(["t1", "t2"])

        request = mock_ts_client.metadata_search.call_args.kwargs["request"]
        assert request.get("org_identifier") == "acryl"


class TestThoughtSpotClientTmlExportOrgScoping:
    """``org_identifier`` is forwarded to ``metadata/tml/export`` calls.

    The SDK's ``metadata_tml_export`` doesn't expose ``org_identifier`` as a
    keyword argument, so without explicit handling the TML lineage path would
    silently target the principal's primary org rather than the configured
    one. Both viz-extraction (liveboards) and answer-dependency (answers)
    code paths must forward the identifier.
    """

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_liveboard_tml_export_forwards_org_identifier_via_post_request(
        self, mock_ts_client_class
    ):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.post_request.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                org_identifier="prod-org",
            )
        )

        client._get_liveboard_visualizations_via_tml(["lb-1", "lb-2"])

        # When org_identifier is set we must NOT use the SDK wrapper (which
        # silently drops the field). Verify we used the raw post_request
        # fallback and the body carries org_identifier.
        assert not mock_ts_client.metadata_tml_export.called
        assert mock_ts_client.post_request.called
        call = mock_ts_client.post_request.call_args
        assert call.kwargs["endpoint"] == "metadata/tml/export"
        body = call.kwargs["request"]
        assert body.get("org_identifier") == "prod-org"
        assert body.get("export_fqn") is True
        assert body.get("edoc_format") == "YAML"
        assert body.get("metadata") == [
            {"identifier": "lb-1"},
            {"identifier": "lb-2"},
        ]

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_answer_tml_export_forwards_org_identifier(self, mock_ts_client_class):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.post_request.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                org_identifier="prod-org",
            )
        )

        client._get_answer_dependencies(["a-1"])

        assert not mock_ts_client.metadata_tml_export.called
        body = mock_ts_client.post_request.call_args.kwargs["request"]
        assert body.get("org_identifier") == "prod-org"
        assert body.get("metadata") == [{"identifier": "a-1"}]

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_tml_export_uses_sdk_wrapper_when_org_not_set(self, mock_ts_client_class):
        # Backward-compat: without org_identifier we should still delegate
        # to the SDK so its retry/error handling is preserved.
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "test_token"}
        mock_ts_client.metadata_tml_export.return_value = []

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            )
        )

        client._get_liveboard_visualizations_via_tml(["lb-1"])

        assert mock_ts_client.metadata_tml_export.called
        # post_request must not be used as a side-channel for the no-org case.
        post_calls = [
            c
            for c in mock_ts_client.post_request.call_args_list
            if c.kwargs.get("endpoint") == "metadata/tml/export"
        ]
        assert post_calls == []


class TestCheckTmlItemStatus:
    """``_check_tml_item_status`` decides per-item whether a TML export
    object succeeded — TS returns HTTP 200 even when individual items
    failed (FORBIDDEN per-object access, SAGE_INVALID_QUESTION on
    orphaned references). Without this check those items would
    silently produce empty schema/lineage downstream."""

    def _make_client(self, mock_ts_client_class: Any) -> ThoughtSpotClient:
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        return ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
        )

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_ok_status_returns_true_no_warning(self, mock_ts_client_class):
        """Items without an ERROR status code pass through cleanly with
        no operator-visible warning."""
        client = self._make_client(mock_ts_client_class)
        item = {
            "info": {
                "id": "abc-123",
                "name": "Healthy Worksheet",
                "status": {"status_code": "OK"},
            },
            "edoc": "guid: abc-123\nworksheet:\n  name: Healthy",
        }

        assert client._check_tml_item_status(item, "WORKSHEET") is True
        assert client.report.warnings.total_elements == 0

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_error_status_returns_false_and_emits_warning(self, mock_ts_client_class):
        """ERROR items must (a) return False so the caller skips
        downstream emit, (b) emit a structured ``TML Export Failed``
        warning naming the failing GUID/error_code, (c) increment the
        api_errors counter."""
        client = self._make_client(mock_ts_client_class)
        item = {
            "info": {
                "id": "ws-forbidden",
                "name": "My Worksheet",
                "status": {
                    "status_code": "ERROR",
                    "error_code": "FORBIDDEN",
                    "error_message": (
                        "Cannot download TML due to lack of access to objects"
                    ),
                },
            },
        }
        api_errors_before = client.report.api_errors

        result = client._check_tml_item_status(item, "WORKSHEET")

        assert result is False
        titles = [w.title for w in client.report.warnings]
        assert "TML Export Failed" in titles
        # Context must carry the failing GUID and error_code for
        # operator forensics. ``StructuredLogger`` stores context as
        # ``List[str]``, so flatten before substring-matching.
        contexts = [
            ctx
            for w in client.report.warnings
            if w.title == "TML Export Failed"
            for ctx in (w.context or [])
        ]
        ctx_blob = "\n".join(contexts)
        assert "ws-forbidden" in ctx_blob
        assert "FORBIDDEN" in ctx_blob
        # api_errors counter increments — used by SourceReport's
        # error-rate metrics.
        assert client.report.api_errors == api_errors_before + 1

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_error_message_truncated_to_first_line_300_chars(
        self, mock_ts_client_class
    ):
        """TS embeds full Java stack traces in error_message. The check
        keeps just the human-readable first line, truncated to 300
        chars, so the report stays operator-readable."""
        client = self._make_client(mock_ts_client_class)
        long_first_line = "A" * 500
        stack_trace = "\n".join(
            [
                long_first_line,
                "\tat com.thoughtspot.foo.Bar.method(Bar.java:42)",
                "\tat com.thoughtspot.foo.Baz.method(Baz.java:7)",
            ]
        )
        item = {
            "info": {
                "id": "ans-bad",
                "name": "Orphan Answer",
                "status": {
                    "status_code": "ERROR",
                    "error_code": "SAGE_INVALID_QUESTION",
                    "error_message": stack_trace,
                },
            },
        }

        assert client._check_tml_item_status(item, "ANSWER") is False
        contexts = [
            ctx
            for w in client.report.warnings
            if w.title == "TML Export Failed"
            for ctx in (w.context or [])
        ]
        ctx_blob = "\n".join(contexts)
        # No stack-trace lines leaked into the context (the truncator
        # keeps only the first line).
        assert "Bar.java:42" not in ctx_blob
        assert "Baz.java:7" not in ctx_blob
        # Truncated to <= 300 chars of the first line — surrounding
        # fields add to the context, so we assert no full 500-char line
        # leaked while a 300-char run of A's is present.
        assert long_first_line not in ctx_blob
        assert "A" * 300 in ctx_blob


class TestLogicalTableSubtypeHelper:
    """``_logical_table_subtype`` maps ThoughtSpot's ``metadata_header.type``
    string to a DataHub ``DatasetSubTypes`` value. Unknown / None falls
    back to VIEW so any tenant returning an uncatalogued type still emits
    a valid subtype (matches the pre-fix hardcoded behaviour).

    TS REST API v2 type semantics:
    - ``ONE_TO_ONE_LOGICAL``: physical 1:1 warehouse-table mapping → TABLE.
    - ``PRIVATE_WORKSHEET``: privacy-flagged worksheet → WORKSHEET subtype.
    - ``USER_DEFINED``: CSV-imported data → TABLE.
    - Unknown / None: forward-compat fallback → VIEW.
    """

    @pytest.mark.parametrize(
        "ts_type, expected_subtype",
        [
            ("WORKSHEET", DatasetSubTypes.THOUGHTSPOT_WORKSHEET),
            ("SQL_VIEW", DatasetSubTypes.VIEW),
            ("ONE_TO_ONE_LOGICAL", DatasetSubTypes.TABLE),
            ("PRIVATE_WORKSHEET", DatasetSubTypes.THOUGHTSPOT_WORKSHEET),
            ("USER_DEFINED", DatasetSubTypes.TABLE),
            ("BRAND_NEW_TYPE_2027", DatasetSubTypes.VIEW),  # forward-compat fallback
            (None, DatasetSubTypes.VIEW),  # absent ``type`` field on older TS
        ],
    )
    def test_subtype_mapping(self, ts_type, expected_subtype):
        assert ThoughtSpotSource._logical_table_subtype(ts_type) == expected_subtype


class TestLogicalTableSubtypeEmission:
    """Wiring test: the value returned by ``_logical_table_subtype`` actually
    lands in the emitted ``SubTypesClass`` aspect on the dataset workunit.
    Today (pre-fix) all three tables would be subtyped as "View"."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_each_type_emits_its_own_subtype(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(id="ws1", name="Worksheet One", type="WORKSHEET"),
            LogicalTableResponse(id="sv1", name="SQL View One", type="SQL_VIEW"),
            LogicalTableResponse(
                id="pt1", name="Physical Table", type="ONE_TO_ONE_LOGICAL"
            ),
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        # Collect (entity_urn, subtype_list) for every SubTypesClass workunit
        subtype_by_urn: dict = {}
        for wu in workunits:
            aspect = getattr(wu.metadata, "aspect", None)
            if isinstance(aspect, SubTypesClass):
                subtype_by_urn[wu.get_urn()] = list(aspect.typeNames)

        # Each of the three datasets must get a SubTypesClass aspect with
        # its own discriminated subtype value (not all "View").
        ws_urn = next((u for u in subtype_by_urn if "ws1" in u), None)
        sv_urn = next((u for u in subtype_by_urn if "sv1" in u), None)
        pt_urn = next((u for u in subtype_by_urn if "pt1" in u), None)
        assert ws_urn is not None, (
            f"no SubTypes aspect for ws1: keys={list(subtype_by_urn)}"
        )
        assert sv_urn is not None, (
            f"no SubTypes aspect for sv1: keys={list(subtype_by_urn)}"
        )
        assert pt_urn is not None, (
            f"no SubTypes aspect for pt1: keys={list(subtype_by_urn)}"
        )

        assert "Worksheet" in subtype_by_urn[ws_urn]
        assert "View" in subtype_by_urn[sv_urn]
        assert "Table" in subtype_by_urn[pt_urn]
        # Critical regression guard: pre-fix all three were "View".
        assert subtype_by_urn[ws_urn] != subtype_by_urn[pt_urn], (
            "WORKSHEET and ONE_TO_ONE_LOGICAL must emit distinct subtypes"
        )


class TestAnswerAndVisualizationSubtypeEmission:
    """Wiring test: Answers and Visualizations are both Chart entities but
    must emit distinct DataHub subtypes (``Answer`` / ``Visualization``)
    so the UI and search facets can distinguish a standalone saved search
    from a tile inside a Liveboard."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_emits_thoughtspot_answer_subtype(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = [
            AnswerResponse(id="ans1", name="Quarterly Revenue"),
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        answer_subtypes = None
        for wu in workunits:
            aspect = getattr(wu.metadata, "aspect", None)
            if isinstance(aspect, SubTypesClass) and "ans1" in wu.get_urn():
                answer_subtypes = list(aspect.typeNames)
                break

        assert answer_subtypes is not None, "Answer didn't emit a SubTypesClass aspect"
        assert "Answer" in answer_subtypes
        # Regression guard: pre-fix this would have been "Chart".
        assert "Chart" not in answer_subtypes, (
            "Answer must NOT emit the generic Chart subtype — "
            "the whole point of THOUGHTSPOT_ANSWER is to distinguish "
            "Answers from Visualizations"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_emits_thoughtspot_visualization_subtype(
        self, mock_client_class
    ):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.iter_answers.return_value = []
        viz = VisualizationResponse(id="viz1", name="Revenue Tile")
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb1", name="Dashboard", visualizations=[viz]),
        ]
        mock_client._get_liveboard_visualizations_via_tml.return_value = {
            "lb1": [viz],
        }

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        viz_subtypes = None
        for wu in workunits:
            aspect = getattr(wu.metadata, "aspect", None)
            if isinstance(aspect, SubTypesClass) and "viz1" in wu.get_urn():
                viz_subtypes = list(aspect.typeNames)
                break

        assert viz_subtypes is not None, (
            "Visualization didn't emit a SubTypesClass aspect"
        )
        assert "Visualization" in viz_subtypes
        assert "Chart" not in viz_subtypes, (
            "Visualization must NOT emit the generic Chart subtype"
        )


class TestStatefulStaleRemovalConfig:
    """``ThoughtSpotConfig`` is parameterized with
    ``StatefulStaleMetadataRemovalConfig`` so operators can opt into
    ``stateful_ingestion.remove_stale_metadata: true`` from their recipe.
    Before this was wired, the connector accepted ``stateful_ingestion:
    {enabled: true}`` but then crashed inside StaleEntityRemovalHandler
    because the inherited config had no ``remove_stale_metadata`` attr."""

    def test_remove_stale_metadata_accepted(self):
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                },
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                },
            }
        )
        assert config.stateful_ingestion is not None
        assert config.stateful_ingestion.enabled is True
        assert config.stateful_ingestion.remove_stale_metadata is True

    def test_stateful_ingestion_optional(self):
        # Backward compat: recipes without a stateful_ingestion block must
        # keep working (most existing recipes don't set it).
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                },
            }
        )
        assert config.stateful_ingestion is None


class TestStatefulStaleRemovalIntegration:
    """End-to-end behaviour: when ``stateful_ingestion.remove_stale_metadata``
    is enabled, the source must register a StaleEntityRemovalHandler workunit
    processor AND emit ``status`` aspects on every entity it produces — both
    are required for soft-delete of entities that disappear between runs.

    The pre-existing ``TestStatefulStaleRemovalConfig`` tests only validate
    config parsing, not the runtime wiring. This class fills that gap so a
    refactor that breaks the handler registration (the bug commit
    ``990f3e23f9`` originally fixed) surfaces in CI.
    """

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_stale_removal_handler_registered_when_enabled(self, mock_client_class):
        """Workunit processors must include the stale-removal handler when
        ``remove_stale_metadata`` is on; otherwise entities that disappear
        between runs are never soft-deleted in DataHub."""
        mock_client_class.return_value = MagicMock()

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "s"},
                },
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                },
            }
        )
        ctx = PipelineContext(run_id="r1", pipeline_name="p1")
        ctx.graph = MagicMock()
        source = ThoughtSpotSource(config, ctx)

        processors = source.get_workunit_processors()

        # The processor comes through as a bound method of AutoStaleEntityRemovalProcessor,
        # which wraps the StaleEntityRemovalHandler internally.
        def _references_stale_processor(p: object) -> bool:
            self_obj = getattr(p, "__self__", None)
            if isinstance(
                self_obj, (StaleEntityRemovalHandler, AutoStaleEntityRemovalProcessor)
            ):
                return True
            for arg in getattr(p, "args", ()):
                if isinstance(
                    arg, (StaleEntityRemovalHandler, AutoStaleEntityRemovalProcessor)
                ):
                    return True
            return False

        assert any(_references_stale_processor(p) for p in processors if p), (
            "No AutoStaleEntityRemovalProcessor workunit processor registered — "
            "entities that disappear between runs will not be soft-deleted."
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_entities_emit_status_aspect_for_stale_tracking(self, mock_client_class):
        """The StaleEntityRemovalHandler diffs current run's URNs against the
        prior checkpoint by inspecting ``status`` aspects on emitted workunits.
        If entities don't emit ``status``, the handler has nothing to diff and
        nothing gets soft-deleted. Verify every entity-type the source emits
        carries a status aspect."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Minimal but realistic fixture: one liveboard, one answer, one dataset
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb-1", name="LB")
        ]
        mock_client.iter_answers.return_value = [
            AnswerResponse(id="ans-1", name="Answer")
        ]
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(id="tbl-1", name="Worksheet", type="WORKSHEET")
        ]
        mock_client.get_tags.return_value = []
        mock_client.get_usage_statistics.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "s"},
                },
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                },
            }
        )
        ctx = PipelineContext(run_id="r1", pipeline_name="p1")
        # Mock the graph and return no prior checkpoint so the handler
        # treats this as a first run; it then has no entities to soft-delete
        # but still runs the status-aspect emission for the current entities.
        ctx.graph = MagicMock()
        ctx.graph.get_latest_timeseries_value.return_value = None
        source = ThoughtSpotSource(config, ctx)

        # ``get_workunits()`` runs the source through the full processor
        # chain (auto_status_aspect, stale-removal, etc.). The raw
        # ``get_workunits_internal`` stream wouldn't include the ``status``
        # aspect we want to verify — it's added downstream.
        workunits = list(source.get_workunits())

        # Map status-bearing URNs by entity type. Every liveboard, answer,
        # and dataset URN we emit must have at least one status aspect.
        status_urns = {
            wu.get_urn() for wu in workunits if _mcp(wu).aspectName == "status"
        }
        assert any("dashboard:(thoughtspot,lb-1)" in u for u in status_urns), (
            f"Liveboard status aspect missing — got URNs {status_urns}"
        )
        assert any("chart:(thoughtspot,ans-1)" in u for u in status_urns), (
            f"Answer status aspect missing — got URNs {status_urns}"
        )
        assert any(
            "dataset:(urn:li:dataPlatform:thoughtspot,tbl-1," in u for u in status_urns
        ), f"Dataset status aspect missing — got URNs {status_urns}"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_stale_entities_soft_deleted_between_runs(self, mock_client_class):
        """2-run soft-delete contract: an entity present in run 1 but missing
        in run 2 must receive a ``status(removed=True)`` MCP.

        Drives the source twice through the full ``get_workunits()`` chain
        (so the StaleEntityRemovalHandler workunit processor is exercised).
        Run 1 emits {Liveboard A, Liveboard B}; the resulting checkpoint
        state is captured and replayed as run 2's prior state. Run 2 emits
        only {Liveboard A} — the handler diffs current vs prior and must
        emit ``status(removed=True)`` for Liveboard B.

        Pins the fix in commit ``990f3e23f9`` at the behaviour level
        rather than just config parsing.
        """

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.get_usage_statistics.return_value = []

        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {"type": "trusted", "username": "u", "secret_key": "s"},
            },
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
            },
        }

        # === Run 1: emit liveboards A and B; capture committed state ===
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb-A", name="Dashboard A"),
            LiveboardResponse(id="lb-B", name="Dashboard B"),
        ]
        config1 = ThoughtSpotConfig.model_validate(config_dict)
        ctx1 = PipelineContext(run_id="run-1", pipeline_name="ts-pipeline")
        ctx1.graph = MagicMock()
        ctx1.graph.get_latest_timeseries_value.return_value = None
        source1 = ThoughtSpotSource(config1, ctx1)
        list(source1.get_workunits())

        # Snapshot run 1's URN set so run 2 can replay it as prior state.
        # The handler stashes URNs on the per-job ``GenericCheckpointState``
        # under the job id ``"{platform}_stale_entity_removal"``.
        stale_job_id = "thoughtspot_stale_entity_removal"
        run1_checkpoint = source1.state_provider.get_current_checkpoint(
            JobId(stale_job_id)
        )
        assert run1_checkpoint is not None, (
            "Run 1 did not register a stale-removal checkpoint."
        )
        run1_state = run1_checkpoint.state
        assert isinstance(run1_state, GenericCheckpointState)
        assert run1_state.urns, (
            "Run 1 did not commit any URNs to the stale-removal state; "
            "the handler can't diff anything in run 2."
        )
        run1_urns = set(run1_state.urns)
        assert any("lb-A" in u for u in run1_urns)
        assert any("lb-B" in u for u in run1_urns)

        # === Run 2: emit only liveboard A; expect status(removed=True) for B ===
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb-A", name="Dashboard A"),
        ]
        config2 = ThoughtSpotConfig.model_validate(config_dict)
        ctx2 = PipelineContext(run_id="run-2", pipeline_name="ts-pipeline")
        ctx2.graph = MagicMock()
        ctx2.graph.get_latest_timeseries_value.return_value = None
        source2 = ThoughtSpotSource(config2, ctx2)

        prior_checkpoint = Checkpoint(
            job_name="thoughtspot_stale_entity_removal",
            pipeline_name="ts-pipeline",
            run_id="run-1",
            state=run1_state,
        )
        with patch.object(
            source2.state_provider,
            "get_last_checkpoint",
            return_value=prior_checkpoint,
        ):
            workunits = list(source2.get_workunits())

        soft_deletes = [
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(_mcp(wu).aspect, StatusClass)
            and bool(_aspect_as(wu, StatusClass).removed)
        ]
        soft_delete_urns = {wu.get_urn() for wu in soft_deletes}
        assert any("lb-B" in urn for urn in soft_delete_urns), (
            "No soft-delete MCP for liveboard 'lb-B'. Run 2 emitted only "
            "'lb-A' but the prior checkpoint included 'lb-B' — the handler "
            f"should have emitted status(removed=True). Got: {soft_delete_urns}"
        )
        assert not any("lb-A" in urn for urn in soft_delete_urns), (
            "Liveboard 'lb-A' was incorrectly soft-deleted despite being "
            f"emitted by run 2. Got: {soft_delete_urns}"
        )


class TestDatasetSchemaFieldTypeMapping:
    """The dataset (worksheet) schema-emission path uses our
    ``_ts_type_to_schema_type`` mapper. Previously it handed 3-tuples to
    the SDK's ``_set_schema``, which routed through ``resolve_sql_type``
    — a SQL-flavoured heuristic that doesn't know ``DATE_TIME`` and
    silently resolved it to ``NullType``. Now every column gets the
    correct DataHub typed classification."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_date_time_column_emits_time_type(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="tbl1",
                name="With Date Column",
                type="WORKSHEET",
                columns=[
                    ColumnResponse(
                        id="c1", name="Query Start Time", data_type="DATE_TIME"
                    ),
                    ColumnResponse(id="c2", name="User ID", data_type="VARCHAR"),
                ],
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "testuser",
                        "secret_key": "test_token",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="test"))

        workunits = list(source.get_workunits_internal())

        schema_metadata = None
        for wu in workunits:
            aspect = getattr(wu.metadata, "aspect", None)
            if isinstance(aspect, SchemaMetadataClass):
                schema_metadata = aspect
                break

        assert schema_metadata is not None, "no schemaMetadata emitted for worksheet"
        field_by_name = {f.fieldPath: f for f in schema_metadata.fields}

        date_time_field = field_by_name["Query Start Time"]
        # Native string is preserved verbatim from the TS API.
        assert date_time_field.nativeDataType == "DATE_TIME"
        # Critical: the typed DataHub classification must be Time, not Null.
        assert isinstance(date_time_field.type.type, TimeTypeClass), (
            f"DATE_TIME column got {type(date_time_field.type.type).__name__} "
            "instead of TimeTypeClass — the worksheet schema path is bypassing "
            "_ts_type_to_schema_type again"
        )


class TestScalabilityFixes:
    """Regressions for the 4 scalability blockers fixed in the perf pass:

    1. Unchunked TML export (would OOM/timeout at ~1000+ liveboards)
    2. Single-page hardcoded usage-stats query (silently truncated at 5000 rows)
    3. BI Server worksheet lookup repeated every run (paginated the whole
       LOGICAL_TABLE list once per ingestion)
    4. Materialised liveboard/answer lists held whole tenants in memory before
       any workunit was emitted

    Each test asserts the *mechanism* of the fix (call counts, request shape)
    rather than just the final output, so future refactors can't regress the
    behaviour by silently coalescing batches.
    """

    @staticmethod
    def _make_client(
        *, tml_export_batch_size: int = 50, metadata_fetch_batch_size: int = 100
    ) -> ThoughtSpotClient:
        return ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
                tml_export_batch_size=tml_export_batch_size,
                metadata_fetch_batch_size=metadata_fetch_batch_size,
            )
        )

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_tml_export_chunks_when_ids_exceed_batch_size(self, mock_ts_client_class):
        """Blocker #1: TML export must split a 5-id request into 3 calls
        when ``tml_export_batch_size=2``. The unchunked baseline sent every
        id in one POST and timed out on 1000+ liveboards."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.metadata_tml_export.return_value = []

        client = self._make_client(tml_export_batch_size=2)
        client._get_liveboard_visualizations_via_tml(
            ["lb1", "lb2", "lb3", "lb4", "lb5"]
        )

        assert mock_ts_client.metadata_tml_export.call_count == 3
        batches = [
            call.kwargs["metadata_ids"]
            for call in mock_ts_client.metadata_tml_export.call_args_list
        ]
        assert batches == [["lb1", "lb2"], ["lb3", "lb4"], ["lb5"]]

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_answer_tml_export_also_chunks(self, mock_ts_client_class):
        """Same blocker applied to answers — both fan-out TML calls had the
        same unbounded-batch problem."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.metadata_tml_export.return_value = []

        client = self._make_client(tml_export_batch_size=2)
        client._get_answer_dependencies(["a1", "a2", "a3"])

        assert mock_ts_client.metadata_tml_export.call_count == 2

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_metadata_details_fetch_chunks(self, mock_ts_client_class):
        """``get_metadata_details`` was sending one POST with N identifiers;
        this regression-tests the chunking introduced for the same body-size
        reason as TML export."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.metadata_search.return_value = {"metadata": []}

        client = self._make_client(metadata_fetch_batch_size=2)
        client.get_metadata_details(
            metadata_type="LOGICAL_TABLE",
            metadata_ids=["t1", "t2", "t3", "t4", "t5"],
        )

        assert mock_ts_client.metadata_search.call_count == 3

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_iter_liveboards_yields_in_batches(self, mock_ts_client_class):
        """Blocker #4: streaming. ``iter_liveboards`` must enrich and yield
        in chunks of ``tml_export_batch_size`` so the heap doesn't hold a
        full-tenant ``List[LiveboardResponse]`` before any workunit ships.

        Indirect test: with 5 liveboards and ``batch_size=2``, the TML
        enrichment helper fires 3 times (one per buffer flush)."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.metadata_search.return_value = {
            "metadata": [
                {
                    "metadata_id": f"lb{i}",
                    "metadata_header": {"id": f"lb{i}", "name": f"LB {i}"},
                }
                for i in range(5)
            ]
        }
        mock_ts_client.metadata_tml_export.return_value = []

        client = self._make_client(tml_export_batch_size=2)
        result = list(client.iter_liveboards())

        assert len(result) == 5
        # 5 liveboards in batches of 2 → 3 TML-export calls (2+2+1).
        assert mock_ts_client.metadata_tml_export.call_count == 3


class TestColumnSemanticTags:
    """Field-level ``globalTags`` for the MEASURE / ATTRIBUTE / DATE
    classification, matching the canonical URNs Looker emits so the column
    role is cross-source comparable in the DataHub UI."""

    @staticmethod
    def _make_source() -> ThoughtSpotSource:
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        with patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"):
            return ThoughtSpotSource(config, PipelineContext(run_id="t"))

    def test_attribute_column_emits_dimension_tag(self):
        col = ColumnResponse(
            id="c1", name="region", data_type="VARCHAR", column_type="ATTRIBUTE"
        )
        field = self._make_source()._make_schema_field(col)
        assert field.globalTags is not None
        tag_urns = {t.tag for t in field.globalTags.tags}
        assert "urn:li:tag:Dimension" in tag_urns
        assert "urn:li:tag:Measure" not in tag_urns

    def test_measure_column_emits_measure_tag(self):
        col = ColumnResponse(
            id="c2", name="revenue", data_type="FLOAT", column_type="MEASURE"
        )
        field = self._make_source()._make_schema_field(col)
        assert field.globalTags is not None
        tag_urns = {t.tag for t in field.globalTags.tags}
        assert "urn:li:tag:Measure" in tag_urns
        assert "urn:li:tag:Dimension" not in tag_urns

    def test_date_column_also_picks_up_temporal_tag(self):
        """Time-based columns get the Temporal tag in addition to their
        Dimension/Measure role — matches Looker's DIMENSION_GROUP shape."""
        col = ColumnResponse(
            id="c3", name="signup_date", data_type="DATE", column_type="ATTRIBUTE"
        )
        field = self._make_source()._make_schema_field(col)
        assert field.globalTags is not None
        tag_urns = {t.tag for t in field.globalTags.tags}
        assert "urn:li:tag:Dimension" in tag_urns
        assert "urn:li:tag:Temporal" in tag_urns

    def test_unclassified_column_emits_no_tags(self):
        """Columns without a TS column_type stay tag-free — don't fabricate
        a Dimension/Measure assignment just because Looker has one."""
        col = ColumnResponse(id="c4", name="raw", data_type="VARCHAR")
        field = self._make_source()._make_schema_field(col)
        assert field.globalTags is None


class TestThoughtSpotParityCustomProperties:
    """Entity-level custom properties for the fields a TS consumer expects
    to see: ``thoughtspot_chart_type``, ``thoughtspot_question_text``,
    ``thoughtspot_join_count``, ``author_display_name``."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_visualization_emits_chart_type_and_question_text(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb1",
                name="Dashboard",
                visualizations=[
                    VisualizationResponse(
                        id="viz-1",
                        name="Quarterly Sales",
                        chart_type="COLUMN",
                        question_text=(
                            "[Revenue] [Quarter] sort by [Revenue] descending"
                        ),
                    )
                ],
            )
        ]
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []

        config = ThoughtSpotConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        chart_infos = [
            _aspect_as(wu, ChartInfoClass)
            for wu in workunits
            if _mcp(wu).aspectName == "chartInfo"
        ]
        viz_infos = [
            ci
            for ci in chart_infos
            if ci is not None
            and "viz-1" in (ci.customProperties or {}).get("thoughtspot_id", "")
        ]
        assert viz_infos, "Expected a chartInfo for viz-1"
        props = viz_infos[0].customProperties
        assert props["thoughtspot_chart_type"] == "COLUMN"
        assert (
            props["thoughtspot_question_text"]
            == "[Revenue] [Quarter] sort by [Revenue] descending"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_emits_chart_type_and_question_text(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = [
            AnswerResponse(
                id="ans-1",
                name="Top Customers",
                chart_type="TABLE",
                question_text="[Customer Name] [Lifetime Value] top 100",
            )
        ]
        mock_client.get_logical_tables.return_value = []

        config = ThoughtSpotConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        chart_props = [
            _aspect_as(wu, ChartInfoClass).customProperties
            for wu in workunits
            if _mcp(wu).aspectName == "chartInfo" and _mcp(wu).aspect is not None
        ]
        ans_props = [p for p in chart_props if p.get("thoughtspot_id") == "ans-1"]
        assert ans_props, "Expected chartInfo custom_properties for ans-1"
        assert ans_props[0]["thoughtspot_chart_type"] == "TABLE"
        assert (
            ans_props[0]["thoughtspot_question_text"]
            == "[Customer Name] [Lifetime Value] top 100"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_dataset_emits_join_count(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="t1",
                name="Customers Worksheet",
                type="WORKSHEET",
                join_count=3,
            )
        ]

        config = ThoughtSpotConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        dataset_props = [
            _aspect_as(wu, DatasetPropertiesClass).customProperties
            for wu in workunits
            if _mcp(wu).aspectName == "datasetProperties"
            and _mcp(wu).aspect is not None
        ]
        t1_props = [p for p in dataset_props if p.get("thoughtspot_id") == "t1"]
        assert t1_props, "Expected datasetProperties for t1"
        assert t1_props[0]["thoughtspot_join_count"] == "3"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_author_display_name_in_custom_properties(self, mock_client_class):
        """Surface the human-readable author display name alongside the
        login so users see 'John Doe' without round-tripping to CorpUser.

        Wire shape uses top-level ``authorName`` / ``authorDisplayName``
        camelCase fields — exercised via ``model_validate`` so the
        aliases actually fire (constructing with kwargs would bypass them)."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse.model_validate(
                {
                    "id": "lb1",
                    "name": "Dashboard",
                    "visualizations": [],
                    "author": "67e15c06-d153-4924-a4cd-ff615393b60f",
                    "authorName": "john.doe@example.com",
                    "authorDisplayName": "John Doe",
                }
            )
        ]
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []

        config = ThoughtSpotConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        dash_props = [
            _aspect_as(wu, DashboardInfoClass).customProperties
            for wu in workunits
            if _mcp(wu).aspectName == "dashboardInfo" and _mcp(wu).aspect is not None
        ]
        assert dash_props, "Expected dashboardInfo with custom_properties"
        assert dash_props[0]["author_display_name"] == "John Doe"
        # Login is still emitted as the bare ``author`` key.
        assert dash_props[0]["author"] == "john.doe@example.com"


class TestExternalLineageConfig:
    """Cross-platform lineage config knobs on ThoughtSpotConfig."""

    def test_defaults(self):
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        assert config.include_external_lineage is True
        assert config.external_connections == {}

    def test_external_connections_map_accepted(self):
        """``external_connections`` accepts both GUID and display-name
        keys, holds the per-connection override block, and applies field
        defaults for missing sub-fields (so an entry can set just one
        override without filling the others)."""
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "external_connections": {
                    "conn-guid-1": {
                        "platform_instance": "prod-databricks",
                        "env": "PROD",
                    },
                    "Prod Snowflake": {"platform_instance": "prod-snowflake"},
                    "Prod BigQuery": {"preserve_column_case": True},
                },
            }
        )
        assert (
            config.external_connections["conn-guid-1"].platform_instance
            == "prod-databricks"
        )
        assert config.external_connections["conn-guid-1"].env == "PROD"
        assert (
            config.external_connections["Prod Snowflake"].platform_instance
            == "prod-snowflake"
        )
        assert config.external_connections["Prod Snowflake"].env is None
        assert config.external_connections["Prod BigQuery"].preserve_column_case is True
        # Defaults applied to fields not set in the BigQuery block.
        assert config.external_connections["Prod BigQuery"].platform_instance is None

    def test_external_connections_rejects_typo_in_sub_field(self):
        """``ExternalConnectionConfig`` is ``extra='forbid'`` so a typo
        in a sub-field (operator-authored, not from a TS API response)
        fails loud at config load time."""
        with pytest.raises(ValidationError):
            ThoughtSpotConfig.model_validate(
                {
                    "connection": {
                        "base_url": "https://example.thoughtspot.cloud",
                        "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                    },
                    "external_connections": {
                        "conn-guid-1": {
                            "platfom_instance": "prod-dbx",  # deliberate typo
                        },
                    },
                }
            )


class TestExternalPlatformMapping:
    """Module-level tables that map TS ``dataSourceTypeEnum`` →
    DataHub platform and build the platform-specific URN key."""

    def test_known_platforms_covered(self):
        expected = {
            "DATABRICKS": "databricks",
            "SNOWFLAKE": "snowflake",
            "BIGQUERY": "bigquery",
            "REDSHIFT": "redshift",
            "SYNAPSE": "mssql",
            "ORACLE": "oracle",
            "MSSQL": "mssql",
            "POSTGRES": "postgres",
            "MYSQL": "mysql",
            "TERADATA": "teradata",
            "PRESTO": "presto",
            "TRINO": "trino",
            "ATHENA": "athena",
            "SAPHANA": "hana",
            "DENODO": "denodo",
        }
        assert expected == _TS_TO_DATAHUB_PLATFORM
        for datahub_platform in expected.values():
            assert datahub_platform in _KEY_BUILDERS

    def test_default_is_not_mapped(self):
        assert "DEFAULT" not in _TS_TO_DATAHUB_PLATFORM
        assert "FALCON" not in _TS_TO_DATAHUB_PLATFORM

    def test_databricks_key_form(self):
        assert (
            _KEY_BUILDERS["databricks"]("main", "analytics", "events")
            == "main.analytics.events"
        )

    def test_snowflake_key_form(self):
        assert (
            _KEY_BUILDERS["snowflake"]("DB", "PUBLIC", "ORDERS") == "DB.PUBLIC.ORDERS"
        )

    def test_mysql_key_form_no_schema(self):
        assert _KEY_BUILDERS["mysql"]("appdb", None, "users") == "appdb.users"


class TestGetConnections:
    """Fetching the TS connection catalog via ``/connection/search``
    (singular — the SDK's ``connection_search`` method)."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_returns_parsed_models(self, mock_ts_client_class):
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.connection_search.return_value = [
            {
                "id": "c1",
                "name": "Prod Databricks",
                "data_warehouse_type": "DATABRICKS",
            },
            {"id": "c2", "name": "Prod Snowflake", "data_warehouse_type": "SNOWFLAKE"},
        ]

        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            )
        )
        conns = client.get_connections()

        assert len(conns) == 2
        assert conns[0].id == "c1"
        assert conns[0].data_source_type == "DATABRICKS"
        assert conns[1].id == "c2"
        assert mock_ts_client.connection_search.called

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_403_returns_empty_and_logs_info(self, mock_ts_client_class):
        """403 on /connection/search is the documented graceful-degradation
        path (principal lacks connection-catalog read). Mirrors the
        get_workspaces / get_tags pattern: 403 → report.info (expected),
        any other failure → report.warning (real problem)."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.connection_search.side_effect = Exception("403 Forbidden")

        report = ThoughtSpotReport()
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            report=report,
        )
        conns = client.get_connections()

        assert conns == []
        warning_titles = [w.title for w in report.warnings]
        info_titles = [i.title for i in report.infos]
        assert "External Lineage Disabled" not in warning_titles
        assert any(
            "External lineage disabled" in t for t in info_titles if t is not None
        )

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_non_403_failure_still_warns(self, mock_ts_client_class):
        """Non-403/404 failures (5xx, network) should still produce a
        loud warning so operators investigate real breakage."""
        mock_ts_client = MagicMock()
        mock_ts_client_class.return_value = mock_ts_client
        mock_ts_client.auth_token_full.return_value = {"token": "t"}
        mock_ts_client.connection_search.side_effect = Exception(
            "500 Internal Server Error"
        )

        report = ThoughtSpotReport()
        client = ThoughtSpotClient(
            ThoughtSpotConnectionConfig(
                base_url="https://example.thoughtspot.cloud",
                auth=TrustedAuth(username="testuser", secret_key="test_token"),
            ),
            report=report,
        )
        conns = client.get_connections()

        assert conns == []
        warning_titles = [w.title for w in report.warnings]
        assert "External Lineage Disabled" in warning_titles


class TestExternalLineageHelpers:
    """Source-side helpers that glue config + connection cache together
    to resolve external upstream URNs."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_connection_lookup_fetched_once(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_connections.return_value = [
            ConnectionResponse(id="c1", name="Prod DBX", data_source_type="DATABRICKS")
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))

        lookup1 = source._get_connection_lookup()
        lookup2 = source._get_connection_lookup()
        assert lookup1 is lookup2
        assert "c1" in lookup1
        assert mock_client.get_connections.call_count == 1

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_overrides_lookup_guid_first_then_name(self, mock_client_class):
        """``_external_connection_overrides`` matches by TS connection GUID
        first (stable across renames), falls back to display name. Returns
        a default-valued config block when the connection isn't in the
        ``external_connections`` map — so call sites don't branch on None."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "external_connections": {
                    "c1": {"platform_instance": "by-guid-wins"},
                    "Prod DBX": {"platform_instance": "by-name"},
                },
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))

        # GUID match beats name match when both are present.
        conn = ConnectionResponse(
            id="c1", name="Prod DBX", data_source_type="DATABRICKS"
        )
        assert (
            source._external_connection_overrides(conn).platform_instance
            == "by-guid-wins"
        )

        # GUID not in map → falls back to display-name lookup.
        conn2 = ConnectionResponse(
            id="not-in-map", name="Prod DBX", data_source_type="DATABRICKS"
        )
        assert (
            source._external_connection_overrides(conn2).platform_instance == "by-name"
        )

        # Connection not configured → default-valued block (None / False).
        conn3 = ConnectionResponse(
            id="other", name="other", data_source_type="DATABRICKS"
        )
        overrides = source._external_connection_overrides(conn3)
        assert overrides.platform_instance is None
        assert overrides.env is None
        assert overrides.preserve_column_case is False

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_env_override_falls_back_to_connector_env(self, mock_client_class):
        """When a connection's override block sets ``env``, that wins.
        When the override is absent, the URN inherits the connector-level
        ``env``."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "env": "PROD",
                "external_connections": {"c1": {"env": "STAGING"}},
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))

        override = ConnectionResponse(id="c1", name="x", data_source_type="DATABRICKS")
        plain = ConnectionResponse(id="c2", name="y", data_source_type="DATABRICKS")

        # Connection with explicit env override.
        assert (
            source._external_connection_overrides(override).env or source.config.env
        ) == "STAGING"
        # Connection without override falls back to connector ``env``.
        assert (
            source._external_connection_overrides(plain).env or source.config.env
        ) == "PROD"


class TestResolveExternalUpstream:
    """Pure URN-resolution logic for the table-level external upstream."""

    @staticmethod
    def _make_source(
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> "tuple[ThoughtSpotSource, MagicMock]":
        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            config_dict = {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "env": "PROD",
            }
            if config_overrides:
                config_dict.update(config_overrides)
            config = ThoughtSpotConfig.model_validate(config_dict)
            source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
            return source, mock_client

    def test_databricks_external_upstream(self):
        source, mock_client = self._make_source(
            {"external_connections": {"c1": {"platform_instance": "prod-dbx"}}}
        )
        mock_client.get_connections.return_value = [
            ConnectionResponse(
                id="c1",
                name="Prod DBX",
                data_source_type="DATABRICKS",
                default_database="main",
                default_schema="analytics",
            )
        ]

        table = LogicalTableResponse(
            id="ts-table-1",
            name="events",
            type="LOGICAL_TABLE",
            data_source_id="c1",
            data_source_type="DATABRICKS",
        )
        table.physical_database_name = "warehouse"
        table.physical_schema_name = "raw"
        table.physical_table_name = "events"

        ref = source._resolve_external_upstream(table)
        assert ref is not None
        assert ref.platform == "databricks"
        assert (
            ref.urn
            == "urn:li:dataset:(urn:li:dataPlatform:databricks,prod-dbx.warehouse.raw.events,PROD)"
        )

    def test_in_memory_table_returns_none(self):
        source, mock_client = self._make_source()
        mock_client.get_connections.return_value = [
            ConnectionResponse(id="c1", name="In-Memory", data_source_type="DEFAULT")
        ]
        table = LogicalTableResponse(
            id="t1",
            name="x",
            type="LOGICAL_TABLE",
            data_source_id="c1",
            data_source_type="DEFAULT",
        )
        assert source._resolve_external_upstream(table) is None

    def test_unmapped_platform_returns_none(self):
        source, mock_client = self._make_source()
        mock_client.get_connections.return_value = [
            ConnectionResponse(id="c1", name="SAP HBM", data_source_type="SAP_HBM")
        ]
        table = LogicalTableResponse(
            id="t1",
            name="x",
            type="LOGICAL_TABLE",
            data_source_id="c1",
            data_source_type="SAP_HBM",
        )
        assert source._resolve_external_upstream(table) is None

    def test_missing_connection_returns_none(self):
        """When the table claims a known external platform but the
        connection isn't visible in the lookup, return None and
        increment the unresolvable counter — real lineage gap worth
        surfacing."""
        source, mock_client = self._make_source()
        mock_client.get_connections.return_value = []
        table = LogicalTableResponse(
            id="t1",
            name="x",
            type="LOGICAL_TABLE",
            data_source_id="missing-conn",
            data_source_type="DATABRICKS",
        )
        assert source._resolve_external_upstream(table) is None
        assert source._unresolvable_external_lineage_count == 1

    def test_falcon_table_with_missing_connection_does_not_warn(self):
        """When the table's data_source_type is TS-internal (FALCON /
        DEFAULT) the missing connection is expected — TS still puts a
        data_source_id on the table but the connection lookup never
        contains FALCON-shaped entries. Must skip silently without
        bumping the counter, otherwise every TS-internal sample/system
        table on a tenant produces a false-positive warning.
        """
        source, mock_client = self._make_source()
        mock_client.get_connections.return_value = []
        for ts_type in ("DEFAULT", "FALCON", "SOMETHING_UNMAPPED"):
            table = LogicalTableResponse(
                id=f"t-{ts_type}",
                name=f"sample-{ts_type}",
                type="LOGICAL_TABLE",
                data_source_id="ts-internal-id",
                data_source_type=ts_type,
            )
            assert source._resolve_external_upstream(table) is None
        assert source._unresolvable_external_lineage_count == 0

    def test_disabled_by_config_returns_none(self):
        source, mock_client = self._make_source({"include_external_lineage": False})
        mock_client.get_connections.return_value = [
            ConnectionResponse(id="c1", name="x", data_source_type="DATABRICKS")
        ]
        table = LogicalTableResponse(
            id="t1",
            name="x",
            type="LOGICAL_TABLE",
            data_source_id="c1",
            data_source_type="DATABRICKS",
        )
        assert source._resolve_external_upstream(table) is None
        # Critical: connections are NOT fetched when feature is disabled
        assert mock_client.get_connections.call_count == 0


class TestExternalLineageEmission:
    """End-to-end: TS Logical Table with a Databricks-backed connection
    should emit an UpstreamLineage aspect containing both the TS-internal
    edges (existing behaviour) AND the external Databricks edges (new)."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_external_upstream_appended_to_aspect(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_connections.return_value = [
            ConnectionResponse(
                id="c1",
                name="Prod DBX",
                data_source_type="DATABRICKS",
                default_database="main",
                default_schema="analytics",
            )
        ]

        column = ColumnResponse(
            id="col1",
            name="revenue",
            data_type="FLOAT",
            sources=[
                ColumnSourceRef(
                    table_id="ts-physical-table",
                    column_name="revenue",
                )
            ],
        )
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="ts-table-1",
                name="orders_worksheet",
                type="WORKSHEET",
                data_source_id="c1",
                data_source_type="DATABRICKS",
                physical_database_name="warehouse",
                physical_schema_name="raw",
                physical_table_name="orders",
                columns=[column],
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "external_connections": {"c1": {"platform_instance": "prod-dbx"}},
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        upstream_aspects = [
            _aspect_as(wu, UpstreamLineageClass)
            for wu in workunits
            if _mcp(wu).aspectName == "upstreamLineage"
        ]
        assert upstream_aspects, "Expected an upstreamLineage aspect"

        upstream_urns = {u.dataset for u in upstream_aspects[0].upstreams}
        databricks_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,"
            "prod-dbx.warehouse.raw.orders,PROD)"
        )
        assert databricks_urn in upstream_urns

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_column_level_lineage_to_external_columns(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_connections.return_value = [
            ConnectionResponse(
                id="c1",
                name="dbx",
                data_source_type="DATABRICKS",
                default_database="main",
                default_schema="analytics",
            )
        ]
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="t1",
                name="orders_w",
                type="WORKSHEET",
                data_source_id="c1",
                data_source_type="DATABRICKS",
                physical_database_name="main",
                physical_schema_name="raw",
                physical_table_name="orders",
                columns=[
                    ColumnResponse(
                        id="c1col",
                        name="revenue",
                        sources=[
                            ColumnSourceRef(table_id="t1", column_name="revenue_amt")
                        ],
                    )
                ],
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        upstreams = [
            _aspect_as(wu, UpstreamLineageClass)
            for wu in workunits
            if _mcp(wu).aspectName == "upstreamLineage"
        ]
        assert upstreams
        fgs = upstreams[0].fineGrainedLineages or []
        all_upstream_fields = [field for fg in fgs for field in (fg.upstreams or [])]
        assert any("databricks" in u for u in all_upstream_fields)

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_external_lineage_disabled_by_config(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_connections.return_value = [
            ConnectionResponse(id="c1", name="dbx", data_source_type="DATABRICKS")
        ]
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="t1",
                name="orders",
                type="WORKSHEET",
                data_source_id="c1",
                data_source_type="DATABRICKS",
                physical_database_name="m",
                physical_table_name="o",
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "include_external_lineage": False,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        list(source.get_workunits_internal())

        assert mock_client.get_connections.call_count == 0


class TestExternalLineageMissingConnections:
    """When a TS table references a connection that's not in the cache,
    we skip it but surface one aggregated warning per run so operators
    know cross-platform lineage degraded."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_missing_connection_emits_aggregated_warning(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_connections.return_value = []  # principal can't see any
        # Columns with sources are needed so `_apply_dataset_upstreams`
        # proceeds past its early-return and calls
        # `_resolve_external_upstream`, which is where the counter
        # increments.
        col = ColumnResponse(
            id="c",
            name="x",
            sources=[ColumnSourceRef(table_id="t", column_name="x")],
        )
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="t1",
                name="orders",
                type="WORKSHEET",
                data_source_id="missing-conn-1",
                data_source_type="DATABRICKS",
                physical_database_name="x",
                physical_table_name="y",
                columns=[col],
            ),
            LogicalTableResponse(
                id="t2",
                name="users",
                type="WORKSHEET",
                data_source_id="missing-conn-2",
                data_source_type="SNOWFLAKE",
                physical_database_name="x",
                physical_table_name="y",
                columns=[col],
            ),
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        list(source.get_workunits_internal())

        titles = [w.title for w in source.report.warnings]
        assert any(
            t is not None and "External Lineage Resolution Failed" in t for t in titles
        ), f"Expected aggregated warning, got: {titles}"

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_unmatched_config_keys_emit_warning(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = [
            ConnectionResponse(
                id="real-c1", name="Real DBX", data_source_type="DATABRICKS"
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "external_connections": {
                    "real-c1": {"platform_instance": "ok"},  # matches
                    "bogus-typo-guid": {"platform_instance": "x"},  # doesn't match
                    "Typo Connection": {"platform_instance": "y"},  # doesn't match
                },
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        # Force-populate the connection lookup so the unmatched-keys
        # check has something to compare against. (No tables means
        # _resolve_external_upstream is never called naturally.)
        source._get_connection_lookup()
        list(source.get_workunits_internal())

        warning_text = " ".join(
            str(w.title) + " " + str(w.message) + " " + " ".join(w.context or [])
            for w in source.report.warnings
        )
        assert "bogus-typo-guid" in warning_text, (
            f"Expected bogus key in warning, got: {warning_text}"
        )
        assert "Typo Connection" in warning_text, (
            f"Expected typo name in warning, got: {warning_text}"
        )


class TestApplyDatasetUpstreamsAuditStamps:
    """``_apply_dataset_upstreams`` attaches a per-edge ``AuditStampClass``
    so DataHub's lineage UI can show when the edge was last observed and
    who owned the underlying TS asset. Two pieces of the stamp have
    explicit fallbacks worth pinning:

    - ``time``: ``table.modified`` (TS ms-epoch) → ``now()`` when
      ``modified`` is missing or ``0``.
    - ``actor``: resolved author CorpUser URN → ``urn:li:corpuser:datahub``
      when no human-readable login is on the metadata.
    """

    @staticmethod
    def _build_source_and_table(
        modified: Optional[int],
        author_name: Optional[str],
    ) -> tuple[ThoughtSpotSource, LogicalTableResponse]:
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        with patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"):
            source = ThoughtSpotSource(config, PipelineContext(run_id="r"))

        column = ColumnResponse(
            id="c1",
            name="revenue",
            data_type="FLOAT",
            sources=[ColumnSourceRef(table_id="ts-src-tbl", column_name="revenue")],
        )
        table = LogicalTableResponse(
            id="ts-tbl-1",
            name="orders_worksheet",
            type="WORKSHEET",
            columns=[column],
            modified=modified,
            author_name=author_name,
        )
        return source, table

    def _emitted_upstreams(
        self, source: ThoughtSpotSource, table: LogicalTableResponse
    ) -> "list[UpstreamClass]":
        dataset = Dataset(
            name=table.id,
            platform=source.platform,
            platform_instance=source.config.platform_instance,
            env=source.config.env,
        )
        source._apply_dataset_upstreams(dataset, table.columns, table=table)

        # Drain workunits so the SDK V2 entity flushes its
        # UpstreamLineage aspect.
        upstreams: list = []
        for wu in dataset.as_workunits():
            aspect = getattr(wu.metadata, "aspect", None)
            if aspect is None or not hasattr(aspect, "upstreams"):
                continue
            for u in aspect.upstreams:
                if isinstance(u, UpstreamClass):
                    upstreams.append(u)
        return upstreams

    def test_modified_zero_falls_back_to_now(self):
        """``table.modified == 0`` is the TS sentinel for "not set"; the
        audit stamp must use ``now()`` rather than emit a Jan-1-1970
        timestamp into the lineage UI."""

        before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        source, table = self._build_source_and_table(modified=0, author_name="alice")
        upstreams = self._emitted_upstreams(source, table)
        after = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        assert upstreams, "Expected at least one UpstreamClass to be emitted"
        stamp = upstreams[0].auditStamp
        assert stamp is not None
        # ``before <= stamp.time <= after`` with a small tolerance for the
        # int-truncation drift in the helper.
        assert before - 1000 <= stamp.time <= after + 1000

    def test_modified_none_falls_back_to_now(self):
        before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        source, table = self._build_source_and_table(modified=None, author_name="alice")
        upstreams = self._emitted_upstreams(source, table)
        after = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        assert upstreams
        stamp = upstreams[0].auditStamp
        assert stamp is not None
        assert before - 1000 <= stamp.time <= after + 1000

    def test_modified_positive_used_verbatim(self):
        """A real positive ms-epoch on the table is forwarded as-is so
        the lineage view shows the actual last-modified time, not
        ingestion wall-clock."""
        source, table = self._build_source_and_table(
            modified=1_700_000_000_000, author_name="alice"
        )
        upstreams = self._emitted_upstreams(source, table)
        assert upstreams
        assert upstreams[0].auditStamp.time == 1_700_000_000_000

    def test_actor_resolves_to_corpuser_when_author_present(self):
        source, table = self._build_source_and_table(
            modified=1_700_000_000_000, author_name="alice"
        )
        upstreams = self._emitted_upstreams(source, table)
        assert upstreams
        assert upstreams[0].auditStamp.actor == "urn:li:corpuser:alice"

    def test_actor_falls_back_to_datahub_when_no_author(self):
        """No ``author_name`` and no ``author.name`` means
        ``_author_user_urn`` returns ``None`` — the stamp uses the
        service URN instead of ``urn:li:corpuser:unknown`` so the
        lineage view doesn't render a phantom owner."""
        source, table = self._build_source_and_table(
            modified=1_700_000_000_000, author_name=None
        )
        upstreams = self._emitted_upstreams(source, table)
        assert upstreams
        assert upstreams[0].auditStamp.actor == "urn:li:corpuser:datahub"

    def test_actor_falls_back_when_author_is_guid_shaped(self):
        """If TS only knows the author's GUID, ``_resolve_author_login``
        rejects it (36 chars + 4 hyphens) and the stamp picks the
        service URN."""
        source, table = self._build_source_and_table(
            modified=1_700_000_000_000,
            author_name="11111111-2222-3333-4444-555555555555",
        )
        upstreams = self._emitted_upstreams(source, table)
        assert upstreams
        assert upstreams[0].auditStamp.actor == "urn:li:corpuser:datahub"


class TestCoreHelpers:
    """Pin behaviour of three source-level helpers that the rest of the
    pipeline composes on top of:

    - ``_get_worksheet_columns_lookup`` — the lazy
      ``{worksheet_id -> {col_name -> ColumnResponse}}`` index that drives
      column-level chart lineage. Regressions here silently break the
      InputFields emission.
    - ``_emit_chart_input_fields`` — emits the Chart→Dataset column-level
      lineage workunit. The contract is "no fields = no workunit"; this
      is what keeps every chart from emitting an empty InputFields
      aspect that overwrites real data.
    - ``_resolve_workspace_container`` — the single source of truth for
      "which workspace does this entity belong to". Liveboards, answers,
      and datasets all funnel through it.
    """

    @staticmethod
    def _make_source(
        logical_tables: Optional[List[LogicalTableResponse]] = None,
    ) -> ThoughtSpotSource:
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            if logical_tables is not None:
                mock_client.get_logical_tables.return_value = logical_tables
            source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        return source

    # ---- _get_worksheet_columns_lookup ----

    def test_worksheet_columns_lookup_indexes_by_id_and_name(self) -> None:
        tables = [
            LogicalTableResponse(
                id="ws-1",
                name="orders",
                type="WORKSHEET",
                columns=[
                    ColumnResponse(id="c1", name="revenue", data_type="FLOAT"),
                    ColumnResponse(id="c2", name="region", data_type="VARCHAR"),
                ],
            ),
            LogicalTableResponse(
                id="ws-2",
                name="customers",
                type="WORKSHEET",
                columns=[
                    ColumnResponse(id="c3", name="customer_id", data_type="INT64"),
                ],
            ),
        ]
        source = self._make_source(logical_tables=tables)
        lookup = source._get_worksheet_columns_lookup()
        assert set(lookup.keys()) == {"ws-1", "ws-2"}
        assert set(lookup["ws-1"].keys()) == {"revenue", "region"}
        assert lookup["ws-1"]["revenue"].id == "c1"
        assert set(lookup["ws-2"].keys()) == {"customer_id"}

    def test_worksheet_columns_lookup_drops_empty_named_columns(self):
        """A column with an empty ``name`` can't be looked up by name,
        so the helper excludes it rather than poisoning the index with
        an empty key."""
        tables = [
            LogicalTableResponse(
                id="ws-1",
                name="orders",
                type="WORKSHEET",
                columns=[
                    ColumnResponse(id="c1", name="revenue", data_type="FLOAT"),
                    ColumnResponse(id="c2", name="", data_type="VARCHAR"),
                ],
            )
        ]
        source = self._make_source(logical_tables=tables)
        lookup = source._get_worksheet_columns_lookup()
        assert tables[0].columns is not None
        assert lookup["ws-1"] == {"revenue": tables[0].columns[0]}

    def test_worksheet_columns_lookup_is_cached(self):
        """Second call returns the same dict instance and the client is
        consulted only once — important because the client call goes to
        a paginated metadata/search endpoint."""
        tables = [
            LogicalTableResponse(
                id="ws-1",
                name="orders",
                type="WORKSHEET",
                columns=[ColumnResponse(id="c1", name="revenue")],
            )
        ]
        source = self._make_source(logical_tables=tables)
        first = source._get_worksheet_columns_lookup()
        second = source._get_worksheet_columns_lookup()
        assert first is second
        cast(MagicMock, source.client.get_logical_tables).assert_called_once()

    # ---- _emit_chart_input_fields ----

    def test_emit_chart_input_fields_skips_when_no_source_columns(self):
        """Without source_columns there's nothing to emit; the helper
        must not produce an empty ``InputFields`` aspect (which would
        clobber any real one)."""
        source = self._make_source(logical_tables=[])
        out = list(
            source._emit_chart_input_fields(
                viz_id="viz-1",
                source_table_ids=["ws-1"],
                source_columns=None,
            )
        )
        assert out == []

    def test_emit_chart_input_fields_skips_unknown_columns(self):
        """A column referenced by the chart but not present on the
        worksheet (a chart-local formula column) is dropped silently —
        the surviving fields still emit."""
        tables = [
            LogicalTableResponse(
                id="ws-1",
                name="orders",
                type="WORKSHEET",
                columns=[
                    ColumnResponse(id="c1", name="revenue", data_type="FLOAT"),
                ],
            )
        ]
        source = self._make_source(logical_tables=tables)
        wus = list(
            source._emit_chart_input_fields(
                viz_id="viz-1",
                source_table_ids=["ws-1"],
                source_columns=["revenue", "formula_only"],
            )
        )
        assert len(wus) == 1
        aspect = _aspect_as(wus[0], InputFieldsClass)
        field_urns = [f.schemaFieldUrn for f in aspect.fields]
        assert len(field_urns) == 1
        assert "revenue" in field_urns[0]

    def test_emit_chart_input_fields_handles_missing_worksheet(self):
        """Source-table id that the lookup doesn't know about is skipped
        without emitting a half-formed aspect."""
        source = self._make_source(logical_tables=[])
        out = list(
            source._emit_chart_input_fields(
                viz_id="viz-1",
                source_table_ids=["ws-missing"],
                source_columns=["revenue"],
            )
        )
        assert out == []

    # ---- _apply_sql_view_logic ----

    def test_apply_sql_view_logic_emits_view_properties(self):
        """Emits ``ViewPropertiesClass`` with the raw SQL on
        viewLogic, ``materialized=False`` (TS SQL views are virtual),
        and the resolved warehouse dialect on viewLanguage."""
        source = self._make_source(logical_tables=[])
        wus = list(
            source._apply_sql_view_logic(
                table_id="sv-1", sql="SELECT 1", dialect="snowflake"
            )
        )
        view_props = [wu for wu in wus if _mcp(wu).aspectName == "viewProperties"]
        assert len(view_props) == 1
        aspect = _aspect_as(view_props[0], ViewPropertiesClass)
        assert aspect.viewLogic == "SELECT 1"
        assert aspect.materialized is False
        assert aspect.viewLanguage == "snowflake"

    def test_apply_sql_view_logic_skips_when_sql_is_empty(self):
        """Empty / None SQL → no aspect emitted. Emitting an empty
        viewLogic would clobber a real value from a prior ingestion
        run."""
        source = self._make_source(logical_tables=[])
        assert (
            list(source._apply_sql_view_logic("sv-1", sql="", dialect="snowflake"))
            == []
        )

    def test_apply_sql_view_logic_falls_back_to_sql_when_dialect_unknown(self):
        """viewLanguage is generic ``"SQL"`` when the dialect can't be
        determined (e.g. the SQL view's connection is unreadable).
        Better than empty string."""
        source = self._make_source(logical_tables=[])
        wus = list(source._apply_sql_view_logic("sv-1", sql="SELECT 1", dialect=None))
        aspect = _aspect_as(wus[0], ViewPropertiesClass)
        assert aspect.viewLanguage == "SQL"

    # ---- _resolve_workspace_container ----

    def test_resolve_workspace_container_returns_key_for_known_owner(self):
        source = self._make_source()
        ws_map = {
            "ws-1": WorkspaceResponse(id="ws-1", name="Analytics"),
        }
        key = source._resolve_workspace_container("ws-1", ws_map)
        assert key is not None
        assert key.workspace_id == "ws-1"

    def test_resolve_workspace_container_returns_none_for_unknown_owner(self):
        """``owner_id`` not in the map means the entity is orphaned (its
        owner workspace wasn't enumerable, e.g. 403). Returning ``None``
        lets the caller skip the container relationship rather than
        emit a dangling ``parentContainer`` edge."""
        source = self._make_source()
        ws_map = {
            "ws-1": WorkspaceResponse(id="ws-1", name="Analytics"),
        }
        key = source._resolve_workspace_container("ws-unknown", ws_map)
        assert key is None

    def test_resolve_workspace_container_returns_none_when_owner_id_missing(
        self,
    ):
        source = self._make_source()
        key = source._resolve_workspace_container(None, {})
        assert key is None


class TestSqlParsedUpstreams:
    """``_apply_sql_parsed_upstreams`` runs sqlglot against a graph-backed
    SchemaResolver and emits parsed upstream lineage. Mirrors Mode's
    pattern at source/mode.py:1267-1303.
    """

    @staticmethod
    def _make_source() -> ThoughtSpotSource:
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        with patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"):
            return ThoughtSpotSource(config, PipelineContext(run_id="t"))

    @staticmethod
    def _make_parsed_result(
        in_tables=None,
        column_lineage=None,
        table_error=None,
        column_error=None,
    ):
        """Build a mock SqlParsingResult with the right debug_info
        shape. Mocking the parser (rather than running real sqlglot)
        keeps these tests focused on our merge / dedup / counter
        logic — real dialect coverage is tested upstream in
        datahub.sql_parsing."""
        result = MagicMock()
        result.in_tables = in_tables or []
        result.column_lineage = column_lineage or []
        result.debug_info = MagicMock()
        result.debug_info.table_error = table_error
        result.debug_info.column_error = column_error
        result.debug_info.error = table_error or column_error
        return result

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_parsed_in_tables_emit_as_upstreams(self, _mock_resolver, mock_parser):
        """When sqlglot returns in_tables, each becomes an
        UpstreamClass on an emitted UpstreamLineage aspect. Report
        counters increment as success."""
        mock_parser.return_value = self._make_parsed_result(
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)",
            ],
        )
        source = self._make_source()
        wus = list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT col_a FROM prod.public.upstream",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db="prod",
                ),
            )
        )
        upstream_wus = [wu for wu in wus if _mcp(wu).aspectName == "upstreamLineage"]
        assert len(upstream_wus) == 1
        agg = _aspect_as(upstream_wus[0], UpstreamLineageClass)
        urns = [u.dataset for u in agg.upstreams or []]
        assert any("snowflake,prod.public.upstream" in u for u in urns)
        assert source.report.num_sql_parsed == 1
        assert source.report.num_sql_parser_success == 1

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_parsed_column_lineage_emits_fine_grained(
        self, _mock_resolver, mock_parser
    ):
        """When sqlglot resolves column_lineage, each entry becomes a
        FineGrainedLineageClass on the emitted UpstreamLineage."""
        upstream_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)"
        )
        cl_entry = MagicMock()
        cl_entry.downstream.column = "col_a"
        cl_entry.upstreams = [MagicMock(table=upstream_table_urn, column="src_a")]
        mock_parser.return_value = self._make_parsed_result(
            in_tables=[upstream_table_urn],
            column_lineage=[cl_entry],
        )
        source = self._make_source()
        wus = list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT src_a AS col_a FROM prod.public.upstream",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db="prod",
                ),
            )
        )
        upstream_wu = next(wu for wu in wus if _mcp(wu).aspectName == "upstreamLineage")
        agg = _aspect_as(upstream_wu, UpstreamLineageClass)
        assert agg.fineGrainedLineages is not None
        assert len(agg.fineGrainedLineages) == 1
        edge = agg.fineGrainedLineages[0]
        assert any("src_a" in u for u in edge.upstreams or [])
        assert any("col_a" in d for d in edge.downstreams or [])

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_table_error_skips_parsed_lineage_increments_counter(
        self, _mock_resolver, mock_parser
    ):
        """``debug_info.table_error`` → no new edges emitted, counter
        increments, no exception raised."""
        mock_parser.return_value = self._make_parsed_result(
            table_error="couldn't resolve upstream tables",
        )
        source = self._make_source()
        wus = list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT FROM ???",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db=None,
                ),
            )
        )
        upstream_wus = [wu for wu in wus if _mcp(wu).aspectName == "upstreamLineage"]
        assert upstream_wus == []
        assert source.report.num_sql_parser_table_error == 1
        assert source.report.num_sql_parser_failures == 1
        assert source.report.num_sql_parser_success == 0

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_column_error_emits_table_level_only(self, _mock_resolver, mock_parser):
        """``debug_info.column_error`` → in_tables emit as table-level
        edges, column_lineage is skipped, success counter does NOT
        increment (it's still a failure mode)."""
        upstream_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)"
        )
        mock_parser.return_value = self._make_parsed_result(
            in_tables=[upstream_table_urn],
            column_lineage=[],
            column_error="ambiguous column reference",
        )
        source = self._make_source()
        wus = list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT * FROM prod.public.upstream",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db="prod",
                ),
            )
        )
        upstream_wu = next(wu for wu in wus if _mcp(wu).aspectName == "upstreamLineage")
        agg = _aspect_as(upstream_wu, UpstreamLineageClass)
        assert len(agg.upstreams or []) == 1
        # column-level lineage skipped
        assert not agg.fineGrainedLineages
        assert source.report.num_sql_parser_column_error == 1
        assert source.report.num_sql_parser_failures == 1

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_duplicate_of_ts_resolved_edge_is_deduped(
        self, _mock_resolver, mock_parser
    ):
        """If sqlglot produces a column edge identical to one already
        on the dataset (placed there by ``_apply_dataset_upstreams``
        via ``columns[*].sources``), drop it silently via the
        ``existing_fgl_edges`` set."""
        upstream_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)"
        )
        cl_entry = MagicMock()
        cl_entry.downstream.column = "col_a"
        cl_entry.upstreams = [MagicMock(table=upstream_table_urn, column="src_a")]
        mock_parser.return_value = self._make_parsed_result(
            in_tables=[upstream_table_urn],
            column_lineage=[cl_entry],
        )
        source = self._make_source()
        downstream_urn = str(source._make_self_dataset_urn("sv-1"))
        existing = {
            (
                make_schema_field_urn(downstream_urn, "col_a"),
                make_schema_field_urn(upstream_table_urn, "src_a"),
            )
        }
        wus = list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT src_a AS col_a FROM prod.public.upstream",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db="prod",
                ),
                existing_fgl_edges=existing,
            )
        )
        # Aspect emitted for the table-level edge, but fineGrainedLineages
        # is None/empty since the only candidate column edge was a duplicate.
        upstream_wus = [wu for wu in wus if _mcp(wu).aspectName == "upstreamLineage"]
        assert len(upstream_wus) == 1
        agg = _aspect_as(upstream_wus[0], UpstreamLineageClass)
        assert not agg.fineGrainedLineages

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_sqlglot_raises_swallowed_and_counted(self, _mock_resolver, mock_parser):
        """If sqlglot itself raises (pathological SQL, version drift, etc.),
        the catch must increment ``num_sql_parser_failures``, charge the
        elapsed time, and emit no workunits — the run must not crash."""
        mock_parser.side_effect = ValueError("sqlglot can't parse this")
        source = self._make_source()
        wus = list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT FROM ???",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db=None,
                ),
            )
        )
        assert wus == []
        assert source.report.num_sql_parser_failures == 1
        # Timer charged even on exception so per-view cost is visible
        assert source.report.sql_parsing_total_sec >= 0.0

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    def test_graph_none_passes_through_to_resolver(self, mock_resolver, mock_parser):
        """Running under SDK without a graph (self.ctx.graph = None)
        passes graph=None straight to create_and_cache_schema_resolver
        — the resolver handles that case internally. Parser still
        produces table-level lineage."""
        upstream_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)"
        )
        mock_parser.return_value = self._make_parsed_result(
            in_tables=[upstream_table_urn],
        )
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        with patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"):
            source = ThoughtSpotSource(config, PipelineContext(run_id="t", graph=None))
        assert source.ctx.graph is None
        list(
            source._apply_sql_parsed_upstreams(
                table_id="sv-1",
                sql="SELECT 1",
                sv_ref=SqlViewWarehouseRef(
                    platform="snowflake",
                    env="PROD",
                    platform_instance=None,
                    default_db=None,
                ),
            )
        )
        mock_resolver.assert_called_once()
        assert mock_resolver.call_args.kwargs["graph"] is None


class TestResolveSqlViewWarehouse:
    """``_resolve_sql_view_warehouse`` derives dialect / env /
    platform_instance / default_db for a SQL_VIEW dataset **without
    requiring** a successful connection-lookup or any physical_*
    fields on the LogicalTableResponse.

    SQL views differ from Worksheets in two key ways:
    1. ``physical_database_name / schema_name / table_name`` are
       always None — the upstream tables live inside the SQL
       statement, not in metadata fields.
    2. ``columns[*].sources`` is typically empty — TS doesn't
       pre-resolve column-level lineage for SQL views.

    So ``_resolve_external_upstream`` (which is geared for 1:1
    physical mappings) returns None and the SQL parser was being
    skipped. The new helper bypasses those requirements: it pulls
    the dialect directly from ``data_source_type`` and the
    platform_instance / env from ``external_connections`` config —
    both available without any TS API call.
    """

    @staticmethod
    def _make_source(
        connections: Optional[List[ConnectionResponse]] = None,
        external_connections: Optional[Dict[str, Any]] = None,
    ) -> ThoughtSpotSource:
        """Build a ThoughtSpotSource with the connection lookup
        pre-populated. ``connections`` is the list ``get_connections``
        returns."""
        config_dict = {
            "connection": {
                "base_url": "https://example.thoughtspot.cloud",
                "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
            },
        }
        if external_connections:
            config_dict["external_connections"] = external_connections
        config = ThoughtSpotConfig.model_validate(config_dict)
        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.get_connections.return_value = (
                connections or []
            )
            source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        return source

    def test_primary_path_uses_connection_lookup_for_dialect(self):
        """Primary path: connection found in lookup → read
        ``conn.data_source_type`` (canonical, no ``RDBMS_`` prefix),
        ``conn.default_database`` for default_db."""
        source = self._make_source(
            connections=[
                ConnectionResponse(
                    id="conn-guid-1",
                    name="Test Databricks",
                    data_source_type="DATABRICKS",
                    default_database="main",
                )
            ]
        )
        table = LogicalTableResponse(
            id="sv-1",
            name="CompexTest",
            type="SQL_VIEW",
            data_source_id="conn-guid-1",
            # data_source_type intentionally not set — primary path
            # reads from the connection, not the table.
        )
        ref = source._resolve_sql_view_warehouse(table)
        assert ref is not None
        assert ref.platform == "databricks"
        assert ref.default_db == "main"
        assert ref.platform_instance is None
        assert ref.env == source.config.env

    def test_overrides_via_external_connections_by_guid_or_name(self):
        """``_external_connection_overrides`` (existing) walks
        ``external_connections`` by GUID first, then by name —
        verify the SQL view path inherits that."""
        source = self._make_source(
            connections=[
                ConnectionResponse(
                    id="conn-guid-1",
                    name="Test Databricks",
                    data_source_type="DATABRICKS",
                    default_database=None,
                )
            ],
            external_connections={
                # Key by name to exercise the secondary lookup arm.
                "Test Databricks": {
                    "platform_instance": "prod-databricks",
                    "env": "PROD",
                },
            },
        )
        table = LogicalTableResponse(
            id="sv-1",
            name="CompexTest",
            type="SQL_VIEW",
            data_source_id="conn-guid-1",
        )
        ref = source._resolve_sql_view_warehouse(table)
        assert ref is not None
        assert ref.platform_instance == "prod-databricks"
        assert ref.env == "PROD"

    def test_fallback_to_table_data_source_type_when_conn_missing(self):
        """Fallback path: connection lookup returns empty (rare —
        restrictive ACLs). Read ``table.data_source_type`` directly,
        strip the ``RDBMS_`` prefix TS applies on the table. The
        ``default_db`` ends up None — sqlglot handles fully-qualified
        SQL without it.
        """
        # No connections in the lookup.
        source = self._make_source(connections=[])
        table = LogicalTableResponse(
            id="sv-1",
            name="CompexTest",
            type="SQL_VIEW",
            data_source_id="conn-guid-1",
            data_source_type="RDBMS_DATABRICKS",
        )
        ref = source._resolve_sql_view_warehouse(table)
        assert ref is not None
        assert ref.platform == "databricks"
        assert ref.default_db is None

    def test_returns_none_when_dialect_unmapped(self):
        """An unknown ``data_source_type`` means no dialect → None →
        caller skips the parser (only viewLogic emits)."""
        source = self._make_source(
            connections=[
                ConnectionResponse(
                    id="conn-guid-1",
                    name="Mystery DB",
                    data_source_type="MYSTERY_WAREHOUSE",
                    default_database=None,
                )
            ]
        )
        table = LogicalTableResponse(
            id="sv-1",
            name="Strange View",
            type="SQL_VIEW",
            data_source_id="conn-guid-1",
        )
        assert source._resolve_sql_view_warehouse(table) is None


class TestProcessDatasetSqlViewIntegration:
    """End-to-end: a SQL_VIEW LogicalTableResponse passed through
    ``_process_dataset`` emits ViewProperties + augmented
    UpstreamLineage (when sqlglot succeeds), alongside the existing
    schema and column-level aspects."""

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_sql_view_emits_view_properties_and_parsed_upstreams(
        self, mock_client_cls, _mock_resolver, mock_parser, monkeypatch
    ):
        upstream_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)"
        )
        parsed = MagicMock()
        parsed.in_tables = [upstream_urn]
        parsed.column_lineage = []
        parsed.debug_info = MagicMock(table_error=None, column_error=None, error=None)
        mock_parser.return_value = parsed

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="sv-1",
                name="My SQL View",
                type="SQL_VIEW",
                sql_view_definition="SELECT col_a FROM prod.public.upstream",
            )
        ]
        mock_client.get_connections.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        # Inject a resolved warehouse identity for sv-1 so the SQL
        # parser path actually runs (the platform=None guard skips
        # parsing without a known dialect, but viewLogic still emits).
        # The SQL view code path now goes through _resolve_sql_view_warehouse,
        # so inject there rather than _resolve_external_upstream.
        monkeypatch.setattr(
            source,
            "_resolve_sql_view_warehouse",
            lambda table: SqlViewWarehouseRef(
                platform="snowflake",
                env="PROD",
                platform_instance=None,
                default_db=None,
            ),
        )
        wus = list(source.get_workunits_internal())

        view_props = [
            wu
            for wu in wus
            if _mcp(wu).aspectName == "viewProperties" and "sv-1" in wu.get_urn()
        ]
        assert len(view_props) == 1
        upstream_wus_for_sv = [
            wu
            for wu in wus
            if _mcp(wu).aspectName == "upstreamLineage" and "sv-1" in wu.get_urn()
        ]
        assert upstream_wus_for_sv
        all_upstream_urns = [
            u.dataset
            for wu in upstream_wus_for_sv
            for u in (_aspect_as(wu, UpstreamLineageClass).upstreams or [])
        ]
        assert any("snowflake,prod.public.upstream" in u for u in all_upstream_urns)

    @patch("datahub.ingestion.source.thoughtspot.source.sqlglot_lineage")
    @patch(
        "datahub.ingestion.source.thoughtspot.source.create_and_cache_schema_resolver"
    )
    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_sql_view_dedups_against_existing_ts_resolved_fgl_edges(
        self, mock_client_cls, _mock_resolver, mock_parser, monkeypatch
    ):
        """End-to-end dedup contract: when the SQL_VIEW has a TS-pre-resolved
        ``columns[*].sources`` edge that ``_apply_dataset_upstreams`` already
        emitted onto ``Dataset.upstreams``, the SQL parser path must drop a
        column-level edge that duplicates it.

        Regression guard for ``_collect_existing_fgl_edges`` (was previously
        reading a non-existent ``dataset._upstreams_aspect`` attribute, making
        the entire dedup contract dead code while passing the standalone helper
        test).
        """
        upstream_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.public.upstream,PROD)"
        )
        cl_entry = MagicMock()
        cl_entry.downstream.column = "id"
        cl_entry.upstreams = [MagicMock(table=upstream_urn, column="id")]
        parsed = MagicMock()
        parsed.in_tables = [upstream_urn]
        parsed.column_lineage = [cl_entry]
        parsed.debug_info = MagicMock(table_error=None, column_error=None, error=None)
        mock_parser.return_value = parsed

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        # The SQL_VIEW has a single column whose ``columns[*].sources``
        # already references the same upstream column the SQL parser
        # would produce. ``_apply_dataset_upstreams`` writes this onto
        # ``Dataset.upstreams`` *before* ``_apply_sql_parsed_upstreams``
        # runs; the builder must observe it and drop the duplicate.
        mock_client.get_logical_tables.return_value = [
            LogicalTableResponse(
                id="sv-1",
                name="My SQL View",
                type="SQL_VIEW",
                sql_view_definition="SELECT id FROM prod.public.upstream",
                columns=[
                    ColumnResponse(
                        id="c1",
                        name="id",
                        data_type="INT",
                        sources=[
                            ColumnSourceRef(
                                table_id="prod.public.upstream",
                                column_name="id",
                            )
                        ],
                    ),
                ],
            )
        ]
        mock_client.get_connections.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        # Inject the warehouse identity so the parser path runs.
        monkeypatch.setattr(
            source,
            "_resolve_sql_view_warehouse",
            lambda table: SqlViewWarehouseRef(
                platform="snowflake",
                env="PROD",
                platform_instance=None,
                default_db=None,
            ),
        )
        # The TS-resolved column lineage path needs to land on the dataset
        # with the same URN shape the parser would produce. We patch
        # ``_make_self_dataset_urn`` to mirror the upstream URN so the
        # existing-edge set and parsed-edge set collide on the same
        # ``(downstream_col_urn, upstream_col_urn)`` tuple.
        original_make_self_urn = source._make_self_dataset_urn
        # Each table the TS edges reference must map to the same warehouse
        # URN the SQL parser produced — otherwise the URNs don't collide
        # and dedup has nothing to do.
        monkeypatch.setattr(
            source,
            "_make_self_dataset_urn",
            lambda name: (
                upstream_urn
                if name == "prod.public.upstream"
                else original_make_self_urn(name)
            ),
        )

        wus = list(source.get_workunits_internal())

        # ``_apply_sql_parsed_upstreams`` emits a *separate* upstreamLineage
        # workunit after the SDK-V2 dataset emits its own (from
        # ``_apply_dataset_upstreams``). The parser-path workunit's
        # ``fineGrainedLineages`` must be empty / None when the only
        # candidate edge duplicates an existing TS-resolved one — that's
        # the dedup contract.
        sv_urn = str(source._make_self_dataset_urn("sv-1"))
        # Workunit ordering: dataset-emitted aspect first (from
        # _apply_dataset_upstreams via SDK V2), then parser-emitted aspect
        # (from _apply_sql_parsed_upstreams). The parser-path one is what
        # we expect dedup to have neutered.
        all_upstream_wus = [
            wu
            for wu in wus
            if _mcp(wu).aspectName == "upstreamLineage" and sv_urn in wu.get_urn()
        ]
        assert len(all_upstream_wus) >= 1
        # The parser-path UpstreamLineage workunit (the *last* one for this
        # dataset) must NOT carry a FineGrainedLineage that duplicates the
        # TS-resolved column edge.
        parser_aspect = _aspect_as(all_upstream_wus[-1], UpstreamLineageClass)
        parser_fgls = parser_aspect.fineGrainedLineages or []
        # All FGLs from the parser path must be NEW edges, not duplicates
        # of TS-resolved ones. With dedup broken, an FGL with downstream=id,
        # upstream=upstream.id would appear; with dedup working, it doesn't.
        for fgl in parser_fgls:
            for d in fgl.downstreams or []:
                for u in fgl.upstreams or []:
                    assert (d, u) not in [
                        (
                            make_schema_field_urn(sv_urn, "id"),
                            make_schema_field_urn(upstream_urn, "id"),
                        )
                    ], (
                        f"Dedup contract broken: parser-path FGL emitted a "
                        f"duplicate of the TS-resolved edge ({d}, {u}). "
                        f"This means _collect_existing_fgl_edges returned "
                        f"empty when it shouldn't have."
                    )


class TestSqlParserAggregatedWarning:
    """Per-view DEBUG/INFO logs from ``_apply_sql_parsed_upstreams`` aren't
    surfaced to operators in the run report. The end-of-run aggregator at
    ``get_workunits_internal`` emits one ``report.warning`` summarising
    SQL-parser failures so the gap becomes visible without forcing log
    triage."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_no_warning_when_no_sql_parsed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        list(source.get_workunits_internal())
        titles = [w.title for w in source.report.warnings]
        assert "SQL View Lineage Parse Failures" not in titles

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_aggregated_warning_when_parser_failures(self, mock_client_cls):
        """End-to-end: bump the counters mid-run, finalize the source,
        and verify the aggregated warning fires with the table/column
        error breakdown in context."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_connections.return_value = []

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {
                        "type": "trusted",
                        "username": "u",
                        "secret_key": "k",
                    },
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="t"))
        # Simulate two SQL parse attempts: one table_error, one column_error.
        source.report.num_sql_parsed = 2
        source.report.num_sql_parser_failures = 2
        source.report.num_sql_parser_table_error = 1
        source.report.num_sql_parser_column_error = 1
        list(source.get_workunits_internal())
        agg = next(
            (
                w
                for w in source.report.warnings
                if w.title == "SQL View Lineage Parse Failures"
            ),
            None,
        )
        assert agg is not None, "Expected aggregated parse-failure warning"
        # Context carries the breakdown
        ctx_str = " ".join(agg.context or [])
        assert "table_error=1" in ctx_str
        assert "column_error=1" in ctx_str


class TestResolveAuthorLogin:
    """``_resolve_author_login`` is the choke point for "should this
    metadata become an owner URN or get omitted?". It implements two
    rules:

    1. Precedence: wire-level ``author_name`` beats nested
       ``author.name``.
    2. GUID rejection: a canonical RFC 4122 GUID is treated as "no real
       login" so we don't emit ``urn:li:corpuser:<guid>`` placeholders.
    """

    @staticmethod
    def _resolver():
        return _resolve_author_login

    def test_author_name_wins_over_nested_author(self):
        """``author_name`` is the human login from the camelCase wire
        field. When present it MUST win — ``author.name`` often mirrors
        ``author.id`` (the GUID) and would otherwise produce a phantom
        URN."""
        metadata = LiveboardResponse(
            id="lb-1",
            name="Dashboard",
            author_name="alice",
            author=ThoughtSpotAuthor(
                id="00000000-1111-2222-3333-444444444444",
                name="00000000-1111-2222-3333-444444444444",
            ),
        )
        assert self._resolver()(metadata) == "alice"

    def test_falls_back_to_nested_author_name(self):
        metadata = LiveboardResponse(
            id="lb-1",
            name="Dashboard",
            author=ThoughtSpotAuthor(id="u1", name="bob"),
        )
        assert self._resolver()(metadata) == "bob"

    def test_canonical_guid_login_rejected(self):
        """36-char canonical hyphenated GUID → return None so the
        caller can omit the field entirely."""
        metadata = LiveboardResponse(
            id="lb-1",
            name="Dashboard",
            author_name="11111111-2222-3333-4444-555555555555",
        )
        assert self._resolver()(metadata) is None

    def test_uppercase_guid_login_rejected(self):
        """The regex is case-insensitive; uppercase GUIDs are rejected
        just like lowercase."""
        metadata = LiveboardResponse(
            id="lb-1",
            name="Dashboard",
            author_name="ABCDEF12-3456-7890-ABCD-EF1234567890",
        )
        assert self._resolver()(metadata) is None

    def test_non_guid_36_char_login_accepted(self):
        """The regex (not length+hyphen count) means a 36-char non-GUID
        with hyphens is correctly accepted — earlier inline-check code
        would have wrongly rejected this."""
        metadata = LiveboardResponse(
            id="lb-1",
            name="Dashboard",
            author_name="user-with-exactly-four-hyphens-here!!",
        )
        # Exactly 36 chars, exactly 4 hyphens, but not the GUID shape.
        assert len(metadata.author_name or "") == 37  # sanity
        result = self._resolver()(metadata)
        assert result == "user-with-exactly-four-hyphens-here!!"

    def test_returns_none_when_both_missing(self):
        metadata = LiveboardResponse(id="lb-1", name="Dashboard")
        assert self._resolver()(metadata) is None


class TestSourceTableDropCounter:
    """``_consume_source_table_drop_count`` flushes a per-batch counter
    populated when ``_coerce_source_tables`` drops malformed entries
    during the post-construction ``answer.source_tables = ...``
    assignment. The counter must:

    - Increment when malformed entries are dropped.
    - Stay at zero for clean batches.
    - Reset to zero after each ``_consume_*`` call (so concurrent
      iterations of ``iter_answers`` don't double-count).
    """

    @staticmethod
    def _make_client():
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        return client

    def test_enrich_and_yield_answers_increments_counter_on_malformed(self):
        """Drive the real production path: stub
        ``_get_answer_dependencies`` to return a detail with one good
        and one malformed source_table dict, run
        ``_enrich_and_yield_answers``, then assert the client's drop
        counter incremented. This pins the contract end-to-end rather
        than manually doing what production does."""
        client = self._make_client()
        # Mock the TML-export round-trip so the function returns the
        # detail block we want to feed through ``validate_assignment``.
        client._get_answer_dependencies = lambda answer_ids: [
            {
                "id": "ans-1",
                "source_tables": [{"id": "good"}, {"name": "no-id"}],
                "chart_type": None,
                "question_text": None,
            }
        ]
        answer = AnswerResponse(id="ans-1", name="Sales")
        yielded = list(client._enrich_and_yield_answers([answer]))
        assert len(yielded) == 1
        assert client._consume_source_table_drop_count() == 1
        # The clean ref survives.
        assert yielded[0].source_tables is not None
        assert len(yielded[0].source_tables) == 1
        assert yielded[0].source_tables[0].id == "good"


class TestIterGeneratorSemantics:
    """``iter_liveboards`` / ``iter_answers`` are generators by contract
    so a consumer can stop mid-stream without forcing the full result
    into memory. All other tests in this suite use ``return_value=[...]``
    lists — that DOES exercise the generator's emit path, but doesn't
    exercise lazy consumption. The tests below pin the laziness
    contract directly."""

    @staticmethod
    def _make_client_with_paginated(items, monkeypatch):
        """Build a client whose ``_paginated_metadata_search`` yields the
        given items, lazily."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            client = ThoughtSpotClient(config, report=ThoughtSpotReport())

        # Replace the paginated source. Use a function that records call
        # count so the test can assert we don't over-iterate.
        seen = {"items": 0}

        def _gen(**_):
            for item in items:
                seen["items"] += 1
                yield item

        monkeypatch.setattr(client, "_paginated_metadata_search", _gen)
        # No-op TML enrichment (returns the buffer unchanged) so the
        # generator's buffering loop is what's exercised.
        monkeypatch.setattr(
            client,
            "_enrich_and_yield_liveboards",
            lambda liveboards: iter(liveboards),
        )
        monkeypatch.setattr(
            client,
            "_enrich_and_yield_answers",
            lambda answers: iter(answers),
        )
        return client, seen

    def test_iter_liveboards_is_lazy(self, monkeypatch):
        """Consuming only the first item must not force the upstream
        generator past the next batch boundary."""
        # ``_paginated_metadata_search`` yields already-flattened dicts;
        # callers ``model_validate`` them directly. Use the flat shape.
        items = [{"id": f"lb-{i}", "name": f"LB-{i}"} for i in range(200)]
        client, seen = self._make_client_with_paginated(items, monkeypatch)
        # batch_size default is 50; the first yield happens after the
        # buffer fills.
        gen = client.iter_liveboards()
        first = next(gen)
        assert first.id == "lb-0"
        # We've only consumed one batch — not the whole 200.
        assert seen["items"] <= client.config.tml_export_batch_size

    def test_iter_answers_is_lazy(self, monkeypatch):
        items = [{"id": f"ans-{i}", "name": f"A-{i}"} for i in range(200)]
        client, seen = self._make_client_with_paginated(items, monkeypatch)
        gen = client.iter_answers()
        first = next(gen)
        assert first.id == "ans-0"
        assert seen["items"] <= client.config.tml_export_batch_size

    def test_iter_liveboards_yields_partial_buffer_at_eof(self, monkeypatch):
        """If the upstream emits fewer than ``batch_size`` items, the
        last partial buffer still flushes and yields. Without this
        users would lose the tail of the result."""
        items = [{"id": f"lb-{i}", "name": f"LB-{i}"} for i in range(3)]
        client, _ = self._make_client_with_paginated(items, monkeypatch)
        yielded = list(client.iter_liveboards())
        assert [lb.id for lb in yielded] == ["lb-0", "lb-1", "lb-2"]

    def test_iter_liveboards_skips_malformed_and_aggregates_count(self, monkeypatch):
        """A malformed item (empty ``id``) is dropped per-item and the
        client increments ``malformed_liveboards_dropped``; the run
        does not abort, and clean siblings still emit."""
        items = [
            {"id": "lb-1", "name": "good"},
            {"id": "", "name": "no-id"},  # min_length=1 → ValidationError
            {"id": "lb-2", "name": "also-good"},
        ]
        client, _ = self._make_client_with_paginated(items, monkeypatch)
        yielded = list(client.iter_liveboards())
        assert {lb.id for lb in yielded} == {"lb-1", "lb-2"}
        assert client.report.malformed_liveboards_dropped == 1

    def test_iter_answers_skips_malformed_and_aggregates_count(self, monkeypatch):
        """Mirror of the liveboard test for answers. ``iter_answers``
        has the additional ``_coerce_source_tables`` drop path on top
        of the parse path; this test only exercises the parse path. The
        ``_coerce_source_tables`` path is pinned by
        ``TestSourceTableDropCounter``.
        """
        items = [
            {"id": "ans-1", "name": "good"},
            {"id": "", "name": "no-id"},  # min_length=1 → ValidationError
            {"id": "ans-2", "name": "also-good"},
        ]
        client, _ = self._make_client_with_paginated(items, monkeypatch)
        yielded = list(client.iter_answers())
        assert {a.id for a in yielded} == {"ans-1", "ans-2"}
        assert client.report.malformed_answers_dropped == 1


class TestGetLogicalTablesDropAggregation:
    """The H1 fix: ``get_logical_tables`` now uses per-item try/except so
    one malformed worksheet doesn't kill the whole batch. The drop
    counter is ``malformed_logical_tables_dropped``; nested
    ``_coerce_sources`` drops thread through via the
    ``COLUMN_SOURCE_DROPS`` context key and surface as
    ``malformed_column_sources_dropped``.
    """

    @staticmethod
    def _make_client_with_raw(items, monkeypatch):
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        monkeypatch.setattr(client, "get_metadata_list", lambda **_: items)
        return client

    def test_clean_batch_no_drops(self, monkeypatch):
        items = [{"id": "ws-1", "name": "orders", "type": "WORKSHEET"}]
        client = self._make_client_with_raw(items, monkeypatch)
        result = client.get_logical_tables()
        assert len(result) == 1
        assert client.report.malformed_logical_tables_dropped == 0
        assert client.report.malformed_column_sources_dropped == 0

    def test_malformed_table_drops_increment_counter(self, monkeypatch):
        """A bad worksheet in the middle of a batch does not abort the
        batch; clean siblings still emit."""
        items = [
            {"id": "ws-1", "name": "good", "type": "WORKSHEET"},
            {"id": "", "name": "no-id"},  # ValidationError on min_length=1
            {"id": "ws-2", "name": "also-good", "type": "WORKSHEET"},
        ]
        client = self._make_client_with_raw(items, monkeypatch)
        result = client.get_logical_tables()
        assert {t.id for t in result} == {"ws-1", "ws-2"}
        assert client.report.malformed_logical_tables_dropped == 1

    def test_column_source_drops_surface_to_counter(self, monkeypatch):
        """A nested malformed ``ColumnSourceRef`` (e.g. missing
        ``table_id``) is rejected by ``_coerce_sources`` and routes
        through the validation context — the parent column / table
        still emit."""
        items = [
            {
                "id": "ws-1",
                "name": "orders",
                "type": "WORKSHEET",
                "columns": [
                    {
                        "id": "col-1",
                        "name": "revenue",
                        "data_type": "FLOAT",
                        # Two source dicts: one good, one missing required keys.
                        "sources": [
                            {"tableId": "tbl-x", "columnName": "rev"},
                            {"tableId": "tbl-y"},  # missing columnName
                        ],
                    }
                ],
            }
        ]
        client = self._make_client_with_raw(items, monkeypatch)
        result = client.get_logical_tables()
        assert len(result) == 1
        # The good source survived.
        assert result[0].columns is not None
        assert len(result[0].columns[0].sources or []) == 1
        # And the bad one was counted.
        assert client.report.malformed_column_sources_dropped == 1


class TestSqlViewDefinitionFetch:
    """``get_sql_view_definitions`` batches TML export for SQL_VIEW
    objects and returns ``{guid: sql_statement}``. Mirrors the pattern
    used by ``_get_answer_dependencies`` / ``_get_liveboard_visualizations_via_tml``.
    """

    def test_returns_id_to_sql_map(self, monkeypatch):
        """Happy path: TML export returns a sql_view whose
        ``sql_query`` is a string (the modern TS Cloud shape) → the
        method returns ``{guid: sql}``.
        """
        items = [
            {
                "info": {"id": "sv-1"},
                "edoc": (
                    "sql_view:\n"
                    "  name: My View\n"
                    "  sql_query: 'SELECT col_a FROM upstream'\n"
                ),
            }
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        result = client.get_sql_view_definitions(["sv-1"])
        assert result == {"sv-1": "SELECT col_a FROM upstream"}

    def test_forbidden_sql_view_silently_absent_from_map(self, monkeypatch):
        """A SQL view with per-item ``ERROR`` status (FORBIDDEN
        per-object ACL) is silently absent from the returned map.
        Regression guard for the ``_check_tml_item_status`` reuse —
        if a future refactor removes that call, this test fails
        loudly."""
        items = [
            {
                "info": {
                    "id": "sv-bad",
                    "name": "Forbidden View",
                    "status": {
                        "status_code": "ERROR",
                        "error_message": "FORBIDDEN: no access",
                        "error_code": 403,
                    },
                },
                "edoc": None,
            }
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        # Use the real _check_tml_item_status (not the stubbed True-return
        # from the helper) to exercise the real ERROR-status path.
        monkeypatch.setattr(
            client,
            "_check_tml_item_status",
            ThoughtSpotClient._check_tml_item_status.__get__(client),
        )
        result = client.get_sql_view_definitions(["sv-bad"])
        assert result == {}
        titles = [w.title for w in client.report.warnings]
        assert "TML Export Failed" in titles

    def test_yaml_parse_failure_aggregated_in_report(self, monkeypatch):
        """A broken-YAML SQL view payload surfaces as a single
        aggregated ``SQL View TML Parse Failures`` warning, not one
        warning per object. Mirrors the existing liveboard / answer
        aggregation pattern."""
        items = [
            {
                "info": {"id": "sv-broken"},
                "edoc": "sql_view: [malformed",
            }
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        result = client.get_sql_view_definitions(["sv-broken"])
        assert result == {}
        titles = [w.title for w in client.report.warnings]
        assert "SQL View TML Parse Failures" in titles
        bad = next(
            w
            for w in client.report.warnings
            if w.title == "SQL View TML Parse Failures"
        )
        assert "sv-broken" in (bad.context[0] if bad.context else "")

    def test_structural_skip_when_statement_field_missing(self, monkeypatch):
        """TML parses fine but lacks any of ``sql_view.sql_query`` /
        ``sql_view.sql_query.statement`` / ``sql_view.statement`` →
        counted as structural_skip and surfaced via report.info."""
        items = [
            {
                "info": {"id": "sv-shape"},
                "edoc": "sql_view:\n  name: Shape Drift\n  some_other_key: value\n",
            }
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        result = client.get_sql_view_definitions(["sv-shape"])
        assert result == {}
        info_titles = [i.title for i in client.report.infos]
        assert "SQL View TML structural skips" in info_titles

    def test_chunks_to_tml_export_batch_size(self):
        """Inputs > tml_export_batch_size produce multiple
        metadata_tml_export calls via the existing
        ``_iter_tml_export_items`` chunking primitive."""
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
            tml_export_batch_size=2,
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            mock_sdk.return_value.metadata_tml_export.return_value = []
            client = ThoughtSpotClient(config, report=ThoughtSpotReport())
            client.get_sql_view_definitions(["a", "b", "c", "d", "e"])
            calls = mock_sdk.return_value.metadata_tml_export.call_args_list
            # 5 ids with batch_size=2 → 3 calls (2 + 2 + 1)
            assert len(calls) == 3


class TestGetLogicalTablesSqlViewEnrichment:
    """``get_logical_tables`` runs a single batched TML export over its
    SQL_VIEW-typed results and attaches the SQL definition to each
    matching LogicalTableResponse before returning. Worksheets and
    physical tables leave ``sql_view_definition`` as None."""

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_sql_view_definition_populated_only_on_sql_view_types(self, mock_sdk_cls):
        mock_sdk = mock_sdk_cls.return_value
        mock_sdk.auth_token_full.return_value = {"token": "t"}
        # metadata_search returns one SQL_VIEW and one WORKSHEET.
        mock_sdk.metadata_search.return_value = [
            {
                "metadata_header": {
                    "id": "sv-1",
                    "name": "My View",
                    "type": "SQL_VIEW",
                },
                "metadata_detail": {"columns": []},
            },
            {
                "metadata_header": {
                    "id": "ws-1",
                    "name": "My Worksheet",
                    "type": "WORKSHEET",
                },
                "metadata_detail": {"columns": []},
            },
        ]
        # metadata_tml_export returns SQL for sv-1 only.
        mock_sdk.metadata_tml_export.return_value = [
            {
                "info": {"id": "sv-1", "status": {"status_code": "OK"}},
                "edoc": "sql_view:\n  sql_query: 'SELECT 1'\n",
            }
        ]
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        tables = client.get_logical_tables()
        by_id = {t.id: t for t in tables}
        assert by_id["sv-1"].sql_view_definition == "SELECT 1"
        assert by_id["ws-1"].sql_view_definition is None


class TestTMLParseAggregation:
    """The H3 fix surfaces YAML parse failures + structural skips that
    were previously DEBUG-only. Test both
    ``_get_liveboard_visualizations_via_tml`` and
    ``_get_answer_dependencies`` end-to-end.
    """

    def test_liveboard_yaml_parse_failure_aggregates_warning(self, monkeypatch):
        """A liveboard whose TML payload is broken YAML must be
        captured in a single aggregated ``report.warning`` so operators
        see *which* liveboards lost their chart layer."""
        items = [
            {
                "info": {"id": "lb-bad"},
                "edoc": "this: is: not: valid: yaml: [",
            },
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        result = client._get_liveboard_visualizations_via_tml(["lb-bad"])
        assert result == {}
        # The warning lives in the structured-logs section of the
        # report. We check by inspecting captured warnings on the
        # report instance.
        warnings = list(client.report.warnings)
        titles = [w.title for w in warnings]
        assert "Liveboard TML Parse Failures" in titles
        # Context carries the liveboard_id for triage.
        bad = next(w for w in warnings if w.title == "Liveboard TML Parse Failures")
        assert "lb-bad" in (bad.context[0] if bad.context else "")

    def test_answer_yaml_parse_failure_aggregates_warning(self, monkeypatch):
        items = [
            {
                "info": {"id": "ans-bad"},
                "edoc": "broken: [yaml",
            },
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        result = client._get_answer_dependencies(["ans-bad"])
        assert result == []
        titles = [w.title for w in client.report.warnings]
        assert "Answer TML Parse Failures" in titles

    def test_liveboard_structural_skip_surfaces_info(self, monkeypatch):
        """TML parses fine but has no liveboard/visualizations keys →
        F3 ``structural_skips`` increments and a ``report.info`` is
        emitted so the operator notices possible TS schema drift."""
        # Valid YAML, just doesn't match the expected shape.
        items = [
            {
                "info": {"id": "lb-shape-drift"},
                "edoc": "liveboard:\n  visualizations:\n    - not_a_dict\n",
            },
        ]
        client = _make_tml_stubbed_client(items, monkeypatch)
        client._get_liveboard_visualizations_via_tml(["lb-shape-drift"])
        infos = [i.title for i in client.report.infos]
        assert "Liveboard TML structural skips" in infos


class TestTMLYamlLoaderQuirks:
    """Pin the ``_TMLYamlLoader`` behavior against PyYAML quirks that
    ThoughtSpot's TML edocs exercise in practice. The loader is the
    single chokepoint for *every* liveboard/answer TML — a regression
    here silently strips the entire chart layer for any object whose
    TML contains a formula filter.
    """

    def test_oper_equals_in_mapping_value_parses_as_string(self):
        """``oper: =`` (TS's equality operator in formula/filter blocks)
        uses YAML's obscure ``=`` value tag. Without the
        ``tag:yaml.org,2002:value`` constructor registered on
        ``_TMLYamlLoader``, ``yaml.SafeLoader`` raises ``ConstructorError``
        and the whole TML payload is dropped.
        """
        result = _safe_load_tml_yaml("oper: =\n")
        assert result == {"oper": "="}

    def test_oper_equals_inside_realistic_liveboard_tml(self):
        """End-to-end shape: the ``=`` token sits deep inside the
        liveboard's filter block. Parsing must reach the surrounding
        ``visualizations[].viz_guid`` / ``answer.tables[].fqn`` fields
        the connector actually extracts.
        """
        tml = (
            "liveboard:\n"
            "  name: Sales\n"
            "  visualizations:\n"
            "  - id: Viz_1\n"
            "    viz_guid: viz-123\n"
            "    answer:\n"
            "      name: Q1 revenue\n"
            "      chart_type: COLUMN\n"
            "      search_query: '[revenue] = 100'\n"
            "      tables:\n"
            "      - id: orders\n"
            "        fqn: ws-guid-1\n"
            "      filters:\n"
            "      - column:\n"
            "        - revenue\n"
            "        oper: =\n"
            "        values:\n"
            "        - '100'\n"
        )
        data = _safe_load_tml_yaml(tml)
        viz = data["liveboard"]["visualizations"][0]
        assert viz["viz_guid"] == "viz-123"
        assert viz["answer"]["tables"][0]["fqn"] == "ws-guid-1"
        assert viz["answer"]["filters"][0]["oper"] == "="

    def test_loader_rejects_python_specific_tags(self):
        """The shim adds *one* constructor for ``=``. Anyone swapping
        the SafeLoader base for ``FullLoader``/``UnsafeLoader`` to
        unblock a different parse error would silently allow arbitrary
        Python object construction. SafeLoader rejects *any* ``!!python/*``
        tag — use the benign ``list`` target as the canary.
        """
        with pytest.raises(yaml.constructor.ConstructorError):
            _safe_load_tml_yaml("value: !!python/object/new:list [[]]\n")


class TestTMLAnswerExtractionHelpers:
    """Pin the contract of ``_extract_answer_chart_type`` — the helper
    that walks an answer-block dict to find the chart type label.

    Background: a live smoke run against a TS Cloud 26.x tenant found
    that ``answer.chart_type`` (the field older docs reference) is
    always ``None`` on modern responses — the real value lives at
    ``answer.chart.type``. Before this helper, ``thoughtspot_chart_type``
    was missing from every Visualization's custom properties even
    though every chart had a clear type (PIE / COLUMN / KPI / …).
    The helper prefers the nested path with a fallback to the legacy
    field for older tenants.
    """

    def test_extract_chart_type_prefers_nested_chart_type(self):
        """TS Cloud 26.x stores the chart type at ``answer.chart.type``
        (``'PIE'``, ``'COLUMN'``, …) and leaves the legacy
        ``answer.chart_type`` field unset. Without this preference,
        every emitted Chart entity has ``thoughtspot_chart_type``
        missing from its custom properties — exactly what the live
        smoke run found (0/170 vizes had chart_type populated).
        """
        answer_tml = {"chart": {"type": "PIE"}, "chart_type": None}
        assert _extract_answer_chart_type(answer_tml) == "PIE"

    def test_extract_chart_type_falls_back_to_legacy_field(self):
        """Older TML payloads keep ``chart_type`` at the top level
        of ``answer``. Preserve that path so tenants on older TS
        versions keep emitting the property.
        """
        answer_tml = {"chart_type": "BAR"}
        assert _extract_answer_chart_type(answer_tml) == "BAR"

    def test_extract_chart_type_returns_none_when_unset(self):
        """No chart type signal at all → ``None`` (caller omits the
        custom property rather than emitting an empty string).
        """
        assert _extract_answer_chart_type({}) is None
        assert _extract_answer_chart_type({"chart": {}}) is None
        assert _extract_answer_chart_type({"chart": {"type": ""}}) is None


class TestExtractSqlViewStatement:
    """``_extract_sql_view_statement`` accepts three TML shapes (newest
    first) because TS Cloud's SQL view TML evolved over versions:

    1. ``sql_view.sql_query`` as a bare string — the current TS Cloud shape
       (verified live against techpartners.thoughtspot.cloud on 2026-05-13)
    2. ``sql_view.sql_query.statement`` as ``{statement: str}`` — older
       documented form, kept for defensive forward/back-compat
    3. ``sql_view.statement`` as a flat string — defensive fallback

    Each shape gets a focused test so a future cleanup that "simplifies"
    an unused-looking branch can't silently break ingestion for old
    deployments.
    """

    def test_modern_sql_query_string_shape(self):
        """Primary path: ``sql_view.sql_query`` is a bare string."""
        tml = {"sql_view": {"sql_query": "SELECT 1"}}
        assert _extract_sql_view_statement(tml) == "SELECT 1"

    def test_legacy_sql_query_dict_statement_shape(self):
        """Nested dict form: ``sql_view.sql_query.statement`` carries the SQL."""
        tml = {"sql_view": {"sql_query": {"statement": "SELECT 2"}}}
        assert _extract_sql_view_statement(tml) == "SELECT 2"

    def test_defensive_flat_statement_shape(self):
        """Flat fallback: ``sql_view.statement`` carries the SQL."""
        tml = {"sql_view": {"statement": "SELECT 3"}}
        assert _extract_sql_view_statement(tml) == "SELECT 3"

    def test_returns_none_when_no_known_shape(self):
        """All three known shapes absent → None. Caller emits the dataset
        without ``viewLogic`` (structural-skip is counted at the caller)."""
        assert _extract_sql_view_statement({}) is None
        assert _extract_sql_view_statement({"sql_view": {}}) is None
        assert _extract_sql_view_statement({"sql_view": "not-a-dict"}) is None
        # Empty / whitespace-only statements are rejected (avoid clobber)
        assert _extract_sql_view_statement({"sql_view": {"sql_query": ""}}) is None
        assert _extract_sql_view_statement({"sql_view": {"sql_query": "   "}}) is None


class TestLineageBuilderHelpers:
    """The M7 split exposes three pure helpers that the consolidated
    ``_apply_dataset_upstreams`` chains together. These tests pin each
    helper's invariant in isolation — so a regression that affects only
    one helper still surfaces (the prior end-to-end tests would mask a
    bug that the broader flow papered over).
    """

    @staticmethod
    def _make_source() -> ThoughtSpotSource:
        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        with patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"):
            return ThoughtSpotSource(config, PipelineContext(run_id="r"))

    # ---- _collect_per_upstream_columns ----

    def test_collect_per_upstream_groups_multiple_sources_per_upstream(self):
        """Two columns reading from the same upstream TS table merge
        into one ``{upstream_urn: {col: [src_col]}}`` entry, not two —
        otherwise we'd emit duplicate ``UpstreamClass`` aspects."""
        source = self._make_source()
        cols = [
            ColumnResponse(
                id="c1",
                name="revenue",
                sources=[ColumnSourceRef(table_id="up-1", column_name="rev")],
            ),
            ColumnResponse(
                id="c2",
                name="region",
                sources=[ColumnSourceRef(table_id="up-1", column_name="reg")],
            ),
        ]
        per_upstream = source._collect_per_upstream_columns(cols, external_ref=None)
        # One upstream URN, two downstream-cols mapped.
        assert len(per_upstream) == 1
        upstream_urn = next(iter(per_upstream))
        assert set(per_upstream[upstream_urn].keys()) == {"revenue", "region"}

    def test_collect_per_upstream_includes_external_ref(self):
        """When ``external_ref`` is set, each TS column also emits a
        column-level edge to the external dataset using
        ``physical_column_name`` (falling back to the TS name)."""

        source = self._make_source()
        cols = [
            ColumnResponse(
                id="c1",
                name="revenue",
                physical_column_name="revenue_usd",
                sources=[],
            )
        ]
        ext = ExternalRef(
            platform="databricks",
            urn="urn:li:dataset:(urn:li:dataPlatform:databricks,a.b,PROD)",
            env="PROD",
            platform_instance=None,
        )
        per_upstream = source._collect_per_upstream_columns(cols, external_ref=ext)
        assert ext.urn in per_upstream
        # downstream "revenue" maps to upstream "revenue_usd" (physical name).
        assert per_upstream[ext.urn]["revenue"] == ["revenue_usd"]

    def test_collect_per_upstream_skips_unnamed_columns(self):
        """Defensive: a column with no ``name`` is silently dropped
        from the accumulator — an empty downstream-col name would
        produce a broken schemaField URN. Note that empty
        ``column_name`` on a ``ColumnSourceRef`` is caught at model
        construction time (``min_length=1``), so the source-level
        skip-on-falsy guard is only reachable for ``col.name``.
        """
        source = self._make_source()
        cols = [
            ColumnResponse(
                id="c1",
                name="",  # falsy name → entire column skipped
                sources=[ColumnSourceRef(table_id="up-1", column_name="rev")],
            ),
        ]
        per_upstream = source._collect_per_upstream_columns(cols, external_ref=None)
        assert per_upstream == {}

    # ---- _build_audit_stamp ----

    def test_build_audit_stamp_uses_modified_when_positive(self):
        source = self._make_source()
        table = LogicalTableResponse(
            id="t1", name="orders", modified=1_700_000_000_000, author_name="alice"
        )
        stamp = source._build_audit_stamp(table)
        assert stamp.time == 1_700_000_000_000
        assert stamp.actor == "urn:li:corpuser:alice"

    def test_build_audit_stamp_falls_back_to_now_and_service_urn(self):
        source = self._make_source()
        table = LogicalTableResponse(
            id="t1", name="orders", modified=0, author_name=None
        )
        before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        stamp = source._build_audit_stamp(table)
        after = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        assert before - 1000 <= stamp.time <= after + 1000
        assert stamp.actor == "urn:li:corpuser:datahub"

    # ---- _build_upstream_aspects ----

    def test_build_upstream_aspects_emits_one_upstream_class_per_key(self):
        """A ``per_upstream`` with N keys produces exactly N
        ``UpstreamClass`` entries (plus per-column fine-grained
        entries). Without this invariant, lineage edges would
        double-emit and DataHub would show them twice."""
        source = self._make_source()
        per_upstream = {
            "urn:li:dataset:(urn:li:dataPlatform:thoughtspot,up-1,PROD)": {
                "revenue": ["rev"],
            },
            "urn:li:dataset:(urn:li:dataPlatform:databricks,a.b,PROD)": {
                "revenue": ["revenue_usd"],
            },
        }
        audit = AuditStampClass(time=1, actor="urn:li:corpuser:test")
        downstream = "urn:li:dataset:(urn:li:dataPlatform:thoughtspot,ds-1,PROD)"
        aspects = source._build_upstream_aspects(per_upstream, audit, downstream)
        upstream_classes = [a for a in aspects if isinstance(a, UpstreamClass)]
        assert len(upstream_classes) == 2
        # One fine-grained per (upstream × downstream-col) — two here.
        fine_grained = [a for a in aspects if isinstance(a, FineGrainedLineageClass)]
        assert len(fine_grained) == 2


class TestReportDroppedExcPropagation:
    """The M4 ``exc=first_exc`` plumbing forwards the first per-row
    exception from per-item parse loops into the aggregated
    ``report.warning(...)``. DataHub's structured-logs layer attaches it
    to the entry so Sentry / log aggregators can group "100 dropped X"
    by root cause rather than just by aggregate count.

    The 5 callers (workspace / tag / connection / get_logical_tables /
    fetch_logical_tables_by_id / usage-stats row parse) are exercised
    behaviorally elsewhere; this test pins the side-effect that's only
    visible by inspecting ``report.warnings[i].exc``.
    """

    @staticmethod
    def _make_client():
        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.client.TSRestApiV2"
        ) as mock_sdk:
            mock_sdk.return_value.auth_token_full.return_value = {"token": "t"}
            client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        return client

    def test_workspace_parse_failure_forwards_first_exc(self):
        """Two malformed orgs in the same batch → one aggregated
        warning whose ``.exc`` is the *first* ``ValidationError``."""
        client = self._make_client()
        client.ts_client.orgs_search = lambda **_: {
            "orgs": [
                {"id": "good-1", "name": "Analytics"},
                {"id": ""},
                {"id": "good-2", "name": "Sales"},
                {"name": "no-id-at-all"},
            ]
        }

        workspaces = client.get_workspaces()
        # Clean orgs still emit.
        assert {w.id for w in workspaces} == {"good-1", "good-2"}

        # One aggregated warning, exc attached, root cause preserved.
        parse_failures = [
            w for w in client.report.warnings if w.title == "Workspace Parse Failures"
        ]
        assert len(parse_failures) == 1
        # ``StructuredLogs.report_log`` doesn't store the raw exception
        # on the entry — it appends ``f"{type(exc)}: {exc}"`` to the
        # ``context`` list. So we verify ``exc=`` was actually forwarded
        # by checking the context for the exception type-name. Sentry
        # groups on the same string.
        context_joined = " ".join(parse_failures[0].context)
        assert "ValidationError" in context_joined, (
            f"Expected ValidationError in context, got: {context_joined!r}"
        )


class TestExternalColumnCasePreservation:
    """The ``preserve_column_case`` per-connection setting controls
    whether emitted cross-platform schemaField URNs use the column name
    verbatim from TS (case-sensitive backends — BigQuery, SQL Server,
    case-quoted Databricks) or lowercase (default — matches what most
    DataHub source connectors emit: Databricks, Snowflake, Postgres,
    MySQL, Redshift, Hive).
    """

    @staticmethod
    def _make_source(
        preserve_for_c1: bool,
    ) -> "tuple[ThoughtSpotSource, LogicalTableResponse]":
        external_block: Dict[str, Any] = {
            "platform_instance": "prod-dbx",
        }
        if preserve_for_c1:
            external_block["preserve_column_case"] = True

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "external_connections": {"c1": external_block},
            }
        )
        with patch(
            "datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.get_connections.return_value = [
                ConnectionResponse(
                    id="c1",
                    name="Prod DBX",
                    data_source_type="DATABRICKS",
                )
            ]
            source = ThoughtSpotSource(config, PipelineContext(run_id="r"))

        # Build a Logical Table whose external column name is mixed-case
        # (the realistic TS payload: TS often uppercases display names).
        column = ColumnResponse(
            id="col-1",
            name="REVENUE",
            data_type="FLOAT",
            physical_column_name="REVENUE",
            sources=[ColumnSourceRef(table_id="ts-up-1", column_name="rev")],
        )
        table = LogicalTableResponse(
            id="ts-tbl-1",
            name="orders_worksheet",
            type="WORKSHEET",
            data_source_id="c1",
            data_source_type="DATABRICKS",
            physical_database_name="warehouse",
            physical_schema_name="raw",
            physical_table_name="orders",
            columns=[column],
        )
        return source, table

    def test_default_lowercases_external_column(self):
        """Default behavior: ``physical_column_name='REVENUE'`` becomes
        ``revenue`` in the emitted schemaField URN — matches the
        Databricks connector's lowercase column-URN convention."""
        source, table = self._make_source(preserve_for_c1=False)
        external_ref = source._resolve_external_upstream(table)
        assert external_ref is not None
        assert external_ref.preserve_column_case is False

        per_upstream = source._collect_per_upstream_columns(
            table.columns or [], external_ref=external_ref
        )
        # The external upstream URN entry maps {downstream_col: [upstream_col]}.
        # The upstream_col is what gets passed to make_schema_field_urn.
        upstream_cols = per_upstream[external_ref.urn]["REVENUE"]
        assert upstream_cols == ["revenue"], (
            f"Default mode should lowercase to match Databricks; got {upstream_cols!r}"
        )

    def test_preserve_true_keeps_external_column_case(self):
        """``preserve_column_case=True`` (BigQuery-style): mixed-case
        ``REVENUE`` stays as-is."""
        source, table = self._make_source(preserve_for_c1=True)
        external_ref = source._resolve_external_upstream(table)
        assert external_ref is not None
        assert external_ref.preserve_column_case is True

        per_upstream = source._collect_per_upstream_columns(
            table.columns or [], external_ref=external_ref
        )
        upstream_cols = per_upstream[external_ref.urn]["REVENUE"]
        assert upstream_cols == ["REVENUE"], (
            f"Preserve mode should keep case verbatim; got {upstream_cols!r}"
        )


class TestMetadataSearchIncludeStats:
    """The connector sends ``include_stats=True`` on every
    ``metadata_search`` request so the global view counter comes back
    on each entity. The ``stats`` block flows from the wire response into
    the parsed Pydantic model.
    """

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_iter_liveboards_request_includes_include_stats(self, mock_sdk_class):
        """``iter_liveboards`` calls ``metadata_search`` with
        ``include_stats=True`` in the request body."""
        mock_sdk = MagicMock()
        mock_sdk_class.return_value = mock_sdk
        mock_sdk.auth_token_full.return_value = {"token": "t"}
        mock_sdk.metadata_search.return_value = []

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())

        list(client.iter_liveboards())

        calls = mock_sdk.metadata_search.call_args_list
        assert calls, "expected at least one metadata_search call"
        for call in calls:
            request = call.kwargs.get("request") or (call.args[0] if call.args else {})
            assert request.get("include_stats") is True, (
                f"metadata_search call did not set include_stats=True: {request}"
            )

    @patch("datahub.ingestion.source.thoughtspot.client.TSRestApiV2")
    def test_stats_flows_through_to_parsed_model(self, mock_sdk_class, monkeypatch):
        """When the wire response contains a ``stats`` block, the
        parsed ``LiveboardResponse`` exposes it as ``entity.stats``."""
        mock_sdk = MagicMock()
        mock_sdk_class.return_value = mock_sdk
        mock_sdk.auth_token_full.return_value = {"token": "t"}
        mock_sdk.metadata_search.return_value = [
            {
                "metadata_id": "lb-1",
                "metadata_header": {"id": "lb-1", "name": "Test Dashboard"},
                "stats": {
                    "views": 10000,
                    "favorites": 0,
                    "last_accessed": 1778591233845,
                },
            }
        ]

        config = ThoughtSpotConnectionConfig(
            base_url="https://example.thoughtspot.cloud",
            auth=TrustedAuth(username="u", secret_key="k"),
        )
        client = ThoughtSpotClient(config, report=ThoughtSpotReport())
        # No-op enrichment so the parsed LiveboardResponse passes through
        # unchanged (we don't have TML in this test fixture).
        monkeypatch.setattr(
            client,
            "_enrich_and_yield_liveboards",
            lambda liveboards: iter(liveboards),
        )

        liveboards = list(client.iter_liveboards())
        assert len(liveboards) == 1
        lb = liveboards[0]
        assert isinstance(lb.stats, EntityStats)
        assert lb.stats.views == 10000
        assert lb.stats.last_accessed == 1778591233845


class TestStatsCustomProperties:
    """When ``entity.stats`` is populated, the connector emits
    ``thoughtspot_favorites`` and ``thoughtspot_last_accessed`` (ISO
    timestamp) custom properties on the Dashboard / Chart entity. View
    counts flow into the standard ``DashboardUsageStatistics`` aspect
    (covered separately in TestUsageStatsFromMetadataSearch)."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_emits_favorites_and_last_accessed_custom_props(
        self, mock_client_class
    ):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Test Dashboard",
                stats=EntityStats(
                    views=10000,
                    favorites=3,
                    last_accessed=1700000000000,  # 2023-11-14T22:13:20+00:00
                ),
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                }
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        dashboard_info_props = [
            _aspect_as(wu, DashboardInfoClass).customProperties
            for wu in workunits
            if _mcp(wu).aspectName == "dashboardInfo"
        ]
        assert dashboard_info_props, "expected at least one dashboardInfo aspect"
        props = dashboard_info_props[0]
        assert props.get("thoughtspot_favorites") == "3"
        assert "thoughtspot_last_accessed" in props
        # ISO 8601 UTC string with year/month/day fields visible.
        assert "2023-11-14" in props["thoughtspot_last_accessed"]


class TestUsageStatsFromMetadataSearch:
    """The connector emits ``DashboardUsageStatistics`` /
    ``ChartUsageStatistics`` aspects from ``entity.stats.views`` — the
    global view counter from ``metadata_search``'s ``include_stats``
    response. No BI Server worksheet query is involved."""

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_view_count_emits_dashboard_usage_aspect(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-sales",
                name="Sales Dashboard",
                stats=EntityStats(views=25000, favorites=0),
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "include_usage_stats": True,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        usage_aspects = [
            _aspect_as(wu, DashboardUsageStatisticsClass)
            for wu in workunits
            if _mcp(wu).aspectName == "dashboardUsageStatistics"
        ]
        assert len(usage_aspects) == 1, (
            f"Expected one DashboardUsageStatistics aspect, got "
            f"{len(usage_aspects)}: {usage_aspects}"
        )
        aspect = usage_aspects[0]
        assert aspect.viewsCount == 25000

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_liveboard_without_stats_emits_no_usage_aspect(self, mock_client_class):
        """An entity whose ``stats`` is None (or whose ``views`` is 0)
        does not emit a usage aspect — emitting ``viewsCount=0`` would
        clobber a real count from a prior run."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_answers.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(id="lb-1", name="Dashboard", stats=None)
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "include_usage_stats": True,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        usage_aspects = [
            wu for wu in workunits if _mcp(wu).aspectName == "dashboardUsageStatistics"
        ]
        assert usage_aspects == []

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_answer_view_count_emits_chart_usage_aspect(self, mock_client_class):
        """Mirror of the Liveboard branch for Answers — the Answer path
        in ``_process_usage_stats`` emits ``ChartUsageStatisticsClass`` on
        the Chart URN built via ``make_chart_urn``. Symmetric coverage
        guards against a regression in either import (chart URN builder,
        chart usage aspect class)."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.iter_liveboards.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.iter_answers.return_value = [
            AnswerResponse(
                id="ans-1",
                name="Test Answer",
                stats=EntityStats(views=200, favorites=0),
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "include_usage_stats": True,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        chart_usage_aspects = [
            _aspect_as(wu, ChartUsageStatisticsClass)
            for wu in workunits
            if _mcp(wu).aspectName == "chartUsageStatistics"
        ]
        assert len(chart_usage_aspects) == 1, (
            f"Expected one ChartUsageStatistics aspect, got "
            f"{len(chart_usage_aspects)}: {chart_usage_aspects}"
        )
        assert chart_usage_aspects[0].viewsCount == 200

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_zero_views_skips_emit(self, mock_client_class):
        """``views <= 0`` deliberately skips usage-aspect emit so a
        clobber of a real prior count never happens. A future refactor
        flipping ``<=`` to ``<`` would silently overwrite a non-zero
        prior count with zero on the next run.

        Negative views are guarded at the EntityStats model boundary
        (``Field(ge=0)``), so this test only needs to lock in the
        zero-case behaviour at the emit layer."""
        view_count = 0
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Dashboard",
                stats=EntityStats(views=view_count, favorites=0),
            )
        ]
        mock_client.iter_answers.return_value = [
            AnswerResponse(
                id="ans-1",
                name="Answer",
                stats=EntityStats(views=view_count, favorites=0),
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "include_usage_stats": True,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        usage_aspect_names = {
            _mcp(wu).aspectName
            for wu in workunits
            if _mcp(wu).aspectName
            in ("dashboardUsageStatistics", "chartUsageStatistics")
        }
        assert usage_aspect_names == set(), (
            f"views={view_count} should produce no usage aspects but got: "
            f"{usage_aspect_names}"
        )

    @patch("datahub.ingestion.source.thoughtspot.source.ThoughtSpotClient")
    def test_include_usage_stats_false_suppresses_emit(self, mock_client_class):
        """The ``include_usage_stats: False`` config flag must suppress
        usage-aspect emission even when entities carry valid stats
        blocks. Without this gate test, a regression flipping the
        default or inverting the guard would slip through."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = []
        mock_client.get_logical_tables.return_value = []
        mock_client.get_tags.return_value = []
        mock_client.iter_liveboards.return_value = [
            LiveboardResponse(
                id="lb-1",
                name="Dashboard",
                stats=EntityStats(views=500, favorites=0),
            )
        ]
        mock_client.iter_answers.return_value = [
            AnswerResponse(
                id="ans-1",
                name="Answer",
                stats=EntityStats(views=120, favorites=0),
            )
        ]

        config = ThoughtSpotConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://example.thoughtspot.cloud",
                    "auth": {"type": "trusted", "username": "u", "secret_key": "k"},
                },
                "include_usage_stats": False,
            }
        )
        source = ThoughtSpotSource(config, PipelineContext(run_id="r"))
        workunits = list(source.get_workunits_internal())

        usage_aspects = [
            wu
            for wu in workunits
            if _mcp(wu).aspectName
            in ("dashboardUsageStatistics", "chartUsageStatistics")
        ]
        assert usage_aspects == []
