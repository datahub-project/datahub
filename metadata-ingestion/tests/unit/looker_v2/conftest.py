"""Shared fixtures and helpers for LookerV2 unit tests."""

from typing import Any
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
from datahub.ingestion.source.looker_v2.looker_v2_report import LookerV2SourceReport


def make_ctx(**overrides: Any) -> LookerV2Context:
    """Build a minimal LookerV2Context for unit tests.

    Pass only the fields the processor under test reads. All other mutable
    fields default to empty collections via the dataclass field() defaults.
    """
    config = MagicMock()
    config.platform_instance = None
    config.env = "PROD"
    config.chart_pattern = MagicMock()
    config.chart_pattern.allowed.return_value = True
    config.extract_usage_history = False
    config.emit_used_explores_only = False
    config.model_pattern = MagicMock()
    config.model_pattern.allowed.return_value = True
    config.explore_pattern = MagicMock()
    config.explore_pattern.allowed.return_value = True
    config.tag_measures_and_dimensions = False
    config.api_sql_lineage_field_chunk_size = 20
    config.api_sql_lineage_individual_field_fallback = True
    config.max_concurrent_requests = 1
    config.extract_embed_urls = False
    config.project_name = "test_project"

    defaults: dict[str, Any] = dict(
        config=config,
        looker_api=MagicMock(),
        reporter=LookerV2SourceReport(),
        pipeline_ctx=PipelineContext(run_id="test"),
        platform="looker",
    )
    defaults.update(overrides)
    return LookerV2Context(**defaults)
