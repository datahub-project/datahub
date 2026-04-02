"""
Shared context dataclass for LookerV2Source processor classes.

All processor constructors take a single LookerV2Context argument.
Infrastructure fields are immutable after construction; mutable fields
are populated by processors in pipeline order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from datahub.ingestion.source.looker_v2.looker_v2_config import LookerV2Config

if TYPE_CHECKING:
    from looker_sdk.sdk.api40.models import FolderBase, LookmlModel, LookmlModelExplore

    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.looker.looker_common import LookerUserRegistry
    from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
    from datahub.ingestion.source.looker_v2 import looker_v2_usage as looker_usage
    from datahub.ingestion.source.looker_v2.looker_v2_pdt_graph_parser import (
        PDTDependencyEdge,
    )
    from datahub.ingestion.source.looker_v2.looker_v2_report import LookerV2SourceReport
    from datahub.ingestion.source.looker_v2.lookml_parser import ParsedView
    from datahub.ingestion.source.looker_v2.lookml_view_discovery import (
        ViewDiscoveryResult,
    )


@dataclass
class LookerV2Context:
    # ── Infrastructure (immutable after construction) ──────────────────────
    config: LookerV2Config
    looker_api: "LookerAPI"
    reporter: "LookerV2SourceReport"
    pipeline_ctx: "PipelineContext"
    platform: str
    user_registry: "Optional[LookerUserRegistry]" = None

    # ── Mutable shared state — written and read across processors ──────────
    model_registry: "Dict[str, LookmlModel]" = field(default_factory=dict)
    explore_cache: "Dict[Tuple[str, str], LookmlModelExplore]" = field(
        default_factory=dict
    )
    folder_registry: "Dict[str, FolderBase]" = field(default_factory=dict)
    reachable_explores: "Dict[Tuple[str, str], List[str]]" = field(default_factory=dict)
    pdt_upstream_map: "Dict[str, List[PDTDependencyEdge]]" = field(default_factory=dict)
    view_to_explore_map: "Dict[str, Tuple[str, str]]" = field(default_factory=dict)
    view_discovery_result: "Optional[ViewDiscoveryResult]" = None
    resolved_project_paths: "Dict[str, str]" = field(default_factory=dict)
    parsed_view_file_cache: "Dict[Tuple[str, str], List[ParsedView]]" = field(
        default_factory=dict
    )
    dashboards_for_usage: "List[looker_usage.LookerDashboardForUsage]" = field(
        default_factory=list
    )
