"""Look processor for LookerV2Source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, Set, Union

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import Look, LookWithQuery

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.looker.looker_common import (
    LookerUserRegistry,
    get_urn_looker_element_id,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.chart import Chart

if TYPE_CHECKING:
    from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
    from datahub.ingestion.source.looker_v2.looker_v2_folder_processor import (
        LookerFolderProcessor,
    )

logger = logging.getLogger(__name__)


class LookerLookProcessor:
    """Processes standalone Looker looks into Chart workunits."""

    def __init__(
        self,
        ctx: "LookerV2Context",
        folder_proc: "LookerFolderProcessor",
        user_registry: LookerUserRegistry,
        reachable_look_registry: Set[str],
    ) -> None:
        self._ctx = ctx
        self._folder_proc = folder_proc
        self._user_registry = user_registry
        self._reachable_look_registry = reachable_look_registry

    def process(self) -> Iterable[MetadataWorkUnit]:
        """Process standalone looks."""
        logger.info("Processing looks...")

        look_fields = ["id", "title", "description", "user_id", "folder", "query"]

        deleted_param = None if self._ctx.config.include_deleted else False

        try:
            looks = self._ctx.looker_api.search_looks(
                fields=look_fields, deleted=deleted_param
            )
        except SDKError as e:
            self._ctx.reporter.report_failure("fetch_looks", str(e))
            return

        self._ctx.reporter.looks_discovered = len(looks)

        for look in looks:
            if look.id:
                if str(look.id) in self._reachable_look_registry:
                    continue

                folder = getattr(look, "folder", None)
                if self._folder_proc.should_skip_personal_folder(folder):
                    continue

                chart = self._create_look_entity(look)
                if chart:
                    for mcp in chart.as_mcps():
                        yield mcp.as_workunit()
                    self._ctx.reporter.looks_scanned += 1

    def _create_look_entity(self, look: Union[Look, LookWithQuery]) -> Chart | None:
        """Create a Chart entity from a Look."""
        if not look.id:
            return None

        platform_instance = (
            self._ctx.config.platform_instance
            if self._ctx.config.include_platform_instance_in_urns
            else None
        )

        chart = Chart(
            name=get_urn_looker_element_id(f"looks_{look.id}"),
            display_name=look.title or f"Look {look.id}",
            platform=self._ctx.platform,
            platform_instance=platform_instance,
            description=look.description,
            subtype=BIAssetSubTypes.LOOKER_LOOK,
        )

        if look.user_id and self._ctx.config.extract_owners:
            user = self._user_registry.get_by_id(str(look.user_id))
            if user and user.email:
                chart.add_owner((CorpUserUrn(user.email), "DATAOWNER"))

        return chart
