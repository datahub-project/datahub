import logging
from dataclasses import dataclass
from typing import Iterable, Optional, Set

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import entity_supports_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
)
from datahub.specific.status import StatusPatchBuilder
from datahub.utilities.server_config_util import RestServiceConfig, get_gms_config
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


def _gms_supports_status_patch() -> bool:
    """Check whether the connected GMS server supports PATCH on the status aspect.

    The StatusTemplate was added in DataHub v1.7.0 / Cloud v2.1.0. GMS
    versions without it reject PATCH MCPs for status with a 500 error.
    Returns False when the server config is unavailable (e.g. file sink).
    """
    raw_config = get_gms_config()
    if not raw_config:
        return False
    service_config: RestServiceConfig = (
        raw_config
        if isinstance(raw_config, RestServiceConfig)
        else RestServiceConfig(raw_config=raw_config)
    )

    if service_config.is_datahub_cloud:
        return service_config.is_version_at_least(2, 1, 0)
    else:
        return service_config.is_version_at_least(1, 7, 0)


@dataclass
class AutoStatusAspectProcessorReport(WorkunitProcessorReport):
    """Report for AutoStatusAspectProcessor metrics."""

    num_errors: int = 0  # Unexpected metadata types
    num_status_aspects_emitted: int = 0  # Status aspects added to entities
    status_patch_mode: Optional[bool] = None  # True if using PATCH, False if UPSERT


class AutoStatusAspectProcessor(WorkunitProcessor[AutoStatusAspectProcessorReport]):
    """Add status aspect (removed=False) to entities that don't have one.

    When the connected GMS supports PATCH on the status aspect (v1.7.0+ /
    Cloud v2.1.0+), emits a JSON Patch MCP that only sets ``/removed=false``
    without overwriting other fields like lifecycleStage. Falls back to a
    full UPSERT on older servers.
    """

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """
        For all primary-source entities without a status aspect, add one with removed
        set to false.
        """
        primary_urns: Set[str] = set()
        status_urns: Set[str] = set()
        for wu in stream:
            urn = wu.get_urn()
            # An entity can have both primary and non-primary workunits (e.g. a
            # same-platform sibling); only the former is owed a status aspect.
            if wu.is_primary_source:
                primary_urns.add(urn)
            if self._has_status_aspect(wu):
                status_urns.add(urn)

            yield wu

        use_patch = _gms_supports_status_patch()
        self.report.status_patch_mode = use_patch

        for urn in sorted(primary_urns - status_urns):
            entity_type = guess_entity_type(urn)
            if not entity_supports_aspect(entity_type, StatusClass):
                # If any entity does not support aspect 'status' then skip that entity from adding status aspect.
                # Example like dataProcessInstance doesn't suppport status aspect.
                # If not skipped gives error: java.lang.RuntimeException: Unknown aspect status for entity dataProcessInstance
                continue
            self.report.num_status_aspects_emitted += 1
            if use_patch:
                # PATCH only sets removed=false without overwriting other
                # fields (lifecycleStage, lifecycleState, lifecycleLastUpdated).
                patch_builder = StatusPatchBuilder(urn).set_removed(False)
                for mcp in patch_builder.build():
                    yield MetadataWorkUnit(
                        id=f"{urn}-auto-status",
                        mcp_raw=mcp,
                    )
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=StatusClass(removed=False),
                ).as_workunit()

    def _has_status_aspect(self, wu: MetadataWorkUnit) -> bool:
        if isinstance(wu.metadata, MetadataChangeEventClass):
            return any(
                isinstance(aspect, StatusClass)
                for aspect in wu.metadata.proposedSnapshot.aspects
            )
        elif isinstance(wu.metadata, MetadataChangeProposalWrapper):
            return isinstance(wu.metadata.aspect, StatusClass)
        elif isinstance(wu.metadata, MetadataChangeProposalClass):
            return wu.metadata.aspectName == StatusClass.ASPECT_NAME
        else:
            self.report.num_errors += 1
            raise ValueError(f"Unexpected type {type(wu.metadata)}")
