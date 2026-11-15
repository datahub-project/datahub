import logging
from dataclasses import dataclass
from typing import Iterable, Optional

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
from datahub.utilities.file_backed_collections import FileBackedDict
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
        For all entities that don't have a status aspect, add one with removed set to false.
        """
        # Status may arrive after other aspects, so retain one flag per URN until
        # the source ends. Spill the flags instead of retaining the entire catalog.
        with FileBackedDict[bool](serializer=int, deserializer=bool) as urn_status:
            for wu in stream:
                urn = wu.get_urn()
                has_status = False
                if not wu.is_primary_source:
                    # If this is a non-primary source, we pretend like we've seen the status
                    # aspect so that we don't try to emit a removal for it.
                    has_status = True
                elif isinstance(wu.metadata, MetadataChangeEventClass):
                    if any(
                        isinstance(aspect, StatusClass)
                        for aspect in wu.metadata.proposedSnapshot.aspects
                    ):
                        has_status = True
                elif isinstance(wu.metadata, MetadataChangeProposalWrapper):
                    if isinstance(wu.metadata.aspect, StatusClass):
                        has_status = True
                elif isinstance(wu.metadata, MetadataChangeProposalClass):
                    if wu.metadata.aspectName == StatusClass.ASPECT_NAME:
                        has_status = True
                else:
                    self.report.num_errors += 1
                    raise ValueError(f"Unexpected type {type(wu.metadata)}")

                if has_status or urn not in urn_status:
                    urn_status[urn] = has_status
                yield wu

            use_patch = _gms_supports_status_patch()
            self.report.status_patch_mode = use_patch

            # Flush pending flags, then stream keys in the same order as sorted(URNs).
            # The unique key index supplies the ordering without a Python set/list copy.
            for row in urn_status.sql_query_iterator(
                f"SELECT key FROM {urn_status.tablename} WHERE value = 0 ORDER BY key"
            ):
                urn = row[0]
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
