import json
from datetime import datetime
from typing import Callable, Iterable, List, Optional, Set
from uuid import NAMESPACE_URL, uuid5

import requests
from databricks.sdk.errors import DatabricksError, PermissionDenied
from pydantic import ValidationError
from typing_extensions import LiteralString

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.genie_client import (
    GenieSpace,
    GenieSpaceClient,
    GenieSpaceDefinition,
    InvalidGenieExport,
    InvalidGenieListing,
    UnsupportedGenieExportVersion,
)
from datahub.ingestion.source.unity.identifier_helper import split_databricks_identifier
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    AIAgentInfoClass,
    AIAgentSourceClass,
    AIAgentSourceTypeClass,
    AuditStampClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import AiAgentUrn

GENIE_PLATFORM = "databricks"
GENIE_ACTOR = "urn:li:corpuser:datahub"
# Leave room for system metadata and the transport envelope added downstream.
GENIE_MAX_MCP_BYTES = int(INGEST_MAX_PAYLOAD_BYTES * 0.985)


def _millis_or_zero(dt: Optional[datetime]) -> int:
    # The Get Genie Space response may omit timestamps. Zero stays stable across
    # runs, so aiAgentInfo and upstreamLineage are not rewritten every schedule.
    if dt is None:
        return 0
    return make_ts_millis(dt)


class _GenieSpaceWarning(Exception):
    def __init__(
        self,
        message: LiteralString,
        *,
        identifier: Optional[str] = None,
        aspect_name: Optional[str] = None,
        size: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.report_message: LiteralString = message
        self.identifier = identifier
        self.aspect_name = aspect_name
        self.size = size


# What the SDK transport and response parsing raise. The SDK raises TimeoutError
# when retries run out, and requests errors that it does not retry pass through.
_GENIE_API_ERRORS = (
    DatabricksError,
    requests.RequestException,
    TimeoutError,
    json.JSONDecodeError,
    ValidationError,
)
_GENIE_LISTING_ERRORS = (*_GENIE_API_ERRORS, InvalidGenieListing)
_GENIE_SPACE_ERRORS = (
    *_GENIE_API_ERRORS,
    _GenieSpaceWarning,
    InvalidGenieExport,
    UnsupportedGenieExportVersion,
)


class GenieSpaceExtractor:
    def __init__(
        self,
        config: UnityCatalogSourceConfig,
        client: GenieSpaceClient,
        report: UnityCatalogReport,
        table_urn_builder: Callable[[TableReference], str],
        platform_instance: Optional[str],
        metastore: Optional[str],
        preserve_urn: Callable[[str], None],
        preserve_all_urns: Callable[[], None],
    ) -> None:
        self.config = config
        self.client = client
        self.report = report
        self.table_urn_builder = table_urn_builder
        self.platform_instance = platform_instance
        self.metastore = metastore
        self.preserve_urn = preserve_urn
        self.preserve_all_urns = preserve_all_urns
        self.workspace_url = config.workspace_url.rstrip("/").lower()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            for summary in self.client.list_spaces():
                if not self.config.genie_space_pattern.allowed(summary.space_id):
                    self.report.genie_spaces.dropped(summary.space_id)
                    continue
                try:
                    space = self.client.get_space(summary.space_id)
                    if space.space_id != summary.space_id:
                        raise _GenieSpaceWarning("Invalid Genie agent export")
                    # Construct everything before emitting so invalid definitions do
                    # not replace previously ingested lineage with an empty set.
                    workunits = self._space_workunits(space)
                except _GENIE_SPACE_ERRORS as exc:
                    self._warn_space(summary.space_id, exc)
                    continue
                except Exception as exc:
                    self.report.failure(
                        message="Unexpected error ingesting Genie agent",
                        context=f"space_id={summary.space_id}",
                        exc=exc,
                    )
                    continue
                yield from workunits
                self.report.genie_spaces.processed(space.space_id)
        except _GENIE_LISTING_ERRORS as exc:
            self._warn_listing(exc)
        except Exception as exc:
            self.report.failure(
                message="Unexpected error listing Genie agents", exc=exc
            )

    def _warn_listing(self, exc: Exception) -> None:
        # The set of spaces is unknown. Agents processed before the failure are
        # already in the current state, and the rest are carried forward.
        self.report.warning(message="Failed to list Genie agents", exc=exc)
        try:
            self.preserve_all_urns()
        except Exception as preserve_exc:
            self.report.failure(
                message="Failed to preserve Genie agent state", exc=preserve_exc
            )

    def _warn_space(self, space_id: str, exc: Exception) -> None:
        self.report.warning(
            message=self._warning_message(exc),
            context=self._warning_context(space_id, exc),
            exc=None if isinstance(exc, _GenieSpaceWarning) else exc,
        )
        try:
            self.preserve_urn(self._agent_urn(space_id))
        except Exception as preserve_exc:
            self.report.failure(
                message="Failed to preserve Genie agent state",
                context=f"space_id={space_id}",
                exc=preserve_exc,
            )

    @staticmethod
    def _warning_message(exc: Exception) -> LiteralString:
        if isinstance(exc, _GenieSpaceWarning):
            return exc.report_message
        if isinstance(exc, PermissionDenied):
            return "Genie agent export requires CAN EDIT"
        if isinstance(exc, UnsupportedGenieExportVersion):
            return "Unsupported Genie export version"
        if isinstance(exc, (json.JSONDecodeError, ValidationError, InvalidGenieExport)):
            return "Invalid Genie agent export"
        return "Failed to fetch Genie agent"

    @staticmethod
    def _warning_context(space_id: str, exc: Exception) -> str:
        parts = [f"space_id={space_id}"]
        if isinstance(exc, _GenieSpaceWarning):
            if exc.identifier is not None:
                parts.append(f"identifier={exc.identifier!r}")
            if exc.aspect_name is not None:
                parts.append(f"aspect={exc.aspect_name}")
            if exc.size is not None:
                parts.append(f"size={exc.size}")
        return " ".join(parts)

    def _agent_urn(self, space_id: str) -> str:
        identity = f"{self.workspace_url}/genie/{space_id}#{self.config.env}"
        return str(AiAgentUrn(f"databricks-genie-{uuid5(NAMESPACE_URL, identity)}"))

    def _space_workunits(self, space: GenieSpace) -> List[MetadataWorkUnit]:
        if space.serialized_space is None:
            raise _GenieSpaceWarning("Genie agent export requires CAN EDIT")
        definition = GenieSpaceDefinition.from_serialized(space.serialized_space)
        upstream_urns: Set[str] = set()
        for source in (
            definition.data_sources.tables + definition.data_sources.metric_views
        ):
            parts = split_databricks_identifier(source.identifier)
            if parts is None or len(parts) != 3 or not all(parts):
                raise _GenieSpaceWarning(
                    "Invalid Genie agent export",
                    identifier=source.identifier,
                )
            upstream_urns.add(
                self.table_urn_builder(
                    TableReference(
                        metastore=self.metastore,
                        catalog=parts[0],
                        schema=parts[1],
                        table=parts[2],
                    )
                )
            )

        agent_urn = self._agent_urn(space.space_id)
        created_ms = _millis_or_zero(space.create_time)
        modified_ms = _millis_or_zero(space.update_time)
        audit = AuditStampClass(time=modified_ms, actor=GENIE_ACTOR)
        instructions = "\n\n".join(
            "".join(instruction.content)
            for instruction in definition.instructions.text_instructions
            if instruction.content
        )
        workunits = [
            MetadataChangeProposalWrapper(
                entityUrn=agent_urn,
                aspect=AIAgentInfoClass(
                    name=space.title,
                    description=space.description,
                    instructions=instructions or None,
                    source=AIAgentSourceClass(type=AIAgentSourceTypeClass.EXTERNAL),
                    created=AuditStampClass(time=created_ms, actor=GENIE_ACTOR),
                    lastModified=AuditStampClass(time=modified_ms, actor=GENIE_ACTOR),
                ),
            ).as_workunit(),
            MetadataChangeProposalWrapper(
                entityUrn=agent_urn, aspect=StatusClass(removed=False)
            ).as_workunit(),
        ]
        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=agent_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(GENIE_PLATFORM),
                    instance=make_dataplatform_instance_urn(
                        GENIE_PLATFORM, self.platform_instance
                    )
                    if self.platform_instance
                    else None,
                ),
            ).as_workunit()
        )
        if self.config.include_table_lineage:
            workunits.append(
                MetadataChangeProposalWrapper(
                    entityUrn=agent_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=urn,
                                type=DatasetLineageTypeClass.COPY,
                                auditStamp=audit,
                            )
                            for urn in sorted(upstream_urns)
                        ]
                    ),
                ).as_workunit()
            )
        for workunit in workunits:
            proposal = workunit.metadata
            assert isinstance(proposal, MetadataChangeProposalWrapper)
            # Check the serialized proposal, including JSON escaping. The shared
            # size processor does not truncate aiAgentInfo, and truncating either
            # instructions or declared dependencies would misrepresent the space.
            size = len(json.dumps(proposal.to_obj()).encode("utf-8"))
            if size > GENIE_MAX_MCP_BYTES:
                raise _GenieSpaceWarning(
                    "Genie agent metadata exceeds the ingestion payload limit",
                    aspect_name=proposal.aspectName,
                    size=size,
                )
        return workunits
