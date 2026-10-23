import json
from datetime import datetime, timezone
from typing import Callable, Iterable, List, Optional, Set
from uuid import NAMESPACE_URL, uuid5

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import AIAgentSubTypes
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.genie_client import (
    GenieSpace,
    GenieSpaceClient,
    GenieSpaceDefinition,
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
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import AiAgentUrn

GENIE_PLATFORM = "databricks"
GENIE_ACTOR = "urn:li:corpuser:datahub"
# Leave room for system metadata and the transport envelope added downstream.
GENIE_MAX_MCP_BYTES = int(INGEST_MAX_PAYLOAD_BYTES * 0.985)


class GenieSpaceExtractor:
    def __init__(
        self,
        config: UnityCatalogSourceConfig,
        client: GenieSpaceClient,
        report: UnityCatalogReport,
        table_urn_builder: Callable[[TableReference], str],
        platform_instance: Optional[str],
        metastore: Optional[str],
    ) -> None:
        self.config = config
        self.client = client
        self.report = report
        self.table_urn_builder = table_urn_builder
        self.platform_instance = platform_instance
        self.metastore = metastore
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
                        raise ValueError("Genie agent response has a different ID")
                    # Construct everything before emitting so invalid definitions do
                    # not replace previously ingested lineage with an empty set.
                    workunits = self._space_workunits(space)
                except Exception as exc:
                    self.report.failure(
                        message="Failed to ingest Genie agent",
                        context=summary.space_id,
                        exc=exc,
                    )
                    continue
                yield from workunits
                self.report.genie_spaces.processed(space.space_id)
        except Exception as exc:
            self.report.failure(message="Failed to list Genie agents", exc=exc)

    def _space_workunits(self, space: GenieSpace) -> List[MetadataWorkUnit]:
        if space.serialized_space is None:
            raise ValueError(
                "Genie agent export is missing; CAN EDIT permission is required"
            )
        definition = GenieSpaceDefinition.model_validate_json(space.serialized_space)
        upstream_urns: Set[str] = set()
        for source in (
            definition.data_sources.tables + definition.data_sources.metric_views
        ):
            parts = split_databricks_identifier(source.identifier)
            if parts is None or len(parts) != 3 or not all(parts):
                raise ValueError(
                    "Genie data source must be a three-part table identifier"
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

        identity = f"{self.workspace_url}/genie/{space.space_id}#{self.config.env}"
        agent_urn = str(
            AiAgentUrn(f"databricks-genie-{uuid5(NAMESPACE_URL, identity)}")
        )
        now = make_ts_millis(datetime.now(timezone.utc))
        audit = AuditStampClass(time=now, actor=GENIE_ACTOR)
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
                    created=AuditStampClass(
                        time=int(space.create_time.timestamp() * 1000)
                        if space.create_time
                        else now,
                        actor=GENIE_ACTOR,
                    ),
                    lastModified=AuditStampClass(
                        time=int(space.update_time.timestamp() * 1000)
                        if space.update_time
                        else now,
                        actor=GENIE_ACTOR,
                    ),
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
        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=agent_urn,
                aspect=SubTypesClass(typeNames=[AIAgentSubTypes.GENIE_AGENT]),
            ).as_workunit()
        )
        for workunit in workunits:
            proposal = workunit.metadata
            assert isinstance(proposal, MetadataChangeProposalWrapper)
            # Check the serialized proposal, including JSON escaping. The shared
            # size processor does not truncate aiAgentInfo, and truncating either
            # instructions or declared dependencies would misrepresent the space.
            if len(json.dumps(proposal.to_obj()).encode("utf-8")) > GENIE_MAX_MCP_BYTES:
                raise ValueError(
                    "Genie agent metadata exceeds the ingestion payload limit"
                )
        return workunits
