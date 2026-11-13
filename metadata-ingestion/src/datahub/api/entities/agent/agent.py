from __future__ import annotations

import time
from typing import Callable, Dict, Iterable, List, Optional, Union

from pydantic import field_validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AIAgentDependenciesClass,
    AIAgentInfoClass,
    AIAgentSourceClass,
    AIAgentSourceTypeClass,
    AuditStampClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DomainsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    UpstreamClass,
    UpstreamLineageClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import (
    AgentSkillUrn,
    AiAgentUrn,
    ApiUrn,
    DatasetUrn,
    MlModelUrn,
    StructuredPropertyUrn,
)

_DEFAULT_ACTOR = "urn:li:corpuser:datahub"

# Value type accepted by structured property assignments (urn:li... is a str,
# numeric properties take a float).
StructuredPropertyValue = Union[str, float, List[Union[str, float]]]


class AgentOwner(ConfigModel):
    """An owner of the agent.

    Args:
        id: Owner urn, or a bare user/group id (resolved to a corpuser urn).
        type: Ownership type (mappable name or a custom ownership-type urn).
    """

    id: str
    type: str = OwnershipTypeClass.TECHNICAL_OWNER


class Agent(ConfigModel):
    """High-level helper for registering an AI Agent with DataHub.

    Supports the full slide flow: name/description, framework + version as
    structured properties, ownership, domain scoping, linked skills/tools/
    models (the ``aiAgentDependencies`` aspect), and consumed datasets modeled
    as upstream lineage.

    Args:
        id: The id of the agent (bare id or a full ``urn:li:aiAgent:`` urn).
        name: Display name of the agent.
        description: What the agent does and when Ask DataHub should route to it.
        instructions: Custom base instructions injected into the system prompt.
        source_type: One of ``AIAgentSourceTypeClass`` (SYSTEM/NATIVE/EXTERNAL).
            Defaults to EXTERNAL for cataloged, externally-managed agents.
        structured_properties: Map of structured-property urn -> value(s).
            Used here to carry ``framework`` and ``version``.
        owners: Owners of the agent.
        domain: Domain the agent belongs to (name or ``urn:li:domain:`` urn).
            Emitted as the standard ``domains`` cataloging aspect.
        skills: AgentSkill urns the agent adopts.
        tools: API urns the agent invokes.
        models: mlModel urns the agent runs on.
        consumes_datasets: Dataset urns the agent consumes, modeled as upstreams.
        lineage_type: Lineage type for consumed datasets. Defaults to COPY.
        platform: The platform (framework/runtime) the agent runs on, e.g.
            ``langchain`` or ``google-adk``. A bare id or a full
            ``urn:li:dataPlatform:`` urn. Surfaces the platform logo on the
            agent's profile.
        version: Version tag (e.g. ``"2.0"``). Setting it links the agent into a
            VersionSet so the profile shows a version picker; the highest version
            is flagged latest. Each version is its own aiAgent entity (distinct
            ``id``), grouped by ``version_set``.
        version_set: The family the version belongs to — a full
            ``urn:li:versionSet:(<id>,aiAgent)`` urn or a bare id that expands to
            one. Defaults to a set derived from this agent's own id.
        version_sort_id: Lexicographic ordering key (highest = latest).
            Auto-derived from ``version`` when dotted-numeric.
        version_comment: Optional note describing what changed in this version.
    """

    id: str
    name: str
    description: Optional[str] = None
    instructions: Optional[str] = None
    source_type: str = AIAgentSourceTypeClass.EXTERNAL
    structured_properties: Optional[Dict[str, StructuredPropertyValue]] = None
    owners: Optional[List[Union[str, AgentOwner]]] = None
    domain: Optional[str] = None
    skills: Optional[List[str]] = None
    tools: Optional[List[str]] = None
    models: Optional[List[str]] = None
    consumes_datasets: Optional[List[str]] = None
    lineage_type: str = DatasetLineageTypeClass.COPY
    platform: Optional[str] = None
    version: Optional[str] = None
    version_set: Optional[str] = None
    version_sort_id: Optional[str] = None
    version_comment: Optional[str] = None

    @field_validator("skills", mode="after")
    @classmethod
    def skills_must_be_skill_urns(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        for skill in v or []:
            AgentSkillUrn.from_string(skill)
        return v

    @field_validator("tools", mode="after")
    @classmethod
    def tools_must_be_tool_urns(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        for tool in v or []:
            ApiUrn.from_string(tool)
        return v

    @field_validator("models", mode="after")
    @classmethod
    def models_must_be_model_urns(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        for model in v or []:
            MlModelUrn.from_string(model)
        return v

    @field_validator("consumes_datasets", mode="after")
    @classmethod
    def consumes_must_be_dataset_urns(
        cls, v: Optional[List[str]]
    ) -> Optional[List[str]]:
        for dataset in v or []:
            DatasetUrn.from_string(dataset)
        return v

    @property
    def urn(self) -> str:
        if self.id.startswith("urn:li:aiAgent:"):
            return self.id
        return str(AiAgentUrn(self.id))

    @property
    def _bare_id(self) -> str:
        prefix = "urn:li:aiAgent:"
        return self.id[len(prefix) :] if self.id.startswith(prefix) else self.id

    @property
    def version_set_urn(self) -> str:
        """The effective VersionSet urn: an explicit full urn, a bare id expanded
        to one, or (default) a set derived from this agent's own id."""
        vs = self.version_set
        if vs is None:
            return f"urn:li:versionSet:({self._bare_id},aiAgent)"
        if vs.startswith("urn:li:versionSet:"):
            return vs
        return f"urn:li:versionSet:({vs},aiAgent)"

    @staticmethod
    def _derive_sort_id(version: str) -> str:
        """Zero-pad dotted-numeric versions so LEXICOGRAPHIC_STRING ordering
        matches numeric ordering (``1.9`` < ``1.10`` < ``2.0``); non-numeric
        versions fall back to the raw tag."""
        parts = version.lstrip("vV").split(".")
        if parts and all(p.isdigit() for p in parts):
            return ".".join(f"{int(p):08d}" for p in parts)
        return version

    def _mint_auditstamp(self) -> AuditStampClass:
        return AuditStampClass(time=int(time.time() * 1000.0), actor=_DEFAULT_ACTOR)

    def _mint_owner(self, owner: Union[str, AgentOwner]) -> OwnerClass:
        if isinstance(owner, str):
            return OwnerClass(
                owner=builder.make_user_urn(owner),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        ownership_type, ownership_type_urn = builder.validate_ownership_type(owner.type)
        return OwnerClass(
            owner=owner.id,
            type=ownership_type,
            typeUrn=ownership_type_urn,
        )

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        audit_stamp = self._mint_auditstamp()

        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=AIAgentInfoClass(
                name=self.name,
                description=self.description,
                instructions=self.instructions,
                source=AIAgentSourceClass(type=self.source_type),
                created=audit_stamp,
                lastModified=audit_stamp,
            ),
        )

        if self.skills or self.tools or self.models:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=AIAgentDependenciesClass(
                    skills=self.skills,
                    tools=self.tools,
                    models=self.models,
                ),
            )

        if self.structured_properties:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=str(StructuredPropertyUrn(property_id)),
                            values=value if isinstance(value, list) else [value],
                            created=audit_stamp,
                            lastModified=audit_stamp,
                        )
                        for property_id, value in self.structured_properties.items()
                    ]
                ),
            )

        if self.consumes_datasets:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=dataset,
                            type=self.lineage_type,
                            auditStamp=audit_stamp,
                        )
                        for dataset in self.consumes_datasets
                    ]
                ),
            )

        if self.domain:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=DomainsClass(domains=[builder.make_domain_urn(self.domain)]),
            )

        if self.owners:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=OwnershipClass(
                    owners=[self._mint_owner(o) for o in self.owners]
                ),
            )

        if self.platform:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.platform)
                ),
            )

        if self.version:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=VersionPropertiesClass(
                    versionSet=self.version_set_urn,
                    version=VersionTagClass(versionTag=self.version),
                    sortId=self.version_sort_id or self._derive_sort_id(self.version),
                    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
                    comment=self.version_comment,
                    metadataCreatedTimestamp=audit_stamp,
                ),
            )

        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn, aspect=StatusClass(removed=False)
        )

    def emit(
        self,
        emitter: Emitter,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> str:
        """Emit the Agent and all its aspects to DataHub and return its urn."""
        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)
        return self.urn
