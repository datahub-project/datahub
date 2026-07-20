from __future__ import annotations

import time
from typing import Any, Callable, Iterable, List, Optional

from pydantic import field_validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AgentSkillInfoClass,
    AuditStampClass,
    DataPlatformInstanceClass,
    SkillSourceRepositoryClass,
    StatusClass,
)
from datahub.metadata.urns import AgentSkillUrn, ApiUrn

_DEFAULT_ACTOR = "urn:li:corpuser:datahub"


class SkillSourceRepository(ConfigModel):
    """Pointer to the git location where a skill is defined.

    Skills follow a "git as source of truth" pattern: the repository owns the
    skill definition and DataHub catalogs it.

    Args:
        url: URL of the git repository containing the skill definition.
        path: Path within the repository to the skill definition
            (e.g. ``customer-service/SKILL.md``).
    """

    url: str
    path: Optional[str] = None


class AgentSkill(ConfigModel):
    """High-level helper for registering an AgentSkill with DataHub.

    A skill is a higher-level capability bundle than a tool: it packages
    domain expertise (specialized instructions) plus the low-level tools it
    relies on. The model is chosen by the adopting agent, not the skill.

    Args:
        id: The id of the skill (bare id or a full ``urn:li:agentSkill:`` urn).
        name: Display name of the skill.
        description: What the skill does and when to use it.
        instructions: Specialized prompts/context that define the skill.
        source_repository: Git location where the skill is defined.
        required_tools: API urns this skill requires to operate.
    """

    id: str
    name: str
    description: Optional[str] = None
    instructions: Optional[str] = None
    source_repository: Optional[SkillSourceRepository] = None
    required_tools: Optional[List[str]] = None
    platform: Optional[str] = None

    @field_validator("required_tools", mode="after")
    @classmethod
    def required_tools_must_be_tool_urns(
        cls, v: Optional[List[str]]
    ) -> Optional[List[str]]:
        for tool in v or []:
            ApiUrn.from_string(tool)
        return v

    @property
    def urn(self) -> str:
        if self.id.startswith("urn:li:agentSkill:"):
            return self.id
        return str(AgentSkillUrn(self.id))

    def _mint_auditstamp(self) -> AuditStampClass:
        return AuditStampClass(time=int(time.time() * 1000.0), actor=_DEFAULT_ACTOR)

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        audit_stamp = self._mint_auditstamp()
        source_repository: Optional[SkillSourceRepositoryClass] = None
        if self.source_repository is not None:
            source_repository = SkillSourceRepositoryClass(
                url=self.source_repository.url,
                path=self.source_repository.path,
            )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=AgentSkillInfoClass(
                name=self.name,
                description=self.description,
                instructions=self.instructions,
                sourceRepository=source_repository,
                requiredTools=self.required_tools,
                created=audit_stamp,
                lastModified=audit_stamp,
            ),
        )
        if self.platform:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.platform)
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
        """Emit the AgentSkill to DataHub and return its urn."""
        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)
        return self.urn

    @field_validator("source_repository", mode="before")
    @classmethod
    def _coerce_source_repository(cls, v: Any) -> Any:
        if isinstance(v, dict):
            return SkillSourceRepository.parse_obj(v)
        return v
