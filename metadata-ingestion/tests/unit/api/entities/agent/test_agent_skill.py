from typing import List

import pytest

from datahub.api.entities.agent.agent_skill import AgentSkill, SkillSourceRepository
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AgentSkillInfoClass,
    DataPlatformInstanceClass,
    StatusClass,
)
from datahub.utilities.urns.error import InvalidUrnError

TOOL_URN = "urn:li:api:get_order"


def _aspects(skill: AgentSkill) -> list:
    return [m.aspect for m in skill.generate_mcp()]


def test_urn_from_bare_id_and_passthrough_full_urn() -> None:
    assert AgentSkill(id="refund", name="Refund").urn == "urn:li:agentSkill:refund"
    full = "urn:li:agentSkill:refund"
    assert AgentSkill(id=full, name="Refund").urn == full


def test_minimal_skill_emits_info_and_status() -> None:
    skill = AgentSkill(id="refund", name="Refund Flow", description="Handles refunds")
    aspects = _aspects(skill)

    infos = [a for a in aspects if isinstance(a, AgentSkillInfoClass)]
    assert len(infos) == 1
    assert infos[0].name == "Refund Flow"
    assert infos[0].description == "Handles refunds"
    assert infos[0].sourceRepository is None
    assert infos[0].requiredTools is None

    assert any(isinstance(a, StatusClass) and a.removed is False for a in aspects)
    assert not any(isinstance(a, DataPlatformInstanceClass) for a in aspects)


def test_source_repository_carried_onto_info() -> None:
    skill = AgentSkill(
        id="refund",
        name="Refund",
        source_repository=SkillSourceRepository(
            url="https://github.com/acme/skills", path="refund/SKILL.md"
        ),
    )
    info = next(a for a in _aspects(skill) if isinstance(a, AgentSkillInfoClass))
    assert info.sourceRepository is not None
    assert info.sourceRepository.url == "https://github.com/acme/skills"
    assert info.sourceRepository.path == "refund/SKILL.md"


def test_source_repository_coerced_from_dict() -> None:
    # The _coerce_source_repository validator accepts a plain dict.
    skill = AgentSkill(
        id="refund",
        name="Refund",
        source_repository={"url": "https://github.com/acme/skills"},  # type: ignore[arg-type]
    )
    assert isinstance(skill.source_repository, SkillSourceRepository)
    assert skill.source_repository.url == "https://github.com/acme/skills"
    assert skill.source_repository.path is None


def test_required_tools_carried_onto_info() -> None:
    skill = AgentSkill(id="refund", name="Refund", required_tools=[TOOL_URN])
    info = next(a for a in _aspects(skill) if isinstance(a, AgentSkillInfoClass))
    assert info.requiredTools == [TOOL_URN]


def test_required_tools_urn_validation_rejects_non_api() -> None:
    with pytest.raises(InvalidUrnError):
        AgentSkill(id="refund", name="Refund", required_tools=["urn:li:agentSkill:x"])


def test_platform_emits_data_platform_instance() -> None:
    skill = AgentSkill(id="refund", name="Refund", platform="anthropic")
    dpi = next(a for a in _aspects(skill) if isinstance(a, DataPlatformInstanceClass))
    assert dpi.platform == "urn:li:dataPlatform:anthropic"


def test_emit_sends_all_mcps_and_returns_urn() -> None:
    class _RecordingEmitter:
        def __init__(self) -> None:
            self.emitted: List[MetadataChangeProposalWrapper] = []

        def emit(self, mcp, callback=None):  # type: ignore[no-untyped-def]
            self.emitted.append(mcp)

    skill = AgentSkill(id="refund", name="Refund")
    emitter = _RecordingEmitter()
    urn = skill.emit(emitter)  # type: ignore[arg-type]
    assert urn == "urn:li:agentSkill:refund"
    assert all(m.entityUrn == urn for m in emitter.emitted)
