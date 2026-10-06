from typing import List

import pytest

from datahub.api.entities.agent.agent import Agent, AgentOwner
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AIAgentDependenciesClass,
    AIAgentInfoClass,
    AIAgentSourceTypeClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DomainsClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    StructuredPropertiesClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.error import InvalidUrnError

SKILL_URN = "urn:li:agentSkill:refund-flow"
TOOL_URN = "urn:li:api:get_order"
MODEL_URN = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my-model,PROD)"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)"


def _aspects(agent: Agent) -> list:
    return [m.aspect for m in agent.generate_mcp()]


def test_urn_from_bare_id_and_passthrough_full_urn() -> None:
    assert Agent(id="support", name="Support").urn == "urn:li:aiAgent:support"
    full = "urn:li:aiAgent:support"
    assert Agent(id=full, name="Support").urn == full


def test_minimal_agent_emits_info_and_status_only() -> None:
    agent = Agent(id="support", name="Support Agent", description="Helps users")
    aspects = _aspects(agent)

    infos = [a for a in aspects if isinstance(a, AIAgentInfoClass)]
    assert len(infos) == 1
    info = infos[0]
    assert info.name == "Support Agent"
    assert info.description == "Helps users"
    # Default source type is EXTERNAL.
    assert info.source is not None
    assert info.source.type == AIAgentSourceTypeClass.EXTERNAL

    assert any(isinstance(a, StatusClass) and a.removed is False for a in aspects)
    # No domain aspect emitted when domain is not set.
    assert not any(isinstance(a, DomainsClass) for a in aspects)
    # None of the optional aspects present.
    assert not any(isinstance(a, AIAgentDependenciesClass) for a in aspects)
    assert not any(isinstance(a, OwnershipClass) for a in aspects)
    assert not any(isinstance(a, UpstreamLineageClass) for a in aspects)
    assert not any(isinstance(a, StructuredPropertiesClass) for a in aspects)


def test_domain_emits_domains_aspect() -> None:
    agent = Agent(id="support", name="Support", domain="customer-success")
    aspects = _aspects(agent)
    domains = [a for a in aspects if isinstance(a, DomainsClass)]
    assert len(domains) == 1
    assert domains[0].domains == ["urn:li:domain:customer-success"]


def test_dependencies_aspect_carries_skills_tools_models() -> None:
    agent = Agent(
        id="support",
        name="Support",
        skills=[SKILL_URN],
        tools=[TOOL_URN],
        models=[MODEL_URN],
    )
    deps = [a for a in _aspects(agent) if isinstance(a, AIAgentDependenciesClass)]
    assert len(deps) == 1
    assert deps[0].skills == [SKILL_URN]
    assert deps[0].tools == [TOOL_URN]
    assert deps[0].models == [MODEL_URN]


def test_dependencies_emitted_when_only_one_kind_present() -> None:
    agent = Agent(id="support", name="Support", tools=[TOOL_URN])
    deps = [a for a in _aspects(agent) if isinstance(a, AIAgentDependenciesClass)]
    assert len(deps) == 1
    assert deps[0].skills is None
    assert deps[0].tools == [TOOL_URN]


def test_consumes_datasets_emits_upstream_lineage_with_default_copy_type() -> None:
    agent = Agent(id="support", name="Support", consumes_datasets=[DATASET_URN])
    ul = [a for a in _aspects(agent) if isinstance(a, UpstreamLineageClass)]
    assert len(ul) == 1
    assert len(ul[0].upstreams) == 1
    up = ul[0].upstreams[0]
    assert up.dataset == DATASET_URN
    assert up.type == DatasetLineageTypeClass.COPY


def test_lineage_type_override_applied_to_upstreams() -> None:
    agent = Agent(
        id="support",
        name="Support",
        consumes_datasets=[DATASET_URN],
        lineage_type=DatasetLineageTypeClass.VIEW,
    )
    ul = next(a for a in _aspects(agent) if isinstance(a, UpstreamLineageClass))
    assert ul.upstreams[0].type == DatasetLineageTypeClass.VIEW


def test_structured_properties_scalar_wrapped_in_list() -> None:
    agent = Agent(
        id="support",
        name="Support",
        structured_properties={
            "io.acryl.agent.framework": "LangChain",
            "io.acryl.agent.rating": 4.5,
        },
    )
    sp = next(a for a in _aspects(agent) if isinstance(a, StructuredPropertiesClass))
    by_urn = {p.propertyUrn: p.values for p in sp.properties}
    assert by_urn["urn:li:structuredProperty:io.acryl.agent.framework"] == ["LangChain"]
    assert by_urn["urn:li:structuredProperty:io.acryl.agent.rating"] == [4.5]


def test_structured_properties_list_value_preserved() -> None:
    agent = Agent(
        id="support",
        name="Support",
        structured_properties={"io.acryl.agent.tags": ["a", "b"]},
    )
    sp = next(a for a in _aspects(agent) if isinstance(a, StructuredPropertiesClass))
    assert sp.properties[0].values == ["a", "b"]


def test_owner_bare_string_becomes_technical_corpuser() -> None:
    agent = Agent(id="support", name="Support", owners=["jdoe"])
    own = next(a for a in _aspects(agent) if isinstance(a, OwnershipClass))
    assert len(own.owners) == 1
    assert own.owners[0].owner == "urn:li:corpuser:jdoe"
    assert own.owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


def test_owner_object_preserves_urn_and_type() -> None:
    agent = Agent(
        id="support",
        name="Support",
        owners=[
            AgentOwner(
                id="urn:li:corpGroup:platform",
                type=OwnershipTypeClass.BUSINESS_OWNER,
            )
        ],
    )
    own = next(a for a in _aspects(agent) if isinstance(a, OwnershipClass))
    assert own.owners[0].owner == "urn:li:corpGroup:platform"
    assert own.owners[0].type == OwnershipTypeClass.BUSINESS_OWNER


def test_platform_emits_data_platform_instance() -> None:
    agent = Agent(id="support", name="Support", platform="langchain")
    dpi = next(a for a in _aspects(agent) if isinstance(a, DataPlatformInstanceClass))
    assert dpi.platform == "urn:li:dataPlatform:langchain"


def test_skill_urn_validation_rejects_wrong_entity_type() -> None:
    with pytest.raises(InvalidUrnError):
        Agent(id="support", name="Support", skills=["urn:li:api:not-a-skill"])


def test_tool_urn_validation_rejects_wrong_entity_type() -> None:
    with pytest.raises(InvalidUrnError):
        Agent(id="support", name="Support", tools=["urn:li:agentSkill:not-a-tool"])


def test_dataset_urn_validation_rejects_non_dataset() -> None:
    with pytest.raises(InvalidUrnError):
        Agent(id="support", name="Support", consumes_datasets=["urn:li:api:x"])


def test_emit_sends_all_mcps_and_returns_urn() -> None:
    class _RecordingEmitter:
        def __init__(self) -> None:
            self.emitted: List[MetadataChangeProposalWrapper] = []

        def emit(self, mcp, callback=None):  # type: ignore[no-untyped-def]
            self.emitted.append(mcp)

    agent = Agent(id="support", name="Support", tools=[TOOL_URN])
    emitter = _RecordingEmitter()
    urn = agent.emit(emitter)  # type: ignore[arg-type]
    assert urn == "urn:li:aiAgent:support"
    assert all(m.entityUrn == urn for m in emitter.emitted)
    assert any(isinstance(m.aspect, AIAgentInfoClass) for m in emitter.emitted)
