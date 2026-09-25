from datahub.api.entities.agent.agent import Agent
from datahub.metadata.schema_classes import (
    VersioningSchemeClass,
    VersionPropertiesClass,
)


def _version_properties(agent: Agent) -> VersionPropertiesClass:
    aspects = [m.aspect for m in agent.generate_mcp()]
    vps = [a for a in aspects if isinstance(a, VersionPropertiesClass)]
    assert len(vps) == 1
    return vps[0]


def test_no_version_emits_no_version_properties() -> None:
    agent = Agent(id="support-agent", name="Support Agent")
    aspects = [m.aspect for m in agent.generate_mcp()]
    assert not any(isinstance(a, VersionPropertiesClass) for a in aspects)


def test_version_emits_version_properties_with_derived_defaults() -> None:
    agent = Agent(id="support-agent", name="Support Agent", version="2.0")
    vp = _version_properties(agent)
    assert vp.versionSet == "urn:li:versionSet:(support-agent,aiAgent)"
    assert vp.version.versionTag == "2.0"
    assert vp.versioningScheme == VersioningSchemeClass.LEXICOGRAPHIC_STRING


def test_shared_version_set_groups_versions() -> None:
    old = Agent(
        id="support-agent-v1.0",
        name="Support Agent",
        version="1.0",
        version_set="support-agent",
    )
    current = Agent(id="support-agent", name="Support Agent", version="2.0")
    assert (
        _version_properties(old).versionSet
        == _version_properties(current).versionSet
        == "urn:li:versionSet:(support-agent,aiAgent)"
    )


def test_full_version_set_urn_passed_through() -> None:
    agent = Agent(
        id="a",
        name="A",
        version="1.0",
        version_set="urn:li:versionSet:(custom-family,aiAgent)",
    )
    assert _version_properties(agent).versionSet == (
        "urn:li:versionSet:(custom-family,aiAgent)"
    )


def test_sort_ids_order_numerically_under_lexicographic_scheme() -> None:
    sort_ids = [
        _version_properties(Agent(id="a", name="A", version=v)).sortId
        for v in ("1.9", "1.10", "2.0")
    ]
    assert sort_ids == sorted(sort_ids)


def test_explicit_sort_id_overrides_derivation() -> None:
    agent = Agent(id="a", name="A", version="nightly", version_sort_id="ZZZ")
    assert _version_properties(agent).sortId == "ZZZ"


def test_platform_emits_data_platform_instance() -> None:
    from datahub.metadata.schema_classes import DataPlatformInstanceClass

    agent = Agent(id="a", name="A", platform="langchain")
    aspects = [m.aspect for m in agent.generate_mcp()]
    dpis = [a for a in aspects if isinstance(a, DataPlatformInstanceClass)]
    assert len(dpis) == 1
    assert dpis[0].platform == "urn:li:dataPlatform:langchain"
