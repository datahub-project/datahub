from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    SnowflakeOpenflowSource,
)


def _capabilities():
    # The @capability decorator stores CapabilitySetting objects and exposes them
    # via get_capabilities(); there is no get_capability_report(). get_capabilities
    # is added to the class dynamically by the decorator, so it is invisible to
    # mypy's static view of the class -- same pattern as SnowflakeV2Source.
    return {
        setting.capability: setting
        for setting in SnowflakeOpenflowSource.get_capabilities()  # type: ignore[attr-defined]
    }


def test_supported_capabilities_are_declared():
    capabilities = _capabilities()
    for capability in [
        SourceCapability.CONTAINERS,
        SourceCapability.LINEAGE_COARSE,
        SourceCapability.OWNERSHIP,
        SourceCapability.PLATFORM_INSTANCE,
        SourceCapability.DELETION_DETECTION,
        SourceCapability.TEST_CONNECTION,
    ]:
        assert capabilities[capability].supported is True


def test_declined_capabilities_are_declared_not_omitted():
    # Declaring these as unsupported is what puts the "use the snowflake source
    # instead" guidance in the generated docs. Omitting them just looks like a
    # gap.
    capabilities = _capabilities()
    declined = {
        SourceCapability.SCHEMA_METADATA,
        SourceCapability.LINEAGE_FINE,
        SourceCapability.DATA_PROFILING,
        SourceCapability.USAGE_STATS,
        SourceCapability.TAGS,
        SourceCapability.DOMAINS,
    }
    for capability in declined:
        assert capabilities[capability].supported is False

    # Pins membership, not just presence: a capability declared unsupported here
    # by mistake would pass a subset check silently.
    unsupported = {
        capability
        for capability, setting in capabilities.items()
        if not setting.supported
    }
    assert unsupported == declined


def test_exactly_twelve_capabilities_are_declared():
    assert len(_capabilities()) == 12


def test_container_capability_names_both_container_subtypes():
    modifiers = _capabilities()[SourceCapability.CONTAINERS].subtype_modifier
    assert SourceCapabilityModifier.OPENFLOW_DEPLOYMENT in modifiers
    assert SourceCapabilityModifier.OPENFLOW_RUNTIME in modifiers
