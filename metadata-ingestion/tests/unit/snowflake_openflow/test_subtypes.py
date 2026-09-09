from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier


def test_subtypes_generate_capability_modifiers():
    # SourceCapabilityModifier is generated from the subtype enums, so the
    # @capability decorators in the source module only work once the subtypes
    # above exist. Distinct names matter: the generator keeps the first member
    # for a repeated name and silently drops the rest.
    assert SourceCapabilityModifier.OPENFLOW_DEPLOYMENT == "Openflow Deployment"
    assert SourceCapabilityModifier.OPENFLOW_RUNTIME == "Openflow Runtime"
    assert SourceCapabilityModifier.OPENFLOW_CONNECTOR == "Openflow Connector"
