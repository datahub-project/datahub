from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn
from datahub.sdk import DataHubClient, GlossaryNodeUrn
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

term = GlossaryTerm(
    id="3a4b5c6d",
    display_name="Personally Identifiable Information",
    definition="Information that can be used to identify, contact, or locate a single person, or to identify an individual in context. Examples include name, email address, phone number, and social security number.",
    parent_node=GlossaryNodeUrn("2f3a4b5c"),
    custom_properties={
        "sensitivity_level": "HIGH",
        "data_retention_period": "7_years",
        "regulatory_framework": "GDPR,CCPA",
    },
    owners=[CorpUserUrn("datahub"), CorpGroupUrn("privacy-team")],
    links=[
        (
            "https://wiki.company.com/privacy/pii-guidelines",
            "Internal PII Handling Guidelines",
        ),
        ("https://gdpr.eu/", "GDPR Official Documentation"),
    ],
)

client.entities.upsert(term)
