from datahub.metadata.urns import GlossaryTermUrn
from datahub.sdk import DataHubClient
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

# Email — is a type of PII and Sensitive, related to PhoneNumber and Contact
email_term = GlossaryTerm(
    id="1a2b3c4d",
    display_name="Email",
    definition="Email addresses that can identify individuals.",
    is_a=[
        GlossaryTermUrn("a1b2c3d4"),  # Classification.PII
        GlossaryTermUrn("b2c3d4e5"),  # Classification.Sensitive
    ],
    related_terms=[
        GlossaryTermUrn("c3d4e5f6"),  # PersonalInformation.PhoneNumber
        GlossaryTermUrn("d4e5f6a7"),  # PersonalInformation.Contact
    ],
)

# Address — is a type of PII, has components (ZipCode, Street, City, Country)
address_term = GlossaryTerm(
    id="5e6f7a8b",
    display_name="Address",
    definition="Physical addresses that can identify individuals.",
    is_a=[GlossaryTermUrn("a1b2c3d4")],  # Classification.PII
    has_a=[
        GlossaryTermUrn("e5f6a7b8"),  # PersonalInformation.ZipCode
        GlossaryTermUrn("f6a7b8c9"),  # PersonalInformation.Street
        GlossaryTermUrn("a7b8c9d0"),  # PersonalInformation.City
        GlossaryTermUrn("b8c9d0e1"),  # PersonalInformation.Country
    ],
)

# ColorEnum — an enumeration with fixed allowed values
color_enum_term = GlossaryTerm(
    id="9c0d1e2f",
    display_name="Color",
    definition="An enumeration of allowed color values.",
    values=[
        GlossaryTermUrn("c9d0e1f2"),  # Colors.Red
        GlossaryTermUrn("d0e1f2a3"),  # Colors.Green
        GlossaryTermUrn("e1f2a3b4"),  # Colors.Blue
        GlossaryTermUrn("f2a3b4c5"),  # Colors.Yellow
    ],
)

client.entities.upsert(email_term)
client.entities.upsert(address_term)
client.entities.upsert(color_enum_term)
