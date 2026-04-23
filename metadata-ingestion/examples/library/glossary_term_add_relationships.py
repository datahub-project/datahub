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
        GlossaryTermUrn("Classification.PII"),
        GlossaryTermUrn("Classification.Sensitive"),
    ],
    related_terms=[
        GlossaryTermUrn("PersonalInformation.PhoneNumber"),
        GlossaryTermUrn("PersonalInformation.Contact"),
    ],
)

# Address — is a type of PII, has components (ZipCode, Street, City, Country)
address_term = GlossaryTerm(
    id="5e6f7a8b",
    display_name="Address",
    definition="Physical addresses that can identify individuals.",
    is_a=[GlossaryTermUrn("Classification.PII")],
    has_a=[
        GlossaryTermUrn("PersonalInformation.ZipCode"),
        GlossaryTermUrn("PersonalInformation.Street"),
        GlossaryTermUrn("PersonalInformation.City"),
        GlossaryTermUrn("PersonalInformation.Country"),
    ],
)

# ColorEnum — an enumeration with fixed allowed values
color_enum_term = GlossaryTerm(
    id="9c0d1e2f",
    display_name="Color",
    definition="An enumeration of allowed color values.",
    values=[
        GlossaryTermUrn("Colors.Red"),
        GlossaryTermUrn("Colors.Green"),
        GlossaryTermUrn("Colors.Blue"),
        GlossaryTermUrn("Colors.Yellow"),
    ],
)

client.entities.upsert(email_term)
client.entities.upsert(address_term)
client.entities.upsert(color_enum_term)
