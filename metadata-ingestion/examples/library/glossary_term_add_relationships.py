import warnings

from datahub.errors import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.metadata.urns import GlossaryTermUrn  # noqa: E402
from datahub.sdk import DataHubClient  # noqa: E402
from datahub.sdk.glossary_term import GlossaryTerm  # noqa: E402

client = DataHubClient.from_env()

# Email — is a type of PII and Sensitive, related to PhoneNumber and Contact
email_term = GlossaryTerm(
    name="PersonalInformation.Email",
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
    name="PersonalInformation.Address",
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
    name="ColorEnum",
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
