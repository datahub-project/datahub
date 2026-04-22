from datahub.sdk import DataHubClient, GlossaryNode
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

# Create multi-level hierarchy:
# DataGovernance
#   ├── Classification
#   │   ├── Public (term)
#   │   └── Confidential (term)
#   └── PersonalInformation
#       ├── Email (term)
#       └── SSN (term)

root = GlossaryNode(
    name="DataGovernance",
    display_name="Data Governance",
    definition="Top-level governance structure for data classification and management.",
)

classification = GlossaryNode(
    name="Classification",
    display_name="Classification",
    definition="Data classification categories.",
    parent_node=root,
)

pii = GlossaryNode(
    name="PersonalInformation",
    display_name="Personal Information",
    definition="Personal and sensitive data categories.",
    parent_node=root,
)

terms = [
    GlossaryTerm(
        name="Public",
        display_name="Public",
        definition="Publicly available data with no restrictions.",
        parent_node=classification,
    ),
    GlossaryTerm(
        name="Confidential",
        display_name="Confidential",
        definition="Restricted access data for internal use only.",
        parent_node=classification,
    ),
    GlossaryTerm(
        name="Email",
        display_name="Email Address",
        definition="Email addresses that can identify individuals.",
        parent_node=pii,
    ),
    GlossaryTerm(
        name="SSN",
        display_name="Social Security Number",
        definition="Social Security Numbers - highly sensitive personal identifiers.",
        parent_node=pii,
    ),
]

for entity in [root, classification, pii, *terms]:
    client.entities.upsert(entity)
