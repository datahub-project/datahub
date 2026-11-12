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
    id="8b9c0d1e",
    display_name="Data Governance",
    definition="Top-level governance structure for data classification and management.",
)

classification = GlossaryNode(
    id="2f3a4b5c",
    display_name="Classification",
    definition="Data classification categories.",
    parent_node=root,
)

pii = GlossaryNode(
    id="6d7e8f90",
    display_name="Personal Information",
    definition="Personal and sensitive data categories.",
    parent_node=root,
)

terms = [
    GlossaryTerm(
        id="abcdef12",
        display_name="Public",
        definition="Publicly available data with no restrictions.",
        parent_node=classification,
    ),
    GlossaryTerm(
        id="34567890",
        display_name="Confidential",
        definition="Restricted access data for internal use only.",
        parent_node=classification,
    ),
    GlossaryTerm(
        id="abcd1234",
        display_name="Email Address",
        definition="Email addresses that can identify individuals.",
        parent_node=pii,
    ),
    GlossaryTerm(
        id="5678abcd",
        display_name="Social Security Number",
        definition="Social Security Numbers - highly sensitive personal identifiers.",
        parent_node=pii,
    ),
]

for entity in [root, classification, pii, *terms]:
    client.entities.upsert(entity)
