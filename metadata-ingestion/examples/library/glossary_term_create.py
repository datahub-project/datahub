from datahub.sdk import DataHubClient
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

term = GlossaryTerm(
    id="a1b2c3d4",
    display_name="Customer Lifetime Value",
    definition="The total revenue a business can expect from a single customer account throughout the business relationship. This metric helps prioritize customer retention efforts and marketing spend.",
)

client.entities.upsert(term)
