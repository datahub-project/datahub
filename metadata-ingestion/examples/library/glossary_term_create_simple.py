from datahub.sdk import DataHubClient
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

term = GlossaryTerm(
    id="e5f67890",
    display_name="Rate of Return",
    definition="A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
)

client.entities.upsert(term)
