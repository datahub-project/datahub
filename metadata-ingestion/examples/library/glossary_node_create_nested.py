from datahub.sdk import DataHubClient, GlossaryNode

client = DataHubClient.from_env()

parent_node = GlossaryNode(
    id="7f3d2c1a",
    display_name="Finance",
    definition="Top-level category for financial metrics and terms.",
)

child_node = GlossaryNode(
    id="4b5e6f7a",
    display_name="Revenue Metrics",
    definition="Metrics related to revenue recognition and reporting.",
    parent_node=parent_node,
)

client.entities.upsert(parent_node)
client.entities.upsert(child_node)
