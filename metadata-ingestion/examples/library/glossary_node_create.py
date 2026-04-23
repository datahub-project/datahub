from datahub.sdk import DataHubClient, GlossaryNode

client = DataHubClient.from_env()

# Top-level glossary node
node = GlossaryNode(
    id="7f3d2c1a",
    display_name="Financial Metrics",
    definition="Category for all financial and accounting-related business terms including revenue, costs, and profitability measures.",
)

# Child node nested under Finance
child_node = GlossaryNode(
    id="4b5e6f7a",
    display_name="Revenue Metrics",
    definition="Metrics related to revenue recognition and reporting.",
    parent_node=node,
)

client.entities.upsert(node)
client.entities.upsert(child_node)
