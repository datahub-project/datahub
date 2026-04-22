import warnings

from datahub.errors import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.sdk import DataHubClient, GlossaryNode  # noqa: E402

client = DataHubClient.from_env()

# Top-level glossary node
node = GlossaryNode(
    name="Finance",
    display_name="Financial Metrics",
    definition="Category for all financial and accounting-related business terms including revenue, costs, and profitability measures.",
)

# Child node nested under Finance
child_node = GlossaryNode(
    name="RevenueMetrics",
    display_name="Revenue Metrics",
    definition="Metrics related to revenue recognition and reporting.",
    parent_node=node,
)

client.entities.upsert(node)
client.entities.upsert(child_node)
