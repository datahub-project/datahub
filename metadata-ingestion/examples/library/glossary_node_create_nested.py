import warnings

from datahub.errors import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.sdk import DataHubClient, GlossaryNode  # noqa: E402

client = DataHubClient.from_env()

parent_node = GlossaryNode(
    name="Finance",
    display_name="Finance",
    definition="Top-level category for financial metrics and terms.",
)

child_node = GlossaryNode(
    name="RevenueMetrics",
    display_name="Revenue Metrics",
    definition="Metrics related to revenue recognition and reporting.",
    parent_node=parent_node,
)

client.entities.upsert(parent_node)
client.entities.upsert(child_node)
