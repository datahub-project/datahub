from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.analytics.engine import AnalyticsEngine
from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection


class SnowflakeAnalyticsEngine(AnalyticsEngine):
    def __init__(self, account: str, graph: DataHubGraph) -> None:
        self.account = account
        self.graph = graph
        self.connection = SnowflakeConnection.from_datahub(graph=graph)
