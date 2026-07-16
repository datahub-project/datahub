from datahub.metadata.urns import ChartUrn, DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# add dataset -> chart lineage
client.lineage.add_lineage(
    upstream=DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    downstream=ChartUrn(dashboard_tool="looker", chart_id="chart_1"),
)
