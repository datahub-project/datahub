from datetime import datetime

from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataHubClient
from datahub.sdk.chart import Chart

client = DataHubClient.from_env()

chart = Chart(
    platform="looker",
    name="my_chart_1",
    display_name="Baz Chart 1",
    description="Sample Baz chart",
    last_modified=datetime.now(),
    input_datasets=[
        DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    ],
)

chart.add_input_dataset(DatasetUrn(platform="hdfs", name="dataset2", env="PROD"))

client.entities.upsert(chart)
