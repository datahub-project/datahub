from datahub.metadata.urns import DatasetUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)


lineage_client.add_dataset_transform_lineage(
    upstream=DatasetUrn(platform="snowflake", name="customer_data"),
    downstream=DatasetUrn(platform="snowflake", name="customer_metrics"),
    column_lineage={
        "customer_id": ["id"],
        "full_name": ["first_name", "last_name"],
        "total_spend": ["purchase_amount"],
        "loyalty_score": ["purchase_frequency", "purchase_amount"],
    },
)
