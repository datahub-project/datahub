from datahub.metadata.urns import DatasetUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)


lineage_client.add_dataset_transform_lineage(
    upstream=DatasetUrn(platform="snowflake", name="source_table"),
    downstream=DatasetUrn(platform="snowflake", name="target_table"),
)
# when no column_lineage is provided, table-level lineage is inferred.
# can also provide a column_lineage dict to specify column-level lineage.
# e.g.
# column_lineage={
#     "customer_id": ["id"],
#     "full_name": ["first_name", "last_name"],
# }
