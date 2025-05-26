from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
datajob_urn = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,PROD.example_dag,PROD),example_datajob)"
)

client.lineage.add_lineage(upstream=datajob_urn, downstream=dataset_urn)

# you can add datajob lineage with following combinations:
# 1. dataset -> datajob
# 2. datajob -> dataset
# 3. datajob -> datajob
