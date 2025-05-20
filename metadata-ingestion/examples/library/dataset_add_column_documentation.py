from datahub.sdk import DataHubClient, Dataset, DatasetUrn

client = DataHubClient.from_env()

dataset: Dataset = client.entities.get(
    DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
    )
)

dataset["user_name"].set_description(
    "Name of the user who was deleted. This description was updated via the Python SDK."
)

client.entities.update(dataset)
