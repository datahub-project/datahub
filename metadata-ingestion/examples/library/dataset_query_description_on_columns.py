from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="fct_users_created", env="PROD")
)

# Print descriptions for each column
column_descriptions = {}
for field in dataset.schema:
    column_descriptions[field.field_path] = field.description

print(column_descriptions)
