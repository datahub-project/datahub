from datahub.sdk import DataHubClient, MLPrimaryKeyUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MLPrimaryKeyUrn.from_string(...)
mlprimarykey_urn = MLPrimaryKeyUrn("user_features", "user_id")

mlprimarykey_entity = client.entities.get(mlprimarykey_urn)
print("MLPrimaryKey name:", mlprimarykey_entity.name)
print("MLPrimaryKey feature table:", mlprimarykey_entity.feature_table_urn)
print("MLPrimaryKey description:", mlprimarykey_entity.description)
