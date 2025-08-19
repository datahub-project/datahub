from datahub.sdk import DataHubClient, MLPrimaryKeyUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MLPrimaryKeyUrn.from_string(...)
mlprimarykey_urn = MLPrimaryKeyUrn("user_features", "user_id")

mlprimarykey_entity = client.entities.get(mlprimarykey_urn)
print("MLPrimary Key name:", mlprimarykey_entity.name)
print("MLPrimary Key feature table:", mlprimarykey_entity.feature_table_urn)
print("MLPrimary Key description:", mlprimarykey_entity.description)
