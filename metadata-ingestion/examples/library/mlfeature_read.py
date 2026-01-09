from datahub.sdk import DataHubClient, MLFeatureUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MLFeatureUrn.from_string(...)
mlfeature_urn = MLFeatureUrn(
    "test_feature_table_all_feature_dtypes", "test_BOOL_feature"
)

mlfeature_entity = client.entities.get(mlfeature_urn)
print("MLFeature name:", mlfeature_entity.name)
print("MLFeature table:", mlfeature_entity.feature_table_urn)
print("MLFeature description:", mlfeature_entity.description)
