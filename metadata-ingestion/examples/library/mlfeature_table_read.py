from datahub.sdk import DataHubClient, MLFeatureTableUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MLFeatureTableUrn.from_string(...)
mlfeature_table_urn = MLFeatureTableUrn(
    "feast", "test_feature_table_all_feature_dtypes"
)

mlfeature_table_entity = client.entities.get(mlfeature_table_urn)
print("MLFeature Table name:", mlfeature_table_entity.name)
print("MLFeature Table platform:", mlfeature_table_entity.platform)
print("MLFeature Table description:", mlfeature_table_entity.description)
