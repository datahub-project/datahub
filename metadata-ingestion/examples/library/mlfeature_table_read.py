# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
