# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
