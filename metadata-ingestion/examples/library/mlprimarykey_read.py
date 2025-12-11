# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient, MLPrimaryKeyUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MLPrimaryKeyUrn.from_string(...)
mlprimarykey_urn = MLPrimaryKeyUrn("user_features", "user_id")

mlprimarykey_entity = client.entities.get(mlprimarykey_urn)
print("MLPrimaryKey name:", mlprimarykey_entity.name)
print("MLPrimaryKey feature table:", mlprimarykey_entity.feature_table_urn)
print("MLPrimaryKey description:", mlprimarykey_entity.description)
