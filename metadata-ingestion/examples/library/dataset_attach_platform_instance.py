# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(
        platform="mysql",
        name="production-mysql-cluster.ecommerce.customers",
        env="PROD",
    )
)

dataset._set_platform_instance(platform="mysql", instance="production-mysql-cluster")

client.entities.update(dataset)

print("Attached platform instance 'production-mysql-cluster'")
print(f"to dataset {dataset.urn}")
