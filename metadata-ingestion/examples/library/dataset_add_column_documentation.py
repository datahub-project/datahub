# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
