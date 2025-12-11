# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.schema_classes import DomainsClass
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="snowflake", name="example_dataset"))

# Remove the dataset from its current domain by setting an empty domains list
dataset._set_aspect(DomainsClass(domains=[]))

client.entities.update(dataset)
