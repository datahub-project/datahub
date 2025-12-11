# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DatasetUrn, DomainUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="snowflake", name="example_dataset"))

# If you don't know the domain urn, you can look it up:
# domain_urn = client.resolve.domain(name="marketing")

# NOTE: This will overwrite the existing domain
dataset.set_domain(DomainUrn(id="marketing"))

client.entities.update(dataset)
