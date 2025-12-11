# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a basic tag
basic_tag = Tag(name="deprecated")

# Create a more detailed tag with all properties
detailed_tag = Tag(
    name="data-quality",
    display_name="Data Quality",
    description="Tag used to mark datasets with quality issues or requirements",
    color="#FF5733",
    owners=[
        CorpUserUrn("data-team@company.com"),
    ],
)

# Upsert the tags
client.entities.upsert(basic_tag)
client.entities.upsert(detailed_tag)

print(f"Created basic tag: {basic_tag.urn}")
print(f"Created detailed tag: {detailed_tag.urn}")
