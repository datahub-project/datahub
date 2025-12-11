# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/tag_add_ownership.py
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a tag with ownership
tag = Tag(
    name="data_quality",
    owners=[
        CorpUserUrn("data_steward"),
    ],
)

# Upsert the tag
client.entities.upsert(tag)

print(f"Created tag with ownership: {tag.urn}")
