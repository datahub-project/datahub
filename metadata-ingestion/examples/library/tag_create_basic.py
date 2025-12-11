# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/tag_create_basic.py
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a basic tag with properties
tag = Tag(
    name="pii",
    display_name="Personally Identifiable Information",
    description="This tag indicates that the asset contains PII data and should be handled according to data privacy regulations.",
    color="#FF0000",
)

# Upsert the tag
client.entities.upsert(tag)

print(f"Created tag: {tag.urn}")
