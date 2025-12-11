# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/tag_apply_to_dataset.py
from datahub.sdk import DataHubClient, Dataset, Tag

client = DataHubClient.from_env()

# Create Dataset entity
dataset = Dataset(platform="snowflake", name="db.schema.customers", env="PROD")

# Create Tag entity
tag = Tag(name="pii")

# Apply tag to dataset
dataset.add_tag(tag.urn)

# Update the dataset with the new tag
client.entities.upsert(dataset)

print(f"Applied tag {tag.urn} to dataset {dataset.urn}")
