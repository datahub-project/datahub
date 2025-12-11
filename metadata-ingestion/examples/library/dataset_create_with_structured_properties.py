# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

# Use the structured property created by structured_property_create_basic.py
retention_property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    structured_properties={retention_property_urn: [90.0]},
)

# update the structured property
dataset.set_structured_property(retention_property_urn, [365.0])

client.entities.upsert(dataset)
print(f"Created dataset: {dataset.urn}")
