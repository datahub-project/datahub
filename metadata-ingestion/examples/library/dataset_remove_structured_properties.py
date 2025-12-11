# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient
from datahub.sdk.dataset import Dataset

client = DataHubClient.from_env()

# NOTE: structured porperties should be created before adding them to the dataset
# client.entities.get() also works to retrieve the dataset
dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    structured_properties={
        "urn:li:structuredProperty:sp1": ["PROD"],
        "urn:li:structuredProperty:sp2": [100.0],
    },
)

dataset.remove_structured_property("urn:li:structuredProperty:sp1")

client.entities.upsert(dataset)
