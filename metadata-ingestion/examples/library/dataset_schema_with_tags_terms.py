# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient, Dataset, GlossaryTermUrn, TagUrn

client = DataHubClient.from_env()

dataset = Dataset(
    platform="hive",
    name="foodb.barTable",
    env="PROD",
    schema=[
        (
            "address.zipcode",
            "VARCHAR(100)",
            "This is the zipcode of the address. Specified using extended form and limited to addresses in the United States",
        ),
    ],
)

dataset["address.zipcode"].add_tag(TagUrn("location"))
dataset["address.zipcode"].add_term(GlossaryTermUrn("Classification.PII"))

client.entities.upsert(dataset)
