# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="realestate_db.sales"))

# Add dataset documentation
documentation = """## The Real Estate Sales Dataset
This is a really important Dataset that contains all the relevant information about sales that have happened organized by address.
"""
dataset.set_description(documentation)

# Add link to institutional memory
dataset.add_link(
    (
        "https://wikipedia.com/real_estate",
        "This is the definition of what real estate means",  # link description
    )
)

client.entities.update(dataset)
