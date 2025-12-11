# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.emitter.mcp_builder import DatabaseKey
from datahub.sdk import Container, DataHubClient

client = DataHubClient.from_env()

container = Container(
    container_key=DatabaseKey(platform="snowflake", database="my_database"),
    display_name="MY_DATABASE",
)

client.entities.upsert(container)
