# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

user_email = "bar@acryl.io"

user: CorpUser = CorpUser(
    id=user_email,
    display_name="The Bar",
    email=user_email,
    title="Software Engineer",
    first_name="The",
    last_name="Bar",
    full_name="The Bar",
)

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
for event in user.generate_mcp(
    generation_config=CorpUserGenerationConfig(override_editable=False)
):
    datahub_graph.emit(event)
log.info(f"Upserted user {user.urn}")
