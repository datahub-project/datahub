# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/corpuser_update_profile.py
import logging

from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Update a user's editable profile information
user = CorpUser(
    id="jdoe",
    email="jdoe@company.com",
    description="Passionate about data quality and building reliable data pipelines. "
    "10+ years of experience in data engineering.",
    slack="@jdoe",
    phone="+1-555-0123",
    picture_link="https://company.com/photos/jdoe.jpg",
)

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Emit with override_editable=True to update editable fields
for event in user.generate_mcp(
    generation_config=CorpUserGenerationConfig(override_editable=True)
):
    datahub_graph.emit(event)

log.info(f"Updated profile for user {user.urn}")
