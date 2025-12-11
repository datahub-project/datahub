# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/corpuser_create_with_groups.py
import logging

from datahub.api.entities.corpuser.corpuser import CorpUser
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a user with group memberships
user = CorpUser(
    id="jsmith",
    display_name="Jane Smith",
    email="jsmith@company.com",
    title="Data Analyst",
    first_name="Jane",
    last_name="Smith",
    full_name="Jane Smith",
    department_name="Analytics",
    country_code="US",
    groups=["data-engineering", "analytics-team"],
)

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Emit the user entity with group memberships
for event in user.generate_mcp():
    datahub_graph.emit(event)

log.info(f"Created user {user.urn} with group memberships")
