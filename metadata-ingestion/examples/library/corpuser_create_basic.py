# metadata-ingestion/examples/library/corpuser_create_basic.py
import logging
import os

from datahub.api.entities.corpuser.corpuser import CorpUser
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a basic user with essential information
user = CorpUser(
    id="jdoe",
    display_name="John Doe",
    email="jdoe@company.com",
    title="Senior Data Engineer",
    first_name="John",
    last_name="Doe",
    full_name="John Doe",
    department_name="Data Engineering",
    country_code="US",
)

# Create graph client
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
datahub_graph = DataHubGraph(DataHubGraphConfig(server=gms_server, token=token))

# Emit the user entity
for event in user.generate_mcp():
    datahub_graph.emit(event)

log.info(f"Created user {user.urn}")
