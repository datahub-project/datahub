import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import RolePropertiesClass

# Create the role URN
# Role URNs follow the pattern: urn:li:role:{role_id}
role_urn = "urn:li:role:snowflake_reader_role"

# Define the role properties
role_properties = RolePropertiesClass(
    name="Snowflake Reader Role",
    description="Provides read-only access to analytics datasets in Snowflake",
    type="READ",
    requestUrl="https://mycompany.okta.com/access/request/snowflake-reader",
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    entityUrn=role_urn,
    aspect=role_properties,
)

# Emit the metadata change
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
emitter.emit(mcp)
print(f"Created role: {role_urn}")
