from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import AccessClass, RoleAssociationClass

# Create the dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="analytics_db.public.user_events", env="PROD"
)

# Define the roles that provide access to this dataset
# Role URNs follow the pattern: urn:li:role:{role_id}
access_aspect = AccessClass(
    roles=[
        RoleAssociationClass(urn="urn:li:role:snowflake_reader_role"),
        RoleAssociationClass(urn="urn:li:role:snowflake_writer_role"),
        RoleAssociationClass(urn="urn:li:role:snowflake_admin_role"),
    ]
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=access_aspect,
)

# Emit the metadata change
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit(mcp)
print(f"Associated roles with dataset: {dataset_urn}")
