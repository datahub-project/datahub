import logging

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import CorpUserInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

user_urn = make_user_urn("bar@acryl.io")
event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=user_urn,
    aspect=CorpUserInfoClass(
        active=True,
        displayName="The Bar",
        email="bar@acryl.io",
        title="Software Engineer",
        firstName="The",
        lastName="Bar",
        fullName="The Bar",
    ),
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Upserted user {user_urn}")
