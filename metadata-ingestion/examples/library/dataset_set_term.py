import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# First we get the current terms
gms_endpoint = "http://localhost:8080"
rest_emitter = DatahubRestEmitter(gms_server=gms_endpoint)

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

term_to_add = make_term_urn("Classification.HighlyConfidential")
term_association_to_add = GlossaryTermAssociationClass(urn=term_to_add)
# an audit stamp that basically says we have no idea when these terms were added to this dataset
# change the time value to (time.time() * 1000) if you want to specify the current time of running this code as the time of the application
unknown_audit_stamp = AuditStampClass(time=0, actor="urn:li:corpuser:ingestion")

# create a brand new terms aspect
terms_aspect = GlossaryTermsClass(
    terms=[term_association_to_add],
    auditStamp=unknown_audit_stamp,
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=dataset_urn,
    aspectName="glossaryTerms",
    aspect=terms_aspect,
)
rest_emitter.emit(event)
log.info(f"Attached term {term_to_add} to dataset {dataset_urn}")
