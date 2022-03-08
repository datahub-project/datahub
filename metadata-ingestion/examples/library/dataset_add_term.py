import logging
from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

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
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

current_terms: Optional[GlossaryTermsClass] = graph.get_aspect_v2(
    entity_urn=dataset_urn,
    aspect="glossaryTerms",
    aspect_type=GlossaryTermsClass,
)

term_to_add = make_term_urn("Classification.HighlyConfidential")
term_association_to_add = GlossaryTermAssociationClass(urn=term_to_add)
# an audit stamp that basically says we have no idea when these terms were added to this dataset
# change the time value to (time.time() * 1000) if you want to specify the current time of running this code as the time
unknown_audit_stamp = AuditStampClass(time=0, actor="urn:li:corpuser:ingestion")
need_write = False
if current_terms:
    if term_to_add not in [x.urn for x in current_terms.terms]:
        # terms exist, but this term is not present in the current terms
        current_terms.terms.append(term_association_to_add)
        need_write = True
else:
    # create a brand new terms aspect
    current_terms = GlossaryTermsClass(
        terms=[term_association_to_add],
        auditStamp=unknown_audit_stamp,
    )
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspectName="glossaryTerms",
        aspect=current_terms,
    )
    graph.emit(event)
else:
    log.info(f"Term {term_to_add} already exists, omitting write")
