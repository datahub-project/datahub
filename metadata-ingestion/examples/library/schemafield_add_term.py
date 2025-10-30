import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="snowflake", name="analytics.public.orders", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="customer_id"
)

current_terms = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.GlossaryTermsClass
)

term_to_add = builder.make_term_urn("CustomerIdentifier")
term_association = models.GlossaryTermAssociationClass(urn=term_to_add)

if current_terms and current_terms.terms:
    if term_to_add not in [term.urn for term in current_terms.terms]:
        current_terms.terms.append(term_association)
else:
    current_terms = models.GlossaryTermsClass(
        terms=[term_association],
        auditStamp=models.AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
    )

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_terms,
    )
)
