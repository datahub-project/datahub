# metadata-ingestion/examples/library/query_add_subjects.py
import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import QuerySubjectClass, QuerySubjectsClass
from datahub.metadata.urns import DatasetUrn, QueryUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

query_urn = QueryUrn("my-unique-query-id")

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

existing_subjects = graph.get_aspect(
    entity_urn=query_urn.urn(),
    aspect_type=QuerySubjectsClass,
)

subjects = existing_subjects.subjects if existing_subjects else []

new_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
)
new_subject = QuerySubjectClass(entity=new_dataset_urn.urn())

if new_subject not in subjects:
    subjects.append(new_subject)

query_subjects_aspect = QuerySubjectsClass(subjects=subjects)

event = MetadataChangeProposalWrapper(
    entityUrn=query_urn.urn(),
    aspect=query_subjects_aspect,
)

emitter.emit(event)
log.info(f"Added subject to query {query_urn}")
