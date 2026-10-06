# metadata-ingestion/examples/library/application_query_rest_api.py
import json

from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import ApplicationPropertiesClass
from datahub.metadata.urns import ApplicationUrn

graph = get_default_graph()

application_urn = ApplicationUrn("customer-analytics-service")

props = graph.get_aspect(
    entity_urn=str(application_urn), aspect_type=ApplicationPropertiesClass
)
if props is None:
    raise SystemExit(f"Application not found: {application_urn}")

print(f"Application: {application_urn}")
print(f"Application Name: {props.name}")
print(f"Description: {props.description}")
if props.customProperties:
    print(f"Custom Properties: {json.dumps(props.customProperties, indent=2)}")
