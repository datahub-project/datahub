from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import FormUrn

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

form_urn = FormUrn("metadata_initiative_1")

# Hard delete the form
graph.delete_entity(urn=str(form_urn), hard=True)
# Delete references to this form (must do)
graph.delete_references_to_urn(urn=str(form_urn), dry_run=False)

print(f"Deleted form {form_urn}")
