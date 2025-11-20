# metadata-ingestion/examples/library/dataflow_add_tags_terms.py
from datahub.metadata.urns import DataFlowUrn, GlossaryTermUrn, TagUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Get the existing DataFlow
dataflow_urn = DataFlowUrn("airflow", "daily_sales_pipeline", "prod")
dataflow = client.entities.get(dataflow_urn)

# Add tags
dataflow.add_tag(TagUrn(name="pii"))
dataflow.add_tag(TagUrn(name="quarterly-review"))

# Add glossary terms
dataflow.add_term(GlossaryTermUrn("DataQuality.Validated"))
dataflow.add_term(GlossaryTermUrn("BusinessCritical.Revenue"))

# Save changes
client.entities.upsert(dataflow)

print(f"Updated DataFlow: {dataflow.urn}")
print(f"Tags: {[str(tag) for tag in dataflow.tags] if dataflow.tags else []}")
print(f"Terms: {[str(term) for term in dataflow.terms] if dataflow.terms else []}")
