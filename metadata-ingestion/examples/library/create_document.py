# Inlined from metadata-ingestion/examples/library/create_document.py
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Create a simple document
urn = doc_api.create(
    text="# Getting Started with DataHub\n\nThis tutorial will help you get started...",
    title="DataHub Tutorial",
    sub_type="Tutorial",
    state="PUBLISHED",
)

print(f"Created document with URN: {urn}")

# Create a document with full metadata
urn = doc_api.create(
    text="# Data Quality FAQ\n\n## Q: How do we measure data quality?\n\nA: We use...",
    title="Data Quality FAQ",
    id="faq-data-quality",
    sub_type="FAQ",
    state="PUBLISHED",
    related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"],
    owners=[{"urn": "urn:li:corpuser:john", "type": "TECHNICAL_OWNER"}],
)

print(f"Created document with metadata, URN: {urn}")
