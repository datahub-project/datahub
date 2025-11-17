# Inlined from metadata-ingestion/examples/library/update_document.py
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Update document contents
result = doc_api.update(
    urn="urn:li:document:my-tutorial-doc",
    text="# Updated Getting Started Guide\n\nThis is the updated content...",
)

print(f"Document contents updated: {result}")

# Update document title
result = doc_api.update(
    urn="urn:li:document:my-tutorial-doc", title="Updated Tutorial Title"
)

print(f"Document title updated: {result}")

# Update both contents and title
result = doc_api.update(
    urn="urn:li:document:my-tutorial-doc",
    text="# Comprehensive Guide\n\nFully updated content...",
    title="Comprehensive DataHub Guide",
)

print(f"Document fully updated: {result}")
