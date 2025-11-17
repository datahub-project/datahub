# Inlined from metadata-ingestion/examples/library/publish_document.py
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Publish a document (make it visible to AI)
result = doc_api.publish(urn="urn:li:document:my-tutorial-doc")

print(f"Document published: {result}")

# Unpublish a document (hide it from AI)
result = doc_api.unpublish(urn="urn:li:document:my-tutorial-doc")

print(f"Document unpublished: {result}")
