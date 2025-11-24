# Inlined from metadata-ingestion/examples/library/delete_document.py
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Delete a document
result = doc_api.delete(urn="urn:li:document:my-tutorial-doc")

print(f"Document deleted: {result}")

# You can also check if a document exists before deleting
if doc_api.exists(urn="urn:li:document:another-doc"):
    result = doc_api.delete(urn="urn:li:document:another-doc")
    print(f"Document deleted: {result}")
else:
    print("Document does not exist")
