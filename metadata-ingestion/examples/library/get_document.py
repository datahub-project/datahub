# Inlined from metadata-ingestion/examples/library/get_document.py
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Get a document by URN
document = doc_api.get(urn="urn:li:document:my-tutorial-doc")

if document:
    print(f"Document: {document['info']['title']}")
    print(f"Type: {document.get('subType', 'N/A')}")
    print(f"Status: {document['info']['status']['state']}")
    print(f"\nContents:\n{document['info']['contents']['text']}")

    # Check for related entities
    if "relatedEntities" in document["info"]:
        related = document["info"]["relatedEntities"]
        print(f"\nRelated assets: {len(related.get('assetUrns', []))}")
else:
    print("Document not found")
