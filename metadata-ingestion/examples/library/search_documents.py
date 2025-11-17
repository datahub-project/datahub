# Inlined from metadata-ingestion/examples/library/search_documents.py
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Search for documents with a text query
results = doc_api.search(query="data quality", start=0, count=10)

print(f"Found {results['total']} documents")
for doc in results["documents"]:
    print(f"  - {doc['info']['title']} ({doc['urn']})")

# Search with filters
results = doc_api.search(
    query="tutorial", types=["Tutorial"], states=["PUBLISHED"], count=20
)

print(f"\nPublished tutorials: {results['total']}")

# Search by domain
results = doc_api.search(domains=["urn:li:domain:engineering"], states=["PUBLISHED"])

print(f"\nEngineering domain documents: {results['total']}")

# Search by owner
results = doc_api.search()

print(f"\nDocuments owned by John: {results['total']}")
