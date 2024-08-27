# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Query multiple aspects from entity
query = """
mutation unsetDomain {
    unsetDomain(
      entityUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
    )
}
"""
result = graph.execute_graphql(query=query)

print(result)
