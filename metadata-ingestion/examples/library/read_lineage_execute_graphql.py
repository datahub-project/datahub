# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Query multiple aspects from entity
query = """
query scrollAcrossLineage($input: ScrollAcrossLineageInput!) {
  scrollAcrossLineage(input: $input) {
    searchResults {
      degree
      entity {
        urn
        type
      }
    }
  }
}
"""

variables = {
    "input": {
        "query": "*",
        "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)",
        "count": 10,
        "direction": "DOWNSTREAM",
        "orFilters": [
            {
                "and": [
                    {
                        "condition": "EQUAL",
                        "negated": "false",
                        "field": "degree",
                        "values": ["1", "2", "3+"],
                    }
                ]
            }
        ],
    }
}
result = graph.execute_graphql(query=query, variables=variables)

print(result)
