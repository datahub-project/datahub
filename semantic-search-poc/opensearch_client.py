"""
OpenSearch client for retrieving DataHub entities
"""
from typing import Dict, List, Any, Optional
from opensearchpy import OpenSearch
import json


class OpenSearchClient:
    """Client for interacting with DataHub's OpenSearch instance"""
    
    def __init__(self, host: str = "localhost", port: int = 9200, 
                 username: Optional[str] = None, password: Optional[str] = None):
        """Initialize OpenSearch client"""
        
        # Build connection parameters for OpenSearch
        auth = (username, password) if username and password else None
        
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=auth,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        # Test connection
        if not self.client.ping():
            raise ConnectionError(f"Failed to connect to OpenSearch at {host}:{port}")
        
        print(f"✅ Connected to OpenSearch at {host}:{port}")
    
    def get_sample_entities(self, index: str = "datasetindex_v2", 
                           sample_size: int = 10,
                           diversity_query: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Retrieve sample entities from OpenSearch
        
        Args:
            index: The index to query (default: datasetindex_v2)
            sample_size: Number of entities to retrieve
            diversity_query: Optional query to get diverse samples
        """
        
        # Default query to get diverse samples
        if diversity_query is None:
            # Get a mix of entities with different characteristics
            diversity_query = {
                "bool": {
                    "should": [
                        {"exists": {"field": "description"}},
                        {"exists": {"field": "tags"}},
                        {"exists": {"field": "customProperties"}},
                        {"match": {"name": "customer"}},
                        {"match": {"name": "pet"}},
                        {"match": {"name": "order"}},
                        {"match": {"name": "product"}},
                        {"match": {"platform": "dbt"}},
                        {"match": {"platform": "snowflake"}},
                        {"match": {"platform": "looker"}}
                    ],
                    "minimum_should_match": 1
                }
            }
        
        # Build search request
        search_body = {
            "size": sample_size,
            "query": diversity_query,
            "_source": [
                "name", "description", "qualifiedName", 
                "tags", "glossaryTerms", "customProperties",
                "fieldPaths", "fieldDescriptions", "platform", 
                "origin", "owners", "domains"
            ]
        }
        
        try:
            response = self.client.search(index=index, body=search_body)
            entities = [hit["_source"] for hit in response["hits"]["hits"]]
            
            print(f"📊 Retrieved {len(entities)} entities from {index}")
            return entities
            
        except Exception as e:
            print(f"❌ Error retrieving entities: {e}")
            return []
    
    def get_entity_counts(self) -> Dict[str, int]:
        """Get counts of entities in each index"""
        indices = [
            "datasetindex_v2",
            "chartindex_v2", 
            "dashboardindex_v2",
            "corpuserindex_v2",
            "domainindex_v2",
            "tagindex_v2"
        ]
        
        counts = {}
        for index in indices:
            try:
                response = self.client.count(index=index)
                counts[index] = response["count"]
            except:
                counts[index] = 0
        
        return counts
    
    def search_entities(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for entities matching a query string.
        
        Args:
            query: Search query
            limit: Maximum number of entities to return
            
        Returns:
            List of entities matching the query
        """
        # Search across dataset index (most populated)
        index = "datasetindex_v2"
        
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["name^2", "description", "qualifiedName", "customProperties"]
                }
            },
            "size": limit
        }
        
        try:
            response = self.client.search(index=index, body=search_body)
            entities = []
            
            for hit in response["hits"]["hits"]:
                entity = hit["_source"]
                entity["_score"] = hit["_score"]
                entities.append(entity)
            
            return entities
            
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def get_rich_entities(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get entities with rich metadata for better comparison
        """
        # Query for entities with lots of metadata
        rich_query = {
            "bool": {
                "must": [
                    {"exists": {"field": "description"}},
                    {"exists": {"field": "customProperties"}}
                ],
                "should": [
                    {"exists": {"field": "tags"}},
                    {"exists": {"field": "glossaryTerms"}},
                    {"exists": {"field": "fieldPaths"}}
                ]
            }
        }
        
        return self.get_sample_entities(
            index="datasetindex_v2",
            sample_size=limit,
            diversity_query=rich_query
        )
