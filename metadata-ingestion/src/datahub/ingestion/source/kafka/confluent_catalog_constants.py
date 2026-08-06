from typing import Final

TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

TOPIC_CATALOG_QUERY: Final[str] = """
{
  kafka_topic(limit: {limit}, offset: {offset}) {
    name
    qualifiedName
    logical_cluster_id
    tags
    business_metadata {
      name
      value
    }
  }
}
"""
