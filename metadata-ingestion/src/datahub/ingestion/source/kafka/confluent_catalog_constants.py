from typing import Final

TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Unknown fields fail the whole query. Cluster field is `logical_cluster_id`
# (not `clusterId`; verified 2026-08-05). Pagination: see LIMIT_PLACEHOLDER.
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
