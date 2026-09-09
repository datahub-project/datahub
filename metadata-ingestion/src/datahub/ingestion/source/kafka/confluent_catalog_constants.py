from typing import Final

TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Unknown fields fail the whole query. The cluster field is `logical_cluster_id`
# (not the more intuitive `clusterId` — Confluent Cloud rejects that name).
# Pagination placeholders: see LIMIT_PLACEHOLDER / OFFSET_PLACEHOLDER.
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
