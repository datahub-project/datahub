from typing import Final

# GraphQL field the Stream Catalog exposes Kafka topics under.
TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Read-only query for topics in this Schema Registry environment. Unknown fields
# fail the whole query, so keep the selection tight. Cluster field is
# `logical_cluster_id` (not `clusterId`; verified 2026-08-05). Pagination
# placeholders: see LIMIT_PLACEHOLDER in confluent.constants.
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
