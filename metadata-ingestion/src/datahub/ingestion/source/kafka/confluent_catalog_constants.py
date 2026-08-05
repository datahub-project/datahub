from typing import Final

# GraphQL field the Stream Catalog exposes Kafka topics under.
TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Read-only query for the topics in the environment the Schema Registry endpoint
# belongs to. Kept to fields the catalog has carried since Stream Governance shipped,
# because an unknown field fails the whole query rather than returning null.
#
# Pagination is inlined as {limit}/{offset} placeholders (substituted by the client)
# rather than GraphQL variables: the live Confluent Cloud catalog endpoint returns
# HTTP 500 for any operation that carries a variables map, and names the topic's
# cluster field `logical_cluster_id`, not `clusterId` (verified 2026-08-05).
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
