from typing import Final

# GraphQL field the Stream Catalog exposes Kafka topics under.
TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Read-only query for the topics in the environment the Schema Registry endpoint
# belongs to. Kept to fields the catalog has carried since Stream Governance shipped,
# because an unknown field fails the whole query rather than returning null.
#
# The topic's cluster field is named `logical_cluster_id`, not `clusterId`
# (verified against the live API 2026-08-05). Pagination placeholders are substituted
# by the client; see LIMIT_PLACEHOLDER in `datahub.ingestion.source.confluent.constants`
# for why they are not GraphQL variables.
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
