from typing import Final

# GraphQL field the Stream Catalog exposes Kafka topics under.
TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Read-only query for the topics in the environment the Schema Registry endpoint
# belongs to. Kept to fields the catalog has carried since Stream Governance shipped,
# because an unknown field fails the whole query rather than returning null.
TOPIC_CATALOG_QUERY: Final[str] = """
query DataHubKafkaTopicCatalog($limit: Int, $offset: Int) {
  kafka_topic(limit: $limit, offset: $offset) {
    name
    qualifiedName
    clusterId
    tags
    business_metadata {
      name
      value
    }
  }
}
"""
