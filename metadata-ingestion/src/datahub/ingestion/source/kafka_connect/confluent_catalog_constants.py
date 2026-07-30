from typing import Final

# Recipe path of the Stream Catalog block, used to point config errors at the right place.
CONFLUENT_CATALOG_CONFIG_PATH: Final[str] = "confluent_catalog"

# GraphQL field the Stream Catalog exposes Kafka Connect connectors under.
CONNECTOR_ROOT_KEY: Final[str] = "cn_connector"

# Marker written into the DataJob property bag so operators can tell catalog-derived
# lineage apart from lineage inferred by the transform-pipeline matcher.
LINEAGE_SOURCE_PROPERTY: Final[str] = "lineage_source"
LINEAGE_SOURCE_CATALOG: Final[str] = "confluent_stream_catalog"

# `class` is a reserved word in Python, so the model aliases it.
CONNECTOR_CLASS_FIELD: Final[str] = "class"

# Read-only query against the Stream Catalog. `cn_connector.topics` already reflects
# post-SMT topic names, which is why it can replace the transform-matching heuristic.
#
# Deliberately not filtered by `clusterId`: on `cn_connector` that field holds the
# logical *Connect* cluster id (lcc-*), whereas the Connect REST URI only gives us the
# Kafka cluster id (lkc-*). The Schema Registry endpoint is already environment-scoped,
# so connectors are matched by name instead.
CONNECTOR_CATALOG_QUERY: Final[str] = """
query DataHubKafkaConnectCatalog($limit: Int, $offset: Int) {
  cn_connector(limit: $limit, offset: $offset) {
    name
    qualifiedName
    class
    type
    status
    description
    tags
    business_metadata {
      name
      value
    }
    topics {
      name
      qualifiedName
      tags
      business_metadata {
        name
        value
      }
    }
  }
}
"""
