from typing import Final

CONNECTOR_ROOT_KEY: Final[str] = "cn_connector"

LINEAGE_SOURCE_PROPERTY: Final[str] = "lineage_source"
LINEAGE_SOURCE_CATALOG: Final[str] = "confluent_stream_catalog"

# `cn_connector.topics` is post-SMT, so it replaces the transform heuristic.
# Unknown fields fail the whole query — only select live+consumed fields.
# Not filtered by cluster id: on `cn_connector` that is the Connect cluster
# (lcc-*), not the Kafka cluster (lkc-*) from the REST URI. Match by name.
# Pagination: see LIMIT_PLACEHOLDER.
CONNECTOR_CATALOG_QUERY: Final[str] = """
{
  cn_connector(limit: {limit}, offset: {offset}) {
    name
    qualifiedName
    tags
    business_metadata {
      name
      value
    }
    topics {
      name
    }
  }
}
"""
