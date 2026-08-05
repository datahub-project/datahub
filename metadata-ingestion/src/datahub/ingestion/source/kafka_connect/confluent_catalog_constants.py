from typing import Final

# Recipe path of the Stream Catalog block, used to point config errors at the right place.
CONFLUENT_CATALOG_CONFIG_PATH: Final[str] = "confluent_catalog"

# GraphQL field the Stream Catalog exposes Kafka Connect connectors under.
CONNECTOR_ROOT_KEY: Final[str] = "cn_connector"

# Marker written into the DataJob property bag so operators can tell catalog-derived
# lineage apart from lineage inferred by the transform-pipeline matcher.
LINEAGE_SOURCE_PROPERTY: Final[str] = "lineage_source"
LINEAGE_SOURCE_CATALOG: Final[str] = "confluent_stream_catalog"

# `cn_connector.topics` already reflects post-SMT names, so it replaces the
# transform-matching heuristic. Unknown fields fail the whole query — only select
# fields that are both live-verified and consumed. Not filtered by cluster id:
# on `cn_connector` that field is the Connect cluster (lcc-*), not the Kafka
# cluster (lkc-*) the REST URI gives us; match by name instead. Pagination
# placeholders: see LIMIT_PLACEHOLDER in confluent.constants.
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
