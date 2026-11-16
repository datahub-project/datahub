from typing import Final

TOPIC_ROOT_KEY: Final[str] = "kafka_topic"

# Unknown fields fail the whole query. The cluster field is `logical_cluster_id`
# (not the more intuitive `clusterId` - Confluent Cloud rejects that name).
# Pagination placeholders: see LIMIT_PLACEHOLDER / OFFSET_PLACEHOLDER.
# MIRROR_FIELDS_PLACEHOLDER is replaced by TOPIC_MIRROR_FIELDS or nothing; see
# `build_topic_catalog_query`.
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
    }{mirror_fields}
  }
}
"""

MIRROR_FIELDS_PLACEHOLDER: Final[str] = "{mirror_fields}"

# Cluster-link source of a mirror topic, requested only when `include_lineage` is
# on. Its response shape has not been confirmed against a tenant with cluster
# links, so keeping it out of the default query means a surprise there can never
# cost the tags and business metadata that every other catalog user depends on.
TOPIC_MIRROR_FIELDS: Final[str] = """
    source_topic {
      name
      logical_cluster_id
    }"""
