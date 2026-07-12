"""Dataplex identity primitives.

This module centralizes the low-level *identity* building blocks used to turn a
Dataplex entry into DataHub identifiers:

- ``extract_entry_type_short_name`` extracts the short entry type from a full
  ``entry_type`` path (for example ``cloudsql-mysql-table``).
- The compiled ``*_FQN_REGEX`` / ``*_PARENT_ENTRY_REGEX`` patterns parse a
  ``fully_qualified_name`` / ``parent_entry`` into named identity fields.
- The ``ContainerKey`` subclasses model the DataHub container hierarchy
  (project -> instance -> database -> ...); ``PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM``
  maps a platform to its project-level key class.
- ``parse_with_regex`` and ``instantiate_key`` are the generic glue that turns a
  regex + a key class into a populated ``ContainerKey``.

The *mapping* from a Dataplex entry type to DataHub entities (which regex, which
key class, which subtype) lives in ``dataplex_mappers.py``. Each mapper imports
the primitives it needs from here.

Primary reference for FQN structures:
https://docs.cloud.google.com/dataplex/docs/fully-qualified-names

Important observed deviations from public docs (based on real Dataplex entries):
- Cloud Spanner FQNs use an ``instanceConfigId`` token like ``regional-us-west2``.
  We normalize that token into ``location=us-west2`` for key fields, because
  ``parent_entry`` also exposes ``locations/{location}``.
- Cloud SQL MySQL uses ``cloudsql-mysql-database`` entry types in practice, while
  docs may refer to the logical level as schema.

Regex/ContainerKey contract:
- Named capture groups in the FQN / parent-entry regexes are expected to match
  field names on the target container-key classes. Example: ``(?P<project_id>...)``
  maps to ``DataplexProjectId.project_id``. This contract is enforced by a
  registry-wide test in ``test_dataplex_mappers.py`` so new entry type mappings
  fail fast if regex groups and key fields drift.
"""

from __future__ import annotations

import re
from typing import Optional, Pattern

from datahub.emitter.mcp_builder import ContainerKey


class DataplexProjectId(ContainerKey):
    """Base Dataplex key that carries ``project_id``."""

    project_id: str

    def parent_key(self) -> Optional[ContainerKey]:
        """Return the parent key, terminating at project-level keys."""
        if type(self) in PROJECT_KEY_CLASSES:
            # Dataplex project is the root container for a platform-specific
            # hierarchy.
            return None
        # Subclasses should keep standard class-hierarchy traversal.
        return ContainerKey.parent_key(self)


class DataplexBigQueryProject(DataplexProjectId):
    platform: str = "bigquery"


class DataplexBigQueryDataset(DataplexBigQueryProject):
    dataset_id: str


class DataplexCloudSqlProject(DataplexProjectId):
    platform: str = "cloudsql"


class DataplexCloudSqlMySqlInstance(DataplexCloudSqlProject):
    location: str
    instance_id: str


class DataplexCloudSqlMySqlDatabase(DataplexCloudSqlMySqlInstance):
    database_id: str


class DataplexSpannerProject(DataplexProjectId):
    platform: str = "spanner"


class DataplexCloudSpannerInstance(DataplexSpannerProject):
    location: str
    instance_id: str


class DataplexCloudSpannerDatabase(DataplexCloudSpannerInstance):
    database_id: str


class DataplexPubSubProject(DataplexProjectId):
    platform: str = "pubsub"


class DataplexBigtableProject(DataplexProjectId):
    platform: str = "bigtable"


class DataplexBigtableInstance(DataplexBigtableProject):
    instance_id: str


class DataplexVertexAiProject(DataplexProjectId):
    platform: str = "vertexai"


class DataplexDataprocMetastoreProject(DataplexProjectId):
    platform: str = "dataproc-metastore"


class DataplexDataprocMetastoreService(DataplexDataprocMetastoreProject):
    location: str
    service_id: str


class DataplexDataprocMetastoreDatabase(DataplexDataprocMetastoreService):
    database_id: str


PROJECT_KEY_CLASSES: tuple[type[DataplexProjectId], ...] = (
    DataplexProjectId,
    DataplexBigQueryProject,
    DataplexCloudSqlProject,
    DataplexSpannerProject,
    DataplexPubSubProject,
    DataplexBigtableProject,
    DataplexVertexAiProject,
    DataplexDataprocMetastoreProject,
)


ENTRY_TYPE_SHORT_NAME_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryTypes/(?P<entry_type_short_name>[^/]+)$"
)

# FQN regexes (authoritative format:
# https://docs.cloud.google.com/dataplex/docs/fully-qualified-names)
BIGQUERY_DATASET_FQN_REGEX = re.compile(
    r"^bigquery:(?P<project_id>[^.]+)\.(?P<dataset_id>[^.]+)$"
)
BIGQUERY_TABLE_FQN_REGEX = re.compile(
    r"^bigquery:(?P<project_id>[^.]+)\.(?P<dataset_id>[^.]+)\.(?P<table_id>[^.]+)$"
)
MYSQL_INSTANCE_FQN_REGEX = re.compile(
    r"^cloudsql_mysql:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<instance_id>[^.]+)$"
)
# Cloud SQL docs often use "schema" terminology; in observed Dataplex entries this
# level is emitted as "database" and entry type short-name "cloudsql-mysql-database".
MYSQL_DATABASE_FQN_REGEX = re.compile(
    r"^cloudsql_mysql:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<instance_id>[^.]+)\.(?P<database_id>[^.]+)$"
)
MYSQL_TABLE_FQN_REGEX = re.compile(
    r"^cloudsql_mysql:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<instance_id>[^.]+)\.(?P<database_id>[^.]+)\.(?P<table_id>[^.]+)$"
)
# Spanner docs define instanceConfigId as the second token.
# In observed Dataplex payloads, this appears as "regional-{location}".
# We normalize this to "location" in extracted identity fields.
SPANNER_INSTANCE_FQN_REGEX = re.compile(
    r"^spanner:(?P<project_id>[^.]+)\.regional-(?P<location>[^.]+)\.(?P<instance_id>[^.]+)$"
)
SPANNER_DATABASE_FQN_REGEX = re.compile(
    r"^spanner:(?P<project_id>[^.]+)\.regional-(?P<location>[^.]+)\.(?P<instance_id>[^.]+)\.(?P<database_id>[^.]+)$"
)
SPANNER_TABLE_FQN_REGEX = re.compile(
    r"^spanner:(?P<project_id>[^.]+)\.regional-(?P<location>[^.]+)\.(?P<instance_id>[^.]+)\.(?P<database_id>[^.]+)\.(?P<table_id>[^.]+)$"
)
SPANNER_GRAPH_FQN_REGEX = re.compile(
    r"^spanner:graph:(?P<project_id>[^.]+)\.regional-(?P<location>[^.]+)\.(?P<instance_id>[^.]+)\.(?P<database_id>[^.]+)\.(?P<graph_id>[^.]+)$"
)
PUBSUB_TOPIC_FQN_REGEX = re.compile(
    r"^pubsub:topic:(?P<project_id>[^.]+)\.(?P<topic_id>[^.]+)$"
)
BIGTABLE_INSTANCE_FQN_REGEX = re.compile(
    r"^bigtable:(?P<project_id>[^.]+)\.(?P<instance_id>[^.]+)$"
)
BIGTABLE_TABLE_FQN_REGEX = re.compile(
    r"^bigtable:(?P<project_id>[^.]+)\.(?P<instance_id>[^.]+)\.(?P<table_id>[^.]+)$"
)
VERTEX_AI_DATASET_FQN_REGEX = re.compile(
    r"^vertex_ai:dataset:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<dataset_id>[^.]+)$"
)
DATAPROC_METASTORE_SERVICE_FQN_REGEX = re.compile(
    r"^dataproc_metastore:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<service_id>[^.]+)$"
)
DATAPROC_METASTORE_DATABASE_FQN_REGEX = re.compile(
    r"^dataproc_metastore:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<service_id>[^.]+)\.(?P<database_id>[^.]+)$"
)
DATAPROC_METASTORE_TABLE_FQN_REGEX = re.compile(
    r"^dataproc_metastore:(?P<project_id>[^.]+)\.(?P<location>[^.]+)\.(?P<service_id>[^.]+)\.(?P<database_id>[^.]+)\.(?P<table_id>[^.]+)$"
)

# parent_entry regexes
BIGQUERY_DATASET_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/bigquery\.googleapis\.com/projects/(?P<project_id>[^/]+)/datasets/(?P<dataset_id>[^/]+)$"
)
MYSQL_INSTANCE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/cloudsql\.googleapis\.com/projects/(?P<project_id>[^/]+)/locations/(?P<location>[^/]+)/instances/(?P<instance_id>[^/]+)$"
)
MYSQL_DATABASE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/cloudsql\.googleapis\.com/projects/(?P<project_id>[^/]+)/locations/(?P<location>[^/]+)/instances/(?P<instance_id>[^/]+)/databases/(?P<database_id>[^/]+)$"
)
SPANNER_INSTANCE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/(?P<location>[^/]+)/entryGroups/[^/]+/entries/spanner\.googleapis\.com/projects/(?P<project_id>[^/]+)/instances/(?P<instance_id>[^/]+)$"
)
SPANNER_DATABASE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/(?P<location>[^/]+)/entryGroups/[^/]+/entries/spanner\.googleapis\.com/projects/(?P<project_id>[^/]+)/instances/(?P<instance_id>[^/]+)/databases/(?P<database_id>[^/]+)$"
)
BIGTABLE_INSTANCE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/bigtable\.googleapis\.com/projects/(?P<project_id>[^/]+)/instances/(?P<instance_id>[^/]+)$"
)
DATAPROC_METASTORE_SERVICE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/metastore\.googleapis\.com/projects/(?P<project_id>[^/]+)/locations/(?P<location>[^/]+)/services/(?P<service_id>[^/]+)$"
)
DATAPROC_METASTORE_DATABASE_PARENT_ENTRY_REGEX = re.compile(
    r"^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/metastore\.googleapis\.com/projects/(?P<project_id>[^/]+)/locations/(?P<location>[^/]+)/services/(?P<service_id>[^/]+)/databases/(?P<database_id>[^/]+)$"
)


# Platform -> project-level key class. Keyed by plain ``str`` because the
# platform value flows through the mappers as a str (see dataplex_mappers).
PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM: dict[str, type[DataplexProjectId]] = {
    "bigquery": DataplexBigQueryProject,
    "cloudsql": DataplexCloudSqlProject,
    "spanner": DataplexSpannerProject,
    "pubsub": DataplexPubSubProject,
    "bigtable": DataplexBigtableProject,
    "vertexai": DataplexVertexAiProject,
    "dataproc-metastore": DataplexDataprocMetastoreProject,
}


def extract_entry_type_short_name(entry_type: str) -> Optional[str]:
    """Extract Dataplex entry-type short name from a full ``entry_type`` path."""
    match = ENTRY_TYPE_SHORT_NAME_REGEX.match(entry_type)
    if not match:
        return None
    return match.group("entry_type_short_name")


def parse_with_regex(regex: Pattern[str], value: str) -> Optional[dict[str, str]]:
    """Parse ``value`` with a named-group regex into identity fields.

    Note: regex named groups are expected to match fields on ``ContainerKey``
    classes. For example ``(?P<project_id>...)`` maps to
    ``DataplexProjectId.project_id``.
    """
    match = regex.match(value)
    if not match:
        return None
    return {
        key: matched_value
        for key, matched_value in match.groupdict().items()
        if matched_value is not None
    }


def instantiate_key(
    key_class: type[DataplexProjectId], identity_fields: dict[str, str]
) -> DataplexProjectId:
    """Build a ``ContainerKey`` from identity fields, keeping only valid ones."""
    valid_fields = set(key_class.model_fields.keys())
    constructor_args = {
        field_name: field_value
        for field_name, field_value in identity_fields.items()
        if field_name in valid_fields
    }
    return key_class(**constructor_args)
