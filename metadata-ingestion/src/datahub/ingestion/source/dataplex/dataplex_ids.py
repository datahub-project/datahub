"""Dataplex entry identity parsing and DataHub key mapping.

This module centralizes all Dataplex identity concerns:
- Entry type identification from ``entry_type``
- FQN parsing from ``fully_qualified_name``
- Parent extraction from ``parent_entry``
- Static Dataplex entry-type -> DataHub entity mapping

Example Dataplex entry payload (anonymized):

.. code-block:: text

   name: "projects/example-project-123456/locations/us-west2/entryGroups/@cloudsql/entries/cloudsql.googleapis.com/projects/example-project-123456/locations/us-west2/instances/example-instance/databases/example-db/tables/example_table"
   entry_type: "projects/655216118709/locations/global/entryTypes/cloudsql-mysql-table"
   create_time {
     seconds: 1774092556
     nanos: 511835000
   }
   update_time {
     seconds: 1774092556
     nanos: 511835000
   }
   parent_entry: "projects/example-project-123456/locations/us-west2/entryGroups/@cloudsql/entries/cloudsql.googleapis.com/projects/example-project-123456/locations/us-west2/instances/example-instance/databases/example-db"
   fully_qualified_name: "cloudsql_mysql:example-project-123456.us-west2.example-instance.example-db.example_table"
   entry_source {
     resource: "projects/example-project-123456/locations/us-west2/instances/example-instance/databases/example-db/tables/example_table"
     system: "CLOUD_SQL"
     platform: "GCP"
     display_name: "example_table"
     ancestors {
       name: "projects/example-project-123456/locations/us-west2/instances/example-instance/databases/example-db"
       type_: "dataplex-types.global.cloudsql-mysql-database"
     }
     ancestors {
       name: "projects/example-project-123456/locations/us-west2/instances/example-instance"
       type_: "dataplex-types.global.cloudsql-mysql-instance"
     }
     location: "us-west2"
   }

How identity resolution works:
- ``ENTRY_TYPE_SHORT_NAME_REGEX`` extracts the short entry type from ``entry_type``
  (for example ``cloudsql-mysql-table``).
- That short name indexes ``DATAPLEX_ENTRY_TYPE_MAPPINGS``, which determines the
  target DataHub entity type (``Dataset`` or ``Container``) and emitted subtype.
- The mapping's ``fqn_regex`` parses ``fully_qualified_name`` into identity fields
  (for example project, location, instance, database, table). Those fields are used
  to build DataHub entity identity (dataset name / key-class identity / URNs).
- The mapping's ``parent_entry_regex`` plus ``parent_container_key_class`` parses
  ``parent_entry`` to build the parent container key that is later emitted as the
  parent-container relationship.

Primary reference for FQN structures:
https://docs.cloud.google.com/dataplex/docs/fully-qualified-names

Important observed deviations from public docs (based on real Dataplex entries):
- Cloud Spanner FQNs use an ``instanceConfigId`` token like ``regional-us-west2``.
  In this module we normalize that token into ``location=us-west2`` for key fields,
  because ``parent_entry`` also exposes ``locations/{location}``.
- Cloud SQL MySQL uses ``cloudsql-mysql-database`` entry types in practice, while
  docs may refer to the logical level as schema.

Regex/ContainerKey contract:
- Named capture groups in ``fqn_regex`` and ``parent_entry_regex`` are expected to
  match field names on the target container-key classes.
- Example: ``(?P<project_id>...)`` maps to ``DataplexProjectId.project_id``.
- This contract is enforced by
  ``test_mapping_regex_groups_match_container_key_fields`` in
  ``test_dataplex_ids.py`` so new entry type mappings fail fast if regex groups
  and key fields drift.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional, Pattern

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


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


PROJECT_KEY_CLASSES: tuple[type[DataplexProjectId], ...] = (
    DataplexProjectId,
    DataplexBigQueryProject,
    DataplexCloudSqlProject,
    DataplexSpannerProject,
    DataplexPubSubProject,
    DataplexBigtableProject,
    DataplexVertexAiProject,
)


@dataclass(frozen=True)
class DataplexEntryTypeMapping:
    """Static mapping contract from Dataplex entry types to DataHub semantics."""

    # Keep platform values strict because they feed URN generation.
    # DataHub platform used for URN generation for mapped entities.
    datahub_platform: Literal[
        "bigquery", "cloudsql", "spanner", "pubsub", "bigtable", "vertexai"
    ]
    # Whether this Dataplex entry type maps to a DataHub Dataset or Container.
    datahub_entity_type: Literal["Dataset", "Container"]
    # DataHub subtype to emit for the mapped entity.
    datahub_subtype: str
    # Regex that parses fully_qualified_name into identity fields for this type.
    fqn_regex: Pattern[str]
    # Regex that parses parent_entry for Container-aspect hierarchy linkage.
    parent_entry_regex: Optional[Pattern[str]]
    # Parent key class used with parent_entry_regex for Container-aspect hierarchy.
    parent_container_key_class: Optional[type[DataplexProjectId]]
    # Key class used when this entry maps to a Container entity itself.
    container_key_class: Optional[type[DataplexProjectId]]
    # Required format string for dataset URN names from parsed identity fields.
    # Example: "{project_id}.{location}.{dataset_id}".
    datahub_dataset_name_format: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate:
        1) dataset/container field constraints,
        2) parent linkage constraints,
        3) regex group -> key field compatibility.
        """
        if (
            self.datahub_entity_type == "Dataset"
            and self.datahub_dataset_name_format is None
        ):
            raise ValueError("Dataset mappings must define datahub_dataset_name_format")
        if (
            self.datahub_entity_type == "Container"
            and self.datahub_dataset_name_format is not None
        ):
            raise ValueError(
                "Container mappings must not define datahub_dataset_name_format"
            )
        if self.datahub_entity_type == "Container" and self.container_key_class is None:
            raise ValueError("Container mappings must define container_key_class")
        if (
            self.datahub_entity_type == "Dataset"
            and self.container_key_class is not None
        ):
            raise ValueError("Dataset mappings must not define container_key_class")
        if (
            self.parent_entry_regex is not None
            and self.parent_container_key_class is None
        ):
            raise ValueError(
                "Mappings with parent_entry_regex must define parent_container_key_class"
            )

        fqn_group_names = set(self.fqn_regex.groupindex.keys())
        if self.container_key_class is not None:
            container_field_names = set(self.container_key_class.model_fields.keys())
            if not fqn_group_names.issubset(container_field_names):
                raise ValueError(
                    f"fqn_regex groups {fqn_group_names} must be subset of "
                    f"{self.container_key_class.__name__} fields {container_field_names}"
                )

        if self.parent_entry_regex is not None and self.parent_container_key_class:
            parent_group_names = set(self.parent_entry_regex.groupindex.keys())
            parent_container_field_names = set(
                self.parent_container_key_class.model_fields.keys()
            )
            if not parent_group_names.issubset(parent_container_field_names):
                raise ValueError(
                    f"parent_entry_regex groups {parent_group_names} must be subset of "
                    f"{self.parent_container_key_class.__name__} fields "
                    f"{parent_container_field_names}"
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


DATAPLEX_ENTRY_TYPE_MAPPINGS: dict[str, DataplexEntryTypeMapping] = {
    "bigquery-dataset": DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.BIGQUERY_DATASET,
        fqn_regex=BIGQUERY_DATASET_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        container_key_class=DataplexBigQueryDataset,
        parent_container_key_class=None,
    ),
    "bigquery-table": DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=BIGQUERY_TABLE_FQN_REGEX,
        parent_entry_regex=BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
        container_key_class=None,
        parent_container_key_class=DataplexBigQueryDataset,
        datahub_dataset_name_format="{project_id}.{dataset_id}.{table_id}",
    ),
    "bigquery-view": DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.VIEW,
        # BigQuery views share the same FQN and parent_entry shape as tables.
        fqn_regex=BIGQUERY_TABLE_FQN_REGEX,
        parent_entry_regex=BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
        container_key_class=None,
        parent_container_key_class=DataplexBigQueryDataset,
        datahub_dataset_name_format="{project_id}.{dataset_id}.{table_id}",
    ),
    "cloudsql-mysql-instance": DataplexEntryTypeMapping(
        datahub_platform="cloudsql",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.INSTANCE,
        fqn_regex=MYSQL_INSTANCE_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        container_key_class=DataplexCloudSqlMySqlInstance,
        parent_container_key_class=None,
    ),
    "cloudsql-mysql-database": DataplexEntryTypeMapping(
        datahub_platform="cloudsql",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.DATABASE,
        fqn_regex=MYSQL_DATABASE_FQN_REGEX,
        parent_entry_regex=MYSQL_INSTANCE_PARENT_ENTRY_REGEX,
        container_key_class=DataplexCloudSqlMySqlDatabase,
        parent_container_key_class=DataplexCloudSqlMySqlInstance,
    ),
    "cloudsql-mysql-table": DataplexEntryTypeMapping(
        datahub_platform="cloudsql",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=MYSQL_TABLE_FQN_REGEX,
        parent_entry_regex=MYSQL_DATABASE_PARENT_ENTRY_REGEX,
        container_key_class=None,
        parent_container_key_class=DataplexCloudSqlMySqlDatabase,
        datahub_dataset_name_format=(
            "{project_id}.{location}.{instance_id}.{database_id}.{table_id}"
        ),
    ),
    "cloud-spanner-instance": DataplexEntryTypeMapping(
        datahub_platform="spanner",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.INSTANCE,
        fqn_regex=SPANNER_INSTANCE_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        container_key_class=DataplexCloudSpannerInstance,
        parent_container_key_class=None,
    ),
    "cloud-spanner-database": DataplexEntryTypeMapping(
        datahub_platform="spanner",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.DATABASE,
        fqn_regex=SPANNER_DATABASE_FQN_REGEX,
        parent_entry_regex=SPANNER_INSTANCE_PARENT_ENTRY_REGEX,
        container_key_class=DataplexCloudSpannerDatabase,
        parent_container_key_class=DataplexCloudSpannerInstance,
    ),
    "cloud-spanner-table": DataplexEntryTypeMapping(
        datahub_platform="spanner",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=SPANNER_TABLE_FQN_REGEX,
        parent_entry_regex=SPANNER_DATABASE_PARENT_ENTRY_REGEX,
        container_key_class=None,
        parent_container_key_class=DataplexCloudSpannerDatabase,
        datahub_dataset_name_format=(
            "{project_id}.regional-{location}.{instance_id}.{database_id}.{table_id}"
        ),
    ),
    "pubsub-topic": DataplexEntryTypeMapping(
        datahub_platform="pubsub",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TOPIC,
        fqn_regex=PUBSUB_TOPIC_FQN_REGEX,
        parent_entry_regex=None,
        container_key_class=None,
        parent_container_key_class=DataplexPubSubProject,
        datahub_dataset_name_format="{project_id}.{topic_id}",
    ),
    "cloud-bigtable-instance": DataplexEntryTypeMapping(
        datahub_platform="bigtable",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.INSTANCE,
        fqn_regex=BIGTABLE_INSTANCE_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        container_key_class=DataplexBigtableInstance,
        parent_container_key_class=None,
    ),
    "cloud-bigtable-table": DataplexEntryTypeMapping(
        datahub_platform="bigtable",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=BIGTABLE_TABLE_FQN_REGEX,
        parent_entry_regex=BIGTABLE_INSTANCE_PARENT_ENTRY_REGEX,
        container_key_class=None,
        parent_container_key_class=DataplexBigtableInstance,
        datahub_dataset_name_format="{project_id}.{instance_id}.{table_id}",
    ),
    "vertexai-dataset": DataplexEntryTypeMapping(
        datahub_platform="vertexai",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=VERTEX_AI_DATASET_FQN_REGEX,
        parent_entry_regex=None,
        container_key_class=None,
        parent_container_key_class=DataplexVertexAiProject,
        datahub_dataset_name_format="{project_id}.{location}.{dataset_id}",
    ),
}

PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM: dict[
    Literal["bigquery", "cloudsql", "spanner", "pubsub", "bigtable", "vertexai"],
    type[DataplexProjectId],
] = {
    "bigquery": DataplexBigQueryProject,
    "cloudsql": DataplexCloudSqlProject,
    "spanner": DataplexSpannerProject,
    "pubsub": DataplexPubSubProject,
    "bigtable": DataplexBigtableProject,
    "vertexai": DataplexVertexAiProject,
}


def extract_entry_type_short_name(entry_type: str) -> Optional[str]:
    """Extract Dataplex entry-type short name from a full ``entry_type`` path."""
    match = ENTRY_TYPE_SHORT_NAME_REGEX.match(entry_type)
    if not match:
        return None
    return match.group("entry_type_short_name")


def _normalize_entry_type_short_name(entry_type_or_short_name: str) -> str:
    short_name = extract_entry_type_short_name(entry_type_or_short_name)
    if short_name is not None:
        return short_name
    return entry_type_or_short_name


def _get_mapping(entry_type_or_short_name: str) -> Optional[DataplexEntryTypeMapping]:
    return DATAPLEX_ENTRY_TYPE_MAPPINGS.get(
        _normalize_entry_type_short_name(entry_type_or_short_name)
    )


def _parse_with_regex(regex: Pattern[str], value: str) -> Optional[dict[str, str]]:
    """Parse value with named-group regex.

    Note: regex named groups are expected to match fields on SchemaKey classes.
    For example ``(?P<project_id>...)`` maps to ``DataplexProjectId.project_id``.
    """
    match = regex.match(value)
    if not match:
        return None
    return {
        key: matched_value
        for key, matched_value in match.groupdict().items()
        if matched_value is not None
    }


def parse_fully_qualified_name(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[dict[str, str]]:
    """Parse ``fully_qualified_name`` into identity fields for a mapped entry type."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None:
        return None
    return _parse_with_regex(mapping.fqn_regex, fully_qualified_name)


def parse_parent_entry(
    entry_type_or_short_name: str, parent_entry: str
) -> Optional[dict[str, str]]:
    """Parse ``parent_entry`` into parent identity fields for mapped entry types."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.parent_entry_regex is None:
        return None
    return _parse_with_regex(mapping.parent_entry_regex, parent_entry)


def _instantiate_key(
    key_class: type[DataplexProjectId], identity_fields: dict[str, str]
) -> DataplexProjectId:
    valid_fields = set(key_class.model_fields.keys())
    constructor_args = {
        field_name: field_value
        for field_name, field_value in identity_fields.items()
        if field_name in valid_fields
    }
    return key_class(**constructor_args)


def build_container_key_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[DataplexProjectId]:
    """Build container-entity key from FQN for mappings that emit Containers."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.container_key_class is None:
        return None

    identity_fields = parse_fully_qualified_name(
        entry_type_or_short_name=entry_type_or_short_name,
        fully_qualified_name=fully_qualified_name,
    )
    if identity_fields is None:
        return None

    return _instantiate_key(mapping.container_key_class, identity_fields)


def build_parent_container_key(
    entry_type_or_short_name: str, parent_entry: str
) -> Optional[DataplexProjectId]:
    """Build parent-container key from ``parent_entry`` for dataset hierarchy links."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.parent_container_key_class is None:
        return None

    identity_fields = parse_parent_entry(
        entry_type_or_short_name=entry_type_or_short_name,
        parent_entry=parent_entry,
    )
    if identity_fields is None:
        return None

    return _instantiate_key(mapping.parent_container_key_class, identity_fields)


def build_dataset_urn_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str, env: str
) -> Optional[str]:
    """Build dataset URN from mapped entry type, FQN, and target environment."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.datahub_entity_type != "Dataset":
        return None
    dataset_name = extract_datahub_dataset_name_from_fqn(
        entry_type_or_short_name=entry_type_or_short_name,
        fully_qualified_name=fully_qualified_name,
    )
    if dataset_name is None:
        return None

    return make_dataset_urn_with_platform_instance(
        platform=mapping.datahub_platform,
        name=dataset_name,
        platform_instance=None,
        env=env,
    )


def extract_datahub_dataset_name_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[str]:
    """Extract DataHub dataset name from Dataplex FQN with mapping validation."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.datahub_entity_type != "Dataset":
        return None
    return _extract_dataset_name_from_fqn(
        entry_type_or_short_name=entry_type_or_short_name,
        fully_qualified_name=fully_qualified_name,
    )


def build_dataset_urn_from_fqn_only(
    fully_qualified_name: str, env: str
) -> Optional[str]:
    """Extract DataHub dataset URN by matching FQN against dataset mappings."""
    for entry_type_short_name, mapping in DATAPLEX_ENTRY_TYPE_MAPPINGS.items():
        if mapping.datahub_entity_type != "Dataset":
            continue
        dataset_name = _extract_dataset_name_from_fqn(
            entry_type_or_short_name=entry_type_short_name,
            fully_qualified_name=fully_qualified_name,
        )
        if dataset_name is None:
            continue
        return make_dataset_urn_with_platform_instance(
            platform=mapping.datahub_platform,
            name=dataset_name,
            platform_instance=None,
            env=env,
        )

    return None


def _extract_dataset_name_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[str]:
    """Validate FQN against mapping regex and return DataHub dataset name."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.datahub_entity_type != "Dataset":
        return None

    identity_fields = parse_fully_qualified_name(
        entry_type_or_short_name, fully_qualified_name
    )
    if identity_fields is None:
        return None

    if mapping.datahub_dataset_name_format is None:
        return None
    try:
        dataset_name = mapping.datahub_dataset_name_format.format(**identity_fields)
    except KeyError:
        return None
    return dataset_name or None


def is_supported_lineage_entry_type(entry_type_short_name: str) -> bool:
    """Return whether an entry type is supported as a lineage dataset node."""
    mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS.get(entry_type_short_name)
    return bool(mapping and mapping.datahub_entity_type == "Dataset")


def build_lineage_parent(project_id: str, location: str) -> str:
    """Build Data Lineage API parent for an explicit project/location pair."""
    return f"projects/{project_id}/locations/{location}"


def build_container_urn_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[str]:
    """Build container URN from FQN for mappings that emit Container entities."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.datahub_entity_type != "Container":
        return None

    schema_key = build_container_key_from_fqn(
        entry_type_or_short_name, fully_qualified_name
    )
    if schema_key is None:
        return None
    return schema_key.as_urn()


def build_project_schema_key_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[DataplexProjectId]:
    """Build project-level container key from FQN using mapped DataHub platform."""
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None:
        return None

    identity_fields = parse_fully_qualified_name(
        entry_type_or_short_name=entry_type_or_short_name,
        fully_qualified_name=fully_qualified_name,
    )
    if identity_fields is None:
        return None

    project_key_class = PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM.get(
        mapping.datahub_platform
    )
    if project_key_class is None:
        return None

    return _instantiate_key(project_key_class, identity_fields)


def build_project_container_urn_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[str]:
    """Build project container URN from FQN for a mapped Dataplex entry type."""
    project_key = build_project_schema_key_from_fqn(
        entry_type_or_short_name=entry_type_or_short_name,
        fully_qualified_name=fully_qualified_name,
    )
    if project_key is None:
        return None
    return project_key.as_urn()


def build_parent_container_urn(
    entry_type_or_short_name: str, parent_entry: str
) -> Optional[str]:
    """Build parent container URN from a Dataplex ``parent_entry`` reference."""
    parent_schema_key = build_parent_container_key(
        entry_type_or_short_name=entry_type_or_short_name,
        parent_entry=parent_entry,
    )
    if parent_schema_key is None:
        return None
    return parent_schema_key.as_urn()
