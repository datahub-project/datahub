"""Dataplex entry identity parsing and DataHub key mapping.

This module centralizes all Dataplex identity concerns:
- Entry type identification from ``entry_type``
- FQN parsing from ``fully_qualified_name``
- Parent extraction from ``parent_entry``
- Static Dataplex entry-type -> DataHub entity mapping

Primary reference for FQN structures:
https://docs.cloud.google.com/dataplex/docs/fully-qualified-names

Important observed deviations from public docs (based on real Dataplex entries):
- Cloud Spanner FQNs use an ``instanceConfigId`` token like ``regional-us-west2``.
  In this module we normalize that token into ``location=us-west2`` for key fields,
  because ``parent_entry`` also exposes ``locations/{location}``.
- Cloud SQL MySQL uses ``cloudsql-mysql-database`` entry types in practice, while
  docs may refer to the logical level as schema.

Regex/SchemaKey contract:
- Named capture groups in ``fqn_regex`` and ``parent_entry_regex`` are expected to
  match field names on the target SchemaKey classes.
- Example: ``(?P<project_id>...)`` maps to ``DataplexProjectId.project_id``.
- This contract is enforced by
  ``test_mapping_regex_groups_match_schema_key_fields`` in
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


class DataplexBigQueryTable(DataplexBigQueryDataset):
    table_id: str


class DataplexCloudSqlProject(DataplexProjectId):
    platform: str = "cloudsql"


class DataplexCloudSqlMySqlInstance(DataplexCloudSqlProject):
    location: str
    instance_id: str


class DataplexCloudSqlMySqlDatabase(DataplexCloudSqlMySqlInstance):
    database_id: str


class DataplexCloudSqlMySqlTable(DataplexCloudSqlMySqlDatabase):
    table_id: str


class DataplexSpannerProject(DataplexProjectId):
    platform: str = "spanner"


class DataplexCloudSpannerInstance(DataplexSpannerProject):
    location: str
    instance_id: str


class DataplexCloudSpannerDatabase(DataplexCloudSpannerInstance):
    database_id: str


class DataplexCloudSpannerTable(DataplexCloudSpannerDatabase):
    table_id: str


class DataplexPubSubProject(DataplexProjectId):
    platform: str = "pubsub"


class DataplexPubSubTopic(DataplexPubSubProject):
    topic_id: str


PROJECT_KEY_CLASSES: tuple[type[DataplexProjectId], ...] = (
    DataplexProjectId,
    DataplexBigQueryProject,
    DataplexCloudSqlProject,
    DataplexSpannerProject,
    DataplexPubSubProject,
)


@dataclass(frozen=True)
class DataplexEntryTypeMapping:
    # Keep platform values strict because they feed URN generation.
    datahub_platform: Literal["bigquery", "cloudsql", "spanner", "pubsub"]
    datahub_entity_type: Literal["Dataset", "Container"]
    datahub_subtype: str
    fqn_regex: Pattern[str]
    parent_entry_regex: Optional[Pattern[str]]
    schema_key_class: type[DataplexProjectId]
    parent_schema_key_class: Optional[type[DataplexProjectId]]


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


DATAPLEX_ENTRY_TYPE_MAPPINGS: dict[str, DataplexEntryTypeMapping] = {
    "bigquery-dataset": DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.BIGQUERY_DATASET,
        fqn_regex=BIGQUERY_DATASET_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        schema_key_class=DataplexBigQueryDataset,
        parent_schema_key_class=None,
    ),
    "bigquery-table": DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=BIGQUERY_TABLE_FQN_REGEX,
        parent_entry_regex=BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
        schema_key_class=DataplexBigQueryTable,
        parent_schema_key_class=DataplexBigQueryDataset,
    ),
    "cloudsql-mysql-instance": DataplexEntryTypeMapping(
        datahub_platform="cloudsql",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.INSTANCE,
        fqn_regex=MYSQL_INSTANCE_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        schema_key_class=DataplexCloudSqlMySqlInstance,
        parent_schema_key_class=None,
    ),
    "cloudsql-mysql-database": DataplexEntryTypeMapping(
        datahub_platform="cloudsql",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.DATABASE,
        fqn_regex=MYSQL_DATABASE_FQN_REGEX,
        parent_entry_regex=MYSQL_INSTANCE_PARENT_ENTRY_REGEX,
        schema_key_class=DataplexCloudSqlMySqlDatabase,
        parent_schema_key_class=DataplexCloudSqlMySqlInstance,
    ),
    "cloudsql-mysql-table": DataplexEntryTypeMapping(
        datahub_platform="cloudsql",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=MYSQL_TABLE_FQN_REGEX,
        parent_entry_regex=MYSQL_DATABASE_PARENT_ENTRY_REGEX,
        schema_key_class=DataplexCloudSqlMySqlTable,
        parent_schema_key_class=DataplexCloudSqlMySqlDatabase,
    ),
    "cloud-spanner-instance": DataplexEntryTypeMapping(
        datahub_platform="spanner",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.INSTANCE,
        fqn_regex=SPANNER_INSTANCE_FQN_REGEX,
        # Top-level in Dataplex entry hierarchy: no parent_entry on payload.
        parent_entry_regex=None,
        schema_key_class=DataplexCloudSpannerInstance,
        parent_schema_key_class=None,
    ),
    "cloud-spanner-database": DataplexEntryTypeMapping(
        datahub_platform="spanner",
        datahub_entity_type="Container",
        datahub_subtype=DatasetContainerSubTypes.DATABASE,
        fqn_regex=SPANNER_DATABASE_FQN_REGEX,
        parent_entry_regex=SPANNER_INSTANCE_PARENT_ENTRY_REGEX,
        schema_key_class=DataplexCloudSpannerDatabase,
        parent_schema_key_class=DataplexCloudSpannerInstance,
    ),
    "cloud-spanner-table": DataplexEntryTypeMapping(
        datahub_platform="spanner",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TABLE,
        fqn_regex=SPANNER_TABLE_FQN_REGEX,
        parent_entry_regex=SPANNER_DATABASE_PARENT_ENTRY_REGEX,
        schema_key_class=DataplexCloudSpannerTable,
        parent_schema_key_class=DataplexCloudSpannerDatabase,
    ),
    "pubsub-topic": DataplexEntryTypeMapping(
        datahub_platform="pubsub",
        datahub_entity_type="Dataset",
        datahub_subtype=DatasetSubTypes.TOPIC,
        fqn_regex=PUBSUB_TOPIC_FQN_REGEX,
        parent_entry_regex=None,
        schema_key_class=DataplexPubSubTopic,
        parent_schema_key_class=None,
    ),
}

PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM: dict[
    Literal["bigquery", "cloudsql", "spanner", "pubsub"],
    type[DataplexProjectId],
] = {
    "bigquery": DataplexBigQueryProject,
    "cloudsql": DataplexCloudSqlProject,
    "spanner": DataplexSpannerProject,
    "pubsub": DataplexPubSubProject,
}


def extract_entry_type_short_name(entry_type: str) -> Optional[str]:
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
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None:
        return None
    return _parse_with_regex(mapping.fqn_regex, fully_qualified_name)


def parse_parent_entry(
    entry_type_or_short_name: str, parent_entry: str
) -> Optional[dict[str, str]]:
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


def build_schema_key_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[DataplexProjectId]:
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None:
        return None

    identity_fields = parse_fully_qualified_name(
        entry_type_or_short_name=entry_type_or_short_name,
        fully_qualified_name=fully_qualified_name,
    )
    if identity_fields is None:
        return None

    return _instantiate_key(mapping.schema_key_class, identity_fields)


def build_parent_schema_key(
    entry_type_or_short_name: str, parent_entry: str
) -> Optional[DataplexProjectId]:
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.parent_schema_key_class is None:
        return None

    identity_fields = parse_parent_entry(
        entry_type_or_short_name=entry_type_or_short_name,
        parent_entry=parent_entry,
    )
    if identity_fields is None:
        return None

    return _instantiate_key(mapping.parent_schema_key_class, identity_fields)


def build_dataset_urn_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str, env: str
) -> Optional[str]:
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.datahub_entity_type != "Dataset":
        return None

    # Validate FQN with the entry-type-specific regex contract first.
    # This is where fqn_regex is used for URN building: we reject malformed FQNs
    # before extracting the canonical dataset identifier.
    if (
        parse_fully_qualified_name(entry_type_or_short_name, fully_qualified_name)
        is None
    ):
        return None

    # Keep dataset-name extraction as "everything after the first ':'" to
    # preserve provider-specific suffixes exactly (for example, Pub/Sub keeps
    # "topic:project.topic"), instead of reconstructing from parsed fields.
    if ":" not in fully_qualified_name:
        return None
    _, dataset_name = fully_qualified_name.split(":", 1)

    return make_dataset_urn_with_platform_instance(
        platform=mapping.datahub_platform,
        name=dataset_name,
        platform_instance=None,
        env=env,
    )


def build_container_urn_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[str]:
    mapping = _get_mapping(entry_type_or_short_name)
    if mapping is None or mapping.datahub_entity_type != "Container":
        return None

    schema_key = build_schema_key_from_fqn(
        entry_type_or_short_name, fully_qualified_name
    )
    if schema_key is None:
        return None
    return schema_key.as_urn()


def build_project_schema_key_from_fqn(
    entry_type_or_short_name: str, fully_qualified_name: str
) -> Optional[DataplexProjectId]:
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
    parent_schema_key = build_parent_schema_key(
        entry_type_or_short_name=entry_type_or_short_name,
        parent_entry=parent_entry,
    )
    if parent_schema_key is None:
        return None
    return parent_schema_key.as_urn()
