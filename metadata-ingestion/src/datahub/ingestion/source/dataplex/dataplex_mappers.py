"""Dataplex entry -> DataHub entity mappers (factory / strategy).

Each Dataplex entry type maps to DataHub entities through a dedicated
:class:`EntryMapper` implementation. A mapper is a pure function of
``(entry, EntryMappingContext)``: given a Dataplex entry payload it returns an
:class:`EntryMappingResult` describing the *main* DataHub entity it produces
(Dataset or Container), any *additional* entities it may emit alongside it (the
owning project Container), and — for dataset-producing types — a lineage record.

Cross-entry / stateful concerns are deliberately kept OUT of mappers and live in
``DataplexEntriesProcessor``:

* the project Container is emitted once per project (global dedup), and
* lineage records are appended to a shared side-channel.

Mappers only *declare* those as data on the result; the orchestrator performs the
dedup and the append. This keeps mappers trivially unit-testable
(payload -> result) with no locks, report accumulation, or shared context.

Adding support for a new Dataplex entry type means adding one small
:class:`EntryMapper` subclass here and registering it in ``ENTRY_MAPPERS``. The
identity primitives it needs (FQN / parent-entry regexes, ``ContainerKey``
classes, project-key lookup) live in ``dataplex_ids.py``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Pattern, Union

from google.cloud import dataplex_v1
from typing_extensions import assert_never

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    BIGQUERY_DATASET_FQN_REGEX,
    BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
    BIGQUERY_TABLE_FQN_REGEX,
    BIGTABLE_INSTANCE_FQN_REGEX,
    BIGTABLE_INSTANCE_PARENT_ENTRY_REGEX,
    BIGTABLE_TABLE_FQN_REGEX,
    DATAPROC_METASTORE_DATABASE_FQN_REGEX,
    DATAPROC_METASTORE_DATABASE_PARENT_ENTRY_REGEX,
    DATAPROC_METASTORE_SERVICE_FQN_REGEX,
    DATAPROC_METASTORE_SERVICE_PARENT_ENTRY_REGEX,
    DATAPROC_METASTORE_TABLE_FQN_REGEX,
    MYSQL_DATABASE_FQN_REGEX,
    MYSQL_DATABASE_PARENT_ENTRY_REGEX,
    MYSQL_INSTANCE_FQN_REGEX,
    MYSQL_INSTANCE_PARENT_ENTRY_REGEX,
    MYSQL_TABLE_FQN_REGEX,
    PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM,
    PUBSUB_TOPIC_FQN_REGEX,
    SPANNER_DATABASE_FQN_REGEX,
    SPANNER_DATABASE_PARENT_ENTRY_REGEX,
    SPANNER_GRAPH_FQN_REGEX,
    SPANNER_INSTANCE_FQN_REGEX,
    SPANNER_INSTANCE_PARENT_ENTRY_REGEX,
    SPANNER_TABLE_FQN_REGEX,
    VERTEX_AI_DATASET_FQN_REGEX,
    DataplexBigQueryDataset,
    DataplexBigtableInstance,
    DataplexCloudSpannerDatabase,
    DataplexCloudSpannerInstance,
    DataplexCloudSqlMySqlDatabase,
    DataplexCloudSqlMySqlInstance,
    DataplexDataprocMetastoreDatabase,
    DataplexDataprocMetastoreService,
    DataplexProjectId,
    extract_entry_type_short_name,
    instantiate_key,
    parse_with_regex,
)
from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_entry_custom_properties,
)
from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_graph_schema_from_entry_aspects,
    extract_schema_from_entry_aspects,
)
from datahub.metadata.schema_classes import SchemaMetadataClass
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

# ----------------------------------------------------------------------------
# Context + result
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class EntryMappingContext:
    """Everything a mapper legitimately needs beyond the entry payload itself.

    ``config`` (rather than individual fields) keeps this stable as new config
    knobs appear; the mapper reads ``config.env`` and ``config.include_schema``.
    ``location`` is the Dataplex scan location — not reliably present in the
    payload, so it is threaded in from the scan loop and used only to build the
    lineage record. ``report`` lets mappers emit warnings for malformed payloads.
    """

    config: DataplexConfig
    location: str
    report: SourceReport


@dataclass
class EntryMappingResult:
    """The DataHub entities a mapper produced for a single Dataplex entry.

    ``additional_entities`` (e.g. the owning project Container) are global-dedup
    candidates handled by the orchestrator. ``main_entity`` is ``None`` only in
    the edge case where the primary entity could not be built but an additional
    entity still could, mirroring the previous independent-emission behavior.
    """

    main_entity: Optional[Entity] = None
    additional_entities: list[Entity] = field(default_factory=list)
    lineage_entry: Optional[EntryDataTuple] = None


class DatahubIdentity:
    """How a mapper turns parsed FQN identity fields into its main entity's id.

    Shared base for the two per-family variants — see :class:`DatasetIdentity`
    and :class:`ContainerIdentity`. The variant a mapper carries also determines
    ``EntryMapper.datahub_main_entity_type``. It is a plain base (not an ABC): the
    variants expose different builders (``dataset_name`` vs ``container_key``), so
    there is no shared abstract method. New identity shapes are added by
    subclassing this and extending the ``datahub_main_entity_type`` dispatch.
    """


@dataclass(frozen=True)
class DatasetIdentity(DatahubIdentity):
    """Dataset identity: a dotted name built from FQN fields via a format string.

    ``name_format`` references FQN named groups, e.g.
    ``"{project_id}.{dataset_id}.{table_id}"``. The DataHub platform and env are
    supplied at build time (they are not part of the FQN), so this only owns the
    name.
    """

    name_format: str

    def dataset_name(self, identity_fields: dict[str, str]) -> Optional[str]:
        """Build the dataset name from FQN identity fields.

        ``identity_fields`` are the named-group matches produced by parsing the
        entry's ``fully_qualified_name`` with the mapper's ``dataplex_fqn_regex``
        (e.g. ``{"project_id": ..., "dataset_id": ..., "table_id": ...}``).
        """
        try:
            return self.name_format.format(**identity_fields) or None
        except KeyError:
            return None


@dataclass(frozen=True)
class ContainerIdentity(DatahubIdentity):
    """Container identity: a ``ContainerKey`` built directly from FQN fields.

    ``key_class`` fields map 1:1 to FQN named groups (project_id, dataset_id, …).
    """

    key_class: type[DataplexProjectId]

    def container_key(self, identity_fields: dict[str, str]) -> DataplexProjectId:
        """Build the ``ContainerKey`` from FQN identity fields.

        ``identity_fields`` are the named-group matches produced by parsing the
        entry's ``fully_qualified_name`` with the mapper's ``dataplex_fqn_regex``;
        their names must match ``key_class`` fields.
        """
        return instantiate_key(self.key_class, identity_fields)


@dataclass(frozen=True)
class ParentEntryLink:
    """How to derive a mapped entry's parent container from its Dataplex parent_entry.

    ``dataplex_parent_entry_regex`` parses the ``parent_entry`` path; its named
    groups populate ``datahub_schemakey_class``. The two are inseparable, so they
    travel together and cannot drift apart. A mapper with no ``ParentEntryLink``
    (the property returns ``None``) has no parent container finer than its owning
    project; parent linkage then falls back to the project key.
    """

    dataplex_parent_entry_regex: Pattern[str]
    datahub_schemakey_class: type[DataplexProjectId]

    def container_key(self, parent_entry: str) -> Optional[DataplexProjectId]:
        """Build the parent ``ContainerKey`` from a ``parent_entry`` path."""
        fields = parse_with_regex(self.dataplex_parent_entry_regex, parent_entry)
        if fields is None:
            return None
        return instantiate_key(self.datahub_schemakey_class, fields)


# ----------------------------------------------------------------------------
# Mapper interface
# ----------------------------------------------------------------------------


class EntryMapper(ABC):
    """Maps one Dataplex entry type to DataHub entities.

    Pure: reads only the entry + context, never touches locks, dedup state, or
    the shared ingestion context. The required contract descriptors are abstract
    properties so a mapper that forgets one cannot be instantiated (it fails when
    ``ENTRY_MAPPERS`` is built at import). Concrete classes satisfy them with
    plain class attributes.

    Example Dataplex entry payload (anonymized) and how its fields drive mapping:

    .. code-block:: text

       name: "projects/example-project-123456/locations/us-west2/entryGroups/@cloudsql/entries/cloudsql.googleapis.com/projects/example-project-123456/locations/us-west2/instances/example-instance/databases/example-db/tables/example_table"
       entry_type: "projects/655216118709/locations/global/entryTypes/cloudsql-mysql-table"
       create_time { seconds: 1774092556 nanos: 511835000 }
       update_time { seconds: 1774092556 nanos: 511835000 }
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

    Field -> mapping:

    - ``entry_type``: its short name (``cloudsql-mysql-table``, extracted by
      ``extract_entry_type_short_name``) selects which mapper handles the entry
      via ``ENTRY_MAPPERS``. Matches ``dataplex_entry_type_short_name``.
    - ``fully_qualified_name``: parsed by ``dataplex_fqn_regex`` into identity
      fields (project/location/instance/database/table). Those fields build the
      dataset name (dataset entries), the container key (container entries), and
      always the owning project container key.
    - ``parent_entry``: parsed by the type's parent-entry regex to build the
      parent container key (dataset -> its containing dataset/database; container
      -> its parent container). When absent, parent linkage falls back to the
      project key.
    - ``entry_source.display_name`` / ``description`` / ``create_time`` /
      ``update_time``: become the DataHub display name, description, and
      created/last-modified timestamps (see the ``_extract_*`` helpers).
    - ``name``: its ``entryGroups/<id>`` segment and trailing segment provide the
      entry group id (custom property) and the display-name fallback.

    Authoritative FQN formats:
    https://docs.cloud.google.com/dataplex/docs/fully-qualified-names
    """

    @property
    @abstractmethod
    def dataplex_entry_type_short_name(self) -> str:
        """Dataplex entry-type short name this mapper handles (e.g. ``bigquery-table``)."""

    @property
    @abstractmethod
    def datahub_platform(self) -> str:
        """DataHub platform for URN generation (e.g. ``bigquery``)."""

    @property
    @abstractmethod
    def datahub_identity(self) -> Union[DatasetIdentity, ContainerIdentity]:
        """How this entry's parsed FQN fields become the main entity's DataHub id.

        A :class:`DatasetIdentity` (name built from a format string) or a
        :class:`ContainerIdentity` (``ContainerKey`` built from the fields). The
        variant also determines :attr:`datahub_main_entity_type`.
        """

    @property
    def datahub_main_entity_type(self) -> type[Entity]:
        """The DataHub entity this entry maps to directly — derived from the identity."""
        identity = self.datahub_identity
        if isinstance(identity, DatasetIdentity):
            return Dataset
        elif isinstance(identity, ContainerIdentity):
            return Container
        else:
            assert_never(identity)

    @property
    @abstractmethod
    def dataplex_fqn_regex(self) -> Pattern[str]:
        """Regex that parses this entry type's Dataplex ``fully_qualified_name``.

        Universal to every mapper (both Dataset and Container entries parse the
        FQN for identity), so it is part of the contract rather than an inline
        argument. Its named groups must supply whatever ``datahub_identity``
        consumes: the ``ContainerKey`` fields for a :class:`ContainerIdentity`, or
        the name-format fields for a :class:`DatasetIdentity`.
        """

    @property
    def dataplex_parent_entry(self) -> Optional[ParentEntryLink]:
        """How to derive the parent container from the entry's ``parent_entry``.

        Defaults to ``None``, meaning the entry has no parent container finer than
        its owning project — parent linkage falls back to the project key. Mappers
        with an intermediate parent override it with a single ``ParentEntryLink``,
        keeping the parent-entry regex and schema-key class together.
        """
        return None

    @property
    def datahub_additional_entity_types(self) -> tuple[type[Entity], ...]:
        """Entities this mapper may also emit alongside the main one.

        Declarative; the orchestrator dedups them. Every mapper emits the owning
        project Container, hence the default.
        """
        return (Container,)

    @abstractmethod
    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        """Build DataHub entities for ``entry``; ``None`` when nothing is produced."""


# ----------------------------------------------------------------------------
# Shared payload extraction helpers
# ----------------------------------------------------------------------------


def _extract_display_name(entry: dataplex_v1.Entry) -> str:
    """Return the entry's display name, falling back to the last name segment.

    Dataplex only populates ``entry_source.display_name`` for some systems, so
    when it is missing (or not a real string) we use the trailing segment of the
    entry resource name as a stable human-readable label.
    """
    if entry.entry_source:
        display_name = getattr(entry.entry_source, "display_name", None)
        if isinstance(display_name, str) and display_name.strip():
            return display_name
    return entry.name.split("/")[-1]


def _extract_description(entry: dataplex_v1.Entry) -> str:
    """Return the entry's description, or an empty string when absent."""
    if entry.entry_source and entry.entry_source.description:
        return entry.entry_source.description
    return ""


def _extract_datetime(entry: dataplex_v1.Entry, field_name: str) -> Optional[datetime]:
    """Read a timestamp field off ``entry_source`` as a UTC ``datetime``.

    ``field_name`` is a proto timestamp attribute such as ``create_time`` or
    ``update_time``. Returns ``None`` when there is no ``entry_source`` or the
    field is unset.
    """
    if not entry.entry_source:
        return None
    value = getattr(entry.entry_source, field_name, None)
    if value is None:
        return None
    return datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)


def _extract_entry_group_id(entry_name: str) -> str:
    """Extract the entry group id from a Dataplex entry resource name.

    The id is the path segment immediately after ``entryGroups``. Returns
    ``"unknown"`` when the name does not follow the expected shape.

    Example:
        ``projects/my-project/locations/us/entryGroups/@bigquery/entries/foo``
        -> ``"@bigquery"``
    """
    parts = entry_name.split("/")
    try:
        entry_groups_index = parts.index("entryGroups")
        return parts[entry_groups_index + 1]
    except (ValueError, IndexError):
        return "unknown"


@dataclass(frozen=True)
class _CommonFields:
    """Fields shared by every mapped entity, extracted once per entry."""

    display_name: str
    description: str
    created: Optional[datetime]
    last_modified: Optional[datetime]
    custom_properties: dict[str, str]


def _extract_common_fields(entry: dataplex_v1.Entry) -> _CommonFields:
    entry_group_id = _extract_entry_group_id(entry.name)
    return _CommonFields(
        display_name=_extract_display_name(entry),
        description=_extract_description(entry),
        created=_extract_datetime(entry, "create_time"),
        last_modified=_extract_datetime(entry, "update_time"),
        custom_properties=extract_entry_custom_properties(
            entry, entry.name, entry_group_id
        ),
    )


# ----------------------------------------------------------------------------
# Shared identity helpers (explicit regex/key inputs, no table lookup)
# ----------------------------------------------------------------------------


def _dataset_name_from_fqn(
    fqn_regex: Pattern[str], identity: DatasetIdentity, fully_qualified_name: str
) -> Optional[str]:
    identity_fields = parse_with_regex(fqn_regex, fully_qualified_name)
    if identity_fields is None:
        return None
    return identity.dataset_name(identity_fields)


def dataset_urn_from_fqn(
    fully_qualified_name: str,
    fqn_regex: Pattern[str],
    identity: DatasetIdentity,
    platform: str,
    env: str,
) -> Optional[str]:
    """Build a dataset URN from an FQN using an explicit regex + dataset identity."""
    dataset_name = _dataset_name_from_fqn(fqn_regex, identity, fully_qualified_name)
    if dataset_name is None:
        return None
    return make_dataset_urn_with_platform_instance(
        platform=platform,
        name=dataset_name,
        platform_instance=None,
        env=env,
    )


def _project_schema_key(
    fqn_regex: Pattern[str], platform: str, fully_qualified_name: str
) -> Optional[DataplexProjectId]:
    identity_fields = parse_with_regex(fqn_regex, fully_qualified_name)
    if identity_fields is None:
        return None
    project_key_class = PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM.get(platform)
    if project_key_class is None:
        return None
    return instantiate_key(project_key_class, identity_fields)


def _build_project_container(
    entry: dataplex_v1.Entry, fqn_regex: Pattern[str], platform: str
) -> Optional[Container]:
    """Build the project-level Container that owns ``entry`` (dedup by caller)."""
    if not entry.fully_qualified_name:
        return None
    identity_fields = parse_with_regex(fqn_regex, entry.fully_qualified_name)
    if identity_fields is None:
        return None
    project_id = identity_fields.get("project_id")
    if project_id is None:
        return None
    project_schema_key = _project_schema_key(
        fqn_regex, platform, entry.fully_qualified_name
    )
    if project_schema_key is None:
        return None
    return Container(
        container_key=project_schema_key,
        display_name=project_id,
        parent_container=None,
        # BIGQUERY_PROJECT is used for every platform's project container by
        # historical convention; keep it to preserve emitted metadata.
        subtype=DatasetContainerSubTypes.BIGQUERY_PROJECT,
        extra_properties={
            "dataplex_ingested": "true",
            "dataplex_project_id": project_id,
            "dataplex_entry_type": entry.entry_type,
            "dataplex_source_platform": platform,
        },
    )


# ----------------------------------------------------------------------------
# Shared build algorithms (Dataset / Container)
# ----------------------------------------------------------------------------


def build_dataset(
    entry: dataplex_v1.Entry,
    ctx: EntryMappingContext,
    *,
    short_name: str,
    platform: str,
    subtype: str,
    fqn_regex: Pattern[str],
    identity: DatasetIdentity,
    parent: Optional[ParentEntryLink],
    include_graph_schema_fallback: bool = False,
) -> Optional[EntryMappingResult]:
    """Map a Dataplex entry to a DataHub Dataset (+ project container + lineage)."""
    if not entry.fully_qualified_name:
        return None

    project_container = _build_project_container(entry, fqn_regex, platform)
    additional: list[Entity] = (
        [project_container] if project_container is not None else []
    )

    dataset_name = _dataset_name_from_fqn(
        fqn_regex, identity, entry.fully_qualified_name
    )
    if dataset_name is None:
        ctx.report.warning(
            title="Unparseable Dataplex fully_qualified_name",
            message=(
                "Recognized the entry type but could not derive a dataset name "
                "from its fully_qualified_name. Skipping the dataset."
            ),
            context=(
                f"entry_type={entry.entry_type}, "
                f"entry_name={entry.name}, "
                f"fully_qualified_name={entry.fully_qualified_name}"
            ),
        )
        # A project container may still be emitted even when the primary entity
        # cannot be built (matches the prior independent-emission behavior).
        if additional:
            return EntryMappingResult(additional_entities=additional)
        return None

    schema_metadata: Optional[SchemaMetadataClass] = None
    if ctx.config.include_schema:
        schema_metadata = extract_schema_from_entry_aspects(entry, entry.name, platform)
        if schema_metadata is None and include_graph_schema_fallback:
            # cloud-spanner-graph entries store schema in a graph-schema aspect
            # rather than the standard schema aspect.
            schema_metadata = extract_graph_schema_from_entry_aspects(
                entry, entry.name, platform
            )

    parent_container_key: Optional[DataplexProjectId] = None
    if parent is not None:
        if not entry.parent_entry:
            ctx.report.warning(
                title="Missing Dataplex parent_entry",
                message=(
                    "Dataplex mapping expects parent_entry for parent container "
                    "derivation. Emitting dataset without parent container."
                ),
                context=(
                    f"entry_type={entry.entry_type}, "
                    f"entry_name={entry.name}, "
                    f"fully_qualified_name={entry.fully_qualified_name}"
                ),
            )
        else:
            parent_container_key = parent.container_key(entry.parent_entry)
            if parent_container_key is None:
                ctx.report.warning(
                    title="Unparseable Dataplex parent_entry",
                    message=(
                        "Could not parse parent_entry into a parent container. "
                        "Emitting dataset without parent container."
                    ),
                    context=(
                        f"entry_type={entry.entry_type}, "
                        f"entry_name={entry.name}, "
                        f"parent_entry={entry.parent_entry}"
                    ),
                )
    else:
        parent_container_key = _project_schema_key(
            fqn_regex, platform, entry.fully_qualified_name
        )

    common = _extract_common_fields(entry)
    # Only pass parent_container when resolved: the SDK emits an (empty)
    # browsePathsV2 aspect when the kwarg is provided even as None, so omitting
    # it preserves the exact emitted metadata. Two explicit calls (rather than a
    # **kwargs splat) keep the whole SDK signature type-checked.
    if parent_container_key is not None:
        dataset = Dataset(
            platform=platform,
            name=dataset_name,
            env=ctx.config.env,
            display_name=common.display_name,
            description=common.description or None,
            custom_properties=common.custom_properties,
            created=common.created,
            last_modified=common.last_modified,
            subtype=subtype,
            schema=schema_metadata,
            parent_container=parent_container_key,
        )
    else:
        dataset = Dataset(
            platform=platform,
            name=dataset_name,
            env=ctx.config.env,
            display_name=common.display_name,
            description=common.description or None,
            custom_properties=common.custom_properties,
            created=common.created,
            last_modified=common.last_modified,
            subtype=subtype,
            schema=schema_metadata,
        )

    lineage_entry = EntryDataTuple(
        dataplex_entry_short_name=entry.name.split("/")[-1],
        dataplex_entry_name=entry.name,
        dataplex_location=ctx.location,
        dataplex_entry_fqn=entry.fully_qualified_name,
        dataplex_entry_type_short_name=short_name,
        datahub_platform=platform,
        datahub_dataset_name=dataset_name,
        datahub_dataset_urn=make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            platform_instance=None,
            env=ctx.config.env,
        ),
    )

    return EntryMappingResult(
        main_entity=dataset,
        additional_entities=additional,
        lineage_entry=lineage_entry,
    )


def build_container(
    entry: dataplex_v1.Entry,
    ctx: EntryMappingContext,
    *,
    platform: str,
    subtype: str,
    fqn_regex: Pattern[str],
    identity: ContainerIdentity,
    parent: Optional[ParentEntryLink],
) -> Optional[EntryMappingResult]:
    """Map a Dataplex entry to a DataHub Container (+ project container)."""
    if not entry.fully_qualified_name:
        return None

    project_container = _build_project_container(entry, fqn_regex, platform)
    additional: list[Entity] = (
        [project_container] if project_container is not None else []
    )

    identity_fields = parse_with_regex(fqn_regex, entry.fully_qualified_name)
    if identity_fields is None:
        ctx.report.warning(
            title="Unparseable Dataplex fully_qualified_name",
            message=(
                "Recognized the entry type but could not build a container key "
                "from its fully_qualified_name. Skipping the container."
            ),
            context=(
                f"entry_type={entry.entry_type}, "
                f"entry_name={entry.name}, "
                f"fully_qualified_name={entry.fully_qualified_name}"
            ),
        )
        # A project container may still be emitted even when the primary entity
        # cannot be built (matches the prior independent-emission behavior).
        if additional:
            return EntryMappingResult(additional_entities=additional)
        return None
    container_key = identity.container_key(identity_fields)

    # Prefer parent linkage from Dataplex parent_entry; fall back to project key.
    container_parent_key: Optional[DataplexProjectId] = None
    if parent is not None and entry.parent_entry:
        container_parent_key = parent.container_key(entry.parent_entry)
    if container_parent_key is None:
        container_parent_key = _project_schema_key(
            fqn_regex, platform, entry.fully_qualified_name
        )

    common = _extract_common_fields(entry)
    # container_parent_key is effectively always set here — the project key is
    # the fallback and only fails to build when the FQN is unparseable, in which
    # case the container key above already failed and we returned. Passed
    # directly (as the pre-refactor code did) so the SDK signature stays checked.
    container = Container(
        container_key=container_key,
        display_name=common.display_name,
        description=common.description or None,
        created=common.created,
        last_modified=common.last_modified,
        parent_container=container_parent_key,
        subtype=subtype,
        extra_properties=common.custom_properties,
    )
    return EntryMappingResult(main_entity=container, additional_entities=additional)


# ----------------------------------------------------------------------------
# Concrete mappers — one per Dataplex entry type
# ----------------------------------------------------------------------------


class BigQueryDatasetMapper(EntryMapper):
    dataplex_entry_type_short_name = "bigquery-dataset"
    datahub_platform = "bigquery"
    dataplex_fqn_regex = BIGQUERY_DATASET_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexBigQueryDataset)

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.BIGQUERY_DATASET,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class BigQueryTableMapper(EntryMapper):
    dataplex_entry_type_short_name = "bigquery-table"
    datahub_platform = "bigquery"
    dataplex_fqn_regex = BIGQUERY_TABLE_FQN_REGEX
    datahub_identity = DatasetIdentity("{project_id}.{dataset_id}.{table_id}")
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexBigQueryDataset,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TABLE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class BigQueryViewMapper(EntryMapper):
    # Views share the FQN and parent_entry shape of tables; only the subtype differs.
    dataplex_entry_type_short_name = "bigquery-view"
    datahub_platform = "bigquery"
    dataplex_fqn_regex = BIGQUERY_TABLE_FQN_REGEX
    datahub_identity = DatasetIdentity("{project_id}.{dataset_id}.{table_id}")
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexBigQueryDataset,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.VIEW,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSqlMySqlInstanceMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloudsql-mysql-instance"
    datahub_platform = "cloudsql"
    dataplex_fqn_regex = MYSQL_INSTANCE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexCloudSqlMySqlInstance)

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.SERVICE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSqlMySqlDatabaseMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloudsql-mysql-database"
    datahub_platform = "cloudsql"
    dataplex_fqn_regex = MYSQL_DATABASE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexCloudSqlMySqlDatabase)
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=MYSQL_INSTANCE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexCloudSqlMySqlInstance,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.DATABASE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSqlMySqlTableMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloudsql-mysql-table"
    datahub_platform = "cloudsql"
    dataplex_fqn_regex = MYSQL_TABLE_FQN_REGEX
    datahub_identity = DatasetIdentity(
        "{project_id}.{location}.{instance_id}.{database_id}.{table_id}"
    )
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=MYSQL_DATABASE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexCloudSqlMySqlDatabase,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TABLE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSpannerInstanceMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloud-spanner-instance"
    datahub_platform = "spanner"
    dataplex_fqn_regex = SPANNER_INSTANCE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexCloudSpannerInstance)

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.INSTANCE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSpannerDatabaseMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloud-spanner-database"
    datahub_platform = "spanner"
    dataplex_fqn_regex = SPANNER_DATABASE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexCloudSpannerDatabase)
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=SPANNER_INSTANCE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexCloudSpannerInstance,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.DATABASE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSpannerTableMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloud-spanner-table"
    datahub_platform = "spanner"
    dataplex_fqn_regex = SPANNER_TABLE_FQN_REGEX
    datahub_identity = DatasetIdentity(
        "{project_id}.regional-{location}.{instance_id}.{database_id}.{table_id}"
    )
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=SPANNER_DATABASE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexCloudSpannerDatabase,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TABLE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudSpannerGraphMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloud-spanner-graph"
    datahub_platform = "spanner"
    dataplex_fqn_regex = SPANNER_GRAPH_FQN_REGEX
    # Spanner tables and graphs share a namespace so names never collide.
    datahub_identity = DatasetIdentity(
        "{project_id}.regional-{location}.{instance_id}.{database_id}.{graph_id}"
    )
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=SPANNER_DATABASE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexCloudSpannerDatabase,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.GRAPH,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
            include_graph_schema_fallback=True,
        )


class CloudBigtableInstanceMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloud-bigtable-instance"
    datahub_platform = "bigtable"
    dataplex_fqn_regex = BIGTABLE_INSTANCE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexBigtableInstance)

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.INSTANCE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class CloudBigtableTableMapper(EntryMapper):
    dataplex_entry_type_short_name = "cloud-bigtable-table"
    datahub_platform = "bigtable"
    dataplex_fqn_regex = BIGTABLE_TABLE_FQN_REGEX
    datahub_identity = DatasetIdentity("{project_id}.{instance_id}.{table_id}")
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=BIGTABLE_INSTANCE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexBigtableInstance,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TABLE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class PubSubTopicMapper(EntryMapper):
    dataplex_entry_type_short_name = "pubsub-topic"
    datahub_platform = "pubsub"
    dataplex_fqn_regex = PUBSUB_TOPIC_FQN_REGEX
    datahub_identity = DatasetIdentity("{project_id}.{topic_id}")

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        # Top-level dataset: no parent_entry, parent linkage is the project key.
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TOPIC,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class VertexAiDatasetMapper(EntryMapper):
    dataplex_entry_type_short_name = "vertexai-dataset"
    datahub_platform = "vertexai"
    dataplex_fqn_regex = VERTEX_AI_DATASET_FQN_REGEX
    datahub_identity = DatasetIdentity("{project_id}.{location}.{dataset_id}")

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TABLE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class DataprocMetastoreServiceMapper(EntryMapper):
    dataplex_entry_type_short_name = "dataproc-metastore-service"
    datahub_platform = "dataproc-metastore"
    dataplex_fqn_regex = DATAPROC_METASTORE_SERVICE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexDataprocMetastoreService)
    # No parent_entry - services are top-level under project

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.SERVICE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class DataprocMetastoreDatabaseMapper(EntryMapper):
    dataplex_entry_type_short_name = "dataproc-metastore-database"
    datahub_platform = "dataproc-metastore"
    dataplex_fqn_regex = DATAPROC_METASTORE_DATABASE_FQN_REGEX
    datahub_identity = ContainerIdentity(DataplexDataprocMetastoreDatabase)
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=DATAPROC_METASTORE_SERVICE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexDataprocMetastoreService,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_container(
            entry,
            ctx,
            platform=self.datahub_platform,
            subtype=DatasetContainerSubTypes.DATABASE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


class DataprocMetastoreTableMapper(EntryMapper):
    dataplex_entry_type_short_name = "dataproc-metastore-table"
    datahub_platform = "dataproc-metastore"
    dataplex_fqn_regex = DATAPROC_METASTORE_TABLE_FQN_REGEX
    datahub_identity = DatasetIdentity(
        "{project_id}.{location}.{service_id}.{database_id}.{table_id}"
    )
    dataplex_parent_entry = ParentEntryLink(
        dataplex_parent_entry_regex=DATAPROC_METASTORE_DATABASE_PARENT_ENTRY_REGEX,
        datahub_schemakey_class=DataplexDataprocMetastoreDatabase,
    )

    def map(
        self, entry: dataplex_v1.Entry, ctx: EntryMappingContext
    ) -> Optional[EntryMappingResult]:
        return build_dataset(
            entry,
            ctx,
            short_name=self.dataplex_entry_type_short_name,
            platform=self.datahub_platform,
            subtype=DatasetSubTypes.TABLE,
            fqn_regex=self.dataplex_fqn_regex,
            identity=self.datahub_identity,
            parent=self.dataplex_parent_entry,
        )


# ----------------------------------------------------------------------------
# Registry (the factory)
# ----------------------------------------------------------------------------

_ALL_MAPPERS: list[EntryMapper] = [
    BigQueryDatasetMapper(),
    BigQueryTableMapper(),
    BigQueryViewMapper(),
    CloudSqlMySqlInstanceMapper(),
    CloudSqlMySqlDatabaseMapper(),
    CloudSqlMySqlTableMapper(),
    CloudSpannerInstanceMapper(),
    CloudSpannerDatabaseMapper(),
    CloudSpannerTableMapper(),
    CloudSpannerGraphMapper(),
    CloudBigtableInstanceMapper(),
    CloudBigtableTableMapper(),
    PubSubTopicMapper(),
    VertexAiDatasetMapper(),
    DataprocMetastoreServiceMapper(),
    DataprocMetastoreDatabaseMapper(),
    DataprocMetastoreTableMapper(),
]

ENTRY_MAPPERS: dict[str, EntryMapper] = {
    mapper.dataplex_entry_type_short_name: mapper for mapper in _ALL_MAPPERS
}


def get_entry_mapper(
    entry_type: str, report: SourceReport, *, entry: Optional[dataplex_v1.Entry] = None
) -> Optional[EntryMapper]:
    """Resolve the mapper for a Dataplex ``entry_type`` path, or warn and return None.

    ``entry`` is optional and only enriches warning context.
    """
    context = (
        f"entry_type={entry_type}, entry_name={entry.name}, entry_payload={entry}"
        if entry is not None
        else f"entry_type={entry_type}"
    )
    short_name = extract_entry_type_short_name(entry_type)
    if short_name is None:
        report.warning(
            title="Invalid Dataplex entry type format",
            message="Failed to extract short entry type from Dataplex entry_type. Skipping entry.",
            context=context,
        )
        return None

    mapper = ENTRY_MAPPERS.get(short_name)
    if mapper is None:
        report.warning(
            title="Unsupported Dataplex entry type",
            message="Encountered Dataplex entry with unsupported entry_type. Skipping entry.",
            context=context,
        )
        return None
    return mapper


def is_lineage_supported(entry_type_short_name: str) -> bool:
    """Return whether an entry type is a lineage (dataset) node."""
    mapper = ENTRY_MAPPERS.get(entry_type_short_name)
    # Route through datahub_main_entity_type (assert_never-guarded) so a future
    # identity variant is handled explicitly rather than silently dropped.
    return bool(mapper and mapper.datahub_main_entity_type is Dataset)


def dataset_urn_from_fqn_only(fully_qualified_name: str, env: str) -> Optional[str]:
    """Resolve a dataset URN from an FQN alone by trying every dataset mapper.

    Used for cross-platform upstream lineage where only the FQN shape is known.
    """
    for mapper in ENTRY_MAPPERS.values():
        if mapper.datahub_main_entity_type is not Dataset:
            continue
        # A Dataset-producing mapper must carry a DatasetIdentity; assert to
        # surface (loudly) any future variant that breaks this coupling.
        identity = mapper.datahub_identity
        assert isinstance(identity, DatasetIdentity)
        urn = dataset_urn_from_fqn(
            fully_qualified_name,
            mapper.dataplex_fqn_regex,
            identity,
            mapper.datahub_platform,
            env,
        )
        if urn:
            return urn
    return None
