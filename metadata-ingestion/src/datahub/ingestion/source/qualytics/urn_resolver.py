"""Resolve a Qualytics datastore + container to the dataset URN DataHub already has.

This is the highest-consequence module in the connector. Everything else -- assertions,
assertion results, profiles -- attaches to whatever URN comes out of here. Attach to the wrong one
and the metadata is invisible and quietly misleading, which is worse than emitting
nothing. So the rule throughout is **skip and warn rather than guess**.

Resolution order per datastore:

1. ``datastore_to_platform_map``, keyed by datastore name or integer id. Explicit and
   always preferred.
2. Inference from the Qualytics connection type, when ``infer_source_platform`` is on.
3. Neither -- report it and return None.

Prior art: Qualytics' own DataHub push integration, which reconstructs JDBC dataset URNs
the same way. This module extends that to object stores and native catalogs, adds
``platform_instance`` and ``env``, and builds URNs through DataHub's typed builders.
"""

from dataclasses import dataclass

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig
from datahub.ingestion.source.qualytics.models import (
    Container,
    Datastore,
    DfsDatastore,
    FileContainer,
    JdbcDatastore,
    NativeDatastore,
)
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport

# Qualytics connection type -> DataHub platform, for every connection type Qualytics
# supports: 20 JDBC, 3 DFS and 4 native. Every target was checked against DataHub's
# own data-platforms.yaml. The list is closed on purpose: a connection type missing
# from it is skipped with a warning rather than passed through as a platform name,
# because a type DataHub does not know (glue_native, say) produces URNs that match no
# dataset. A new Qualytics connection type therefore needs a line here.
PLATFORM_MAP: dict[str, str] = {
    # JDBC. Most names are DataHub's own.
    "athena": "athena",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "db2": "db2",
    "dremio": "dremio",
    "hana": "hana",
    "hive": "hive",
    # mariadb deliberately maps to itself. DataHub ships a dedicated mariadb source
    # that emits mariadb URNs; mapping to mysql would attach assertions to a URN a
    # MariaDB-sourced DataHub never emitted.
    "mariadb": "mariadb",
    "mysql": "mysql",
    "oracle": "oracle",
    # DataHub registers "postgres", not "postgresql".
    "postgresql": "postgres",
    "presto": "presto",
    "redshift": "redshift",
    "snowflake": "snowflake",
    # DataHub registers "mssql", not "sqlserver".
    "sqlserver": "mssql",
    # No "synapse" platform in DataHub. Synapse dedicated SQL pools speak the SQL
    # Server dialect and there is no Synapse ingestion source, so nothing would ever
    # emit synapse URNs to match against.
    "synapse": "mssql",
    "teradata": "teradata",
    # TimescaleDB is a Postgres extension; DataHub has no separate platform, and the
    # postgres source is what would have ingested it.
    "timescale": "postgres",
    "trino": "trino",
    # DFS. DataHub registers Azure Blob Storage as "abs".
    "abfs": "abs",
    "gcs": "gcs",
    "s3": "s3",
    # Native connectors carry a _native suffix that is a Qualytics implementation
    # detail; the platform is the one the matching DataHub source emits.
    "databricks_native": "databricks",
    "unity_native": "databricks",
    "hive_native": "hive",
    "glue_native": "glue",
}

# Native connection types whose Qualytics "catalog" exists only because Spark routes
# identifiers through one. Hive and Glue namespaces are two levels, database and
# table, and their DataHub sources name datasets ``database.table``; the catalog name
# Qualytics derives would put every URN on a dataset that does not exist. Unity
# Catalog's catalog, by contrast, is real, and the Databricks source includes it.
DERIVED_CATALOG_TYPES: frozenset[str] = frozenset({"hive_native", "glue_native"})

# Qualytics connection types with no DataHub equivalent. Listed explicitly so the
# warning can say "unsupported" rather than "unknown", and so a future DataHub release
# adding the platform is a one-line change.
UNMAPPABLE_TYPES: frozenset[str] = frozenset(
    {
        # Microsoft Fabric has a registered platform but no ingestion source, so no
        # DataHub instance contains fabric datasets to attach to yet.
        "fabric",
    }
)

# Platforms whose DataHub source lowercases dataset URNs unless told otherwise. The
# framework default is False, but the Snowflake source overrides it to True
# (snowflake_config.py: "Changing default value here") and warns when it is turned
# off. Inheriting the framework default made every inferred Snowflake URN miss.
LOWERCASE_BY_DEFAULT: frozenset[str] = frozenset({"snowflake"})

_URI_SCHEME = "://"


@dataclass(frozen=True)
class PlatformResolution:
    """Where a Qualytics datastore's assets live in DataHub."""

    platform: str
    platform_instance: str | None
    env: str
    lowercase_urns: bool
    # False when the dataset *name* is a best-effort reconstruction rather than a
    # convention DataHub sources follow deterministically. Currently only object
    # stores, where the customer's path_spec decides where the "table" boundary sits.
    confident: bool = True


class UrnResolver:
    """Maps Qualytics datastores and containers onto source-platform dataset URNs."""

    def __init__(
        self, config: QualyticsSourceConfig, report: QualyticsSourceReport
    ) -> None:
        self.config = config
        self.report = report
        # Per datastore id, so an unresolvable datastore warns once however many
        # containers it has, without relying on the caller to skip them.
        self._resolutions: dict[int, PlatformResolution | None] = {}

    # --- platform resolution ------------------------------------------------------

    def resolve_platform(self, datastore: Datastore) -> PlatformResolution | None:
        """Resolve one datastore, or None if it cannot be resolved safely.

        Memoised per datastore id, so an unresolvable datastore warns exactly once no
        matter how many containers hang off it.
        """
        if datastore.id in self._resolutions:
            return self._resolutions[datastore.id]
        resolution = self._resolve_uncached(datastore)
        self._resolutions[datastore.id] = resolution
        return resolution

    def _resolve_uncached(self, datastore: Datastore) -> PlatformResolution | None:
        explicit = self._lookup_explicit(datastore)
        if explicit is not None:
            return explicit

        if not self.config.infer_source_platform:
            self._unresolved(
                datastore,
                "not in datastore_to_platform_map and infer_source_platform is disabled",
            )
            return None

        return self._infer(datastore)

    def _lookup_explicit(self, datastore: Datastore) -> PlatformResolution | None:
        """Look the datastore up in datastore_to_platform_map, by name then by id.

        Both are accepted because neither alone is good enough: names read well in a
        recipe but Qualytics lets you rename a datastore, and a rename would silently
        stop the mapping matching. Ids are immutable but opaque.
        """
        mapping = self.config.datastore_to_platform_map
        detail = mapping.get(datastore.name) or mapping.get(str(datastore.id))
        if detail is None:
            return None

        return PlatformResolution(
            platform=detail.platform,
            platform_instance=detail.platform_instance,
            env=detail.env,
            lowercase_urns=self._lowercase(
                detail.platform, detail.convert_urns_to_lowercase
            ),
            # The map pins the platform, not the name: an object-store name is still
            # reconstructed from the path, and is no more certain for being mapped.
            confident=datastore.store_type != "dfs",
        )

    def _lowercase(self, platform: str, override: bool | None = None) -> bool:
        """Per-datastore override, then an explicit top-level value, then the default
        of the DataHub source that catalogued the platform."""
        if override is not None:
            return override
        if self.config.convert_urns_to_lowercase is not None:
            return self.config.convert_urns_to_lowercase
        return platform in LOWERCASE_BY_DEFAULT

    def _infer(self, datastore: Datastore) -> PlatformResolution | None:
        """Infer the platform from the Qualytics connection type."""
        qualytics_type = (datastore.type or "").strip()
        if not qualytics_type:
            self._unresolved(datastore, "the datastore has no connection type")
            return None

        if qualytics_type in UNMAPPABLE_TYPES:
            self._unresolved(
                datastore,
                f"Qualytics connection type '{qualytics_type}' has no DataHub "
                f"equivalent; set an explicit datastore_to_platform_map entry if your "
                f"DataHub catalogues it under some other platform",
            )
            return None

        platform = PLATFORM_MAP.get(qualytics_type)
        if platform is None:
            self._unresolved(
                datastore,
                f"Qualytics connection type '{qualytics_type}' is not one this build "
                f"of the connector knows; set an explicit datastore_to_platform_map "
                f"entry naming the DataHub platform it was ingested under",
            )
            return None

        # Deliberately NOT config.platform_instance: that identifies the *Qualytics*
        # deployment, and pushing it into a Snowflake dataset URN would target
        # `snowflake,acme.DB.SCHEMA.TABLE` -- a dataset their warehouse source never
        # emitted. The warehouse's own instance is a separate setting, defaulting to
        # none because most warehouses are ingested without one.
        return PlatformResolution(
            platform=platform,
            platform_instance=self.config.default_source_platform_instance,
            env=self.config.default_source_env or self.config.env,
            lowercase_urns=self._lowercase(platform),
            confident=datastore.store_type != "dfs",
        )

    def _unresolved(self, datastore: Datastore, why: str) -> None:
        self.report.report_unresolved_datastore(datastore.name)
        self.report.warning(
            title="Could not resolve a datastore to a DataHub platform",
            message=(
                "Qualytics metadata for this datastore will be skipped. Add an entry "
                "to datastore_to_platform_map keyed by the datastore's name or id."
            ),
            context=f"datastore={datastore.name} (id={datastore.id}), reason={why}",
        )

    # --- dataset naming -----------------------------------------------------------

    def dataset_name(self, datastore: Datastore, container: Container) -> str | None:
        """Build the platform-native dataset name DataHub would already know it by.

        Returns None when the shape is unknown, which the caller reports as
        unresolved. Each branch mirrors what the corresponding DataHub source emits.
        """
        if isinstance(datastore, JdbcDatastore):
            # Every SQL source in DataHub names datasets database.schema.table, with
            # missing levels simply absent.
            parts = [datastore.database, datastore.schema_, container.name]
            return ".".join(p for p in parts if p) or None

        if isinstance(datastore, NativeDatastore):
            # Unity Catalog: catalog.schema.table. Hive and Glue: database.table,
            # since their catalog is a Spark artefact; see DERIVED_CATALOG_TYPES.
            catalog = (
                None if datastore.type in DERIVED_CATALOG_TYPES else datastore.catalog
            )
            parts = [catalog, datastore.schema_, container.name]
            return ".".join(p for p in parts if p) or None

        if isinstance(datastore, DfsDatastore):
            return self._dfs_dataset_name(datastore, container)

        return None

    @staticmethod
    def _dfs_dataset_name(datastore: DfsDatastore, container: Container) -> str | None:
        """Reconstruct the object-store dataset name.

        DataHub's S3/GCS/ABS sources name a dataset after its table path with the URI
        scheme stripped and slashes trimmed -- `s3://bucket/events/2026` becomes
        `bucket/events/2026` (see s3/source.py, which does exactly this with
        URI_SCHEME_REGEX).

        The catch, and why these resolutions are marked not-confident: *where* the
        table boundary sits is decided by the customer's `path_spec`. A folder of
        partitioned parquet files may be one dataset in their DataHub while Qualytics
        sees each file as a container. We reconstruct the path faithfully; whether it
        lines up is something only the explicit map can guarantee.
        """
        uri = datastore.uri or ""
        bucket = uri.split(_URI_SCHEME, 1)[1] if _URI_SCHEME in uri else uri
        # isinstance, not getattr: FileContainer declares relative_path, so this way
        # mypy checks it and a rename cannot silently fall back to container.name and
        # produce a plausible-but-wrong object path.
        relative = (
            container.relative_path
            if isinstance(container, FileContainer)
            else container.name
        )

        segments = [
            segment.strip("/")
            for segment in (bucket, datastore.root_path, relative)
            if segment and segment.strip("/")
        ]
        return "/".join(segments) or None

    def field_path(self, datastore: Datastore, name: str) -> str:
        """A column's DataHub fieldPath, cased the way the dataset name is.

        Snowflake is the case that matters: its DataHub source lowercases column paths
        by default (``preserve_column_case: false``) as well as dataset names, while
        Qualytics reports them as Snowflake stores them, upper-case. Emitting them as-is
        put every field profile and column-level assertion on a column that does not
        exist. Tying the two together matches every source default we know of; a
        warehouse ingested with the Snowflake defaults changed needs the explicit
        per-datastore setting either way.
        """
        resolution = self.resolve_platform(datastore)
        return (
            name.lower()
            if resolution is not None and resolution.lowercase_urns
            else name
        )

    # --- the public entry point ---------------------------------------------------

    def dataset_urn(self, datastore: Datastore, container: Container) -> str | None:
        """Resolve a container to the dataset URN DataHub already has, or None."""
        resolution = self.resolve_platform(datastore)
        if resolution is None:
            return None

        name = self.dataset_name(datastore, container)
        if name is None:
            # containers_unnamed, not report_unresolved_datastore: this is per
            # container, and using the datastore counter reported "500 datastores
            # unresolved" for one datastore with 500 unnameable containers.
            self.report.containers_unnamed += 1
            self.report.warning(
                title="Could not build a dataset name for a container",
                message=(
                    "The datastore resolved to a platform, but its shape is not one "
                    "this connector knows how to name. The container will be skipped."
                ),
                context=(
                    f"datastore={datastore.name} (store_type={datastore.store_type}), "
                    f"container={container.name}"
                ),
            )
            return None

        if resolution.lowercase_urns:
            name = name.lower()

        self.report.urns_resolved += 1
        if not resolution.confident:
            self.report.urns_resolved_by_path_reconstruction += 1

        return make_dataset_urn_with_platform_instance(
            platform=resolution.platform,
            name=name,
            platform_instance=resolution.platform_instance,
            env=resolution.env,
        )
