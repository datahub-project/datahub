from typing import Dict, List, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
)
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.iceberg.iceberg_common import IcebergSourceConfig


def _dotted(identifier: Identifier) -> str:
    # The spelling ingestion matches patterns against: _get_namespaces and
    # _process_dataset in iceberg.py both join the identifier tuple on ".".
    return ".".join(identifier)


class IcebergMetadataProbe:
    """Metadata-only probe over a pyiceberg catalog.

    Reads catalog listings and table metadata files. Never scans a table,
    reads a data file or manifest, or returns catalog/FileIO properties
    (REST catalogs put vended storage credentials in the latter).
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    @classmethod
    def for_config(cls, config: IcebergSourceConfig) -> "IcebergMetadataProbe":
        # get_catalog, not load_catalog: it carries the Glue role-assumption
        # workaround and the REST retry/timeout adapter, so the probe
        # authenticates exactly as ingestion does.
        return cls(config.get_catalog())

    def __enter__(self) -> "IcebergMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._catalog.close()

    def _top_level_namespaces(self) -> List[Identifier]:
        # No parent argument, as in iceberg.py _get_namespaces: every pyiceberg
        # catalog then returns top-level namespaces only, which is all
        # ingestion ever reads.
        return list(self._catalog.list_namespaces())

    def _resolve_namespace(self, namespace: str) -> Identifier:
        """The identifier tuple for a namespace named as ingestion spells it.

        Resolved against the listing rather than split on ".": a REST catalog
        can hold a single-level namespace whose name contains a dot, and
        splitting it would address a namespace that does not exist.
        """
        for candidate in self._top_level_namespaces():
            if _dotted(candidate) == namespace:
                return candidate
        raise ValueError(
            f"no top-level namespace '{namespace}' in this catalog. Ingestion "
            f"reads only top-level namespaces (see the `namespaces` command), "
            f"so a nested namespace is never ingested"
        )

    @probe_method(kind=DatasetContainerSubTypes.NAMESPACE, row_limit_param="limit")
    def namespaces(self, limit: int = 500) -> List[str]:
        """Top-level namespaces in the catalog, spelled the way namespace_pattern
        is matched (identifier parts joined with "."). Includes namespaces the
        recipe's namespace_pattern excludes -- a denied namespace is reported,
        not hidden. Nested namespaces are not listed because ingestion never
        reads them. Metadata only."""
        return sorted(_dotted(ns) for ns in self._top_level_namespaces())[:limit]

    def _load_table(self, namespace: str, table: str) -> Table:
        identifier = (*self._resolve_namespace(namespace), table)
        try:
            return self._catalog.load_table(identifier)
        except (NoSuchIcebergTableError, NoSuchPropertyException) as exc:
            # Checked before NoSuchTableError, its base class. Ingestion skips
            # these with a warning (iceberg.py _try_processing_dataset).
            raise ValueError(
                f"'{namespace}.{table}' is not an Iceberg table; ingestion "
                f"skips it with a warning"
            ) from exc
        except NoSuchTableError as exc:
            raise ValueError(f"no table '{table}' in namespace '{namespace}'") from exc
        except ValueError as exc:
            if "Could not initialize FileIO" not in str(exc):
                raise
            # The same message ingestion matches on to skip the table. The
            # py-io-impl it names comes from the catalog's merged properties
            # (often the server's), and pyiceberg raises this only when that
            # FileIO implementation module is missing from this environment:
            # an environment problem, not caller input, so exit 3 rather than 2.
            raise ProbeConnectionError(
                f"could not load the FileIO implementation for "
                f"'{namespace}.{table}': {exc}"
            ) from exc

    @probe_method()
    def namespace_properties(self, namespace: str) -> Dict[str, str]:
        """Properties of one top-level namespace (location, owner, comment...),
        the ones ingestion attaches to the namespace container. Ingestion skips
        a namespace, and every table in it, if this read fails -- so an error
        here explains a namespace that ingests nothing."""
        resolved = self._resolve_namespace(namespace)
        try:
            properties = self._catalog.load_namespace_properties(resolved)
        except NoSuchNamespaceError as exc:
            raise ValueError(f"no namespace '{namespace}'") from exc
        return {str(k): str(v) for k, v in properties.items()}

    @probe_method(
        kind=DatasetSubTypes.TABLE,
        row_limit_param="limit",
        parent_params=("namespace",),
    )
    def tables(self, namespace: str, limit: int = 500) -> List[str]:
        """Tables in one top-level namespace, as bare names. Includes tables the
        recipe's table_pattern excludes -- a denied table is reported, not
        hidden. table_pattern is matched against "<namespace>.<table>";
        `probe filter` builds that from the namespace reported as the parent.
        Metadata only."""
        resolved = self._resolve_namespace(namespace)
        try:
            identifiers = self._catalog.list_tables(resolved)
        except NoSuchNamespaceError as exc:
            raise ValueError(f"no namespace '{namespace}'") from exc
        return sorted(identifier[-1] for identifier in identifiers)[:limit]

    @probe_method()
    def columns(self, namespace: str, table: str) -> List[Dict[str, object]]:
        """Top-level fields of the table's current schema: field id, name,
        Iceberg type, required, doc. Nested types are shown as their Iceberg
        type string. Reads the table metadata file through the same load_table
        call ingestion makes, so a storage permission problem shows up here as
        it would during ingestion. Never reads data files."""
        loaded = self._load_table(namespace, table)
        return [
            {
                "id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
                "doc": field.doc,
            }
            for field in loaded.schema().fields
        ]

    @probe_method()
    def table_metadata(self, namespace: str, table: str) -> Dict[str, object]:
        """Table-level metadata ingestion records as custom properties: format
        version, location, partition spec, current snapshot id and table
        properties. Never includes FileIO or catalog properties, which can hold
        vended storage credentials."""
        loaded = self._load_table(namespace, table)
        snapshot = loaded.current_snapshot()
        snapshot_id: Optional[int] = snapshot.snapshot_id if snapshot else None
        return {
            "format_version": loaded.metadata.format_version,
            "location": loaded.metadata.location,
            "partition_spec": str(loaded.spec()),
            "current_snapshot_id": snapshot_id,
            "properties": dict(loaded.metadata.properties),
        }
