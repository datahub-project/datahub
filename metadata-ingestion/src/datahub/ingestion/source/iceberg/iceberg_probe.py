from typing import List

from pyiceberg.catalog import Catalog
from pyiceberg.typedef import Identifier

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
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
