# Keep sqlglot off the module-load path: annotations stay strings and the schema_resolver
# import is deferred to __init__ (test_module_import_does_not_pull_sqlglot).
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, List, Optional

from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.models import (
    EXACT,
    NORMALIZED,
    UNRESOLVED,
    Resolution,
)
from datahub.metadata.schema_classes import AliasesClass, SchemaMetadataClass
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.sql_parsing.schema_resolver import SchemaInfo

# The keyword field GMS indexes each dataset's lowercased URN under (Aliases.pdl).
_LOWERCASED_URN_FIELD = "lowercasedUrn"


class AliasLookupStrategy:
    """Resolve casing by looking each reference up in the server's alias index.

    Matches only entities that differ from the reference by casing alone.
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        self._ctx = ctx
        graph = ctx.pipeline_context.graph
        assert graph is not None  # should_enable guarantees a graph exists
        self._graph: DataHubGraph = graph
        self._require_alias_support()
        # Reused rather than reimplemented because it drops struct-nested field paths,
        # which column matching relies on. Private until the schema store is extracted.
        from datahub.sql_parsing.schema_resolver import _convert_schema_aspect_to_info

        self._to_schema_info: Callable[[SchemaMetadataClass], SchemaInfo] = (
            _convert_schema_aspect_to_info
        )

        # References whose collision had no principled winner, for one end-of-run warning.
        self._ambiguous_refs = 0

    def _require_alias_support(self) -> None:
        """Fail the run unless the server is known to index dataset aliases.

        Filtering on a field the server doesn't index returns zero hits rather than an
        error, so continuing would emit unhealed lineage that looks reconciled.
        """
        specs = self._graph.get_entity_aspect_specs()
        if specs is None:
            raise ValueError(
                "auto_resolve_lineage_urns mode 'alias_lookup' could not read the "
                "DataHub server's entity registry, so it cannot confirm that the "
                "'aliases' aspect is registered on datasets. Retry, or set "
                "mode: bulk_catalog."
            )
        try:
            supported = specs.supports("dataset", AliasesClass.ASPECT_NAME)
        except ValueError:
            supported = False  # dataset entity type not registered at all
        if not supported:
            raise ValueError(
                "auto_resolve_lineage_urns mode 'alias_lookup' needs a DataHub server "
                "that registers the 'aliases' aspect on datasets, which this one does "
                "not. Upgrade the server, or set mode: bulk_catalog."
            )

    def resolve(self, urn: str, *, need_schema: bool = False) -> Resolution:
        resolution = self._resolve(urn)
        if need_schema and resolution.match_type in (EXACT, NORMALIZED):
            return Resolution(
                resolution.urn, self._schema(resolution.urn), resolution.match_type
            )
        return resolution

    def _resolve(self, urn: str) -> Resolution:
        try:
            key = lowercase_dataset_urn(urn)
        except InvalidUrnError:
            # Not a dataset reference, so there is no key to look up: out of scope.
            return Resolution(urn, None, None)
        resolved = self._pick(urn, self._lookup(key))
        if resolved is None:
            return Resolution(urn, None, UNRESOLVED)
        return Resolution(resolved, None, EXACT if resolved == urn else NORMALIZED)

    def _lookup(self, key: str) -> List[str]:
        """Every live dataset URN stored under `key`, so _pick can see a collision."""
        return list(
            self._graph.get_urns_by_filter(
                entity_types=["dataset"],
                extra_or_filters=[
                    {
                        "and": [
                            SearchFilterRule(
                                field=_LOWERCASED_URN_FIELD,
                                condition="EQUAL",
                                values=[key],
                            ).to_raw()
                        ]
                    }
                ],
            )
        )

    def _pick(self, urn: str, candidates: List[str]) -> Optional[str]:
        if not candidates:
            return None
        if urn in candidates:
            return urn
        if len(candidates) == 1:
            return candidates[0]
        # A collision is the residue of convert_urns_to_lowercase being turned on or off,
        # so of the two the lowercased entity is the one that flag produced.
        lowercased = lowercase_dataset_urn(urn)
        if lowercased in candidates:
            return lowercased
        self._ambiguous_refs += 1
        return None

    def _schema(self, urn: str) -> Optional[SchemaInfo]:
        aspect = self._graph.get_aspect(urn, SchemaMetadataClass)
        return self._to_schema_info(aspect) if aspect is not None else None

    def finish(self) -> None:
        """Report collisions apart from plain misses -- they need opposite fixes."""
        if not self._ambiguous_refs:
            return
        self._ctx.source_report.warning(
            title="Lineage references matched two entities differing only by casing",
            message="Some upstream references matched more than one existing entity whose "
            "URNs differ only by casing, so they were emitted unchanged. This usually "
            "means the same physical table was ingested twice under different casings; "
            "removing the wrong one lets these references resolve.",
            context=f"{self._ambiguous_refs} reference(s)",
        )
